"""模拟园区电视节目源.

控制面 (HTTP/FastAPI):
  POST /join/{channel}    代理请求加入频道; 按频道配置可延迟首包或拒绝加入
  POST /leave/{channel}   代理请求退订频道 (幂等)
  GET  /state             查看各频道订阅状态与统计
  POST /config/{channel}  动态配置频道行为 (首包延迟 / 拒绝加入 / 发包间隔 /
                          每组原包数 / 每组漏发的原包数 / 组内乱序发送)
  POST /restart/{channel} 频道数据面复位: 段号与组号归零并开启新一轮 (订阅保持)
  POST /restart           全部频道数据面复位
  POST /reset             全部复位 (含配置)

数据面: 订阅建立后, 每 fec_group_size 个连续节目包加发 1 个校验包, 按 interval
向代理的 UDP 端口发送 (模拟组播转发). 校验数据为同组各节目包完整 JSON 报文的
逐字节 XOR (短包补零), 因此代理在等待期内收到组内任意 2 个原包 + 校验包即可
还原第三个. 校验封装 (JSON/base64) 与长度表的开销另计.
"""

from __future__ import annotations

import asyncio
import base64
import json
import os
import socket
import time
import uuid

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

PROXY_HOST = os.getenv("PROXY_HOST", "127.0.0.1")
PROXY_UDP_PORT = int(os.getenv("PROXY_UDP_PORT", "5000"))
PACKET_INTERVAL = float(os.getenv("PACKET_INTERVAL", "0.5"))
CHANNEL_MIN = int(os.getenv("CHANNEL_MIN", "1"))
CHANNEL_MAX = int(os.getenv("CHANNEL_MAX", "10"))
FEC_GROUP_SIZE = int(os.getenv("FEC_GROUP_SIZE", "3"))
DROP_COUNT = int(os.getenv("DROP_COUNT", "0"))
MAX_FEC_GROUP = 32

# 每个源进程一个会话号: 代理据此识别"源进程重启"与频道的轮次复位.
BOOT_ID = uuid.uuid4().hex

app = FastAPI(
    title="Simulated TV Program Source",
    description="本地模拟节目源: 支持延迟首包、拒绝加入、按组漏发原包与组内乱序, 用于演示组播换台代理.",
)

_udp = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)


class Channel:
    """一个频道的节目源状态."""

    def __init__(self, number: int):
        self.number = number
        self.group = f"239.0.0.{number}"      # 模拟组播组
        self.port = 5000 + number             # 模拟组播端口
        self.delay = 0.0                      # 首包延迟(秒)
        self.refuse = False                   # 是否拒绝加入
        self.interval = PACKET_INTERVAL       # 发包间隔(秒)
        self.fec_group_size = max(1, min(FEC_GROUP_SIZE, MAX_FEC_GROUP))
        self.drop_count = max(0, min(DROP_COUNT, self.fec_group_size))
        self.reorder = False                  # 组内按 index 逆序发送 (模拟乱序)
        self.subscribed = False
        self.seq = 0                          # 节目包段号 (含漏发, 跨组递增)
        self.round = 0                        # 轮次: 每次复位 +1, 代理据此识别新一轮
        self.group_id = 0                     # 校验组号 (每轮从 1 开始)
        self.start_group = 1                  # 本轮首个校验组号 (供代理初始化交付游标)
        self.joins = 0
        self.leaves = 0
        self.task: asyncio.Task | None = None

    def info(self) -> dict:
        return {
            "channel": self.number,
            "group": self.group,
            "port": self.port,
            "boot": BOOT_ID,
            "subscribed": self.subscribed,
            "seq": self.seq,
            "round": self.round,
            "group_id": self.group_id,
            "joins": self.joins,
            "leaves": self.leaves,
            "delay": self.delay,
            "refuse": self.refuse,
            "interval": self.interval,
            "fec_group_size": self.fec_group_size,
            "drop_count": self.drop_count,
            "reorder": self.reorder,
        }


channels: dict[int, Channel] = {n: Channel(n) for n in range(CHANNEL_MIN, CHANNEL_MAX + 1)}


def _send(raw: bytes) -> None:
    _udp.sendto(raw, (PROXY_HOST, PROXY_UDP_PORT))


def _group_size(ch: Channel) -> int:
    return max(1, min(ch.fec_group_size, MAX_FEC_GROUP))


def _drop_count(ch: Channel, size: int) -> int:
    # 允许漏掉整组原包 (只发校验包), 用于演示"校验包不能触发首包换台"
    return max(0, min(ch.drop_count, size))


def build_group(number: int, group: str, port: int, round_no: int, gid: int,
                size: int, seq_start: int, ts: float | None = None,
                start_group: int = 1) -> tuple[list[bytes], bytes]:
    """构造一个校验组的原始报文: size 个节目包 (seq_start..seq_start+size-1) + 1 个校验包.

    校验数据 = 同组各节目包完整 JSON 字节逐字节 XOR (短包补零), 长度表记录每包原长.
    纯函数, 便于单测与复用; 不涉及网络与时钟 (ts 可注入).
    """
    if ts is None:
        ts = round(time.time(), 3)
    xor = bytearray()
    lengths: list[int] = []
    programs: list[bytes] = []
    for index in range(size):
        seq = seq_start + index
        body = {
            "type": "program",
            "channel": number,
            "boot": BOOT_ID,
            "round": round_no,
            "group": group,
            "port": port,
            "seq": seq,
            "ts": ts,
            "payload": f"program-{number}-segment-{seq}",
            "fec": {"group": gid, "index": index, "size": size},
            "stream": {"start_group": start_group},
        }
        raw = json.dumps(body, separators=(",", ":")).encode()
        if len(xor) < len(raw):
            xor.extend(bytes(len(raw) - len(xor)))
        for pos, byte in enumerate(raw):
            xor[pos] ^= byte
        lengths.append(len(raw))
        programs.append(raw)
    parity = {
        "type": "fec",
        "channel": number,
        "boot": BOOT_ID,
        "round": round_no,
        "group": group,
        "port": port,
        "group_id": gid,
        "size": size,
        "data": base64.b64encode(bytes(xor)).decode(),
        "lengths": lengths,
        "stream": {"start_group": start_group},
        "ts": ts,
    }
    return programs, json.dumps(parity, separators=(",", ":")).encode()


async def _send_group(ch: Channel, round_no: int) -> bool:
    """发送一个校验组: 原包 (可漏发/乱序) + 1 个校验包. 返回本轮是否继续."""
    size = _group_size(ch)
    drop_count = _drop_count(ch, size)
    ch.group_id += 1
    gid = ch.group_id
    programs, parity_raw = build_group(
        ch.number, ch.group, ch.port, round_no, gid, size, ch.seq + 1,
        start_group=ch.start_group,
    )
    ch.seq += size

    order = list(range(size))
    if ch.reorder:
        order.reverse()
    for pos, index in enumerate(order):
        if index >= drop_count:  # 模拟漏发: 跳过发送但仍占用一个发包时隙
            _send(programs[index])
        if pos < size - 1:
            await asyncio.sleep(ch.interval)
            if ch.round != round_no or not ch.subscribed:
                return False

    _send(parity_raw)
    await asyncio.sleep(ch.interval)
    return ch.round == round_no and ch.subscribed


async def _stream(ch: Channel, round_no: int) -> None:
    """订阅期间周期性发送校验组; 首包可延迟."""
    try:
        if ch.delay > 0:
            await asyncio.sleep(ch.delay)  # 延迟发出首包
        while ch.subscribed and ch.round == round_no:
            if not await _send_group(ch, round_no):
                break
    except asyncio.CancelledError:
        pass


def _stop_stream(ch: Channel) -> None:
    if ch.task is not None:
        ch.task.cancel()
        ch.task = None


def _start_stream(ch: Channel) -> None:
    ch.subscribed = True
    ch.start_group = 1  # 新订阅: 段号/组号可能从 0 开始, 交付游标从组 1 起
    ch.task = asyncio.create_task(_stream(ch, ch.round))


def _restart(ch: Channel) -> None:
    """数据面复位: 段号/组号归零并进入新一轮; 订阅关系与配置保持."""
    _stop_stream(ch)
    ch.round += 1
    ch.seq = 0
    ch.group_id = 0
    ch.start_group = 1
    if ch.subscribed:
        _start_stream(ch)


def _get(number: int) -> Channel:
    ch = channels.get(number)
    if ch is None:
        raise HTTPException(404, f"unknown channel {number} (valid {CHANNEL_MIN}..{CHANNEL_MAX})")
    return ch


@app.post("/join/{number}")
async def join(number: int) -> dict:
    ch = _get(number)
    if ch.refuse:
        raise HTTPException(403, f"join refused for channel {number}")
    if not ch.subscribed:
        ch.joins += 1
        ch.seq = 0
        ch.group_id = 0
        _start_stream(ch)
    return {"channel": number, "group": ch.group, "subscribed": True, "first_packet_delay": ch.delay}


@app.post("/leave/{number}")
async def leave(number: int) -> dict:
    ch = _get(number)
    if ch.subscribed:
        ch.subscribed = False
        ch.leaves += 1
        _stop_stream(ch)
    return {"channel": number, "subscribed": False}


class Behavior(BaseModel):
    delay: float | None = None
    refuse: bool | None = None
    interval: float | None = None
    fec_group_size: int | None = None
    drop_count: int | None = None
    reorder: bool | None = None


@app.post("/config/{number}")
async def config(number: int, body: Behavior) -> dict:
    ch = _get(number)
    if body.delay is not None:
        ch.delay = max(0.0, body.delay)
    if body.refuse is not None:
        ch.refuse = body.refuse
    if body.interval is not None:
        ch.interval = max(0.05, body.interval)
    if body.fec_group_size is not None:
        ch.fec_group_size = max(1, min(body.fec_group_size, MAX_FEC_GROUP))
    if body.drop_count is not None:
        ch.drop_count = max(0, min(body.drop_count, _group_size(ch)))
    if body.reorder is not None:
        ch.reorder = body.reorder
    return ch.info()


@app.get("/state")
async def state() -> dict:
    return {str(n): ch.info() for n, ch in channels.items()}


@app.post("/restart/{number}")
async def restart_channel(number: int) -> dict:
    ch = _get(number)
    _restart(ch)
    return ch.info()


@app.post("/restart")
async def restart_all() -> dict:
    for ch in channels.values():
        _restart(ch)
    return {"restarted": True}


@app.post("/reset")
async def reset() -> dict:
    for ch in channels.values():
        _stop_stream(ch)
        ch.subscribed = False
        ch.round += 1
        ch.seq = 0
        ch.group_id = 0
        ch.joins = 0
        ch.leaves = 0
        ch.delay = 0.0
        ch.refuse = False
        ch.interval = PACKET_INTERVAL
        ch.fec_group_size = max(1, min(FEC_GROUP_SIZE, MAX_FEC_GROUP))
        ch.drop_count = max(0, min(DROP_COUNT, ch.fec_group_size))
        ch.reorder = False
    return {"reset": True}
