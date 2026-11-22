"""模拟园区电视节目源.

控制面 (HTTP/FastAPI):
  POST /join/{channel}    代理请求加入频道; 按频道配置可延迟首包或拒绝加入
  POST /leave/{channel}   代理请求退订频道 (幂等)
  GET  /state             查看各频道订阅状态与统计
  POST /config/{channel}  动态配置频道行为 (首包延迟 delay / 拒绝加入 refuse / 发包间隔 interval)
  POST /reset             全部复位

数据面: 订阅建立后, 按 interval 周期向代理的 UDP 端口发送节目包 (模拟组播转发).
"""

from __future__ import annotations

import asyncio
import json
import os
import socket
import time

from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

PROXY_HOST = os.getenv("PROXY_HOST", "127.0.0.1")
PROXY_UDP_PORT = int(os.getenv("PROXY_UDP_PORT", "5000"))
PACKET_INTERVAL = float(os.getenv("PACKET_INTERVAL", "0.5"))
CHANNEL_MIN = int(os.getenv("CHANNEL_MIN", "1"))
CHANNEL_MAX = int(os.getenv("CHANNEL_MAX", "10"))

app = FastAPI(
    title="Simulated TV Program Source",
    description="本地模拟节目源: 支持延迟首包与拒绝加入, 用于演示组播换台代理.",
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
        self.subscribed = False
        self.seq = 0
        self.joins = 0
        self.leaves = 0
        self.task: asyncio.Task | None = None

    def info(self) -> dict:
        return {
            "channel": self.number,
            "group": self.group,
            "port": self.port,
            "subscribed": self.subscribed,
            "seq": self.seq,
            "joins": self.joins,
            "leaves": self.leaves,
            "delay": self.delay,
            "refuse": self.refuse,
            "interval": self.interval,
        }


channels: dict[int, Channel] = {n: Channel(n) for n in range(CHANNEL_MIN, CHANNEL_MAX + 1)}


async def _stream(ch: Channel) -> None:
    """订阅期间周期性向代理发送节目包; 首包可延迟."""
    try:
        if ch.delay > 0:
            await asyncio.sleep(ch.delay)  # 延迟发出首包
        while ch.subscribed:
            ch.seq += 1
            pkt = {
                "channel": ch.number,
                "group": ch.group,
                "port": ch.port,
                "seq": ch.seq,
                "ts": round(time.time(), 3),
                "payload": f"program-{ch.number}-segment-{ch.seq}",
            }
            _udp.sendto(json.dumps(pkt).encode(), (PROXY_HOST, PROXY_UDP_PORT))
            await asyncio.sleep(ch.interval)
    except asyncio.CancelledError:
        pass


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
        ch.subscribed = True
        ch.joins += 1
        ch.task = asyncio.create_task(_stream(ch))
    return {"channel": number, "group": ch.group, "subscribed": True, "first_packet_delay": ch.delay}


@app.post("/leave/{number}")
async def leave(number: int) -> dict:
    ch = _get(number)
    if ch.subscribed:
        ch.subscribed = False
        ch.leaves += 1
        if ch.task is not None:
            ch.task.cancel()
            ch.task = None
    return {"channel": number, "subscribed": False}


class Behavior(BaseModel):
    delay: float | None = None
    refuse: bool | None = None
    interval: float | None = None


@app.post("/config/{number}")
async def config(number: int, body: Behavior) -> dict:
    ch = _get(number)
    if body.delay is not None:
        ch.delay = max(0.0, body.delay)
    if body.refuse is not None:
        ch.refuse = body.refuse
    if body.interval is not None:
        ch.interval = max(0.05, body.interval)
    return ch.info()


@app.get("/state")
async def state() -> dict:
    return {str(n): ch.info() for n, ch in channels.items()}


@app.post("/reset")
async def reset() -> dict:
    for ch in channels.values():
        if ch.task is not None:
            ch.task.cancel()
            ch.task = None
        ch.subscribed = False
        ch.seq = 0
        ch.joins = 0
        ch.leaves = 0
        ch.delay = 0.0
        ch.refuse = False
        ch.interval = PACKET_INTERVAL
    return {"reset": True}
