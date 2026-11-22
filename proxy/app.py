"""园区电视组播换台代理 (FastAPI).

机顶盒 (STB) API:
  POST /stbs/{sid}/select       按频道号选台 (202: 等待首包; 200: 已在收看)
  POST /stbs/{sid}/unsubscribe  退订
  GET  /stbs/{sid}/packets      读取代理转发的节目包 (读取即清空)
  GET  /stbs/{sid}/status       当前频道 / 等待中的频道 / 最近错误
  GET  /state                   代理内部状态 (调试用)

核心规则:
  - 同一频道只向上游订阅一路 (收看者与等待首包者共享)
  - 无人收看且无人等候首包时退订上游
  - 收到新频道首包才切换; 加入失败就继续收旧台
  - 等待首包时仍可改选或退订; 已取消的选台请求不因迟到的首包生效
    (每次选台/退订递增机顶盒代次 generation, 首包到达时校验)
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from contextlib import asynccontextmanager

import httpx
from fastapi import FastAPI, HTTPException
from fastapi.responses import RedirectResponse
from pydantic import BaseModel

SOURCE_URL = os.getenv("SOURCE_URL", "http://127.0.0.1:9000").rstrip("/")
UDP_LISTEN_PORT = int(os.getenv("UDP_LISTEN_PORT", "5000"))
CHANNEL_MIN = int(os.getenv("CHANNEL_MIN", "1"))
CHANNEL_MAX = int(os.getenv("CHANNEL_MAX", "10"))
MAX_QUEUE = int(os.getenv("MAX_QUEUE", "200"))
UPSTREAM_TIMEOUT = float(os.getenv("UPSTREAM_TIMEOUT", "5.0"))


class ChannelState:
    """一个频道的代理侧状态."""

    def __init__(self, number: int):
        self.number = number
        self.subscribed = False  # 上游订阅已建立
        self.join_task: asyncio.Task | None = None  # 进行中的上游加入任务
        self.watchers: set[str] = set()  # 正在收看的机顶盒
        self.pending: dict[str, int] = {}  # 等待首包的机顶盒 -> 选台代次


class StbState:
    """一台机顶盒的状态."""

    def __init__(self, sid: str):
        self.sid = sid
        self.current: int | None = None  # 当前收看频道
        self.pending_channel: int | None = None  # 等待首包的目标频道
        self.generation = 0  # 每次选台/退订 +1, 使已取消的请求失效
        self.last_error: str | None = None
        self.queue: list[dict] = []  # 待读取的节目包


channels: dict[int, ChannelState] = {}
stbs: dict[str, StbState] = {}
http: httpx.AsyncClient | None = None


# ---------------------------------------------------------------- 上游交互


async def _leave_upstream(number: int) -> None:
    try:
        assert http is not None
        await http.post(f"{SOURCE_URL}/leave/{number}")
    except httpx.HTTPError:
        pass  # 上游暂时不可达; 模拟源侧包会自行发完即被忽略


def _maybe_leave(cs: ChannelState) -> None:
    """无人收看且无人等候首包时退订上游."""
    if cs.watchers or cs.pending:
        return
    if cs.join_task is not None:
        return  # 加入请求在途: 由 _join_upstream 完成后兜底退订, 避免 join/leave 乱序
    if cs.subscribed:
        cs.subscribed = False
        asyncio.get_running_loop().create_task(_leave_upstream(cs.number))


def _ensure_join(cs: ChannelState) -> None:
    """同一频道只向上游订阅一路."""
    if cs.subscribed or cs.join_task is not None:
        return
    cs.join_task = asyncio.get_running_loop().create_task(_join_upstream(cs))


async def _join_upstream(cs: ChannelState) -> None:
    try:
        assert http is not None
        resp = await http.post(f"{SOURCE_URL}/join/{cs.number}")
        resp.raise_for_status()
    except Exception as exc:  # 加入被拒绝或上游不可达
        cs.join_task = None
        if isinstance(exc, httpx.HTTPStatusError):
            reason = f"upstream rejected join with {exc.response.status_code}"
        else:
            reason = f"upstream join error: {type(exc).__name__}"
        # 等待中的机顶盒选台失败, 继续收旧台
        for sid, gen in list(cs.pending.items()):
            stb = stbs.get(sid)
            if stb is not None and stb.generation == gen and stb.pending_channel == cs.number:
                stb.pending_channel = None
                stb.last_error = f"join channel {cs.number} failed: {reason}"
                cs.pending.pop(sid, None)
        return
    cs.subscribed = True
    cs.join_task = None
    _maybe_leave(cs)  # 加入完成前所有人都取消了 -> 立即退订


# ---------------------------------------------------------------- 机顶盒操作


def _get_stb(sid: str) -> StbState:
    stb = stbs.get(sid)
    if stb is None:
        stb = stbs[sid] = StbState(sid)
    return stb


def _get_channel(number: int) -> ChannelState:
    cs = channels.get(number)
    if cs is None:
        cs = channels[number] = ChannelState(number)
    return cs


def _cancel_pending(stb: StbState) -> None:
    """取消等待中的选台; 之后迟到的首包对该请求不再生效."""
    if stb.pending_channel is not None:
        cs = channels.get(stb.pending_channel)
        if cs is not None:
            cs.pending.pop(stb.sid, None)
            _maybe_leave(cs)
        stb.pending_channel = None


# ---------------------------------------------------------------- 数据面


def handle_packet(pkt: dict) -> None:
    """收到节目源转发来的一个节目包 (在事件循环内调用)."""
    try:
        number = int(pkt.get("channel"))
    except (TypeError, ValueError):
        return
    cs = channels.get(number)
    if cs is None:
        return  # 未订阅的频道: 忽略

    # 首包到达: 把仍在等待该频道的机顶盒切换过来
    for sid, gen in list(cs.pending.items()):
        stb = stbs.get(sid)
        if stb is None or stb.generation != gen or stb.pending_channel != number:
            # 已改选/退订的请求不因迟到的首包生效
            cs.pending.pop(sid, None)
            continue
        old = stb.current
        if old is not None and old != number:
            old_cs = channels.get(old)
            if old_cs is not None:
                old_cs.watchers.discard(sid)
                _maybe_leave(old_cs)
        stb.current = number
        stb.pending_channel = None
        cs.pending.pop(sid, None)
        cs.watchers.add(sid)

    # 分发给所有收看者 (含刚切换过来的)
    for sid in cs.watchers:
        stb = stbs.get(sid)
        if stb is None:
            continue
        pkt = dict(pkt)
        pkt["received_at"] = round(time.time(), 3)
        stb.queue.append(pkt)
        if len(stb.queue) > MAX_QUEUE:
            del stb.queue[: len(stb.queue) - MAX_QUEUE]


class _UdpProtocol(asyncio.DatagramProtocol):
    def datagram_received(self, data: bytes, addr) -> None:  # noqa: ANN001
        try:
            pkt = json.loads(data.decode())
        except (ValueError, UnicodeDecodeError):
            return
        if isinstance(pkt, dict):
            handle_packet(pkt)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http
    http = httpx.AsyncClient(timeout=UPSTREAM_TIMEOUT)
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        _UdpProtocol, local_addr=("0.0.0.0", UDP_LISTEN_PORT)
    )
    yield
    transport.close()
    await http.aclose()


app = FastAPI(
    title="Campus TV Multicast Switching Proxy",
    description="园区电视组播换台代理: 机顶盒选台/退订/读包, 上游每频道一路订阅.",
    lifespan=lifespan,
)


@app.get("/", include_in_schema=False)
async def index() -> RedirectResponse:
    return RedirectResponse("/docs")


# ---------------------------------------------------------------- REST API


class SelectRequest(BaseModel):
    channel: int


@app.post("/stbs/{sid}/select", status_code=202)
async def select(sid: str, body: SelectRequest) -> dict:
    """按频道号选台. 收到新频道首包后才真正切换."""
    number = body.channel
    if not (CHANNEL_MIN <= number <= CHANNEL_MAX):
        raise HTTPException(404, f"unknown channel {number} (valid {CHANNEL_MIN}..{CHANNEL_MAX})")
    stb = _get_stb(sid)
    stb.generation += 1  # 使此前未完成的选台请求失效
    stb.last_error = None
    _cancel_pending(stb)
    if stb.current == number:
        return {"stb": sid, "status": "watching", "channel": number, "generation": stb.generation}
    cs = _get_channel(number)
    cs.pending[sid] = stb.generation
    stb.pending_channel = number
    _ensure_join(cs)
    return {
        "stb": sid,
        "status": "pending_first_packet",
        "channel": number,
        "generation": stb.generation,
    }


@app.post("/stbs/{sid}/unsubscribe")
async def unsubscribe(sid: str) -> dict:
    """退订: 停止收看并取消等待中的选台."""
    stb = _get_stb(sid)
    stb.generation += 1
    _cancel_pending(stb)
    if stb.current is not None:
        cs = channels.get(stb.current)
        if cs is not None:
            cs.watchers.discard(sid)
            _maybe_leave(cs)
        stb.current = None
    return {"stb": sid, "status": "unsubscribed"}


@app.get("/stbs/{sid}/packets")
async def packets(sid: str, limit: int = 100) -> dict:
    """读取代理转发给该机顶盒的节目包 (读取即清空)."""
    stb = _get_stb(sid)
    limit = max(1, min(limit, 500))
    out = stb.queue[:limit]
    del stb.queue[:limit]
    return {
        "stb": sid,
        "current_channel": stb.current,
        "returned": len(out),
        "packets": out,
    }


@app.get("/stbs/{sid}/status")
async def status(sid: str) -> dict:
    stb = _get_stb(sid)
    return {
        "stb": sid,
        "current_channel": stb.current,
        "pending_channel": stb.pending_channel,
        "generation": stb.generation,
        "last_error": stb.last_error,
        "queued_packets": len(stb.queue),
    }


@app.get("/stbs")
async def list_stbs() -> dict:
    return {sid: {"current": s.current, "pending": s.pending_channel} for sid, s in stbs.items()}


@app.get("/state")
async def debug_state() -> dict:
    return {
        "channels": {
            str(n): {
                "upstream_subscribed": cs.subscribed,
                "join_in_progress": cs.join_task is not None,
                "watchers": sorted(cs.watchers),
                "pending": sorted(cs.pending),
            }
            for n, cs in sorted(channels.items())
        },
        "stbs": {
            sid: {
                "current_channel": s.current,
                "pending_channel": s.pending_channel,
                "generation": s.generation,
                "last_error": s.last_error,
                "queued_packets": len(s.queue),
            }
            for sid, s in sorted(stbs.items())
        },
    }


@app.post("/reset")
async def reset() -> dict:
    """清空全部机顶盒并退订所有上游频道 (演示/自测用)."""
    for cs in channels.values():
        if cs.join_task is not None:
            cs.join_task.cancel()
            cs.join_task = None
        if cs.subscribed:
            cs.subscribed = False
            await _leave_upstream(cs.number)
    channels.clear()
    stbs.clear()
    return {"reset": True}
