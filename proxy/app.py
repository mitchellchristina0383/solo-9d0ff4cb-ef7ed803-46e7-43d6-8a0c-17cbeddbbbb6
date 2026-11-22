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

数据面 (FEC):
  - 节目源每 N 个连续节目包加发 1 个 XOR 校验包 (N 默认 3)
  - 代理按 (round, group, index) 缓存; 组内缺 1 包且有校验包时立即还原
  - 交付严格按组号/组内序号的原始顺序, 每个包只入队一次
  - 缺包且无法还原时最多等 FEC_WAIT_SECONDS (默认/上限 2 秒), 到期跳过缺包
  - 等候从收到该组或更晚组的首个报文起算
  - 收到新一轮 (源复位) 的首个报文即清空旧轮尚未入队的数据, 之后忽略旧轮报文
  - 首包换台只认"准备入队"的节目包 (校验包与尚未交付的缓存包不触发切换)
"""

from __future__ import annotations

import asyncio
import base64
import binascii
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
# 缺包等候上限. 需求上限 2 秒; 默认 2, 可调小便于测试, 但不会超过 2.
FEC_WAIT_SECONDS = min(2.0, max(0.0, float(os.getenv("FEC_WAIT_SECONDS", "2.0"))))
MAX_FEC_GROUP = 32


class FecGroup:
    """一个校验组在代理侧的缓存."""

    def __init__(self, channel: int, gid: int, size: int, first_seq: int | None):
        self.channel = channel
        self.gid = gid
        self.size = size
        self.first_seq = first_seq  # 由任意原包推得: seq - index
        self.received: dict[int, dict] = {}  # index -> 已解析的节目包
        self.raw: dict[int, bytes] = {}  # index -> 原始 JSON 字节 (用于 XOR)
        self.parity: bytes | None = None  # 校验数据 (已 base64 解码)
        self.lengths: list[int] | None = None
        self.created_at = time.monotonic()
        self.deadline = self.created_at + FEC_WAIT_SECONDS
        self.recovered: set[int] = set()

    def missing(self) -> list[int]:
        return [i for i in range(self.size) if i not in self.received]


class ChannelState:
    """一个频道的代理侧状态."""

    def __init__(self, number: int):
        self.number = number
        self.subscribed = False  # 上游订阅已建立
        self.join_task: asyncio.Task | None = None  # 进行中的上游加入任务
        self.watchers: set[str] = set()  # 正在收看的机顶盒
        self.pending: dict[str, int] = {}  # 等待首包的机顶盒 -> 选台代次

        # FEC / 交付状态
        self.round_key: tuple[str, int] | None = None  # (源会话, 轮次)
        self.ignored_boots: set[str] = set()  # 已作废的源会话号
        self.groups: dict[int, FecGroup] = {}  # 当前轮尚未交付完的组
        self.expired: set[int] = set()  # 已过等候期/已越过的组: 迟到报文一律忽略
        self.next_gid: int | None = None  # 下一个待交付组号
        self.next_index = 0  # 组内下一个待交付下标
        self.round_start_gid: int | None = None  # 本轮起始组号 (源端元数据)
        self.round_delivered = 0  # 本轮已入队包数 (用于判断能否回退)
        self.timer: asyncio.TimerHandle | None = None
        self.stats = {
            "delivered": 0,
            "recovered": 0,
            "skipped": 0,
            "skipped_groups": 0,
            "discarded_groups": 0,
            "rounds": 0,
        }


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


def _discard_fec(cs: ChannelState) -> None:
    """丢弃当前轮尚未入队的数据并停止定时器 (源复位/退订时)."""
    if cs.timer is not None:
        cs.timer.cancel()
        cs.timer = None
    cs.stats["discarded_groups"] += len(cs.groups)
    cs.groups.clear()
    cs.expired.clear()
    cs.next_gid = None
    cs.next_index = 0
    cs.round_start_gid = None
    cs.round_delivered = 0
    cs.round_key = None


def _maybe_leave(cs: ChannelState) -> None:
    """无人收看且无人等候首包时退订上游."""
    if not cs.watchers and not cs.pending:
        _discard_fec(cs)
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


# ---------------------------------------------------------------- FEC 数据面


def _round_of(pkt: dict) -> tuple[str, int] | None:
    try:
        return str(pkt.get("boot", "")), int(pkt.get("round", 0))
    except (TypeError, ValueError):
        return None


def _is_newer(cs: ChannelState, key: tuple[str, int]) -> bool:
    """判断报文是否属于比当前更新的一轮 (源复位)."""
    assert cs.round_key is not None
    old_boot, old_round = cs.round_key
    boot, round_no = key
    if boot == old_boot:
        return round_no > old_round
    # 源进程重启: 会话号变了且不是已作废的旧会话
    return boot not in cs.ignored_boots


def _start_round(cs: ChannelState, key: tuple[str, int], pkt: dict) -> None:
    """切换到新一轮: 清空旧轮尚未入队的数据, 之后旧轮报文一律忽略.

    若源端在报文中声明了本轮起始组号 (stream.start_group), 交付游标从那里开始:
    这样"更晚的组先到"时, 更早的组仍能得到完整的等候期, 不会立刻被越过.
    """
    if cs.round_key is not None:
        cs.ignored_boots.add(cs.round_key[0])
    _discard_fec(cs)
    cs.round_key = key
    cs.stats["rounds"] += 1
    stream = pkt.get("stream")
    if isinstance(stream, dict):
        try:
            start_gid = int(stream.get("start_group"))
        except (TypeError, ValueError):
            start_gid = None
        if start_gid is not None and start_gid >= 0:
            cs.round_start_gid = start_gid
            cs.next_gid = start_gid


def _group_deadline(cs: ChannelState, gid: int, created_at: float) -> float:
    """等候起点 = 收到的该组或更晚组的首个报文时间."""
    origin = created_at
    for other_gid, group in cs.groups.items():
        if other_gid >= gid:
            origin = min(origin, group.created_at)
    return origin + FEC_WAIT_SECONDS


def _register_group(cs: ChannelState, gid: int, size: int,
                    first_seq: int | None) -> FecGroup:
    group = FecGroup(cs.number, gid, size, first_seq)
    group.deadline = _group_deadline(cs, gid, group.created_at)
    cs.groups[gid] = group
    if cs.next_gid is None:
        cs.next_gid = min(cs.groups)
    elif (gid < cs.next_gid and cs.round_delivered == 0
          and gid not in cs.expired
          and (cs.round_start_gid is None or gid >= cs.round_start_gid)):
        # 跨组乱序 (源端未声明起始组时): 尚未交付任何包时可回退到更早的组;
        # 已声明起始组时不允许回退到本轮起点之前 (那是上一轮/异常报文)
        cs.next_gid = gid
        cs.next_index = 0
    return group


def _ingest_program(cs: ChannelState, pkt: dict, raw: bytes) -> None:
    fec = pkt.get("fec")
    if not isinstance(fec, dict):
        return
    try:
        gid = int(fec.get("group"))
        index = int(fec.get("index"))
        size = int(fec.get("size"))
        seq = int(pkt.get("seq"))
    except (TypeError, ValueError):
        return
    if not (0 <= index < size <= MAX_FEC_GROUP) or seq < 0:
        return

    if gid in cs.expired:
        return  # 该组已跳过, 迟到报文不再交付
    if cs.next_gid is not None:
        if gid < cs.next_gid:
            # 已越过的组: 本轮尚未交付任何包时允许回退, 否则忽略
            if cs.round_delivered != 0:
                return
        elif gid == cs.next_gid and index < cs.next_index:
            return  # 已交付/已跳过

    first_seq = seq - index
    if first_seq < 0:
        return
    group = cs.groups.get(gid)
    if group is None:
        group = _register_group(cs, gid, size, first_seq)
    elif group.size != size:
        return
    if group.first_seq is None:
        group.first_seq = first_seq
    elif group.first_seq != first_seq:
        return  # 同组段号不一致, 丢弃异常包
    if index in group.received:
        return  # 重复包
    group.received[index] = pkt
    group.raw[index] = raw


def _ingest_parity(cs: ChannelState, pkt: dict) -> None:
    try:
        gid = int(pkt.get("group_id"))
        size = int(pkt.get("size"))
        data = base64.b64decode(pkt.get("data", ""), validate=True)
        lengths = [int(x) for x in pkt.get("lengths", [])]
    except (TypeError, ValueError, binascii.Error):
        return
    if not (1 <= size <= MAX_FEC_GROUP) or len(lengths) != size:
        return
    if len(data) != max(lengths, default=0):
        return
    if gid in cs.expired:
        return
    if cs.next_gid is not None:
        if gid < cs.next_gid:
            if cs.round_delivered != 0:
                return
        elif gid == cs.next_gid and cs.next_index >= size:
            return
    group = cs.groups.get(gid)
    if group is None:
        group = _register_group(cs, gid, size, None)
    elif group.size != size:
        return
    if group.parity is not None:
        return
    group.parity = data
    group.lengths = lengths


def _try_recover(cs: ChannelState, group: FecGroup) -> None:
    """组内只缺 1 包时用校验包还原它 (XOR, 短包补零)."""
    if group.parity is None or group.lengths is None or group.first_seq is None:
        return
    missing = group.missing()
    if len(missing) != 1:
        return
    index = missing[0]
    data = bytearray(group.parity)
    for raw in group.raw.values():
        if len(raw) > len(data):
            return  # 收到的原包比校验数据还长: 数据不可信, 放弃补包
        for pos, byte in enumerate(raw):
            data[pos] ^= byte
    if not (0 <= group.lengths[index] <= len(data)):
        return
    raw = bytes(data[: group.lengths[index]])
    try:
        pkt = json.loads(raw.decode())
    except (ValueError, UnicodeDecodeError):
        return
    if not isinstance(pkt, dict) or pkt.get("type") != "program":
        return
    try:
        fec = pkt["fec"]
        ok = (
            int(pkt["channel"]) == group.channel
            and _round_of(pkt) == cs.round_key
            and int(fec["group"]) == group.gid
            and int(fec["index"]) == index
            and int(fec["size"]) == group.size
            and int(pkt["seq"]) == group.first_seq + index
        )
    except (KeyError, TypeError, ValueError):
        return
    if not ok:
        return
    group.received[index] = pkt
    group.raw[index] = raw
    group.recovered.add(index)


def _deliver(cs: ChannelState, pkt: dict, recovered: bool) -> None:
    """把一个"准备入队"的节目包交付给所有收看者; 首包换台在此触发."""
    cs.stats["delivered"] += 1
    cs.round_delivered += 1
    if recovered:
        cs.stats["recovered"] += 1

    # 首包换台只认准备入队的节目包
    for sid, gen in list(cs.pending.items()):
        stb = stbs.get(sid)
        if stb is None or stb.generation != gen or stb.pending_channel != cs.number:
            cs.pending.pop(sid, None)
            continue
        old = stb.current
        if old is not None and old != cs.number:
            old_cs = channels.get(old)
            if old_cs is not None:
                old_cs.watchers.discard(sid)
                _maybe_leave(old_cs)
        stb.current = cs.number
        stb.pending_channel = None
        cs.pending.pop(sid, None)
        cs.watchers.add(sid)

    now = round(time.time(), 3)
    for sid in list(cs.watchers):
        stb = stbs.get(sid)
        if stb is None:
            continue
        out = dict(pkt)
        out["received_at"] = now
        if recovered:
            out["recovered"] = True
        stb.queue.append(out)
        if len(stb.queue) > MAX_QUEUE:
            del stb.queue[: len(stb.queue) - MAX_QUEUE]


def _try_deliver(cs: ChannelState) -> None:
    """按原始顺序交付已到齐/已到期的包, 遇到未到期缺包则停下等定时器."""
    if cs.next_gid is None or (not cs.watchers and not cs.pending):
        return
    now = time.monotonic()
    while True:
        gid = cs.next_gid
        group = cs.groups.get(gid)
        if group is None:
            later = [g for g in cs.groups if g > gid]
            if not later:
                break
            nxt = min(later)
            if now >= cs.groups[nxt].deadline:
                cs.stats["skipped_groups"] += nxt - gid
                for stale in [g for g in cs.groups if g < nxt]:
                    del cs.groups[stale]
                    cs.expired.add(stale)
                cs.expired.add(gid)
                cs.next_gid = nxt
                cs.next_index = 0
                continue
            break
        _try_recover(cs, group)
        if cs.next_index >= group.size:
            del cs.groups[gid]
            cs.next_gid = gid + 1
            cs.next_index = 0
            continue
        index = cs.next_index
        if index in group.received:
            _deliver(cs, group.received[index], index in group.recovered)
            cs.next_index = index + 1
            continue
        if now >= group.deadline:
            cs.stats["skipped"] += 1
            cs.expired.add(gid)  # 该组后续缺失包到期跳过; 迟到报文不再交付
            cs.next_index = index + 1
            continue
        break
    if cs.round_delivered and cs.next_gid is not None:
        # 已交付则更早的组本来就会被忽略, 及时回收过期标记避免无限增长
        cs.expired = {g for g in cs.expired if g >= cs.next_gid}


def _schedule_timer(cs: ChannelState) -> None:
    """在下一个与交付游标相关的截止时间点唤醒, 处理到期缺包."""
    if cs.timer is not None:
        cs.timer.cancel()
        cs.timer = None
    if cs.next_gid is None or not (cs.watchers or cs.pending):
        return
    group = cs.groups.get(cs.next_gid)
    if group is None:
        later = [g for g in cs.groups if g > cs.next_gid]
        if not later:
            return
        deadline = cs.groups[min(later)].deadline
    else:
        deadline = group.deadline
    delay = max(0.0, deadline - time.monotonic())
    loop = asyncio.get_running_loop()

    def _fire() -> None:
        cs.timer = None
        _try_deliver(cs)
        _schedule_timer(cs)

    cs.timer = loop.call_later(delay, _fire)


def handle_packet(pkt: dict, raw: bytes | None = None) -> None:
    """收到节目源转发来的一个报文 (在事件循环内调用)."""
    try:
        number = int(pkt.get("channel"))
    except (TypeError, ValueError):
        return
    cs = channels.get(number)
    if cs is None or (not cs.watchers and not cs.pending):
        return  # 未订阅/无人关心: 忽略

    key = _round_of(pkt)
    if key is None:
        return
    if cs.round_key is None:
        _start_round(cs, key, pkt)
    elif key != cs.round_key:
        if _is_newer(cs, key):
            _start_round(cs, key, pkt)
        else:
            return  # 旧轮报文: 一律忽略

    kind = pkt.get("type", "program")
    if kind == "fec":
        _ingest_parity(cs, pkt)
    elif kind == "program":
        if "fec" not in pkt:
            # 无 FEC 元数据的上游 (旧协议/其他源): 按到达顺序直接交付
            _deliver(cs, pkt, recovered=False)
        elif not isinstance(pkt.get("fec"), dict):
            return  # fec 字段存在但格式错误: 丢弃
        else:
            if raw is None:
                raw = json.dumps(pkt, separators=(",", ":")).encode()
            _ingest_program(cs, pkt, raw)
    else:
        return

    _try_deliver(cs)
    _schedule_timer(cs)


class _UdpProtocol(asyncio.DatagramProtocol):
    def datagram_received(self, data: bytes, addr) -> None:  # noqa: ANN001
        try:
            pkt = json.loads(data.decode())
        except (ValueError, UnicodeDecodeError):
            return
        if isinstance(pkt, dict):
            handle_packet(pkt, data)


@asynccontextmanager
async def lifespan(app: FastAPI):
    global http
    http = httpx.AsyncClient(timeout=UPSTREAM_TIMEOUT)
    loop = asyncio.get_running_loop()
    transport, _ = await loop.create_datagram_endpoint(
        _UdpProtocol, local_addr=("0.0.0.0", UDP_LISTEN_PORT)
    )
    yield
    for cs in channels.values():
        if cs.timer is not None:
            cs.timer.cancel()
    transport.close()
    await http.aclose()


app = FastAPI(
    title="Campus TV Multicast Switching Proxy",
    description="园区电视组播换台代理: 机顶盒选台/退订/读包, 上游每频道一路订阅, 支持 XOR FEC 补包.",
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
    """按频道号选台. 收到新频道首个准备入队的节目包后才真正切换."""
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


def _cs_info(cs: ChannelState) -> dict:
    return {
        "upstream_subscribed": cs.subscribed,
        "join_in_progress": cs.join_task is not None,
        "watchers": sorted(cs.watchers),
        "pending": sorted(cs.pending),
        "fec": {
            "round": list(cs.round_key) if cs.round_key is not None else None,
            "next_group": cs.next_gid,
            "next_index": cs.next_index,
            "buffered_groups": sorted(cs.groups),
            "wait_seconds": FEC_WAIT_SECONDS,
            **cs.stats,
        },
    }


@app.get("/state")
async def debug_state() -> dict:
    return {
        "channels": {str(n): _cs_info(cs) for n, cs in sorted(channels.items())},
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
        if cs.timer is not None:
            cs.timer.cancel()
            cs.timer = None
        if cs.subscribed:
            cs.subscribed = False
            await _leave_upstream(cs.number)
    channels.clear()
    stbs.clear()
    return {"reset": True}
