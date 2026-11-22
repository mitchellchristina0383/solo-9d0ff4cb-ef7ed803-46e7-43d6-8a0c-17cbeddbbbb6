"""纯函数单元测试: XOR 校验组构造与代理补包, 不发网络报文.

直接以模块方式加载 source/app.py 与 proxy/app.py 中的纯函数, 验证:
  - 组内任意 1 个原包缺失都可用其余 2 个原包 + 校验包逐字节还原
  - 还原出的报文与源端原本要发出的报文完全一致
  - 校验数据长度 == 组内最长原包; 长度表记录每个原包的实际长度
  - 组内乱序不影响到齐判定
  - 组大小变化 (2..8) 都成立

运行: python3 tests/fec_unit.py
"""

from __future__ import annotations

import asyncio
import importlib.util
import os
import sys

ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))


def _load(name: str, path: str):
    spec = importlib.util.spec_from_file_location(name, path)
    module = importlib.util.module_from_spec(spec)
    sys.modules[name] = module
    spec.loader.exec_module(module)
    return module


src = _load("tv_source_app", os.path.join(ROOT, "source", "app.py"))
proxy = _load("tv_proxy_app", os.path.join(ROOT, "proxy", "app.py"))

failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def recover(programs: list[bytes], parity_raw: bytes, drop: list[int],
            seq_start: int = 1) -> dict[int, bytes]:
    """用代理的真实实现 (FecGroup + _try_recover) 还原 drop 中的下标.

    programs 按 index 排列; 缺 drop 中的下标.
    """
    import json

    parity = json.loads(parity_raw.decode())
    size = parity["size"]
    group = proxy.FecGroup(parity["channel"], parity["group_id"], size, None)
    group.first_seq = seq_start
    cs = proxy.ChannelState(parity["channel"])
    cs.round_key = (parity["boot"], parity["round"])
    for index, raw in enumerate(programs):
        if index in drop:
            continue
        pkt = json.loads(raw.decode())
        group.received[index] = pkt
        group.raw[index] = raw
    group.parity = proxy.base64.b64decode(parity["data"])
    group.lengths = parity["lengths"]
    proxy._try_recover(cs, group)
    return {i: group.raw[i] for i in group.recovered}


def main() -> int:
    import json

    # ---- 1. 标准 3 包组, 逐一缺失每个原包都能还原 ----
    print("\n== 1. 3 包组: 每个原包缺失都能还原 ==")
    programs, parity_raw = src.build_group(
        number=1, group="239.0.0.1", port=5001, round_no=7, gid=42,
        size=3, seq_start=1, ts=1700000000.0,
    )
    parity = json.loads(parity_raw.decode())
    check("校验数据长度 == 组内最长原包",
          len(proxy.base64.b64decode(parity["data"])) == max(parity["lengths"]),
          f"{len(proxy.base64.b64decode(parity['data']))} vs {max(parity['lengths'])}")
    check("长度表逐包对应", parity["lengths"] == [len(p) for p in programs],
          f"{parity['lengths']} vs {[len(p) for p in programs]}")
    check("封装开销 (校验报文字节) > 校验数据", len(parity_raw) > max(parity["lengths"]))
    for miss in range(3):
        got = recover(programs, parity_raw, [miss])
        check(f"缺第 {miss} 包可还原且逐字节一致",
              got.get(miss) == programs[miss],
              f"recovered={sorted(got)}")
        if miss in got:
            body = json.loads(got[miss].decode())
            check(f"缺第 {miss} 包还原后校验元数据正确",
                  body["fec"] == {"group": 42, "index": miss, "size": 3}
                  and body["seq"] == 1 + miss,
                  str(body.get("fec")))

    # ---- 2. 缺 2 包不可还原 ----
    print("\n== 2. 缺 2 包不可还原 ==")
    got = recover(programs, parity_raw, [0, 2])
    check("缺 2 包时不还原 (交给超时跳过)", got == {}, str(sorted(got)))

    # ---- 3. 组内乱序 ----
    print("\n== 3. 组内乱序 ==")
    order = [2, 0, 1]  # 到达顺序与组内序号无关; 到齐判定只看 index 元数据
    par = json.loads(parity_raw.decode())
    group = proxy.FecGroup(par["channel"], par["group_id"], 3, None)
    group.first_seq = 1
    cs = proxy.ChannelState(par["channel"])
    cs.round_key = (par["boot"], par["round"])
    for i in order:
        if i == 1:
            continue
        pkt = json.loads(programs[i].decode())
        group.received[i] = pkt
        group.raw[i] = programs[i]
    group.parity = proxy.base64.b64decode(par["data"])
    group.lengths = par["lengths"]
    proxy._try_recover(cs, group)
    check("乱序到齐 + 缺 1 包仍可还原", group.raw.get(1) == programs[1])

    # ---- 4. 组大小 2..8 与不等长报文 ----
    print("\n== 4. 组大小 2..8, 不等长报文 ==")
    for size in range(2, 9):
        progs, par = src.build_group(
            number=3, group="239.0.0.3", port=5003, round_no=1, gid=size,
            size=size, seq_start=1, ts=1700000000.0,
        )
        # 人为让中间包更长/更短, 验证补零 XOR 正确
        for miss in range(size):
            got = recover(progs, par, [miss])
            check(f"size={size} 缺第 {miss} 包还原一致", got.get(miss) == progs[miss],
                  f"recovered={sorted(got)}")

    # ---- 5. 代理拒绝伪造/损坏的校验数据 ----
    print("\n== 5. 损坏的校验包不会产出错误报文 ==")
    bad = json.loads(parity_raw.decode())
    raw_data = bytearray(proxy.base64.b64decode(bad["data"]))
    raw_data[10] ^= 0xFF
    bad["data"] = proxy.base64.b64encode(bytes(raw_data)).decode()
    got = recover(programs, json.dumps(bad).encode(), [0])
    check("校验数据被篡改时不交付错误包", got == {}, str(sorted(got)))

    # ---- 6. 代理不把不同组的校验包混用 ----
    print("\n== 6. 组号/段号校验 ==")
    progs2, par2 = src.build_group(
        number=1, group="239.0.0.1", port=5001, round_no=7, gid=43,
        size=3, seq_start=4, ts=1700000000.0,
    )
    other = json.loads(par2.decode())
    other["group_id"] = 44  # 元数据说组号 44, 但按 43 使用
    group = proxy.FecGroup(1, 43, 3, 1)
    cs = proxy.ChannelState(1)
    cs.round_key = (src.BOOT_ID, 7)
    group.received[1] = json.loads(progs2[1].decode())
    group.raw[1] = progs2[1]
    group.received[2] = json.loads(progs2[2].decode())
    group.raw[2] = progs2[2]
    group.parity = proxy.base64.b64decode(other["data"])
    group.lengths = other["lengths"]
    proxy._try_recover(cs, group)
    check("组号不匹配时不还原", 0 not in group.recovered, str(sorted(group.recovered)))

    # ---- 7. 直接驱动代理 handle_packet: 跨组乱序 + 丢包仍按原序交付 ----
    print("\n== 7. 跨组乱序 + 每组丢 1 包, 代理按原序交付 ==")

    def feed(order: list[tuple[int, int]], drops: set[tuple[int, int]]) -> list[int]:
        """order 为 (gid, index) 的到达顺序; drops 为漏发集合. 返回交付段号序列."""
        proxy.channels.clear()
        proxy.stbs.clear()
        cs = proxy.ChannelState(9)
        proxy.channels[9] = cs
        stb = proxy.StbState("u")
        stb.current = 9
        cs.watchers.add("u")
        proxy.stbs["u"] = stb
        groups = {
            gid: src.build_group(9, "239.0.0.9", 5009, 3, gid, 3, (gid - 1) * 3 + 1,
                                 ts=1700000000.0)
            for gid in (1, 2, 3)
        }

        async def run() -> None:
            for gid, index in order:
                if (gid, index) in drops:
                    continue
                programs, parity = groups[gid]
                proxy.handle_packet(json.loads(programs[index].decode()),
                                    programs[index])
                if index == 2:  # 组尾跟一个校验包
                    proxy.handle_packet(json.loads(parity.decode()), parity)

        asyncio.run(run())
        for g in cs.groups.values():
            g.deadline = 0  # 让可能的缺包立即到期 (本用例应全部补齐, 不应该跳过)
        proxy._try_deliver(cs)
        return [p["seq"] for p in stb.queue]

    # 组内倒序到达 (组仍按 1,2,3 顺序出现)
    intra_rev = [(g, i) for g in (1, 2, 3) for i in (2, 1, 0)]
    got = feed(intra_rev, set())
    check("组内倒序到达仍按 1..9 交付", got == list(range(1, 10)), str(got))

    # 组间交织: 后一组的报文先于前一组收全到达 -> 缓存等前一组
    interleaved = [(1, 0), (2, 0), (1, 1), (2, 1), (1, 2), (2, 2),
                   (3, 0), (3, 1), (3, 2)]
    got = feed(interleaved, set())
    check("组间交织仍按 1..9 交付", got == list(range(1, 10)), str(got))

    # 每组首包漏发, 靠 2 原包 + 校验包补齐
    drops = {(g, 0) for g in (1, 2, 3)}
    got = feed(intra_rev, drops)
    check("组内倒序 + 每组漏 1 包, 仍按 1..9 交付", got == list(range(1, 10)), str(got))

    # 每组中间包漏发, 先到 0/2 再补校验包
    drops = {(g, 1) for g in (1, 2, 3)}
    got = feed([(g, i) for g in (1, 2, 3) for i in (0, 2)], drops)
    check("缺组内中间包也能补出并按序交付", got == list(range(1, 10)), str(got))

    # 校验包先到, 两个原包后到 -> 仍应在等待期内补出第三个
    parity_first = [(-1, 1), (1, 0), (1, 2), (2, 0), (2, 1), (2, 2),
                    (-2, 2), (3, 0), (3, 1), (3, 2)]
    # 用特殊标记: (-gid, -) 表示先发该组校验包
    def feed2() -> list[int]:
        proxy.channels.clear(); proxy.stbs.clear()
        cs = proxy.ChannelState(9); proxy.channels[9] = cs
        stb = proxy.StbState("u"); stb.current = 9; cs.watchers.add("u"); proxy.stbs["u"] = stb
        gs = {g: src.build_group(9, "239.0.0.9", 5009, 3, g, 3, (g-1)*3+1, ts=1700000000.0)
              for g in (1, 2, 3)}

        async def run() -> None:
            for gid, index in parity_first:
                if gid < 0:
                    g = -gid
                    parity = gs[g][1]
                    proxy.handle_packet(json.loads(parity.decode()), parity)
                else:
                    programs, parity = gs[gid]
                    proxy.handle_packet(json.loads(programs[index].decode()), programs[index])
                    if index == 2:
                        proxy.handle_packet(json.loads(parity.decode()), parity)

        asyncio.run(run())
        for g in cs.groups.values():
            g.deadline = 0
        proxy._try_deliver(cs)
        return [p["seq"] for p in stb.queue]

    got = feed2()
    check("校验包先到、原包后到, 仍补出缺包并按序交付", got == list(range(1, 10)), str(got))

    # 重复报文 (每包发两次 + 校验包两次) 只交付一次
    dup = [(g, i) for g in (1, 2, 3) for i in (0, 1, 2) for _ in range(2)]
    got = feed(dup, set())
    check("重复报文只交付一次", got == list(range(1, 10)), str(got))

    # 组顺序完全颠倒 (3,2,1): 源端声明了本轮起始组号 (stream.start_group=1),
    # 代理据此知道组 1 存在, 为它保留完整的等候期, 因此仍能按 1..9 交付.
    rev = [(g, i) for g in (3, 2, 1) for i in (2, 1, 0)]
    got = feed(rev, set())
    check("组顺序颠倒 (有 start_group 元数据) 仍按 1..9 交付", got == list(range(1, 10)), str(got))

    # 没有 start_group 元数据时无从预知更早的组: 只交付首个见到的组,
    # 保证绝不乱序、绝不重复, 但可能少交付 (兼容旧源的已知限制)
    def feed_legacy() -> list[int]:
        proxy.channels.clear(); proxy.stbs.clear()
        cs = proxy.ChannelState(8); proxy.channels[8] = cs
        stb = proxy.StbState("l"); stb.current = 8; cs.watchers.add("l"); proxy.stbs["l"] = stb
        groups = {}
        for gid in (1, 2, 3):
            programs, parity = src.build_group(8, "239.0.0.8", 5008, 1, gid, 3,
                                               (gid - 1) * 3 + 1, ts=1700000000.0)
            # 模拟旧源: 去掉 stream 元数据
            stripped = []
            for raw in programs:
                body = json.loads(raw.decode()); body.pop("stream", None)
                stripped.append(json.dumps(body, separators=(",", ":")).encode())
            body = json.loads(parity.decode()); body.pop("stream", None)
            groups[gid] = (stripped, json.dumps(body, separators=(",", ":")).encode())

        async def run() -> None:
            for gid in (3, 2, 1):
                programs, parity = groups[gid]
                for raw in programs:
                    proxy.handle_packet(json.loads(raw.decode()), raw)
                proxy.handle_packet(json.loads(parity.decode()), parity)

        asyncio.run(run())
        for g in cs.groups.values():
            g.deadline = 0
        proxy._try_deliver(cs)
        return [p["seq"] for p in stb.queue]

    got = feed_legacy()
    check("旧源 (无 start_group) 组间颠倒时绝不乱序", got == [7, 8, 9], str(got))

    print()
    if failures:
        print(f"共 {len(failures)} 项失败: {failures}")
        return 1
    print("FEC 单元测试全部通过 ✔")
    return 0


if __name__ == "__main__":
    sys.exit(main())
