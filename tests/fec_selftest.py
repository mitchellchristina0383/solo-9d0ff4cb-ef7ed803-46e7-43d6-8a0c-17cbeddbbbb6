"""FEC 补包端到端自测: 模拟节目源每组漏发 + 代理按序补包/超时续播.

前置: 代理 (默认 http://127.0.0.1:8000) 与模拟源 (默认 http://127.0.0.1:9000) 已启动,
并且代理以默认 FEC_WAIT_SECONDS=2 运行 (超时场景至少需要源端 interval 配合).

覆盖场景:
  1. 每组漏发 1 个原包 + 组内乱序 -> 代理收到任意 2 原包+校验包即补出第三个,
     交付顺序与原始段号一致, 内容与源端原始报文一致, 每包只交付一次
  2. 每组漏发 2 个原包 (不可还原) -> 等候期内不交付, 到期跳过缺包, 其余照常交付
  3. 首包换台只认准备入队的节目包: 只有校验包/整组全漏时不切换
  4. 源复位 -> 代理收到新一轮首报即清空旧轮未入队数据, 之后忽略旧轮报文
  5. 两台机顶盒共用一路订阅时 FEC 交付一致
  6. 收尾: 无残留订阅

期望报文内容由源端算法独立重算 (不读代理内部状态): payload = program-<ch>-segment-<seq>,
代理补出的包应与源端本来会发出的报文逐字段一致 (ts 除外, 由源生成, 代理原样搬运).
"""

from __future__ import annotations

import os
import sys
import time

import httpx

PROXY = os.getenv("PROXY_URL", "http://127.0.0.1:8000")
SOURCE = os.getenv("SOURCE_URL", "http://127.0.0.1:9000")

failures: list[str] = []


def check(name: str, cond: bool, detail: str = "") -> None:
    print(f"[{'PASS' if cond else 'FAIL'}] {name}" + (f"  -- {detail}" if detail and not cond else ""))
    if not cond:
        failures.append(name)


def wait_until(fn, timeout: float = 6.0, interval: float = 0.05) -> bool:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            if fn():
                return True
        except httpx.HTTPError:
            pass
        time.sleep(interval)
    return False


def select(sid: str, channel: int) -> dict:
    r = httpx.post(f"{PROXY}/stbs/{sid}/select", json={"channel": channel}, timeout=5)
    r.raise_for_status()
    return r.json()


def unsubscribe(sid: str) -> None:
    httpx.post(f"{PROXY}/stbs/{sid}/unsubscribe", timeout=5)


def status(sid: str) -> dict:
    return httpx.get(f"{PROXY}/stbs/{sid}/status", timeout=5).json()


def drain(sid: str) -> list[dict]:
    return httpx.get(f"{PROXY}/stbs/{sid}/packets", params={"limit": 500}, timeout=5).json()["packets"]


def proxy_state() -> dict:
    return httpx.get(f"{PROXY}/state", timeout=5).json()


def source_state() -> dict:
    return httpx.get(f"{SOURCE}/state", timeout=5).json()


def source_config(channel: int, **kw) -> None:
    httpx.post(f"{SOURCE}/config/{channel}", json=kw, timeout=5).raise_for_status()


def wait_source_seq(channel: int, seq: int, timeout: float = 8.0) -> bool:
    return wait_until(lambda: source_state()[str(channel)]["seq"] >= seq, timeout=timeout)


def reset_all() -> None:
    httpx.post(f"{SOURCE}/reset", timeout=5)
    httpx.post(f"{PROXY}/reset", timeout=5)


def by_seq(pkts: list[dict]) -> dict[int, dict]:
    return {p["seq"]: p for p in pkts}


def assert_contiguous_ordered(name: str, pkts: list[dict], channel: int,
                              start: int | None = None) -> None:
    """段号严格 +1 递增, 无重复, 每包内容与源端应有的内容一致."""
    seqs = [p["seq"] for p in pkts]
    expected_start = seqs[0] if start is None else start
    check(f"{name}: 没有重复包", len(seqs) == len(set(seqs)), f"seqs={seqs}")
    check(f"{name}: 段号严格连续且按原序", seqs == list(range(expected_start, expected_start + len(seqs))),
          f"seqs={seqs}")
    bad = [
        p["seq"]
        for p in pkts
        if p["channel"] != channel
        or p["payload"] != f"program-{channel}-segment-{p['seq']}"
    ]
    check(f"{name}: 每个包内容与源端一致", not bad, f"bad={bad[:5]}")


def main() -> int:
    reset_all()

    # ---------------- 场景 1: 乱序 + 每组漏 1 包, 全部可补 ----------------
    print("\n== 场景 1: 组内乱序 + 每组漏发 1 个原包, 校验包补齐 ==")
    ch = 1
    source_config(ch, interval=0.05, fec_group_size=3, drop_count=1, reorder=True)
    select("stb-a", ch)
    check("stb-a 切到频道 1", wait_until(lambda: status("stb-a")["current_channel"] == ch))
    # 至少跑 5 个完整组 (15 个原包)
    check("源端已发出至少 5 组", wait_source_seq(ch, 15))
    time.sleep(0.35)  # 留出最后一组的校验包与交付时间
    pkts = drain("stb-a")
    assert_contiguous_ordered("场景1", pkts, ch, start=1)
    check("场景1: 至少交付 5 个完整组, 且只交付完整组", len(pkts) >= 15 and len(pkts) % 3 == 0,
          f"got={len(pkts)} src={source_state()['1']['seq']}")
    recovered = [p for p in pkts if p.get("recovered")]
    check("场景1: 每组恰好补出 1 个包, 均为该组首个原包",
          all(p["fec"]["index"] == 0 for p in recovered) and len(recovered) == len(pkts) // 3,
          f"recovered={[(p['seq'], p['fec']['index']) for p in recovered]}")
    stats = proxy_state()["channels"]["1"]["fec"]
    check("场景1: 未发生跳过", stats["skipped"] == 0 and stats["skipped_groups"] == 0, str(stats))

    # 每包只交一次: 再等两轮, 已交付段号不应再次出现
    before = len(pkts)
    time.sleep(0.5)
    more = drain("stb-a")
    check("场景1: 后续只交付新包, 旧包不重复",
          all(p["seq"] > pkts[-1]["seq"] for p in more), f"more={[p['seq'] for p in more][:10]}")
    check("场景1: 新包同样连续", [p["seq"] for p in more] == list(range(pkts[-1]["seq"] + 1,
                                                                  pkts[-1]["seq"] + 1 + len(more))))
    unsubscribe("stb-a")

    # ---------------- 场景 2: 每组漏 2 包 -> 超时跳过, 其余照常交付 ----------------
    print("\n== 场景 2: 每组漏发 2 个原包 (不可还原), 到期跳过缺包 ==")
    ch = 2
    source_config(ch, interval=0.3, fec_group_size=3, drop_count=2)
    select("stb-b", ch)
    first_group_at = time.time()
    # 等候期内 (从首个报文起 <2s) 不应交付任何包, 也不应切换
    time.sleep(1.2)
    check("场景2: 等候期内不交付缺包",
          drain("stb-b") == [] and status("stb-b")["current_channel"] is None,
          str(status("stb-b")))

    # 轮询直到看到 index 2; drain 会清队列, 期间读走的包保留在 collected
    collected: list[dict] = []

    def poll_third() -> bool:
        collected.extend(drain("stb-b"))
        return any(p["fec"]["index"] == 2 for p in collected)

    ok = wait_until(poll_third, timeout=6.0)
    elapsed_after_first = time.time() - first_group_at
    check("场景2: 首个可交付包在约 2s 等候后交付", ok and elapsed_after_first >= 1.8,
          f"ok={ok} elapsed={elapsed_after_first:.2f}s")
    if not ok:
        collected.extend(drain("stb-b"))
    idx2 = [p for p in collected if p["fec"]["index"] == 2]
    check("场景2: 只交付每组最后一个原包, 缺包被跳过",
          all(p["fec"]["index"] == 2 for p in collected) and len(idx2) >= 1,
          f"indices={[(p['seq'], p['fec']['index']) for p in collected]}")
    check("场景2: 交付的段号严格递增且唯一",
          [p["seq"] for p in collected] == sorted({p["seq"] for p in collected}),
          f"seqs={[p['seq'] for p in collected]}")
    stats = proxy_state()["channels"]["2"]["fec"]
    check("场景2: 统计到跳过包", stats["skipped"] >= 1, str(stats))
    unsubscribe("stb-b")

    # ---------------- 场景 3: 首包换台只认准备入队的节目包 ----------------
    print("\n== 场景 3: 首包换台只认准备入队的节目包 ==")
    ch = 3
    # 整组 3 个原包全漏, 只有校验包 -> 无法还原, 不应触发切换, 也不应入队
    source_config(ch, interval=0.1, fec_group_size=3, drop_count=3)
    select("stb-c", ch)
    time.sleep(0.55)
    s = status("stb-c")
    check("场景3: 只收到校验包时保持等待首包",
          s["current_channel"] is None and s["pending_channel"] == ch, str(s))
    check("场景3: 等待期间队列为空", not drain("stb-c"))
    # 改成可还原配置后的新组会正常触发切换
    source_config(ch, drop_count=1)
    check("场景3: 首个可交付节目包到达才切换",
          wait_until(lambda: status("stb-c")["current_channel"] == ch, timeout=6.0))
    time.sleep(0.35)
    pkts = drain("stb-c")
    # 整组全漏期间不可能交付任何包; 切换后从某个可还原的组开始, 且整组连续交付
    seqs = [p["seq"] for p in pkts]
    check("场景3: 整组全漏期间无包交付 (从可还原的组开始)",
          bool(pkts) and all(p["seq"] > 9 for p in pkts),
          f"seqs={seqs[:10]}")
    check("场景3: 切换后各完整组连续交付",
          seqs == list(range(seqs[0], seqs[0] + len(seqs))) if seqs else False,
          f"seqs={seqs[:10]}")
    unsubscribe("stb-c")

    # ---------------- 场景 4: 源复位 -> 清旧轮未入队数据, 忽略旧轮 ----------------
    print("\n== 场景 4: 源复位后清掉旧轮未入队数据, 忽略旧轮报文 ==")
    ch = 4
    source_config(ch, interval=0.5, fec_group_size=3, drop_count=0)
    select("stb-d", ch)
    check("stb-d 切到频道 4", wait_until(lambda: status("stb-d")["current_channel"] == ch))
    # 让代理缓存一些尚未交付的组: 把间隔拉长后读包, 再等半个组
    time.sleep(0.2)
    drain("stb-d")
    old_round = source_state()["4"]["round"]
    httpx.post(f"{SOURCE}/restart/{ch}", timeout=5).raise_for_status()
    check("源端已进入新一轮", source_state()["4"]["round"] == old_round + 1)

    # 复位时在途的旧轮报文可能仍会交付; 一旦看到新一轮, 之后不应再出现旧轮
    seen_new = False
    old_after_new: list[int] = []
    new_pkts: list[dict] = []
    deadline = time.time() + 6.0
    while time.time() < deadline:
        batch = drain("stb-d")
        for p in batch:
            if p["round"] == old_round + 1:
                seen_new = True
                new_pkts.append(p)
            elif seen_new:
                old_after_new.append(p["round"])
        if seen_new and len(new_pkts) >= 6:
            break
        time.sleep(0.05)
    stats = proxy_state()["channels"]["4"]["fec"]
    check("场景4: 复位时丢弃了旧轮尚未入队的组", stats["discarded_groups"] >= 1, str(stats))
    check("场景4: 看到新一轮报文", seen_new, str(new_pkts[:3]))
    check("场景4: 新一轮从段号 1 开始", bool(new_pkts) and new_pkts[0]["seq"] == 1,
          str([(p["round"], p["seq"]) for p in new_pkts[:6]]))
    check("场景4: 看到新一轮后不再出现旧轮报文", not old_after_new, str(old_after_new[:10]))
    check("场景4: 新一轮段号连续且按序",
          [p["seq"] for p in new_pkts] == list(range(1, 1 + len(new_pkts))),
          f"seqs={[p['seq'] for p in new_pkts][:10]}")
    stats = proxy_state()["channels"]["4"]["fec"]
    check("场景4: 代理识别到新一轮", stats["rounds"] == 2, str(stats))
    unsubscribe("stb-d")

    # ---------------- 场景 5: 两台共用订阅时交付一致 ----------------
    print("\n== 场景 5: 两台机顶盒共用一路订阅, FEC 交付一致 ==")
    ch = 5
    reset_all()
    source_config(ch, interval=0.05, fec_group_size=3, drop_count=1, reorder=True)
    select("stb-e", ch)
    select("stb-f", ch)
    check("两台都切到频道 5",
          wait_until(lambda: status("stb-e")["current_channel"] == ch
                     and status("stb-f")["current_channel"] == ch))
    # 两台可能在不同时刻切过来 (先切的那台已收到更早的包): 清空后重新比较共同段号
    drain("stb-e")
    drain("stb-f")
    time.sleep(0.6)
    pe, pf = drain("stb-e"), drain("stb-f")
    check("场景5: 上游只一路订阅", source_state()["5"]["joins"] == 1, str(source_state()["5"]))
    me = {p["seq"]: p for p in pe}
    mf = {p["seq"]: p for p in pf}
    common = sorted(set(me) & set(mf))
    check("场景5: 两台有足够多的共同段号", len(common) >= 6,
          f"e={sorted(me)[:5]}.. f={sorted(mf)[:5]}.. common={len(common)}")
    check("场景5: 共同段号上补包标记一致",
          all(me[s].get("recovered", False) == mf[s].get("recovered", False)
              for s in common),
          f"diff={[s for s in common if me[s].get('recovered', False) != mf[s].get('recovered', False)]}")
    check("场景5: 每台自己的段号连续且无重复",
          sorted(me) == list(range(min(me), min(me) + len(me)))
          and sorted(mf) == list(range(min(mf), min(mf) + len(mf))) if common else False,
          f"e={sorted(me)[:10]} f={sorted(mf)[:10]}")
    # 校验每个包内容
    assert_contiguous_ordered("场景5", pe, ch)
    unsubscribe("stb-e")
    unsubscribe("stb-f")

    # ---------------- 场景 6: 收尾 ----------------
    print("\n== 场景 6: 收尾检查 ==")
    check("所有频道上游均已退订",
          wait_until(lambda: all(not c["subscribed"] for c in source_state().values()), timeout=4.0))
    check("无残留定时器/缓存 (轮转清空后 buffered_groups 为空)",
          all(not c["fec"]["buffered_groups"] for c in proxy_state()["channels"].values()),
          str({k: v["fec"]["buffered_groups"] for k, v in proxy_state()["channels"].items()}))

    print()
    if failures:
        print(f"共 {len(failures)} 项失败: {failures}")
        return 1
    print("FEC 自测全部通过 ✔")
    return 0


if __name__ == "__main__":
    sys.exit(main())
