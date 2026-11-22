"""端到端自测: 园区电视组播换台代理 + 模拟节目源.

前置: 代理 (默认 http://127.0.0.1:8000) 与模拟源 (默认 http://127.0.0.1:9000) 已启动.

覆盖场景:
  1. 两台机顶盒共用同一频道 -> 上游只一路订阅; 都能收包; 全部退订后上游退订
  2. 首包延迟期间不切换, 首包到达才切换
  3. 等待首包时改选 -> 已取消的选台不因迟到的首包生效
  4. 等待首包时退订 -> 上游不再发该频道包
  5. 加入失败 -> 继续收旧台
  6. 两台同时等待同一频道首包 -> 仍只一路订阅, 首包到达同时切换
  7. 结束时不存在残留的上游订阅
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


def wait_until(fn, timeout: float = 6.0, interval: float = 0.1) -> bool:
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


def unsubscribe(sid: str) -> dict:
    r = httpx.post(f"{PROXY}/stbs/{sid}/unsubscribe", timeout=5)
    r.raise_for_status()
    return r.json()


def status(sid: str) -> dict:
    return httpx.get(f"{PROXY}/stbs/{sid}/status", timeout=5).json()


def drain(sid: str) -> list[dict]:
    return httpx.get(f"{PROXY}/stbs/{sid}/packets", params={"limit": 500}, timeout=5).json()["packets"]


def source_state() -> dict:
    return httpx.get(f"{SOURCE}/state", timeout=5).json()


def source_config(channel: int, **kw) -> None:
    httpx.post(f"{SOURCE}/config/{channel}", json=kw, timeout=5).raise_for_status()


def main() -> int:
    httpx.post(f"{SOURCE}/reset", timeout=5)
    httpx.post(f"{PROXY}/reset", timeout=5)

    # ---------------- 场景 1: 两台共用频道 ----------------
    print("\n== 场景 1: 两台机顶盒共用同一频道 ==")
    select("stb-a", 1)
    select("stb-b", 1)
    check("stb-a 切换到频道 1", wait_until(lambda: status("stb-a")["current_channel"] == 1))
    check("stb-b 切换到频道 1", wait_until(lambda: status("stb-b")["current_channel"] == 1))
    st = source_state()["1"]
    check("上游对频道 1 只有一路订阅", st["joins"] == 1 and st["subscribed"], str(st))
    time.sleep(1.2)
    pa, pb = drain("stb-a"), drain("stb-b")
    check("stb-a 收到频道 1 节目包", any(p["channel"] == 1 for p in pa))
    check("stb-b 收到频道 1 节目包", any(p["channel"] == 1 for p in pb))
    unsubscribe("stb-a")
    time.sleep(0.3)
    check("stb-a 退订后上游仍保留订阅 (stb-b 在看)", source_state()["1"]["subscribed"])
    unsubscribe("stb-b")
    check("无人收看后上游退订频道 1",
          wait_until(lambda: not source_state()["1"]["subscribed"]))
    check("上游只收到一次 leave", source_state()["1"]["leaves"] == 1, str(source_state()["1"]))

    # ---------------- 场景 2: 首包延迟, 到包才切换 ----------------
    print("\n== 场景 2: 延迟首包, 收到首包才切换 ==")
    source_config(2, delay=2.0, interval=3.0)
    select("stb-d", 2)
    s = status("stb-d")
    check("选台后进入等待首包状态", s["pending_channel"] == 2 and s["current_channel"] is None, str(s))
    time.sleep(1.0)
    s = status("stb-d")
    check("首包未到不切换", s["current_channel"] is None and s["pending_channel"] == 2, str(s))
    check("首包到达后切换到频道 2",
          wait_until(lambda: status("stb-d")["current_channel"] == 2, timeout=4.0))

    # ---------------- 场景 3: 等待中改选, 迟到首包不生效 ----------------
    print("\n== 场景 3: 等待首包时改选, 已取消的选台不因迟到首包生效 ==")
    select("stb-c", 1)
    wait_until(lambda: status("stb-c")["current_channel"] == 1)
    # 给旧台(1)留出至少一个包, 这样切换后必然能读到"旧台 + 新台"两类包;
    # 否则旧台包在选台前那几毫秒是否恰好到达不确定 (原实现偶发失败).
    time.sleep(1.2)
    drain("stb-c")
    joins_before = source_state()["2"]["joins"]
    select("stb-c", 2)  # 频道 2 已在播 (stb-d), 无需新订阅
    s = status("stb-c")
    check("改选频道 2 进入等待首包", s["pending_channel"] == 2, str(s))
    select("stb-c", 3)  # 等待中再次改选 -> 取消对频道 2 的请求
    s = status("stb-c")
    # 频道 3 无首包延迟, 查询时可能已切换; 关键是绝不在频道 2 上
    check("等待中改选频道 3 (未落到频道 2)",
          (s["pending_channel"] == 3 or s["current_channel"] == 3)
          and s["current_channel"] != 2, str(s))
    check("频道 2 未发起新的上游订阅", source_state()["2"]["joins"] == joins_before)
    check("stb-c 切换到频道 3", wait_until(lambda: status("stb-c")["current_channel"] == 3, timeout=3.0))
    # 等频道 2 的下一包 (interval=3s) 到达代理: 若已取消的请求生效, stb-c 会被切到 2
    check("频道 2 的迟到首包已到达代理",
          wait_until(lambda: source_state()["2"]["seq"] >= 2, timeout=6.0))
    time.sleep(0.3)
    s = status("stb-c")
    check("迟到的频道 2 首包未使已取消的选台生效", s["current_channel"] == 3, str(s))
    pkts = drain("stb-c")
    chans = {p["channel"] for p in pkts}
    check("stb-c 只收到旧台(1)与新台(3)的包, 无频道 2 的包",
          2 not in chans and 1 in chans and 3 in chans, f"channels={chans}")
    check("stb-d 仍在收看频道 2", status("stb-d")["current_channel"] == 2)
    check("stb-d 持续收到频道 2 的包", any(p["channel"] == 2 for p in drain("stb-d")))

    # ---------------- 场景 4: 等待首包时退订 ----------------
    print("\n== 场景 4: 等待首包时退订 ==")
    source_config(4, delay=2.0)
    select("stb-e", 1)
    wait_until(lambda: status("stb-e")["current_channel"] == 1)
    select("stb-e", 4)
    s = status("stb-e")
    check("选台频道 4 进入等待首包", s["pending_channel"] == 4, str(s))
    unsubscribe("stb-e")
    s = status("stb-e")
    check("等待中退订成功", s["current_channel"] is None and s["pending_channel"] is None, str(s))
    check("上游已退订频道 4",
          wait_until(lambda: source_state()["4"]["leaves"] >= 1 and not source_state()["4"]["subscribed"]))
    time.sleep(2.2)  # 超过首包延迟
    st = source_state()["4"]
    check("频道 4 首包始终未发出", st["seq"] == 0, str(st))
    check("stb-e 未收到频道 4 的包", not any(p["channel"] == 4 for p in drain("stb-e")))

    # ---------------- 场景 5: 加入失败继续收旧台 ----------------
    print("\n== 场景 5: 加入被拒绝, 继续收旧台 ==")
    source_config(9, refuse=True)
    select("stb-f", 1)
    wait_until(lambda: status("stb-f")["current_channel"] == 1)
    drain("stb-f")
    select("stb-f", 9)
    check("选台失败已回报",
          wait_until(lambda: status("stb-f")["last_error"] is not None, timeout=4.0))
    s = status("stb-f")
    check("加入失败后仍停留在旧台", s["current_channel"] == 1 and s["pending_channel"] is None, str(s))
    check("错误信息可见", "failed" in (s["last_error"] or ""), str(s["last_error"]))
    time.sleep(1.2)
    pkts = drain("stb-f")
    check("继续收到旧台(频道 1)的包", any(p["channel"] == 1 for p in pkts))
    check("未收到频道 9 的包", not any(p["channel"] == 9 for p in pkts))
    st = source_state()["9"]
    check("上游未建立频道 9 订阅", not st["subscribed"] and st["joins"] == 0, str(st))

    # ---------------- 场景 6: 两台同时等同一频道首包 ----------------
    print("\n== 场景 6: 两台同时等待同一频道首包, 仍只一路订阅 ==")
    source_config(5, delay=1.0)
    select("stb-g", 5)
    select("stb-h", 5)
    time.sleep(0.3)
    st = source_state()["5"]
    check("等待期间上游只有一路订阅", st["joins"] == 1, str(st))
    check("stb-g 切换到频道 5", wait_until(lambda: status("stb-g")["current_channel"] == 5, timeout=4.0))
    check("stb-h 切换到频道 5", wait_until(lambda: status("stb-h")["current_channel"] == 5, timeout=4.0))
    time.sleep(1.2)
    check("stb-g 收到频道 5 的包", any(p["channel"] == 5 for p in drain("stb-g")))
    check("stb-h 收到频道 5 的包", any(p["channel"] == 5 for p in drain("stb-h")))
    unsubscribe("stb-g")
    unsubscribe("stb-h")
    check("全部退订后上游退订频道 5",
          wait_until(lambda: not source_state()["5"]["subscribed"]))

    # ---------------- 场景 7: 无残留订阅 ----------------
    print("\n== 场景 7: 收尾检查 ==")
    for sid in ("stb-c", "stb-d", "stb-e", "stb-f"):
        unsubscribe(sid)
    check("所有频道上游均已退订",
          wait_until(lambda: all(not c["subscribed"] for c in source_state().values()), timeout=4.0))

    print()
    if failures:
        print(f"共 {len(failures)} 项失败: {failures}")
        return 1
    print("全部自测通过 ✔")
    return 0


if __name__ == "__main__":
    sys.exit(main())
