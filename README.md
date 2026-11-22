# 园区电视组播换台 API

FastAPI 实现的园区电视组播换台代理，配套一个本地模拟节目源（可延迟发出首包、可拒绝加入）。
机顶盒（STB）按频道号选台、退订，并读取代理转发的节目包。

## 架构

```
机顶盒 --HTTP--> 代理 proxy:8000 --HTTP(join/leave)--> 模拟节目源 source:9000
机顶盒 <--HTTP读包-- 代理 <--UDP 节目包(模拟组播转发)-- 节目源
```

- `source/`：模拟节目源。控制面 HTTP（join/leave/config/state），数据面 UDP 周期性发节目包。
  每个频道模拟一个组播组 `239.0.0.<频道号>`，可配置首包延迟 `delay`、拒绝加入 `refuse`、发包间隔 `interval`。
- `proxy/`：换台代理（演示入口）。维护机顶盒状态与每频道一路上游订阅，收到节目包后分发给收看中的机顶盒。
- `tests/selftest.py`：端到端自测。

## 启动（Docker Compose）

```bash
docker compose up --build
```

- 机顶盒 API 演示入口： http://localhost:8000/docs （Swagger UI，根路径 `/` 会重定向到这里）
- 节目源控制面： http://localhost:9000/docs

本机无 Docker 时也可裸跑（Python 3.11+，需两个进程）：

```bash
pip install -r source/requirements.txt -r proxy/requirements.txt
cd source && PROXY_HOST=127.0.0.1 PROXY_UDP_PORT=5000 uvicorn app:app --port 9000 &
cd proxy && SOURCE_URL=http://127.0.0.1:9000 uvicorn app:app --port 8000 &
```

## 在 /docs 演示换台

1. `POST /stbs/stb-1/select` 请求体 `{"channel": 1}` → 返回 `202 pending_first_packet`（等待首包）。
2. `GET /stbs/stb-1/status` → 首包到达后 `current_channel` 变为 `1`。
3. `GET /stbs/stb-1/packets` → 读取代理转发的节目包（读取即清空）。
4. 在源侧 `POST /config/2` 设置 `{"delay": 3}` 后选台频道 2，可观察"等待首包"期间旧台照常收包；
   设置 `{"refuse": true}` 可模拟加入失败（机顶盒停留在旧台，`last_error` 可见原因）。
5. `POST /stbs/stb-1/unsubscribe` 退订；`GET /state` 可查看代理内部状态（每频道的收看者/等待者/上游订阅）。

## 核心行为（实现要点）

- **同一频道只向上游订阅一路**：收看者与等待首包者共享引用计数；`join` 幂等，进行中的加入不会重复发起。
- **无人收看且无人等候首包时退订**：引用计数归零即向上游 `leave`；加入请求在途时等其完成后立即补发 `leave`，避免 join/leave 乱序。
- **收到新频道首包才切换**：选台只把机顶盒挂入目标频道的等待集合；UDP 首包到达时才迁移收看关系并分发该包。
- **加入失败继续收旧台**：上游拒绝/不可达时，等待中的机顶盒保持 `current_channel` 不变，`last_error` 记录原因。
- **等待中可改选/退订**：每次选台/退订递增机顶盒 `generation` 并把旧请求移出等待集合。
- **已取消的选台不因迟到首包生效**：首包到达时校验 `generation` 与 `pending_channel`，不一致直接丢弃该等待项。

## 自测

```bash
python3 tests/selftest.py    # 需先启动 proxy 与 source
```

覆盖 7 个场景共 42 项断言：两台共用频道（一路订阅/分别退订）、延迟首包期间不切换、
等待中改选且迟到首包不生效、等待中退订、加入失败保旧台、两台同时等同一频道首包、收尾无残留订阅。

## 验证结果

- 本沙箱无 Docker，`docker compose` 未实际运行；以裸进程方式启动同一套代码完成等价验证。
- `tests/selftest.py` 全部 42 项断言通过；`/docs`（proxy:8000 与 source:9000）均返回 200，
  并手动走通"选台→状态→读包→加入失败→退订"演示流程。

## 已知限制 / 未完成项

- 数据面用"UDP 单播转发"模拟组播，控制面用 HTTP 模拟 IGMP；接真实组播时需替换为
  `IP_ADD_MEMBERSHIP` 组播 socket 与 IGMP 交互。
- 机顶盒读包为轮询模式，队列有界（默认 200，溢出丢最旧）；可扩展 WebSocket/SSE 推送。
- 状态全在内存，单 worker 运行；代理重启即丢失，无鉴权、无持久化、无多副本一致性。
- 上游 `leave` 失败（源不可达）不重试，源侧可能短暂残留发包，代理会直接丢弃未订阅频道的包。
