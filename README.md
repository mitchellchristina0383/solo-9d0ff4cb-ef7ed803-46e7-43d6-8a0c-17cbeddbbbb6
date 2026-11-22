# 园区电视组播换台 API

FastAPI 实现的园区电视组播换台代理，配套一个本地模拟节目源（可延迟发出首包、可拒绝加入、
可按组漏发原包并模拟乱序）。机顶盒（STB）按频道号选台、退订，并读取代理转发的节目包。
数据面带 XOR FEC：源每 3 个连续节目包加发 1 个校验包，代理在丢包时补包并保证按原序交付。

## 架构

```
机顶盒 --HTTP--> 代理 proxy:8000 --HTTP(join/leave)--> 模拟节目源 source:9000
机顶盒 <--HTTP读包-- 代理 <--UDP 节目包/校验包(模拟组播转发)-- 节目源
```

- `source/`：模拟节目源。控制面 HTTP（join/leave/config/restart/state），数据面 UDP 周期性发节目包。
  每个频道模拟一个组播组 `239.0.0.<频道号>`，可配置首包延迟 `delay`、拒绝加入 `refuse`、
  发包间隔 `interval`、每组原包数 `fec_group_size`、每组漏发原包数 `drop_count`、组内乱序 `reorder`。
- `proxy/`：换台代理（演示入口）。维护机顶盒状态与每频道一路上游订阅，收到节目包后
  按 FEC 缓存/补包/排序，再分发给收看中的机顶盒。
- `tests/selftest.py`：换台行为端到端自测（原有）。
- `tests/fec_selftest.py`：FEC 补包/超时/复位端到端自测。
- `tests/fec_unit.py`：校验组构造与补包的纯函数单元测试（不发网络报文）。

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

## 在 /docs 演示换台与补包

1. `POST /stbs/stb-1/select` 请求体 `{"channel": 1}` → 返回 `202 pending_first_packet`（等待首包）。
2. `GET /stbs/stb-1/status` → 首包到达后 `current_channel` 变为 `1`。
3. `GET /stbs/stb-1/packets` → 读取代理转发的节目包（读取即清空）。
4. 在源侧 `POST /config/2` 设置 `{"delay": 3}` 后选台频道 2，可观察"等待首包"期间旧台照常收包；
   设置 `{"refuse": true}` 可模拟加入失败（机顶盒停留在旧台，`last_error` 可见原因）。
5. `POST /stbs/stb-1/unsubscribe` 退订；`GET /state` 可查看代理内部状态（每频道的收看者/等待者/上游订阅）。

### 演示丢包补包（源侧 /docs）

1. 先让频道在播（第 1–2 步）。
2. `POST /config/1`，请求体设置每组漏发的原包数，例如：

   ```json
   {"drop_count": 1, "reorder": true, "interval": 0.5}
   ```

   - `drop_count: 1`：每组 3 个原包只发 2 个，另加 1 个校验包（代理可补出第 3 个）。
   - `drop_count: 2`：每组只发 1 个原包 → 无法补包，代理等满 2 秒后跳过缺包，其余照常交付。
   - `drop_count: 3`：整组原包全漏 → 只有校验包，不会触发首包换台，也不会入队。
   - 也可用 `POST /config/1` 的 `fec_group_size` 调整每组原包数（2–32）。
   - `POST /restart/1`：频道数据面复位（段号归零、轮次 +1），可观察代理清掉旧轮未入队数据。
3. 回代理侧 `GET /stbs/stb-1/packets`：
   - 包内 `fec.group / fec.index / fec.size` 可见校验组信息；补出的包带 `"recovered": true`。
   - `seq` 连续递增即表示按原序交付且每包只交一次。
   - `GET /state` 的 `channels.<n>.fec` 可看到 `delivered / recovered / skipped / rounds` 等统计。

## 核心行为（实现要点）

### 换台

- **同一频道只向上游订阅一路**：收看者与等待首包者共享引用计数；`join` 幂等，进行中的加入不会重复发起。
- **无人收看且无人等候首包时退订**：引用计数归零即向上游 `leave`；加入请求在途时等其完成后立即补发 `leave`，避免 join/leave 乱序。
- **收到新频道首包才切换**：选台只把机顶盒挂入目标频道的等待集合；UDP 首包到达时才迁移收看关系并分发该包。
- **加入失败继续收旧台**：上游拒绝/不可达时，等待中的机顶盒保持 `current_channel` 不变，`last_error` 记录原因。
- **等待中可改选/退订**：每次选台/退订递增机顶盒 `generation` 并把旧请求移出等待集合。
- **已取消的选台不因迟到首包生效**：首包到达时校验 `generation` 与 `pending_channel`，不一致直接丢弃该等待项。

### FEC 数据面

- **每 3 个连续节目包 + 1 个校验包**：默认 `fec_group_size=3`。校验数据为同组各节目包
  **完整 JSON 报文**的逐字节 XOR（短包补零），另带长度表记录每个原包实际长度。
  校验数据长度 == 组内最长原包；JSON/base64 封装开销另计。实测（默认报文字段）：
  原包约 209 字节，校验数据 209 字节（base64 后 280 字节），校验报文共约 464 字节。
- **任意 2 原包 + 校验包补出第三个**：组内缺 1 包且校验包已到时立即还原；乱序到达不影响，
  代理按 `(round, group, index)` 元数据判定是否到齐。补出的包与源端原报文逐字节一致。
- **按原序交付、每包一次**：严格按组号、组内序号的原始顺序入队；重复/陈旧报文丢弃，
  已交付或已跳过的组的迟到报文不再交付。
- **等候最多 2 秒**：从收到该组或更晚组的首个报文起算；到期仍缺包就跳过缺包，其余照常交付。
  `FEC_WAIT_SECONDS` 可调小，但代码内硬上限 2 秒。
- **源复位处理**：每个报文带源会话号 `boot` 与频道轮次 `round`。收到新一轮的首个报文即
  清空旧轮尚未入队的数据，之后旧轮报文一律忽略；复位时已在队列中的旧包不回撤。
- **首包换台只认准备入队的节目包**：校验包、仍在缓存等待补齐的包、因超时被跳过的包都不触发切换；
  只有真正写入读包队列的包（含补出的包）才可作为新频道首包。

## 自测

```bash
python3 tests/fec_unit.py                       # 纯函数单元测试, 无需启动服务
python3 tests/selftest.py                       # 换台端到端, 需先启动 proxy 与 source
python3 tests/fec_selftest.py                   # FEC 端到端, 需先启动 proxy 与 source
```

- `tests/selftest.py`：7 个场景 42 项断言（两台共用频道、延迟首包、改选、等待中退订、
  加入失败、两台同时等首包、收尾无残留订阅）。
- `tests/fec_selftest.py`：6 个场景 38 项断言（乱序+每组漏 1 包全部补齐且按序、每组漏 2 包
  超时跳过、首包换台只认准备入队的包、源复位清旧轮、两台共用订阅交付一致、收尾无残留）。
- `tests/fec_unit.py`：8 组 56 项断言（逐字节还原、长度表、组大小 2–8、乱序/重复/校验包先到、
  篡改与组号不匹配拒绝补包、组内/组间乱序与组序颠倒下的按序交付）。

`tests/selftest.py` 场景 3 有一处时序上的不稳定断言（依赖选台前那几毫秒内旧台包恰好到达），
已改为先等旧台至少一个包再选台，使断言确定。

## 验证结果

- 本沙箱无 Docker 守护进程，`docker compose` 未实际运行；以裸进程方式启动同一套代码完成等价验证。
- `tests/fec_unit.py` 全部通过；`tests/selftest.py` 原有 42 项断言全部通过（无回归）；
  `tests/fec_selftest.py` 全部通过。
- 实测乱序丢一包（interval 0.1s，1.6s 窗口）：交付 15 包，`seq` 1..15 严格连续，
  每组恰好补出 1 个（seq 1/4/7/10/13，`fec.index=0`，带 `recovered: true`），
  所有 `payload` 与 `program-1-segment-<seq>` 一致。
- 实测超时续播（interval 0.3s，每组漏 2 个、不可补）：等候期内无交付；第一个可交付包在
  约 2.7s 时出现（含首个报文前的事件循环/网络时延，等候本身实测 2.004s），交付每组第 3 个原包
  （seq 3/6/9/…，`fec.index=2`），段号严格递增，`skipped` 随组数增长。
- 实测等待上限：把 `FEC_WAIT_SECONDS` 设为 30，实际 `wait_seconds` 仍为 2.0，首个可交付包约 2 秒后出现。
- 实测源复位：`POST /restart/{ch}` 后代理只交付新一轮段号（从 1 重新开始），旧轮报文不再出现。

## 已知限制 / 未完成项

- 数据面用"UDP 单播转发"模拟组播，控制面用 HTTP 模拟 IGMP；接真实组播时需替换为
  `IP_ADD_MEMBERSHIP` 组播 socket 与 IGMP 交互。当前 FEC 元数据放在 JSON 报文内，
  真实部署应改为独立的 FEC 头部/负载格式（避免 base64 膨胀）。
- 单校验包只能补 **1 个**缺包；同组丢 2 个及以上无法还原，只能等 2 秒跳过。需要更强纠错时
  应改为多校验包（如 Reed-Solomon）。
- **组间乱序依赖源端声明起始组号**：源端在每个报文里带 `stream.start_group`（本轮首个组号），
  代理据此为"尚未露面的更早组"保留完整等候期，因此即便更晚的组先到也能按原序交付。
  若上游不带该元数据（旧协议），代理无从预知更早组的存在：此时只交付首个见到的组并丢弃
  迟到的更早组，保证绝不乱序、绝不重复，但可能少交付（兼容旧源的已知限制）。
- 机顶盒读包为轮询模式，队列有界（默认 200，溢出丢最旧）；可扩展 WebSocket/SSE 推送。
  队列溢出可能使已入队但未读取的包被丢弃（FEC 不覆盖此层）。
- 状态全在内存，单 worker 运行；代理重启即丢失，无鉴权、无持久化、无多副本一致性。
- 上游 `leave` 失败（源不可达）不重试，源侧可能短暂残留发包，代理会直接丢弃未订阅频道的包。
- 复位判定依赖源端带 `boot`/`round` 元数据；旧协议（无这两字段）的报文默认视为同一轮。
  无 `fec` 元数据的节目包按到达顺序直接交付（兼容旧源），不做补包。
