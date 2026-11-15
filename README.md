# 铜期货盘口重建服务（CU2601）

行情断线/丢序后可信盘口重建环境：本地行情源 + 采集器，FastAPI + httpx + websockets。
单合约、每侧最多 20 档、整数价量。

## 快速启动

```bash
docker compose up --build
```

| 服务 | 地址 | 说明 |
|---|---|---|
| 采集器 collector | http://localhost:8001/docs | `/book` 查盘口、`/status` 查同步状态 |
| 行情源 market-source | http://localhost:8000/docs | `/snapshot`、`/ws`、`/control/*` 故障注入 |

不开 Docker 时本地直跑：

```bash
pip install -r market_source/requirements.txt -r collector/requirements-dev.txt
(cd market_source && uvicorn app.main:app --port 8000 &)
(cd collector && SOURCE_WS=ws://localhost:8000/ws uvicorn app.main:app --port 8001 &)
python3 scripts/smoke_test.py     # 端到端冒烟
(cd collector && python3 -m pytest tests/ -q)   # 单元测试
```

## 数据协议

- **HTTP 快照** `GET /snapshot`：`{symbol, seq, bids: [[价,量]...], asks: [...]}`，
  `seq` 为快照已包含的最后一条增量序号。
- **WS 增量** `/ws`：`{type:"increment", symbol, seq, changes:[{side,price,qty}...]}`，
  `seq` 全市场连续递增；一条增量可含多档变更；`qty=0` 表示删档；另有心跳帧保活。

## 采集器衔接规则（collector/app/sync_manager.py）

1. **启动 / 断流 / 发现缺号 / 发现同序号内容冲突 → 立即失步**：`/book` 返回 503，
   `/status.synced=false`。
2. **恢复流程**：先连 WS 并**持续缓冲增量**（取快照期间一条不丢），同时用 httpx 取快照。
3. **衔接**：丢弃 `seq ≤ 快照seq` 的缓冲（含重复，保证**恰好应用一次**），
   其余必须从 `快照seq+1` 开始**逐条连续**，否则本轮作废重来。
4. **增量去重（每连接）**：每个连接维护最近 64 条已收增量的 `(序号, 变更内容)` 窗口
   （`DEDUP_WINDOW` 可调），**快照在途缓冲与实时处理共用同一窗口**核对：
   - 同序号同内容 = 正常重发，恰好应用一次（`duplicates_skipped` 计数）；
   - 同序号不同内容 = **冲突**，立即失步（`/book` 503）、整轮重同步，
     `/status.conflict_seqs` 标出冲突序号（`conflicts` 计数）；
   - 快照已覆盖但本连接未见过原帧的序号只丢弃、**不报冲突**；
     窗口只保留最近 64 条，被淘汰的序号同样只丢弃不报冲突。
5. **完整衔接才恢复查询**；同步后每条增量仍校验 `seq == last+1`，缺号即回到第 1 步。
6. **迟到快照防护**：每轮恢复递增 `generation` 并**重建去重窗口**，快照请求带世代号；
   响应返回时世代已更替（重连前发起的迟到快照）一律丢弃，
   **不恢复可用状态、不覆盖新盘口**（`/status` 中 `stale_snapshots` 计数）；
   旧连接的帧不进入新轮次的去重窗口。
7. **原子可见**：同一增量的多档变更先在副本上应用再整体交换，
   查询只能看到应用前或应用后的完整状态；`/book` 返回 `seq` + 买降序/卖升序盘口。

## 故障注入（行情源控制面，均可在 http://localhost:8000/docs 上直接调用）

```bash
curl -X POST 'http://localhost:8000/control/gap?count=1'          # 丢序：跳过 1 个序号
curl -X POST 'http://localhost:8000/control/disconnect'           # 断线：关闭全部 WS
curl -X POST 'http://localhost:8000/control/snapshot_delay?seconds=5'  # 快照延迟 5s
curl -X POST 'http://localhost:8000/control/duplicate'            # 重放上一条增量（测去重）
curl -X POST 'http://localhost:8000/control/conflict'             # 冲突重放：同序号不同内容
curl -X POST 'http://localhost:8000/control/pause?seconds=3'      # 暂停推送（心跳继续）
```

观察采集器行为：`watch curl -s localhost:8001/status`。

典型演练（迟到快照）：先设 `snapshot_delay=5`，再 `disconnect` —— 采集器进入 503；
快照在途期间再次 `disconnect`，旧快照响应迟到后被丢弃（`stale_snapshots` +1），
新世代重新取快照衔接后才恢复 200。

典型演练（冲突重放）：`curl -X POST localhost:8000/control/conflict` —— 行情源把
上一条增量的序号配上不同量重发；采集器立即失步（`/book` 503），
`/status.conflict_seqs` 标出该序号（`conflicts` +1），随后自动重连取新快照恢复 200。
对照：`/control/duplicate` 重放同序号同内容，只计 `duplicates_skipped`，不判冲突。

## API 一览

**采集器**（8001）
- `GET /book` — 已同步：`{symbol, seq, bids, asks}`；失步：`503 {error, detail, last_error}`
- `GET /status` — 同步状态、序号、世代、两侧档数、冲突序号（`conflict_seqs`）、统计计数、配置

**行情源**（8000）
- `GET /snapshot`、`WS /ws`、`GET /status`、`POST /control/*`（见上）

## 目录结构

```
├── docker-compose.yml
├── collector/                 # 采集器
│   ├── app/orderbook.py       #   订单簿 + 去重窗口 + 快照衔接（纯逻辑）
│   ├── app/sync_manager.py    #   WS/快照/世代管理/冲突检测/自愈循环
│   ├── app/main.py            #   FastAPI: /book /status
│   └── tests/                 #   pytest 单测（30 项）
├── market_source/app/main.py  # 行情源 + 故障注入控制面
└── scripts/smoke_test.py      # 端到端冒烟（重复/冲突/丢序/断线/延迟快照/序号单调）
```

## 环境变量（采集器）

| 变量 | 默认 | 说明 |
|---|---|---|
| `SYMBOL` | `CU2601` | 合约 |
| `SOURCE_HTTP` / `SOURCE_WS` | compose 内自动指向行情源 | 行情源地址 |
| `MAX_LEVELS` | `20` | 每侧最大档数（超限判协议错误并重同步） |
| `RECONNECT_DELAY` | `1.0` | 重同步间隔（秒） |
| `DATA_TIMEOUT` | `5.0` | 收不到任何帧（含心跳）判定断流（秒） |
| `SNAPSHOT_TIMEOUT` | `30.0` | 快照 HTTP 超时（需大于注入的快照延迟） |
| `DEDUP_WINDOW` | `64` | 每连接去重窗口：记住最近多少条已收增量的 (序号, 内容) 用于冲突核对 |
