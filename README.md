# 多机曝光任务中心（Exposure Center）

中心平台：远程调用各机器（PS Exposure Service）下发曝光任务，通过**接收桩**接收
机器回调判定任务完成/失败，并把实时状态推送给前端。

技术栈：**Python 3.10+ / FastAPI / Redis / MySQL / WebSocket**

## 一、系统定位

```
【远程机器集群】PS Exposure Service（无 Agent，纯 HTTP 服务）
        ▲ 中心主动调用（set-job / stripe-split / set-ric / start-exposure ...）
        │ 机器主动回调（stripe-split / stripe-status）──────────┐
        ▼                                                      ▼
【中心平台 Exposure Center】                              【接收桩】
  ├─ MySQL：机器 / 曝光任务 / 条带 / 事件流水             判定任务完成/失败
  ├─ Redis：任务快照 + 事件 Pub/Sub                     失败 -> 自动 stop-exposure
  └─ WebSocket：实时推送前端
```

- **下发**：依次调用机器 API —— `set-job` → `stripe-split` → `set-ric`(可选) → `start-exposure`
- **切分回调**：机器切分完成时上报「条带数量 + 全部条带 id」
- **终态回调**：每条条带 done / error（含 disk、run_failed、load_failed、trans_failed）时上报
- **完成判定**：全部条带 `done` → 任务 `completed`；**任一**条带失败终态 → 任务 `failed` 并自动调用机器 `stop-exposure` 停止剩余曝光
- **兜底**：`POLL_ENABLED=1` 时轮询采集器调用 `get-status`，回调丢失也能收敛

## 二、目录结构

```
exposure_center/
├── main.py                  # FastAPI 入口（startup 装配各组件）
├── config.py                # 配置（环境变量可覆盖）
├── database.py              # SQLAlchemy 引擎/会话/建表
├── models.py                # ORM：machine / exposure_task / stripe_record / task_event_log
├── schemas.py               # Pydantic 请求响应模型
├── redis_client.py          # Redis 客户端（RESP2 兼容 Redis 5.x）与事件发布
├── api/
│   ├── routes_machines.py   # 机器注册/查询
│   ├── routes_tasks.py      # 任务创建/查询/停止
│   ├── routes_callbacks.py  # ★ 接收桩（stripe-split / stripe-status）
│   └── routes_ws.py         # WebSocket 实时事件
├── services/
│   ├── ps_client.py         # 远程机器 API 客户端（对接 API.md 全部接口）
│   ├── task_engine.py       # 任务编排状态机（下发 + 等待终结 + 超时保护）
│   ├── callback_handler.py  # 接收桩核心判定（幂等 + 行锁防并发）
│   ├── event_bus.py         # 事件总线（跨线程安全广播 + Redis 转发）
│   ├── collector.py         # 轮询兜底采集
│   └── machine_store.py     # 机器信息辅助
├── mock/
│   └── ps_mock.py           # PS 模拟器（含回调推送，用于本地验证）
├── tests/
│   └── test_e2e.py          # 端到端测试（真实启动中心 + 2 台模拟机器）
├── docs/
│   └── CALLBACK_PROTOCOL.md # ★ 回调协议（机器侧对接文档）
└── sql/schema.sql           # 建表脚本
```

## 三、快速启动

### 1. 依赖与基础设施

```bash
pip install -r requirements.txt
# MySQL 8.x 与 Redis（5.x+）已启动；默认连接配置见 config.py
```

### 2. 启动中心平台

```bash
cd exposure_center
python main.py            # 默认 0.0.0.0:9000
# 或 uvicorn main:app --host 0.0.0.0 --port 9000
```

### 3. 端到端验证（含 PS 模拟器）

```bash
python tests/test_e2e.py
# 自动启动：中心平台 + 2 台模拟机器（ps-01 成功、ps-02 必失败）
# 断言：成功任务 completed(10/10)；失败任务 failed 且机器侧曝光被停止；
#       WebSocket 事件齐全
```

## 四、对外接口

| 方法 | 路径 | 说明 |
|---|---|---|
| POST | `/api/v1/machines` | 注册机器 `{machine_id, base_url, name?}` |
| GET | `/api/v1/machines` | 机器列表 |
| GET | `/api/v1/machines/{id}` | 机器详情 |
| POST | `/api/v1/tasks` | 创建并下发曝光任务 `{machine_id, job_id, ric?}` |
| GET | `/api/v1/tasks` | 任务列表 |
| GET | `/api/v1/tasks/{task_id}` | 任务详情（含全部条带） |
| POST | `/api/v1/tasks/{task_id}/stop` | 手动停止（调用机器 stop-exposure） |
| POST | `/api/v1/callbacks/stripe-split` | ★ 接收桩：切分完成通知 |
| POST | `/api/v1/callbacks/stripe-status` | ★ 接收桩：单条带终态通知 |
| WS | `/ws` | 实时事件推送 |
| GET | `/healthz` | 健康检查 |

> 接收桩校验：`Header: X-Callback-Token`，与 `CALLBACK_TOKEN` 一致。

## 五、任务状态机（中心侧）

```
pending → configuring(set-job) → split_issued(stripe-split)
       → ric_uploading(set-ric, 可选) → starting(start-exposure) → exposing
       → completed（全部条带 done）
       → failed（任一失败终态；自动 stop-exposure）
       → stopped（手动停止）
```

## 六、WebSocket 事件

`{type, task_id, machine_id, data}`：
`task_created` / `task_phase` / `task_started` / `stripe_split` / `stripe_status`
/ `task_completed` / `task_failed` / `task_stopped`

## 七、关键设计

1. **并发安全**：条带终态回调可能并发到达（同批条带同时完成），计数更新使用
   `SELECT ... FOR UPDATE` 行锁串行化，防止丢失更新；终态去重幂等。
2. **幂等**：同一任务同一终态重复回调安全忽略；已终结任务忽略迟到事件。
3. **跨线程事件**：编排在独立线程运行，EventBus 通过 `run_coroutine_threadsafe`
   把广播调度到主事件循环，任意线程可安全 publish。
4. **超时保护**：`TASK_WAIT_TIMEOUT` 内无终态回调自动判失败，避免任务悬挂。
5. **兜底**：轮询采集器（`POLL_INTERVAL` 秒）调用 `get-status` 补齐终态，
   与回调共用同一判定逻辑（幂等）。

## 八、配置项（环境变量）

| 变量 | 默认 | 说明 |
|---|---|---|
| MYSQL_HOST / PORT / USER / PASSWORD / DB | 127.0.0.1 / 3306 / lwx / ... / exposure_center | MySQL |
| REDIS_URL | redis://127.0.0.1:6379/0 | Redis |
| API_HOST / API_PORT | 0.0.0.0 / 9000 | 中心平台 |
| PS_API_TIMEOUT | 10 | 调机器接口超时 |
| TASK_WAIT_TIMEOUT | 1800 | 等待任务终结超时 |
| POLL_ENABLED / POLL_INTERVAL | 1 / 3.0 | 轮询兜底 |
| CALLBACK_TOKEN | change-me | 接收桩共享令牌 |

## 九、对接真实机器

机器侧需实现 `docs/CALLBACK_PROTOCOL.md` 中的两个回调（切分完成、条带终态），
其余接口按 API.md 原生支持即可。本地验证可先用 `mock/ps_mock.py`：
```bash
python mock/ps_mock.py --port 8001 --machine-id ps-01 \
    --callback-url http://127.0.0.1:9000 --callback-token change-me \
    --stripe-count 10 --failure-rate 0.0
```
