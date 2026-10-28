# 中心平台回调协议（CALLBACK_PROTOCOL）

远程机器（PS Exposure Service）在条带切分完成、以及每条条带进入**终态**时，
主动向中心平台推送事件；中心平台据此判定任务完成/失败，无需轮询。

## 1. 通用约定

- 中心平台接收桩地址：`POST {CENTER_BASE}/api/v1/callbacks/...`
- 认证：`Header: X-Callback-Token: <CALLBACK_TOKEN>`（与中心平台配置一致）
- 请求体：`application/json`
- 响应：`{"code": 0, "msg": "ok"}`，`code == 0` 表示已接收并处理成功；
  非 0 表示处理失败（如任务不存在），机器侧可记录日志。

## 2. 事件一：条带切分完成

**时机**：`stripe-split` 接口执行成功后，立即推送一次。

```
POST /api/v1/callbacks/stripe-split
X-Callback-Token: <token>

{
  "machine_id": "ps-01",          # 机器唯一标识
  "job_id": "job-20261019-A",     # 当前 job
  "stripe_count": 100,            # 条带数量
  "stripes": [
    "job-20261019-A_stripe_0000",
    "job-20261019-A_stripe_0001",
    "..."
  ],
  "ts": 1761033600.123
}
```

**中心行为**：记录任务 `stripe_count`，预建全部条带记录（status=idle），
推送 `stripe_split` WebSocket 事件。

## 3. 事件二：单条带终态

**时机**：任一条带进入终态（`done` / `error` / `disk` / `run_failed` /
`load_failed` / `trans_failed`）时推送一次。同一终态只推送一次，重复推送
中心会幂等忽略。

```
POST /api/v1/callbacks/stripe-status
X-Callback-Token: <token>

{
  "machine_id": "ps-01",
  "job_id": "job-20261019-A",
  "stripe_id": "job-20261019-A_stripe_0000",
  "status": "done",               # 终态之一
  "ts": 1761033601.456
}
```

**中心行为**：
- `done` → `done_count + 1`；
- 其他失败终态 → `error_count + 1`，任务标记 `failed`，中心自动调用机器
  `stop-exposure` 停止剩余曝光；
- 判定：`done_count == stripe_count` → 任务 `completed`；
  任一失败终态 → 任务 `failed`（立即终止）。
- 推送 `stripe_status` / `task_completed` / `task_failed` WebSocket 事件。

## 4. 失败终态全集

| status | 含义 |
|---|---|
| error | 通用错误 |
| disk | 磁盘错误 |
| run_failed | 曝光执行失败 |
| load_failed | 数据加载失败 |
| trans_failed | 数据回传失败 |

## 5. 兜底

即使回调全部丢失，中心平台的轮询采集器（默认开启，间隔 `POLL_INTERVAL` 秒）
会调用机器 `get-status`，把终态条带增量补齐到同一判定逻辑，保证任务最终收敛。
