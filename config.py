# -*- coding: utf-8 -*-
"""中心平台全局配置：所有项均可被环境变量覆盖，便于部署切换。"""
import os


def _env(key: str, default: str) -> str:
    return os.getenv(key, default)


# ---- MySQL ----
MYSQL_HOST = _env("MYSQL_HOST", "127.0.0.1")
MYSQL_PORT = int(_env("MYSQL_PORT", "3306"))
MYSQL_USER = _env("MYSQL_USER", "lwx")
MYSQL_PASSWORD = _env("MYSQL_PASSWORD", "zxcvbnm,./11")
MYSQL_DB = _env("MYSQL_DB", "exposure_center")
MYSQL_POOL_SIZE = int(_env("MYSQL_POOL_SIZE", "8"))
SQLALCHEMY_DATABASE_URL = _env(
    "DATABASE_URL",
    f"mysql+pymysql://{MYSQL_USER}:{MYSQL_PASSWORD}@{MYSQL_HOST}:{MYSQL_PORT}/{MYSQL_DB}?charset=utf8mb4",
)

# ---- Redis ----
REDIS_HOST = _env("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(_env("REDIS_PORT", "6379"))
REDIS_DB = int(_env("REDIS_DB", "0"))
REDIS_URL = _env("REDIS_URL", f"redis://{REDIS_HOST}:{REDIS_PORT}/{REDIS_DB}")

# ---- API ----
API_HOST = _env("API_HOST", "0.0.0.0")
API_PORT = int(_env("API_PORT", "9000"))  # 中心平台端口；远程机器 PS 服务默认 8000

# ---- 事件 ----
EVENT_CHANNEL = _env("EVENT_CHANNEL", "exposure:events")

# ---- 任务编排 ----
# 编排各阶段调用远程机器的超时（秒）
PS_API_TIMEOUT = float(_env("PS_API_TIMEOUT", "10"))
# start-exposure 后等待任务终结的最大时长（秒），防止回调丢失导致永久挂起
TASK_WAIT_TIMEOUT = float(_env("TASK_WAIT_TIMEOUT", "1800"))
# 编排轮询任务状态间隔（秒）
TASK_POLL_INTERVAL = float(_env("TASK_POLL_INTERVAL", "0.5"))

# ---- 轮询兜底采集（回调之外的兜底，建议开启）----
POLL_ENABLED = _env("POLL_ENABLED", "1") == "1"
POLL_INTERVAL = float(_env("POLL_INTERVAL", "3.0"))

# ---- 调用方身份（webhook 校验用，简单共享令牌；生产可换签名）----
CALLBACK_TOKEN = _env("CALLBACK_TOKEN", "change-me")
