# -*- coding: utf-8 -*-
"""Redis 客户端与事件发布。"""
import json
from typing import Any, Dict, Optional

import redis

from config import EVENT_CHANNEL, REDIS_URL

# 任务快照 key
def task_snapshot_key(task_id: str) -> str:
    return f"exposure:task:{task_id}"


def task_stripes_key(task_id: str) -> str:
    return f"exposure:task:{task_id}:stripes"


class RedisClient:
    def __init__(self, url: str = REDIS_URL):
        # protocol=2：兼容 Redis 5.x（不支持 RESP3 的 HELLO 命令）
        self._r = redis.Redis.from_url(url, decode_responses=True, protocol=2)
        self.channel = EVENT_CHANNEL

    # ---------- 连接 ----------
    def ping(self) -> bool:
        return bool(self._r.ping())

    def close(self) -> None:
        self._r.close()

    def raw(self) -> redis.Redis:
        return self._r

    # ---------- 任务快照 ----------
    def save_task_snapshot(self, task_id: str, snapshot: Dict[str, Any]) -> None:
        """整体覆盖任务快照（hash）。"""
        mapping = {k: (v if isinstance(v, str) else json.dumps(v, ensure_ascii=False))
                   for k, v in snapshot.items()}
        self._r.hset(task_snapshot_key(task_id), mapping=mapping)

    def get_task_snapshot(self, task_id: str) -> Optional[Dict[str, Any]]:
        h = self._r.hgetall(task_snapshot_key(task_id))
        if not h:
            return None
        out: Dict[str, Any] = {}
        for k, v in h.items():
            try:
                out[k] = json.loads(v)
            except (TypeError, ValueError):
                out[k] = v
        return out

    def update_task_snapshot(self, task_id: str, **fields: Any) -> None:
        mapping = {}
        for k, v in fields.items():
            mapping[k] = v if isinstance(v, str) else json.dumps(v, ensure_ascii=False)
        if mapping:
            self._r.hset(task_snapshot_key(task_id), mapping=mapping)

    def del_task_snapshot(self, task_id: str) -> None:
        self._r.delete(task_snapshot_key(task_id), task_stripes_key(task_id))

    # ---------- 条带状态（hash: stripe_id -> status） ----------
    def set_stripe_status(self, task_id: str, stripe_id: str, status: str) -> None:
        self._r.hset(task_stripes_key(task_id), stripe_id, status)

    def get_stripe_status(self, task_id: str, stripe_id: str) -> Optional[str]:
        return self._r.hget(task_stripes_key(task_id), stripe_id)

    def get_all_stripes(self, task_id: str) -> Dict[str, str]:
        return self._r.hgetall(task_stripes_key(task_id))

    # ---------- 事件发布 ----------
    def publish_event(self, event_type: str, task_id: str, machine_id: str, data: Dict[str, Any]) -> None:
        payload = json.dumps(
            {
                "type": event_type,
                "task_id": task_id,
                "machine_id": machine_id,
                "data": data,
                "ts": __import__("time").time(),
            },
            ensure_ascii=False,
        )
        self._r.publish(self.channel, payload)


# 模块级单例（FastAPI startup 时初始化）
redis_client: Optional[RedisClient] = None


def get_redis() -> RedisClient:
    global redis_client
    if redis_client is None:
        redis_client = RedisClient()
    return redis_client
