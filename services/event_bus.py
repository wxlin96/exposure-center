# -*- coding: utf-8 -*-
"""事件总线：内存 WebSocket 连接管理 + Redis Pub/Sub 广播。

线程模型：
  - 主事件循环（FastAPI app 的 loop）持有 WS 连接集合；
  - 任意线程调用 publish()：通过 run_coroutine_threadsafe 把广播调度到主循环，
    保证线程安全（编排线程 / HTTP 线程池 / 采集协程均可调用）。
"""
import asyncio
import json
import logging
import threading
from typing import Any, Dict, Optional, Set

from fastapi import WebSocket

from config import EVENT_CHANNEL

logger = logging.getLogger("event_bus")


class EventBus:
    def __init__(self, redis_client) -> None:
        self._redis = redis_client
        self._ws: Set[WebSocket] = set()
        self._lock = threading.Lock()
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._stop = threading.Event()

    def bind_loop(self) -> None:
        """在 app startup（主循环内）调用，绑定主事件循环。"""
        self._loop = asyncio.get_running_loop()

    # ---------- WS 连接管理 ----------
    async def connect(self, ws: WebSocket) -> None:
        await ws.accept()
        with self._lock:
            self._ws.add(ws)
        logger.info("ws connected, total=%d", len(self._ws))

    async def disconnect(self, ws: WebSocket) -> None:
        with self._lock:
            self._ws.discard(ws)

    def ws_count(self) -> int:
        with self._lock:
            return len(self._ws)

    # ---------- 发布（任意线程可调） ----------
    def publish(self, event_type: str, task_id: str, machine_id: str, data: Dict[str, Any]) -> None:
        payload = json.dumps(
            {
                "type": event_type,
                "task_id": task_id,
                "machine_id": machine_id,
                "data": data,
            },
            ensure_ascii=False,
        )
        # Redis channel（供多实例/其他订阅者）
        try:
            self._redis.raw().publish(self._redis.channel, payload)
        except Exception as exc:  # pragma: no cover - redis 故障不应阻断业务
            logger.warning("publish to redis failed: %s", exc)
        # 内存广播（调度到主循环）
        loop = self._loop
        if loop is None:
            # startup 尚未完成，丢弃内存广播（Redis 通道兜底）
            return
        try:
            asyncio.run_coroutine_threadsafe(self._broadcast(payload), loop)
        except RuntimeError:
            logger.debug("main loop not running, skip in-memory broadcast")

    async def _broadcast(self, payload: str) -> None:
        with self._lock:
            targets = list(self._ws)
        for ws in targets:
            try:
                await ws.send_text(payload)
            except Exception:
                with self._lock:
                    self._ws.discard(ws)

    # ---------- Redis Pub/Sub 监听（跨实例） ----------
    def start_redis_listener(self) -> None:
        """启动后台线程订阅 Redis channel 并转发到本实例 WS。"""
        thread = threading.Thread(target=self._listen_loop, name="redis-event-listener", daemon=True)
        thread.start()

    def _listen_loop(self) -> None:
        pubsub = self._redis.raw().pubsub()
        pubsub.subscribe(EVENT_CHANNEL)
        logger.info("redis listener subscribed: %s", EVENT_CHANNEL)
        try:
            for msg in pubsub.listen():
                if self._stop.is_set():
                    break
                if msg.get("type") != "message":
                    continue
                payload = msg.get("data")
                if not isinstance(payload, str):
                    continue
                loop = self._loop
                if loop is not None:
                    try:
                        asyncio.run_coroutine_threadsafe(self._broadcast(payload), loop)
                    except RuntimeError:
                        pass
        except Exception as exc:  # pragma: no cover
            logger.warning("redis listener stopped: %s", exc)
        finally:
            try:
                pubsub.close()
            except Exception:
                pass

    def shutdown(self) -> None:
        self._stop.set()
