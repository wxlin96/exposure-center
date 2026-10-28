# -*- coding: utf-8 -*-
"""WebSocket 实时事件推送。

用法：浏览器/客户端连接 ws://<host>:<port>/ws
服务端推送 JSON 消息：{type, task_id, machine_id, data}
事件类型：task_created / task_phase / task_started / stripe_split / stripe_status
          / task_completed / task_failed / task_stopped
"""
import logging

from fastapi import APIRouter, WebSocket, WebSocketDisconnect

logger = logging.getLogger("ws")


def make_ws_router():
    router = APIRouter(tags=["ws"])

    @router.websocket("/ws")
    async def ws_endpoint(ws: WebSocket):
        bus = ws.app.state.bus
        await bus.connect(ws)
        try:
            # 服务端单向推送；客户端可发任意文本（如 ping）保持连接
            while True:
                await ws.receive_text()
        except WebSocketDisconnect:
            await bus.disconnect(ws)
        except Exception:
            await bus.disconnect(ws)
        finally:
            logger.info("ws closed, remaining=%d", bus.ws_count())

    return router
