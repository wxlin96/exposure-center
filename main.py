# -*- coding: utf-8 -*-
"""中心平台入口。
启动：
   python main.py
# 或
   uvicorn main:app --host 0.0.0.0 --port 9000
"""
import logging
from contextlib import asynccontextmanager

import uvicorn
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from api.routes_callbacks import make_callback_router
from api.routes_machines import make_machine_router
from api.routes_tasks import make_task_router
from api.routes_ws import make_ws_router
from config import API_HOST, API_PORT
from database import SessionLocal, init_db
from redis_client import get_redis
from services.callback_handler import CallbackHandler
from services.collector import Collector
from services.event_bus import EventBus
from services.task_engine import TaskEngine

logging.basicConfig(
    level=logging.DEBUG,
    format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
)
logger = logging.getLogger("main")


@asynccontextmanager
async def lifespan(app: FastAPI):
    # ========== startup 阶段 ==========
    init_db()
    redis = get_redis()
    if not redis.ping():
        raise RuntimeError("redis unavailable")

    bus = EventBus(redis)
    bus.bind_loop()
    handler = CallbackHandler(SessionLocal, redis, bus)
    engine = TaskEngine(SessionLocal, redis, bus, handler)
    collector = Collector(SessionLocal, handler)

    # 赋值到state
    app.state.bus = bus
    app.state.handler = handler
    app.state.engine = engine

    bus.start_redis_listener()
    collector.start()
    logger.info("Exposure Center started: http://%s:%d", API_HOST, API_PORT)

    yield

    # ========== shutdown 阶段 ==========
    bus: EventBus | None = getattr(app.state, "bus", None)
    if bus:
        bus.shutdown()


def create_app() -> FastAPI:
    app = FastAPI(
        title="Exposure Center",
        description="多机曝光任务中心：远程下发 + 接收桩判定 + 实时推送",
        version="1.0.0",
        lifespan=lifespan,
    )

    app.add_middleware(
        CORSMiddleware,
        allow_origins=["*"],
        allow_credentials=False,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    app.include_router(make_machine_router())
    app.include_router(make_task_router())
    app.include_router(make_callback_router())
    app.include_router(make_ws_router())

    @app.get("/healthz")
    def healthz():
        redis = get_redis()
        return {"ok": redis.ping()}

    return app


app = create_app()

if __name__ == "__main__":
    uvicorn.run("main:app", host=API_HOST, port=API_PORT, reload=False)
