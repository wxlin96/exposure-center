# -*- coding: utf-8 -*-
"""轮询兜底采集器。

回调之外的安全网：
  - 对处于 exposing 的任务周期调用机器 get-status
  - 把终态条带增量交给 CallbackHandler 统一判定（幂等，重复无副作用）
  - 顺带维护机器在线状态
"""
import asyncio
import logging
from typing import Dict, List

from config import POLL_INTERVAL
from models import ExposureTask, Machine
from services.ps_client import PSClient, TERMINAL_FAILED_STATES, SUCCESS_TERMINAL_STATE

logger = logging.getLogger("collector")


class Collector:
    def __init__(self, session_factory, callback_handler, enabled: bool = True,
                 interval: float = POLL_INTERVAL):
        self._session_factory = session_factory
        self._cb = callback_handler
        self._enabled = enabled
        self._interval = interval
        self._task: asyncio.Task = None  # type: ignore[assignment]

    def start(self) -> None:
        if not self._enabled:
            logger.info("poll collector disabled")
            return
        self._task = asyncio.create_task(self._run())

    async def _run(self) -> None:
        logger.info("poll collector started, interval=%.1fs", self._interval)
        while True:
            try:
                await self._poll_once()
            except Exception as exc:  # noqa: BLE001
                logger.exception("poll once failed: %s", exc)
            await asyncio.sleep(self._interval)

    async def _poll_once(self) -> None:
        with self._session_factory() as db:
            rows = (
                db.query(ExposureTask, Machine.base_url)
                .join(Machine, ExposureTask.machine_id == Machine.machine_id)
                .filter(ExposureTask.status.in_(["exposing", "starting"]))
                .all()
            )
            jobs: List[Dict] = [
                {"job_id": r.ExposureTask.task_id, "machine_id": r.ExposureTask.machine_id,
                 "base_url": r.base_url}
                for r in rows
            ]

        for job in jobs:
            await self._poll_job(job["job_id"], job["machine_id"], job["base_url"])

    async def _poll_job(self, job_id: str, machine_id: str, base_url: str) -> None:
        try:
            client = PSClient(base_url)
            _, groups = await asyncio.to_thread(client.get_status, job_id)
        except Exception as exc:  # noqa: BLE001
            logger.debug("poll get-status failed for %s: %s", job_id, exc)
            return
        # 终态条带增量处理（幂等）
        await asyncio.to_thread(
            self._cb.handle_status_snapshot, machine_id, job_id, groups
        )
        # 标记机器在线
        with self._session_factory() as db:
            m = db.query(Machine).filter_by(machine_id=machine_id).first()
            if m:
                m.status = "online"
                from datetime import datetime
                m.last_seen_at = datetime.now()
                db.commit()
