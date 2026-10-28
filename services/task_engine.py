# -*- coding: utf-8 -*-
"""曝光任务编排引擎。

下发流程（依次调用远程机器 API）：
  set-job -> stripe-split -> set-ric(可选) -> start-exposure

下发完成后进入等待期：
  - 由接收桩回调（stripe-split / stripe-status）驱动完成判定
  - 轮询兜底采集器（get-status）作为回调丢失的补充
  - 本引擎协程在等待期内轮询 Redis 任务快照，发现终态即结束

超时保护：TASK_WAIT_TIMEOUT 内未终结 -> 标记 failed 并调 stop-exposure。
"""
import asyncio
import base64
import logging
import threading
from datetime import datetime
from typing import Any, Dict, Optional

from config import (
    PS_API_TIMEOUT,
    TASK_POLL_INTERVAL,
    TASK_WAIT_TIMEOUT,
)
from models import ExposureTask, TaskEventLog
from services.ps_client import PSClient, PSClientError

logger = logging.getLogger("task_engine")

TASK_TERMINAL = {"completed", "failed", "stopped"}


class TaskEngine:
    def __init__(self, session_factory, redis_client, event_bus, callback_handler):
        self._session_factory = session_factory
        self._redis = redis_client
        self._bus = event_bus
        self._cb = callback_handler

    # ==================================================================
    # 入口：创建并异步下发
    # ==================================================================
    def create_task(self, machine_id: str, job_id: str,
                    ric: Optional[Dict[str, Any]] = None) -> Dict:
        """创建任务记录并返回；编排异步执行。"""
        with self._session_factory() as db:
            exists = db.query(ExposureTask).filter_by(task_id=job_id).first()
            if exists:
                return {"ok": False, "reason": f"task {job_id} already exists"}

            ric_required = 1 if ric else 0
            task = ExposureTask(
                task_id=job_id,
                machine_id=machine_id,
                status="pending",
                phase="none",
                ric_required=ric_required,
                ric_filename=ric.get("filename") if ric else None,
            )
            db.add(task)
            db.add(TaskEventLog(task_id=job_id, machine_id=machine_id,
                                event_type="task_created",
                                payload={"ric_required": bool(ric_required)}))
            db.commit()

        self._redis.save_task_snapshot(job_id, {
            "task_id": job_id,
            "machine_id": machine_id,
            "status": "pending",
            "phase": "none",
            "stripe_count": 0,
            "done_count": 0,
            "error_count": 0,
            "ric_required": ric_required,
        })
        self._bus.publish("task_created", job_id, machine_id, {})
        logger.info("task %s created on machine %s", job_id, machine_id)

        # 异步编排：独立线程 + 独立事件循环（同步端点内调度安全）
        threading.Thread(
            target=lambda: asyncio.run(self._orchestrate(job_id, machine_id, ric)),
            name=f"orchestrate-{job_id}",
            daemon=True,
        ).start()
        return {"ok": True, "task_id": job_id}

    # ==================================================================
    # 编排协程
    # ==================================================================
    async def _orchestrate(self, job_id: str, machine_id: str,
                           ric: Optional[Dict[str, Any]]) -> None:
        try:
            base_url = await self._get_base_url(machine_id)
            if not base_url:
                raise PSClientError("machine not registered")
            client = PSClient(base_url, timeout=PS_API_TIMEOUT)

            # 1) set-job
            self._set_phase(job_id, "configuring", "configured")
            await asyncio.to_thread(client.set_job, job_id)
            self._mark_machine_online(machine_id)

            # 2) stripe-split
            self._set_phase(job_id, "split_issued", "split")
            await asyncio.to_thread(client.stripe_split, job_id)

            # 3) set-ric（可选）
            if ric:
                self._set_phase(job_id, "ric_uploading", "ready")
                content = base64.b64decode(ric.get("content_b64", ""))
                filename = ric.get("filename", "ric.map")
                await asyncio.to_thread(client.set_ric, job_id, filename, content,
                                        ric.get("crc32"))

            # 4) start-exposure
            self._set_phase(job_id, "starting", "exposing")
            await asyncio.to_thread(client.start_exposure, job_id)
            self._mark_started(job_id)

            # 5) 等待终结（回调驱动，此处轮询 Redis 快照）
            loop = asyncio.get_running_loop()
            deadline = loop.time() + TASK_WAIT_TIMEOUT
            while True:
                snap = self._redis.get_task_snapshot(job_id)
                if snap and snap.get("status") in TASK_TERMINAL:
                    logger.info("task %s orchestration done: %s", job_id, snap.get("status"))
                    return
                if loop.time() > deadline:
                    logger.warning("task %s wait timeout, marking failed", job_id)
                    self._cb._fail_task(job_id, machine_id, "wait timeout, no terminal callback")
                    return
                await asyncio.sleep(TASK_POLL_INTERVAL)
        except PSClientError as exc:
            logger.error("task %s orchestration failed: %s", job_id, exc)
            with self._session_factory() as db:
                task = db.query(ExposureTask).filter_by(task_id=job_id).first()
                if task and task.status not in TASK_TERMINAL:
                    task.status = "failed"
                    task.error_msg = f"orchestration error: {exc}"
                    task.finished_at = datetime.now()
                    db.add(TaskEventLog(task_id=job_id, machine_id=machine_id,
                                        event_type="task_failed",
                                        payload={"reason": str(exc)}))
                    db.commit()
            self._redis.update_task_snapshot(job_id, status="failed", error_msg=str(exc))
            self._bus.publish("task_failed", job_id, machine_id, {"reason": str(exc)})
        except Exception as exc:  # noqa: BLE001
            logger.exception("task %s orchestration crashed", job_id)
            self._cb._fail_task(job_id, machine_id, f"orchestration crash: {exc}")

    # ==================================================================
    # 辅助
    # ==================================================================
    async def _get_base_url(self, machine_id: str) -> Optional[str]:
        from services.machine_store import get_machine_base_url
        return await asyncio.to_thread(get_machine_base_url, self._session_factory, machine_id)

    def _set_phase(self, job_id: str, status: str, phase: str) -> None:
        with self._session_factory() as db:
            task = db.query(ExposureTask).filter_by(task_id=job_id).first()
            if task:
                task.status = status
                task.phase = phase
                db.commit()
        self._redis.update_task_snapshot(job_id, status=status, phase=phase)
        self._bus.publish("task_phase", job_id, self._machine_of(job_id),
                          {"status": status, "phase": phase})

    def _mark_started(self, job_id: str) -> None:
        with self._session_factory() as db:
            task = db.query(ExposureTask).filter_by(task_id=job_id).first()
            if task and task.status not in TASK_TERMINAL:
                task.status = "exposing"
                task.started_at = datetime.now()
                db.commit()
        self._redis.update_task_snapshot(job_id, status="exposing")
        self._bus.publish("task_started", job_id, self._machine_of(job_id), {})

    def _mark_machine_online(self, machine_id: str) -> None:
        from services.machine_store import touch_machine
        touch_machine(self._session_factory, machine_id)

    def _machine_of(self, job_id: str) -> str:
        snap = self._redis.get_task_snapshot(job_id)
        return snap.get("machine_id", "") if snap else ""

    # ==================================================================
    # 停止任务
    # ==================================================================
    def stop_task(self, job_id: str) -> Dict:
        """用户手动停止：调机器 stop-exposure，标记任务 stopped。"""
        from services.machine_store import get_machine_base_url
        with self._session_factory() as db:
            task = db.query(ExposureTask).filter_by(task_id=job_id).first()
            if task is None:
                return {"ok": False, "reason": "task not found"}
            if task.status in TASK_TERMINAL:
                return {"ok": False, "reason": f"task already {task.status}"}
            machine_id = task.machine_id

        base_url = get_machine_base_url(self._session_factory, machine_id)
        if base_url:
            try:
                PSClient(base_url, timeout=PS_API_TIMEOUT).stop_exposure()
            except Exception as exc:
                logger.warning("stop-exposure failed for %s: %s", job_id, exc)

        self._cb.mark_stopped(job_id, machine_id)
        return {"ok": True, "task_id": job_id}
