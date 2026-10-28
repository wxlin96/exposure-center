# -*- coding: utf-8 -*-
"""接收桩核心处理逻辑。

机器侧会主动向中心推送两类事件：
  1. 条带切分完成：条带数量 + 每个条带 id      -> handle_stripe_split
  2. 单条带终态（done / error 等失败态）       -> handle_stripe_status

任务完成判定：
  - done_count == stripe_count（且 stripe_count > 0）-> 任务 completed
  - 任意条带进入失败终态                            -> 任务 failed，并触发 stop-exposure 终止剩余曝光

所有处理入口幂等：任务已终结 / 条带状态重复上报均安全跳过。
"""
import logging
from datetime import datetime
from typing import Dict, List, Optional

from sqlalchemy.orm import Session

from models import ExposureTask, StripeRecord, TaskEventLog
from services.ps_client import PSClient, TERMINAL_FAILED_STATES, SUCCESS_TERMINAL_STATE

logger = logging.getLogger("callback_handler")

# 中心侧任务终态
TASK_TERMINAL = {"completed", "failed", "stopped"}


def _now_str() -> str:
    return datetime.now().strftime("%Y-%m-%d %H:%M:%S")


class CallbackHandler:
    """接收桩处理器：同时服务 HTTP 回调与轮询兜底采集的统一入口。"""

    def __init__(self, session_factory, redis_client, event_bus):
        self._session_factory = session_factory
        self._redis = redis_client
        self._bus = event_bus

    # ==================================================================
    # 切分完成回调
    # ==================================================================
    def handle_stripe_split(self, machine_id: str, job_id: str,
                            stripe_count: int, stripes: List[str]) -> Dict:
        """记录切分结果：任务 stripe_count + 预建条带记录（status=idle）。"""
        with self._session_factory() as db:
            task = (
                db.query(ExposureTask)
                .filter_by(task_id=job_id, machine_id=machine_id)
                .with_for_update()
                .first()
            )
            if task is None:
                logger.warning("stripe_split for unknown task job=%s machine=%s", job_id, machine_id)
                return {"ok": False, "reason": "task not found"}
            if task.status in TASK_TERMINAL:
                return {"ok": True, "reason": "task already terminal, ignored"}

            task.stripe_count = stripe_count
            db.commit()

            # 预建条带记录（幂等 upsert）
            existing = {s.stripe_id for s in db.query(StripeRecord).filter_by(task_id=job_id)}
            for sid in stripes:
                if sid not in existing:
                    db.add(StripeRecord(task_id=job_id, machine_id=machine_id,
                                        stripe_id=sid, status="idle"))
            db.commit()

            self._append_event(db, job_id, machine_id, "stripe_split",
                               {"stripe_count": stripe_count, "stripes": stripes[:500]})
            db.commit()

        # Redis 快照
        self._redis.update_task_snapshot(job_id, stripe_count=stripe_count)
        for sid in stripes:
            self._redis.set_stripe_status(job_id, sid, "idle")
        self._bus.publish("stripe_split", job_id, machine_id,
                          {"stripe_count": stripe_count, "stripes": stripes})
        logger.info("task %s split received: %d stripes", job_id, stripe_count)
        return {"ok": True}

    # ==================================================================
    # 单条带终态回调
    # ==================================================================
    def handle_stripe_status(self, machine_id: str, job_id: str,
                             stripe_id: str, status: str) -> Dict:
        """处理单条带状态上报；返回判定结果。"""
        terminal = status in TERMINAL_FAILED_STATES or status == SUCCESS_TERMINAL_STATE
        if not terminal:
            # 只处理终态（中间态由轮询/快照体现）
            logger.debug("ignore non-terminal stripe status: %s -> %s", stripe_id, status)
            return {"ok": True, "ignored": True}

        with self._session_factory() as db:
            task = (
                db.query(ExposureTask)
                .filter_by(task_id=job_id, machine_id=machine_id)
                .with_for_update()  # 行锁：串行化并发回调的计数更新
                .first()
            )
            if task is None:
                logger.warning("stripe_status for unknown task job=%s", job_id)
                return {"ok": False, "reason": "task not found"}
            if task.status in TASK_TERMINAL:
                # 任务已终结，重复/迟到事件忽略
                return {"ok": True, "reason": "task already terminal, ignored"}

            # 去重：同一终态不重复计数（行锁保证读到的 prev_status 是最新提交值）
            rec = (
                db.query(StripeRecord)
                .filter_by(task_id=job_id, stripe_id=stripe_id)
                .with_for_update()
                .first()
            )
            prev_status = rec.status if rec else None
            if prev_status == status:
                return {"ok": True, "reason": "duplicate status, ignored"}
            if rec is None:
                db.add(StripeRecord(task_id=job_id, machine_id=machine_id,
                                    stripe_id=stripe_id, status=status))
            else:
                rec.status = status
                rec.updated_at = datetime.now()

            # 更新计数
            if status == SUCCESS_TERMINAL_STATE:
                task.done_count += 1
            else:
                task.error_count += 1

            self._append_event(db, job_id, machine_id, "stripe_status",
                               {"stripe_id": stripe_id, "status": status})
            db.commit()

            done, failed = task.done_count, task.error_count
            total = task.stripe_count

        # Redis 快照
        self._redis.set_stripe_status(job_id, stripe_id, status)
        self._redis.update_task_snapshot(job_id, done_count=done, error_count=failed)
        self._bus.publish("stripe_status", job_id, machine_id,
                          {"stripe_id": stripe_id, "status": status,
                           "done_count": done, "error_count": failed})

        # 完成判定
        if failed > 0:
            self._fail_task(job_id, machine_id, f"stripe {stripe_id} failed: {status}")
            return {"ok": True, "verdict": "failed"}
        if total > 0 and done >= total:
            self._complete_task(job_id, machine_id)
            return {"ok": True, "verdict": "completed"}
        return {"ok": True, "verdict": "running"}

    # ==================================================================
    # 轮询兜底：整机状态快照（get-status 结果）统一入口
    # ==================================================================
    def handle_status_snapshot(self, machine_id: str, job_id: str,
                               groups: Dict[str, List[str]]) -> Dict:
        """根据 get-status 返回的分组快照，增量补齐终态条带（防止回调丢失）。"""
        results = []
        for state in ("done",) + tuple(sorted(TERMINAL_FAILED_STATES)):
            for sid in groups.get(state, []):
                results.append(self.handle_stripe_status(machine_id, job_id, sid, state))
        return {"ok": True, "processed": results}

    # ==================================================================
    # 任务终结
    # ==================================================================
    def _complete_task(self, job_id: str, machine_id: str) -> None:
        with self._session_factory() as db:
            task = (
                db.query(ExposureTask)
                .filter_by(task_id=job_id)
                .with_for_update()
                .first()
            )
            if task is None or task.status in TASK_TERMINAL:
                return
            task.status = "completed"
            task.phase = "done"
            task.finished_at = datetime.now()
            self._append_event(db, job_id, machine_id, "task_completed", {"stripe_count": task.stripe_count})
            db.commit()
        self._redis.update_task_snapshot(job_id, status="completed", phase="done")
        self._bus.publish("task_completed", job_id, machine_id,
                          {"stripe_count": self._get_count(job_id, "stripe_count")})
        logger.info("task %s completed", job_id)

    def _fail_task(self, job_id: str, machine_id: str, reason: str) -> None:
        with self._session_factory() as db:
            task = (
                db.query(ExposureTask)
                .filter_by(task_id=job_id)
                .with_for_update()
                .first()
            )
            if task is None or task.status in TASK_TERMINAL:
                return
            task.status = "failed"
            task.error_msg = reason
            task.finished_at = datetime.now()
            self._append_event(db, job_id, machine_id, "task_failed", {"reason": reason})
            db.commit()
        self._redis.update_task_snapshot(job_id, status="failed", error_msg=reason)
        self._bus.publish("task_failed", job_id, machine_id, {"reason": reason})
        logger.warning("task %s failed: %s", job_id, reason)
        # 终止机器侧剩余曝光
        self._try_stop_machine(job_id, machine_id)

    def mark_stopped(self, job_id: str, machine_id: str, reason: str = "user stopped") -> None:
        with self._session_factory() as db:
            task = db.query(ExposureTask).filter_by(task_id=job_id).first()
            if task is None or task.status in TASK_TERMINAL:
                return
            task.status = "stopped"
            task.error_msg = reason
            task.finished_at = datetime.now()
            self._append_event(db, job_id, machine_id, "task_stopped", {"reason": reason})
            db.commit()
        self._redis.update_task_snapshot(job_id, status="stopped", error_msg=reason)
        self._bus.publish("task_stopped", job_id, machine_id, {"reason": reason})

    # ---------- 辅助 ----------
    def _try_stop_machine(self, job_id: str, machine_id: str) -> None:
        """失败时通知机器停止曝光（best-effort）。"""
        from services.machine_store import get_machine_base_url
        base_url = get_machine_base_url(self._session_factory, machine_id)
        if not base_url:
            return
        try:
            PSClient(base_url).stop_exposure()
            logger.info("sent stop-exposure to %s for task %s", machine_id, job_id)
        except Exception as exc:
            logger.warning("stop-exposure to %s failed: %s", machine_id, exc)

    def _get_count(self, job_id: str, field: str) -> int:
        snap = self._redis.get_task_snapshot(job_id)
        if snap:
            return int(snap.get(field, 0))
        return 0

    @staticmethod
    def _append_event(db: Session, job_id: str, machine_id: str,
                      event_type: str, payload: Optional[Dict]) -> None:
        db.add(TaskEventLog(task_id=job_id, machine_id=machine_id,
                            event_type=event_type, payload=payload))
