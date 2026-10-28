# -*- coding: utf-8 -*-
"""曝光任务管理接口。"""
from fastapi import APIRouter, Depends, HTTPException, Request
from sqlalchemy.orm import Session

from database import get_db
from models import ExposureTask, StripeRecord
from schemas import StripeOut, TaskCreate, TaskDetailOut, TaskOut


def _serialize_task(t: ExposureTask) -> dict:
    return {
        "task_id": t.task_id,
        "machine_id": t.machine_id,
        "status": t.status,
        "phase": t.phase,
        "stripe_count": t.stripe_count,
        "done_count": t.done_count,
        "error_count": t.error_count,
        "ric_required": t.ric_required,
        "error_msg": t.error_msg,
        "created_at": t.created_at.strftime("%Y-%m-%d %H:%M:%S") if t.created_at else None,
        "started_at": t.started_at.strftime("%Y-%m-%d %H:%M:%S") if t.started_at else None,
        "finished_at": t.finished_at.strftime("%Y-%m-%d %H:%M:%S") if t.finished_at else None,
    }


def make_task_router():
    router = APIRouter(prefix="/api/v1/tasks", tags=["tasks"])

    @router.post("")
    def create_task(body: TaskCreate, request: Request, db: Session = Depends(get_db)):
        engine = request.app.state.engine
        ric = None
        if body.ric:
            ric = {"filename": body.ric.filename, "content_b64": body.ric.content_b64,
                   "crc32": body.ric.crc32}
        result = engine.create_task(body.machine_id, body.job_id, ric)
        if not result["ok"]:
            raise HTTPException(status_code=409, detail=result["reason"])
        return {"task_id": result["task_id"], "msg": "task created, orchestrating"}

    @router.get("", response_model=list[TaskOut])
    def list_tasks(db: Session = Depends(get_db)):
        return [_serialize_task(t) for t in
                db.query(ExposureTask).order_by(ExposureTask.id.desc()).limit(200).all()]

    @router.get("/{task_id}", response_model=TaskDetailOut)
    def get_task(task_id: str, db: Session = Depends(get_db)):
        t = db.query(ExposureTask).filter_by(task_id=task_id).first()
        if t is None:
            raise HTTPException(status_code=404, detail="task not found")
        out = _serialize_task(t)
        stripes = db.query(StripeRecord).filter_by(task_id=task_id).order_by(StripeRecord.id).all()
        out["stripes"] = [StripeOut(stripe_id=s.stripe_id, status=s.status,
                                    updated_at=s.updated_at.strftime("%Y-%m-%d %H:%M:%S")
                                    if s.updated_at else None)
                          for s in stripes]
        return out

    @router.post("/{task_id}/stop")
    def stop_task(task_id: str, request: Request):
        engine = request.app.state.engine
        result = engine.stop_task(task_id)
        if not result["ok"]:
            raise HTTPException(status_code=400, detail=result["reason"])
        return {"task_id": task_id, "msg": "stop requested"}

    return router
