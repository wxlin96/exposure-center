# -*- coding: utf-8 -*-
"""机器管理接口。"""
from datetime import datetime

from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session

from database import get_db
from models import Machine
from schemas import MachineCreate, MachineOut


def make_machine_router():
    router = APIRouter(prefix="/api/v1/machines", tags=["machines"])

    @router.post("", response_model=MachineOut)
    def register_machine(body: MachineCreate, db: Session = Depends(get_db)):
        row = db.query(Machine).filter_by(machine_id=body.machine_id).first()
        if row:
            row.base_url = body.base_url
            if body.name:
                row.name = body.name
            db.commit()
            return row
        row = Machine(machine_id=body.machine_id, name=body.name, base_url=body.base_url,
                      status="offline")
        db.add(row)
        db.commit()
        db.refresh(row)
        return row

    @router.get("", response_model=list[MachineOut])
    def list_machines(db: Session = Depends(get_db)):
        return db.query(Machine).order_by(Machine.machine_id).all()

    @router.get("/{machine_id}", response_model=MachineOut)
    def get_machine(machine_id: str, db: Session = Depends(get_db)):
        row = db.query(Machine).filter_by(machine_id=machine_id).first()
        if row is None:
            raise HTTPException(status_code=404, detail="machine not found")
        return row

    @router.get("/{machine_id}/about")
    def get_machine_about(machine_id: str, db: Session = Depends(get_db)):
        """机器参数：代理调用机器 get-about 返回（前端机器参数面板）。"""
        from services.ps_client import PSClient
        row = db.query(Machine).filter_by(machine_id=machine_id).first()
        if row is None:
            raise HTTPException(status_code=404, detail="machine not found")
        try:
            return PSClient(row.base_url).get_about()
        except Exception as exc:  # noqa: BLE001
            raise HTTPException(status_code=502, detail=f"machine unreachable: {exc}")

    return router
