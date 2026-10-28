# -*- coding: utf-8 -*-
"""机器信息访问辅助（避免路由与业务互相依赖）。"""
from typing import Optional

from sqlalchemy.orm import Session

from models import Machine


def get_machine_base_url(session_factory, machine_id: str) -> Optional[str]:
    with session_factory() as db:
        row = db.query(Machine).filter_by(machine_id=machine_id).first()
        return row.base_url if row else None


def touch_machine(session_factory, machine_id: str) -> None:
    """标记机器在线（最近一次成功交互）。"""
    from datetime import datetime
    with session_factory() as db:
        row = db.query(Machine).filter_by(machine_id=machine_id).first()
        if row:
            row.status = "online"
            row.last_seen_at = datetime.now()
            db.commit()
