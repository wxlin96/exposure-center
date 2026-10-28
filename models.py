# -*- coding: utf-8 -*-
"""ORM 模型：机器、曝光任务、条带、事件流水。"""
from datetime import datetime

from sqlalchemy import (
    JSON,
    Column,
    DateTime,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.sql import func

from database import Base


class Machine(Base):
    """远程机器（PS Exposure Service 实例）。"""

    __tablename__ = "machine"

    id = Column(Integer, primary_key=True, autoincrement=True)
    machine_id = Column(String(64), nullable=False, unique=True, comment="机器唯一标识")
    name = Column(String(128), nullable=True, comment="机器名称")
    base_url = Column(String(256), nullable=False, comment="PS 服务 Base URL，如 http://10.0.0.5:8000")
    status = Column(String(16), default="offline", comment="online / offline")
    last_seen_at = Column(DateTime, nullable=True, comment="最近一次成功交互时间")
    created_at = Column(DateTime, server_default=func.now())
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())


class ExposureTask(Base):
    """曝光任务（job 级）。task_id 即远程机器的 job_id。"""

    __tablename__ = "exposure_task"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(64), nullable=False, unique=True, comment="任务ID = 机器侧 job_id")
    machine_id = Column(String(64), nullable=False, comment="执行机器")
    # 中心侧任务状态机
    # pending -> configuring -> split_issued -> ric_uploading(可选) -> starting -> exposing
    #         -> completed / failed / stopped
    status = Column(String(32), nullable=False, default="pending", index=True)
    # 机器侧 job phase：none / configured / split / ready / exposing / done / stopped
    phase = Column(String(32), nullable=True)
    stripe_count = Column(Integer, default=0, comment="条带总数（切分回调/轮询确认后填充）")
    done_count = Column(Integer, default=0, comment="已完成条带数")
    error_count = Column(Integer, default=0, comment="失败条带数")
    ric_required = Column(Integer, default=0, comment="是否需要上传 RIC MAP")
    ric_filename = Column(String(256), nullable=True)
    error_msg = Column(Text, nullable=True, comment="失败/停止原因")
    created_at = Column(DateTime, server_default=func.now())
    started_at = Column(DateTime, nullable=True, comment="start-exposure 成功时刻")
    finished_at = Column(DateTime, nullable=True, comment="任务终结时刻")

    __table_args__ = (
        Index("idx_task_machine", "machine_id"),
    )


class StripeRecord(Base):
    """条带记录：任务与条带状态明细。"""

    __tablename__ = "stripe_record"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(64), nullable=False)
    machine_id = Column(String(64), nullable=False)
    stripe_id = Column(String(128), nullable=False, comment="如 job-001_stripe_0000")
    status = Column(String(32), nullable=False, default="idle")
    updated_at = Column(DateTime, server_default=func.now(), onupdate=func.now())

    __table_args__ = (
        UniqueConstraint("task_id", "stripe_id", name="uk_task_stripe"),
        Index("idx_stripe_task", "task_id"),
    )


class TaskEventLog(Base):
    """任务事件流水（审计/回放）。"""

    __tablename__ = "task_event_log"

    id = Column(Integer, primary_key=True, autoincrement=True)
    task_id = Column(String(64), nullable=False)
    machine_id = Column(String(64), nullable=False)
    event_type = Column(String(64), nullable=False)
    payload = Column(JSON, nullable=True)
    created_at = Column(DateTime, server_default=func.now())

    __table_args__ = (
        Index("idx_event_task", "task_id"),
    )
