# -*- coding: utf-8 -*-
"""Pydantic 请求/响应模型。"""
from datetime import datetime
from typing import Dict, List, Optional

from pydantic import BaseModel, Field


# ---------- 机器 ----------
class MachineCreate(BaseModel):
    machine_id: str = Field(..., min_length=1, max_length=64, description="机器唯一标识")
    name: Optional[str] = None
    base_url: str = Field(..., description="PS 服务 Base URL，如 http://10.0.0.5:8000")


class MachineOut(BaseModel):
    machine_id: str
    name: Optional[str] = None
    base_url: str
    status: str
    last_seen_at: Optional[datetime] = None

    class Config:
        from_attributes = True


# ---------- 任务 ----------
class RicInfo(BaseModel):
    """RIC MAP（可选）。content 为 base64 编码的文件字节。"""

    filename: str = "ric.map"
    content_b64: str = Field(..., description="RIC MAP 文件内容的 base64 编码")
    crc32: Optional[str] = Field(None, description="可省略，服务端自动计算")


class TaskCreate(BaseModel):
    machine_id: str = Field(..., min_length=1, max_length=64)
    job_id: str = Field(..., min_length=1, max_length=64, description="即机器侧 job_id")
    ric: Optional[RicInfo] = None


class TaskOut(BaseModel):
    task_id: str
    machine_id: str
    status: str
    phase: Optional[str] = None
    stripe_count: int = 0
    done_count: int = 0
    error_count: int = 0
    ric_required: int = 0
    error_msg: Optional[str] = None
    created_at: Optional[str] = None
    started_at: Optional[str] = None
    finished_at: Optional[str] = None


class StripeOut(BaseModel):
    stripe_id: str
    status: str
    updated_at: Optional[str] = None


class TaskDetailOut(TaskOut):
    stripes: List[StripeOut] = []


# ---------- 回调 ----------
class StripeSplitCallback(BaseModel):
    """机器侧条带切分完成回调。"""

    machine_id: str
    job_id: str
    stripe_count: int
    stripes: List[str] = []
    ts: Optional[float] = None


class StripeStatusCallback(BaseModel):
    """机器侧单条带终态回调（done / error / disk / run_failed / load_failed / trans_failed）。"""

    machine_id: str
    job_id: str
    stripe_id: str
    status: str
    ts: Optional[float] = None


# ---------- 通用 ----------
class ApiResponse(BaseModel):
    code: int = 0
    msg: str = "ok"
    data: Optional[Dict] = None
