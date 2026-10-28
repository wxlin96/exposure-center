# -*- coding: utf-8 -*-
"""远程机器（PS Exposure Service）API 客户端。

对接 API.md 的 7 个接口：
  GET  /get-about
  POST /set-job
  POST /stripe-split
  POST /set-ric          (multipart)
  POST /start-exposure
  POST /get-status
  GET  /stop-exposure

统一响应格式 {code, msg, ...}，code == 0 成功。
"""
import zlib
from typing import Any, Dict, List, Optional, Tuple

import httpx

from config import PS_API_TIMEOUT

# 错误终态（用于任务失败判定）
TERMINAL_FAILED_STATES = {"error", "disk", "run_failed", "load_failed", "trans_failed"}
SUCCESS_TERMINAL_STATE = "done"


class PSClientError(Exception):
    """远程机器调用失败（网络/非0 code）。"""

    def __init__(self, message: str, code: Optional[int] = None, raw: Any = None):
        super().__init__(message)
        self.code = code
        self.raw = raw


class PSClient:
    """单台机器的 PS 服务客户端。"""

    def __init__(self, base_url: str, timeout: float = PS_API_TIMEOUT):
        self.base_url = base_url.rstrip("/")
        self.timeout = timeout

    # ---------- 底层 ----------
    def _post_json(self, path: str, body: Optional[dict] = None) -> Dict[str, Any]:
        with httpx.Client(base_url=self.base_url, timeout=self.timeout) as c:
            resp = c.post(path, json=body or {})
            resp.raise_for_status()
            return resp.json()

    def _get(self, path: str) -> Dict[str, Any]:
        with httpx.Client(base_url=self.base_url, timeout=self.timeout) as c:
            resp = c.get(path)
            resp.raise_for_status()
            return resp.json()

    @staticmethod
    def _check(data: Dict[str, Any]) -> None:
        if data.get("code") != 0:
            raise PSClientError(
                data.get("msg", "unknown error"),
                code=data.get("code"),
                raw=data,
            )

    # ---------- 业务接口 ----------
    def get_about(self) -> Dict[str, Any]:
        return self._get("/get-about")

    def set_job(self, job_id: str) -> Dict[str, Any]:
        data = self._post_json("/set-job", {"job_id": job_id})
        self._check(data)
        return data

    def stripe_split(self, job_id: str = "") -> Dict[str, Any]:
        data = self._post_json("/stripe-split", {"job_id": job_id})
        self._check(data)
        return data

    def set_ric(self, job_id: str, filename: str, content: bytes,
                crc32: Optional[str] = None) -> Dict[str, Any]:
        """上传 RIC MAP。crc32 缺省时自动计算。"""
        if crc32 is None:
            crc32 = f"{zlib.crc32(content):08x}"
        with httpx.Client(base_url=self.base_url, timeout=self.timeout) as c:
            resp = c.post(
                "/set-ric",
                data={"job_id": job_id, "crc32": crc32, "map": filename},
                files={"file": (filename, content)},
            )
            resp.raise_for_status()
            data = resp.json()
        self._check(data)
        return data

    def start_exposure(self, job_id: str = "") -> Dict[str, Any]:
        data = self._post_json("/start-exposure", {"job_id": job_id})
        self._check(data)
        return data

    def get_status(self, job_id: str = "") -> Tuple[Dict[str, Any], Dict[str, List[str]]]:
        """查询条带状态，返回 (原始响应, 按状态分组的条带列表)。"""
        data = self._post_json("/get-status", {"job_id": job_id})
        self._check(data)
        groups = {k: v for k, v in data.items() if isinstance(v, list)}
        return data, groups

    def stop_exposure(self) -> Dict[str, Any]:
        return self._get("/stop-exposure")
