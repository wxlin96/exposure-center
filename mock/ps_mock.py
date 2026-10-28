# -*- coding: utf-8 -*-
"""PS 曝光服务模拟器（远程机器侧）。

实现 API.md 的全部接口，并额外支持向中心平台推送两类回调：
  1. 条带切分完成：POST {CALLBACK_URL}/api/v1/callbacks/stripe-split
  2. 单条带终态：  POST {CALLBACK_URL}/api/v1/callbacks/stripe-status

用途：本地端到端验证中心平台（接收桩判定任务完成情况）。真实机器按
docs/CALLBACK_PROTOCOL.md 实现同样的回调即可对接。

启动示例：
  python mock/ps_mock.py --port 8001 --machine-id ps-01 \
      --callback-url http://127.0.0.1:9000 --failure-rate 0.0
"""
import argparse
import logging
import os
import random
import threading
import time
import zlib
from enum import Enum
from pathlib import Path
from typing import Dict, List, Optional, Tuple

import httpx
import uvicorn
from fastapi import FastAPI, File, Form
from pydantic import BaseModel

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s [mock] %(message)s")
logger = logging.getLogger("ps_mock")


class JobPhase(str, Enum):
    NONE = "none"
    CONFIGURED = "configured"
    SPLIT = "split"
    READY = "ready"
    EXPOSING = "exposing"
    DONE = "done"
    STOPPED = "stopped"


# 中间态与终态
_ACTIVE = {"idle", "ready", "pending", "loading", "running", "transfering"}
_TERMINAL = {"done", "error", "disk", "run_failed", "load_failed", "trans_failed"}

ALL_STATES = (
    "idle", "pending", "done", "ready", "running", "transfering",
    "error", "disk", "loading", "run_failed", "load_failed", "trans_failed",
)


class PsMock:
    """模拟一台 PS 曝光机器，线程推进条带状态机，终态触发回调。"""

    def __init__(self, machine_id: str, callback_url: str = "", callback_token: str = "",
                 stripe_count: int = 10, max_loading: int = 4,
                 load_time: float = 0.2, run_time: float = 0.5, transfer_time: float = 0.3,
                 tick: float = 0.05, failure_rate: float = 0.0,
                 ric_dir: str = "data/ric"):
        self.machine_id = machine_id
        self.callback_url = callback_url.rstrip("/")
        self.callback_token = callback_token
        self.stripe_count = stripe_count
        self.max_loading = max_loading
        self.load_time = load_time
        self.run_time = run_time
        self.transfer_time = transfer_time
        self.tick = tick
        self.failure_rate = failure_rate
        self.ric_dir = Path(ric_dir)

        self._lock = threading.RLock()
        self._job_id: Optional[str] = None
        self._phase = JobPhase.NONE
        self._stripes: Dict[str, str] = {}
        self._ts: Dict[str, float] = {}
        self._worker: Optional[threading.Thread] = None
        self._stop_evt = threading.Event()
        self._notified: set = set()  # 已推送过终态回调的条带

    # ---------------- 回调 ----------------
    def _push_callback(self, event: str, payload: dict) -> None:
        if not self.callback_url:
            return
        url = f"{self.callback_url}/api/v1/callbacks/{event}"
        headers = {"X-Callback-Token": self.callback_token} if self.callback_token else {}
        try:
            resp = httpx.post(url, json=payload, headers=headers, timeout=5.0)
            logger.info("callback %s -> %d (%s)", url, resp.status_code, payload.get("stripe_id", ""))
        except Exception as exc:  # noqa: BLE001
            logger.warning("callback failed: %s", exc)

    def _notify_split(self) -> None:
        if not self.callback_url:
            return
        with self._lock:
            job_id, stripes = self._job_id, list(self._stripes.keys())
        threading.Thread(
            target=self._push_callback,
            args=("stripe-split", {
                "machine_id": self.machine_id, "job_id": job_id,
                "stripe_count": len(stripes), "stripes": stripes,
                "ts": time.time(),
            }),
            daemon=True,
        ).start()

    def _notify_stripe(self, stripe_id: str, status: str) -> None:
        if not self.callback_url:
            return
        threading.Thread(
            target=self._push_callback,
            args=("stripe-status", {
                "machine_id": self.machine_id, "job_id": self._job_id,
                "stripe_id": stripe_id, "status": status,
                "ts": time.time(),
            }),
            daemon=True,
        ).start()

    # ---------------- 业务方法 ----------------
    def get_about(self) -> dict:
        return {
            "code": 0, "msg": "ok",
            "ps_name": "PS Exposure Service",
            "ps_version": os.environ.get("PS_SIM_VERSION", f"PS-Mock-{self.machine_id}"),
            "api_version": "1.0",
            "stripe_count": self.stripe_count,
            "max_concurrent_loading": self.max_loading,
            "failure_rate": self.failure_rate,
        }

    def set_job(self, job_id: str) -> Tuple[int, str]:
        with self._lock:
            if self._phase == JobPhase.EXPOSING:
                return 1003, "曝光进行中，禁止切换 job"
            self._job_id = job_id
            self._phase = JobPhase.CONFIGURED
            self._stripes.clear()
            self._ts.clear()
            self._notified.clear()
            return 0, "ok"

    def stripe_split(self, job_id: str = "") -> Tuple[int, str]:
        with self._lock:
            if self._phase == JobPhase.NONE:
                return 1001, "未设置 job"
            jid = self._job_id or ""
            if job_id and job_id != jid:
                return 1002, f"job_id 不匹配：当前 job 为 {jid}"
            if self._phase == JobPhase.EXPOSING:
                return 1003, "曝光进行中，禁止重新切分"
            self._stripes = {f"{jid}_stripe_{i:04d}": "idle" for i in range(self.stripe_count)}
            self._ts = {n: time.monotonic() for n in self._stripes}
            self._phase = JobPhase.SPLIT
        # 切分完成 → 主动通知中心
        self._notify_split()
        return 0, f"条带切分完成，共 {self.stripe_count} 条"

    def set_ric(self, job_id: str, crc32: str, map_name: str, file_bytes: bytes) -> Tuple[int, str]:
        with self._lock:
            if self._phase == JobPhase.NONE:
                return 1001, "未设置 job"
            if job_id and job_id != self._job_id:
                return 1002, "job_id 不匹配"
            if self._phase == JobPhase.EXPOSING:
                return 1003, "曝光进行中，禁止更新 RIC MAP"
            if not file_bytes:
                return 1007, "未上传 RIC MAP 文件"
            computed = zlib.crc32(file_bytes)
            if f"{computed:08x}" not in (crc32.strip().lower().replace("0x", ""), str(computed)):
                return 1006, f"crc32 校验失败：期望 {crc32!r}，实际 {computed:08x}"
            d = self.ric_dir / (job_id or "default")
            try:
                d.mkdir(parents=True, exist_ok=True)
                (d / (map_name or "ric.map")).write_bytes(file_bytes)
            except OSError as exc:
                return 1009, f"RIC MAP 落盘失败：{exc}"
            if self._phase in (JobPhase.SPLIT, JobPhase.READY):
                for n, st in self._stripes.items():
                    if st == "idle":
                        self._stripes[n] = "ready"
                        self._ts[n] = time.monotonic()
                self._phase = JobPhase.READY
            return 0, "RIC MAP 上传成功"

    def start_exposure(self, job_id: str = "") -> Tuple[int, str]:
        with self._lock:
            if self._phase == JobPhase.NONE:
                return 1001, "未设置 job"
            if job_id and job_id != self._job_id:
                return 1002, "job_id 不匹配"
            if self._phase in (JobPhase.EXPOSING, JobPhase.DONE):
                return 1004, "已曝光，不允许再次启动曝光"
            if not self._stripes:
                return 1005, "尚未执行条带切分"
            for n, st in self._stripes.items():
                if st in ("idle", "ready"):
                    self._stripes[n] = "pending"
                    self._ts[n] = time.monotonic()
            self._phase = JobPhase.EXPOSING
            self._stop_evt.clear()
            if self._worker is None or not self._worker.is_alive():
                self._worker = threading.Thread(target=self._run, daemon=True)
                self._worker.start()
            return 0, "曝光已启动"

    def get_status(self, job_id: str = "") -> Tuple[int, str, dict]:
        groups = {st: [] for st in ALL_STATES}
        with self._lock:
            if self._phase == JobPhase.NONE:
                return 1001, "未设置 job", groups
            if job_id and job_id != self._job_id:
                return 1002, "job_id 不匹配", groups
            for n, st in self._stripes.items():
                groups[st].append(n)
        return 0, "ok", groups

    def stop_exposure(self) -> Tuple[int, str]:
        with self._lock:
            if self._phase != JobPhase.EXPOSING:
                return 1008, "当前没有进行中的曝光"
            self._phase = JobPhase.STOPPED
            self._stop_evt.set()
            return 0, "曝光已停止"

    # ---------------- 状态机线程 ----------------
    def _maybe_fail(self, stage: str) -> Optional[str]:
        if self.failure_rate <= 0 or random.random() >= self.failure_rate:
            return None
        cands = {
            "loading": ["load_failed", "error", "disk"],
            "running": ["run_failed", "error", "disk"],
            "transfering": ["trans_failed", "error", "disk"],
        }
        return random.choice(cands[stage])

    def _run(self) -> None:
        try:
            while not self._stop_evt.is_set():
                with self._lock:
                    if self._phase != JobPhase.EXPOSING:
                        break
                    now = time.monotonic()
                    loading = [n for n, s in self._stripes.items() if s == "loading"]
                    for n, st in list(self._stripes.items()):
                        if st == "pending" and len(loading) < self.max_loading:
                            self._stripes[n] = "loading"
                            self._ts[n] = now
                            loading.append(n)
                    for n, st in list(self._stripes.items()):
                        if st == "loading" and now - self._ts[n] >= self.load_time:
                            self._stripes[n] = self._maybe_fail("loading") or "running"
                            self._ts[n] = now
                    for n, st in list(self._stripes.items()):
                        if st == "running" and now - self._ts[n] >= self.run_time:
                            self._stripes[n] = self._maybe_fail("running") or "transfering"
                            self._ts[n] = now
                    for n, st in list(self._stripes.items()):
                        if st == "transfering" and now - self._ts[n] >= self.transfer_time:
                            self._stripes[n] = self._maybe_fail("transfering") or "done"
                            self._ts[n] = now
                    # 终态触发回调
                    for n, st in self._stripes.items():
                        if st in _TERMINAL and n not in self._notified:
                            self._notified.add(n)
                            self._notify_stripe(n, st)
                    if self._stripes and all(s in _TERMINAL for s in self._stripes.values()):
                        self._phase = JobPhase.DONE
                        self._stop_evt.set()
                        break
                time.sleep(self.tick)
        finally:
            pass


def make_app(mock: PsMock) -> FastAPI:
    app = FastAPI(title="PS Mock", version="1.0")

    @app.get("/get-about")
    def get_about():
        return mock.get_about()

    class JobBody(BaseModel):
        job_id: str = ""

    @app.post("/set-job")
    def set_job(body: JobBody):
        code, msg = mock.set_job(body.job_id)
        return {"code": code, "msg": msg}

    @app.post("/stripe-split")
    def stripe_split(body: JobBody):
        code, msg = mock.stripe_split(body.job_id)
        return {"code": code, "msg": msg}

    @app.post("/set-ric")
    def set_ric(job_id: str = Form(""), crc32: str = Form(""), map: str = Form("ric.map"),
                file: bytes = File(None)):
        code, msg = mock.set_ric(job_id, crc32, map, file or b"")
        return {"code": code, "msg": msg}

    @app.post("/start-exposure")
    def start_exposure(body: JobBody):
        code, msg = mock.start_exposure(body.job_id)
        return {"code": code, "msg": msg}

    @app.post("/get-status")
    def get_status(body: JobBody):
        code, msg, groups = mock.get_status(body.job_id)
        out = {"code": code, "msg": msg}
        out.update(groups)
        return out

    @app.get("/stop-exposure")
    def stop_exposure():
        code, msg = mock.stop_exposure()
        return {"code": code, "msg": msg}

    return app


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--port", type=int, default=8000)
    ap.add_argument("--machine-id", default="ps-01")
    ap.add_argument("--callback-url", default="", help="中心平台地址，如 http://127.0.0.1:9000")
    ap.add_argument("--callback-token", default="")
    ap.add_argument("--stripe-count", type=int, default=10)
    ap.add_argument("--max-loading", type=int, default=4)
    ap.add_argument("--load-time", type=float, default=0.2)
    ap.add_argument("--run-time", type=float, default=0.5)
    ap.add_argument("--transfer-time", type=float, default=0.3)
    ap.add_argument("--failure-rate", type=float, default=0.0)
    args = ap.parse_args()

    mock = PsMock(
        machine_id=args.machine_id,
        callback_url=args.callback_url,
        callback_token=args.callback_token,
        stripe_count=args.stripe_count,
        max_loading=args.max_loading,
        load_time=args.load_time,
        run_time=args.run_time,
        transfer_time=args.transfer_time,
        failure_rate=args.failure_rate,
    )
    app = make_app(mock)
    logger.info("PS mock %s listening on :%d (callback=%s, failure=%.2f)",
                args.machine_id, args.port, args.callback_url or "none", args.failure_rate)
    uvicorn.run(app, host="127.0.0.1", port=args.port, log_level="warning")


if __name__ == "__main__":
    main()
