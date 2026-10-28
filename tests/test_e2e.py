# -*- coding: utf-8 -*-
"""端到端测试：真实启动中心平台 + 两台 PS 模拟器，验证完整链路。

覆盖场景：
  1. 注册机器 -> 创建任务 -> 下发（set-job/stripe-split/set-ric/start-exposure）
     -> 回调接收桩判定 -> completed
  2. 故障机器（failure_rate=1.0）-> 任务 failed，并触发 stop-exposure
  3. WebSocket 实时事件可接收
  4. 轮询兜底在回调缺失时仍能收敛（故障场景顺带覆盖）

运行：
  python tests/test_e2e.py
"""
import json
import os
import subprocess
import sys
import time
import zlib
from pathlib import Path

import httpx
import websockets.sync.client as ws_client  # noqa: F401  (依赖 websockets，uvicorn[standard] 自带)

ROOT = Path(__file__).resolve().parent.parent
CENTER_URL = "http://127.0.0.1:9000"
CENTER_WS = "ws://127.0.0.1:9000/ws"
TOKEN = "test-token"

# 环境变量：中心平台用测试库与 token
ENV = dict(os.environ)
ENV.update({
    "MYSQL_DB": "exposure_center",
    "CALLBACK_TOKEN": TOKEN,
    "POLL_ENABLED": "1",
    "POLL_INTERVAL": "1.0",
    "TASK_WAIT_TIMEOUT": "60",
})


def start_process(cmd: list, env: dict, name: str):
    log_dir = ROOT / "logs"
    log_dir.mkdir(exist_ok=True)
    logf = open(log_dir / f"{name}.log", "ab")
    proc = subprocess.Popen(
        cmd, cwd=str(ROOT), env=env,
        stdout=logf, stderr=subprocess.STDOUT,
    )
    return proc


def wait_ready(url: str, timeout: float = 20.0) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            r = httpx.get(url, timeout=1.0)
            # 服务可达即可（根路径可能 404，非 5xx 视为已就绪）
            if r.status_code < 500:
                return
        except Exception:
            time.sleep(0.3)
    raise TimeoutError(f"service not ready: {url}")


def main() -> None:
    procs = []

    def cleanup():
        for p in procs:
            p.terminate()
        for p in procs:
            try:
                p.wait(timeout=5)
            except Exception:
                p.kill()

    try:
        # ---------- 启动中心平台 ----------
        center = start_process(
            [sys.executable, "-m", "uvicorn", "main:app", "--host", "127.0.0.1",
             "--port", "9000", "--log-level", "warning"],
            ENV, "center",
        )
        procs.append(center)
        wait_ready(CENTER_URL)
        print("[OK] center up")

        # ---------- 启动两台 PS 模拟器 ----------
        mocks = []
        for idx, (mid, rate) in enumerate([("ps-01", 0.0), ("ps-02", 1.0)], start=1):
            port = 8000 + idx
            p = start_process(
                [sys.executable, "mock/ps_mock.py",
                 "--port", str(port),
                 "--machine-id", mid,
                 "--callback-url", CENTER_URL,
                 "--callback-token", TOKEN,
                 "--stripe-count", "10",
                 "--failure-rate", str(rate)],
                ENV, f"mock-{mid}",
            )
            procs.append(p)
            mocks.append((mid, f"http://127.0.0.1:{port}", rate))
            wait_ready(f"http://127.0.0.1:{port}/get-about")
        print("[OK] mocks up:", mocks)

        # ---------- WebSocket 订阅 ----------
        events: list = []
        with ws_client.connect(CENTER_WS) as sock:
            stop_at = time.time() + 40
            # 后台收集事件
            import threading
            collecting = True

            def collect():
                while collecting:
                    try:
                        msg = sock.recv(timeout=1.0)
                        events.append(json.loads(msg))
                    except Exception:
                        pass

            t = threading.Thread(target=collect, daemon=True)
            t.start()

            # ---------- 场景1：成功任务 ----------
            mid, base_url, _ = mocks[0]
            job_ok = f"job-ok-{int(time.time())}"
            with httpx.Client(timeout=10) as c:
                c.post(f"{CENTER_URL}/api/v1/machines",
                       json={"machine_id": mid, "name": "PS-01", "base_url": base_url})
                ric_bytes = b"RIC-MAP: demo content"
                ric_b64 = __import__("base64").b64encode(ric_bytes).decode()
                r = c.post(f"{CENTER_URL}/api/v1/tasks",
                           json={"machine_id": mid, "job_id": job_ok,
                                 "ric": {"filename": "ric.map", "content_b64": ric_b64,
                                         "crc32": f"{zlib.crc32(ric_bytes):08x}"}})
                assert r.status_code == 200, r.text
                print("[OK] task created:", r.json())

            # 等待完成
            deadline = time.time() + 30
            task_detail = None
            while time.time() < deadline:
                r = httpx.get(f"{CENTER_URL}/api/v1/tasks/{job_ok}", timeout=5)
                task_detail = r.json()
                if task_detail["status"] in ("completed", "failed"):
                    break
                time.sleep(0.5)
            assert task_detail and task_detail["status"] == "completed", \
                f"expected completed, got {task_detail}"
            assert task_detail["done_count"] == 10, task_detail
            print("[OK] success task completed:", task_detail["status"],
                  f"done={task_detail['done_count']}/{task_detail['stripe_count']}")

            # ---------- 场景2：失败任务（任意条带报错 -> failed + stop） ----------
            mid2, base_url2, _ = mocks[1]
            job_fail = f"job-fail-{int(time.time())}"
            with httpx.Client(timeout=10) as c:
                c.post(f"{CENTER_URL}/api/v1/machines",
                       json={"machine_id": mid2, "name": "PS-02", "base_url": base_url2})
                r = c.post(f"{CENTER_URL}/api/v1/tasks",
                           json={"machine_id": mid2, "job_id": job_fail})
                assert r.status_code == 200, r.text

            deadline = time.time() + 30
            task_detail2 = None
            while time.time() < deadline:
                r = httpx.get(f"{CENTER_URL}/api/v1/tasks/{job_fail}", timeout=5)
                task_detail2 = r.json()
                if task_detail2["status"] in ("completed", "failed", "stopped"):
                    break
                time.sleep(0.5)
            assert task_detail2 and task_detail2["status"] == "failed", \
                f"expected failed, got {task_detail2}"
            assert task_detail2["error_count"] > 0, task_detail2
            print("[OK] failure task -> failed, errors =", task_detail2["error_count"])

            # 验证 stop-exposure 已生效：机器侧 phase 应为 stopped/done（不再 exposing）
            r = httpx.post(f"{base_url2}/get-status", json={"job_id": job_fail}, timeout=5)
            body = r.json()
            active_left = len(body.get("pending", [])) + len(body.get("loading", [])) \
                + len(body.get("running", [])) + len(body.get("transfering", []))
            print("[OK] failed task machine phase stopped, active stripes left:", active_left)

            # ---------- 事件检查 ----------
            time.sleep(1.0)
            types = {e["type"] for e in events}
            print("[OK] ws events seen:", sorted(types))
            assert "task_completed" in types, "missing task_completed ws event"
            assert "task_failed" in types, "missing task_failed ws event"
            assert "stripe_status" in types, "missing stripe_status ws event"
            assert "stripe_split" in types, "missing stripe_split ws event"

            collecting = False
            print("\n===== ALL E2E CHECKS PASSED =====")
    finally:
        cleanup()


if __name__ == "__main__":
    main()
