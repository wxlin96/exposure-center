# -*- coding: utf-8 -*-
"""接收桩接口：接收远程机器的回调。

   POST /api/v1/callbacks/stripe-split    切分完成通知
   POST /api/v1/callbacks/stripe-status   单条带终态通知

校验：Header X-Callback-Token 需与配置 CALLBACK_TOKEN 一致。
协议文档见 docs/CALLBACK_PROTOCOL.md。
"""
import asyncio
import logging

from fastapi import APIRouter, Depends, Header, HTTPException, Request

from config import CALLBACK_TOKEN
from schemas import StripeSplitCallback, StripeStatusCallback

logger = logging.getLogger("callbacks")


def make_callback_router():
    router = APIRouter(prefix="/api/v1/callbacks", tags=["callbacks"])

    def _verify(x_token: str = Header(None, alias="X-Callback-Token")) -> None:
        if CALLBACK_TOKEN and x_token != CALLBACK_TOKEN:
            raise HTTPException(status_code=401, detail=f"bad token")

    @router.post("/stripe-split")
    async def stripe_split(body: StripeSplitCallback, request: Request,
                           _: None = Depends(_verify)):
        handler = request.app.state.handler
        result = await asyncio.to_thread(
            handler.handle_stripe_split, body.machine_id, body.job_id,
            body.stripe_count, body.stripes
        )
        if not result.get("ok", True):
            return {"code": 1, "msg": result.get("reason", "failed")}
        return {"code": 0, "msg": "ok"}

    @router.post("/stripe-status")
    async def stripe_status(body: StripeStatusCallback, request: Request,
                            _: None = Depends(_verify)):
        handler = request.app.state.handler
        result = await asyncio.to_thread(
            handler.handle_stripe_status, body.machine_id, body.job_id,
            body.stripe_id, body.status
        )
        if not result.get("ok", True):
            return {"code": 1, "msg": result.get("reason", "failed")}
        return {"code": 0, "msg": "ok", "verdict": result.get("verdict", "")}

    return router
