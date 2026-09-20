"""保持期限を過ぎたキャプチャを消すループ。

歌のトリガーループと同じ形（起動時に常駐させ、失敗しても止めない）。
消していいかの判定は store.cleanup_expired が持ち、ここは間隔だけを見る。
"""
import asyncio
import logging

from bridge.config import CAPTURE_CLEANUP_INTERVAL
from bridge.features.capture import store

logger = logging.getLogger(__name__)


async def capture_cleanup_loop() -> None:
    logger.info("Capture cleanup loop started: interval=%ds", CAPTURE_CLEANUP_INTERVAL)
    while True:
        try:
            store.cleanup_expired()
        except Exception as e:
            logger.error("Capture cleanup error: %s", e)
        await asyncio.sleep(CAPTURE_CLEANUP_INTERVAL)
