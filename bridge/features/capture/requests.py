"""「いま撮って」と頼んで、届くまで待つ。

依頼は MQTT（stackchan/{deviceId}/capture）、写真は HTTP（/ingest-photo）で
返ってくる。経路が分かれているので、requestId で結びつける必要がある。

ACK（stackchan/ack）は「依頼を受け取った」ことしか示さない。撮影が終わって
写真が届いたかどうかは、/ingest-photo の到着で判断する。そのため待ち合わせは
MQTT の _pending_acks とは別に、ここで持つ。
"""
import asyncio
import logging
import uuid

from bridge.config import MQTT_DEVICE_ID
from bridge.devices.mqtt import publish_capture, wait_for_ack

logger = logging.getLogger(__name__)

# requestId → 届いた写真の行。待っている間は None 入りの Future を置く。
_pending: dict[str, asyncio.Future] = {}

CAPTURE_TIMEOUT = 20.0   # 撮影・保存・送信までの余裕（ACK の 15 秒より長くとる）


def notify_arrival(request_id: str, capture: dict) -> None:
    """/ingest-photo が写真を保存したときに呼ぶ。待っている人がいれば起こす。

    誰も待っていない（デバイス起点のアップロード）場合は何もしない。
    """
    future = _pending.get(request_id)
    if future is None or future.done():
        return
    future.set_result(capture)
    logger.info("依頼した写真が届きました: request_id=%s", request_id)


async def request_capture(
    *,
    kind: str = "photo",
    reason: str = "",
    device_id: str = "",
    params: dict | None = None,
    timeout: float = CAPTURE_TIMEOUT,
    wait: bool = True,
) -> dict:
    """撮影を依頼する。wait=True なら写真が届くまで待って、その行を返す。

    返り値の status は次のいずれか。
      ok          … 写真が届いた
      no_ack      … デバイスが依頼に応答しなかった（電源・MQTT の疑い）
      timeout     … ACK は返ったが、写真が届かなかった（撮影・送信の失敗）
      requested   … wait=False で、送っただけ
    """
    request_id = str(uuid.uuid4())
    loop = asyncio.get_running_loop()
    future: asyncio.Future = loop.create_future()
    if wait:
        # publish より先に登録する。写真が先に届いても取りこぼさないため
        # （ACK 待ちで同じ失敗をしたことがある）。
        _pending[request_id] = future

    try:
        topic = publish_capture(
            request_id=request_id, kind=kind, reason=reason,
            device_id=device_id, params=params,
        )
        logger.info("撮影を依頼: topic=%s kind=%s reason=%s request_id=%s",
                    topic, kind, reason or "-", request_id)

        if not wait:
            return {"status": "requested", "requestId": request_id}

        acked = await wait_for_ack(request_id, timeout=min(timeout, 15.0))
        if not acked and future.done() is False:
            # ACK は来ていないが、写真が先に届くこともあるので即座には諦めない
            logger.info("撮影依頼の ACK なし: request_id=%s（写真の到着は待つ）", request_id)

        try:
            capture = await asyncio.wait_for(asyncio.shield(future), timeout=timeout)
        except asyncio.TimeoutError:
            if not acked:
                logger.warning("撮影依頼に応答なし: request_id=%s", request_id)
                return {"status": "no_ack", "requestId": request_id}
            logger.warning("写真が届きませんでした: request_id=%s", request_id)
            return {"status": "timeout", "requestId": request_id}

        return {"status": "ok", "requestId": request_id, "capture": capture}
    finally:
        _pending.pop(request_id, None)


def pending_count() -> int:
    """待っている依頼の数（デバッグ表示用）。"""
    return len(_pending)
