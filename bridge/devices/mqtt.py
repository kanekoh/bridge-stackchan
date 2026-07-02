import asyncio
import json
import logging
import sys
import threading
from datetime import datetime

import paho.mqtt.client as mqtt

from bridge.config import (
    MQTT_BROKER, MQTT_PORT, MQTT_USERNAME, MQTT_PASSWORD,
    MQTT_TLS, MQTT_DEVICE_ID, MQTT_QOS, MQTT_ACK_TIMEOUT,
    _JST,
)

logger = logging.getLogger(__name__)

# MQTT ACK 待機: requestId → asyncio.Event のマップ
_pending_acks: dict[str, asyncio.Event] = {}

# MQTT スレッドから asyncio へ通知するためのイベントループ参照（lifespan で設定）
_main_loop: asyncio.AbstractEventLoop | None = None


def set_main_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Set the main event loop reference for MQTT → asyncio bridging."""
    global _main_loop
    _main_loop = loop


def _build_mqtt_client() -> mqtt.Client:
    client = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
    if MQTT_USERNAME:
        client.username_pw_set(MQTT_USERNAME, MQTT_PASSWORD)
    if MQTT_TLS:
        client.tls_set()  # uses system CA bundle; works with HiveMQ Cloud
    return client


class _MqttConnection:
    """Persistent MQTT connection; reconnects automatically on the next publish."""

    def __init__(self):
        self._client: mqtt.Client | None = None
        self._lock = threading.Lock()

    def _connect(self) -> mqtt.Client:
        logger.info("MQTT connecting: broker=%s port=%d tls=%s", MQTT_BROKER, MQTT_PORT, MQTT_TLS)
        client = _build_mqtt_client()
        connected = threading.Event()

        def on_connect(client, userdata, flags, reason_code, properties):
            if reason_code == 0:
                client.subscribe("stackchan/ack", qos=MQTT_QOS)
                client.subscribe(f"stackchan/{MQTT_DEVICE_ID}/log", qos=MQTT_QOS)
                client.subscribe(f"stackchan/{MQTT_DEVICE_ID}/metrics", qos=MQTT_QOS)
                logger.info("MQTT (re)connected, subscribed to ack + %s/log + %s/metrics qos=%d",
                            MQTT_DEVICE_ID, MQTT_DEVICE_ID, MQTT_QOS)
                connected.set()
            else:
                logger.error("MQTT connect failed: reason_code=%s", reason_code)

        def on_disconnect(client, userdata, flags, reason_code, properties):
            logger.warning("MQTT disconnected: reason_code=%s", reason_code)

        def _store_device_log(device_id: str, raw: str) -> None:
            try:
                data = json.loads(raw)
                level = data.get("level", "")
                ts_ms = data.get("ts")
                msg   = data.get("msg", "")
                now   = datetime.now(_JST).isoformat()
                # Lazy lookup to avoid circular import with db module
                main_mod = sys.modules.get("main")
                if main_mod is None:
                    return
                _db_lock = getattr(main_mod, "_db_lock", None)
                _db_conn = getattr(main_mod, "_db_conn", None)
                if _db_lock is None or _db_conn is None:
                    return
                with _db_lock:
                    _db_conn.execute(
                        "INSERT INTO device_log (device_id, level, ts_ms, msg, raw_json, received_at)"
                        " VALUES (?,?,?,?,?,?)",
                        (device_id, level, ts_ms, msg, raw, now),
                    )
                    _db_conn.execute(
                        "DELETE FROM device_log WHERE id NOT IN "
                        "(SELECT id FROM device_log WHERE device_id=? ORDER BY id DESC LIMIT 500)",
                        (device_id,),
                    )
                    _db_conn.commit()
                logger.debug("device_log stored: device=%s level=%s msg=%s", device_id, level, msg)
            except Exception as e:
                logger.warning("device_log store error: %s", e)

        def _store_device_metrics(device_id: str, raw: str) -> None:
            try:
                d = json.loads(raw)
                heap   = d.get("heap", {})
                psram  = d.get("psram", {})
                stacks = d.get("stacks", {})
                ts_ms  = d.get("ts") or int(datetime.now(_JST).timestamp() * 1000)
                now    = datetime.now(_JST).isoformat()
                # Lazy lookup to avoid circular import with db module
                main_mod = sys.modules.get("main")
                if main_mod is None:
                    return
                _db_lock = getattr(main_mod, "_db_lock", None)
                _db_conn = getattr(main_mod, "_db_conn", None)
                if _db_lock is None or _db_conn is None:
                    return
                with _db_lock:
                    _db_conn.execute(
                        "INSERT INTO device_metrics"
                        " (device_id, ts_ms, heap_free, heap_min, psram_free,"
                        "  stack_speech, stack_playback, stack_netmon, stack_mqtttask, received_at)"
                        " VALUES (?,?,?,?,?,?,?,?,?,?)",
                        (
                            device_id, ts_ms,
                            heap.get("free"), heap.get("min"), psram.get("free"),
                            stacks.get("speech"), stacks.get("playback"),
                            stacks.get("netmon"), stacks.get("MQTTTask"),
                            now,
                        ),
                    )
                    # 過去 24 時間（1440 件）を超えたら古いものを削除
                    _db_conn.execute(
                        "DELETE FROM device_metrics WHERE id NOT IN"
                        " (SELECT id FROM device_metrics WHERE device_id=? ORDER BY id DESC LIMIT 1440)",
                        (device_id,),
                    )
                    _db_conn.commit()
                logger.debug("device_metrics stored: device=%s heap_free=%s", device_id, heap.get("free"))
            except Exception as e:
                logger.warning("device_metrics store error: %s", e)

        def on_message(client, userdata, message):
            topic = message.topic
            # ── ログトピック ──────────────────────────────────
            parts = topic.split("/")
            if len(parts) == 3 and parts[0] == "stackchan" and parts[2] == "log":
                _store_device_log(parts[1], message.payload.decode("utf-8", errors="replace"))
                return
            # ── メトリクストピック ────────────────────────────
            if len(parts) == 3 and parts[0] == "stackchan" and parts[2] == "metrics":
                _store_device_metrics(parts[1], message.payload.decode("utf-8", errors="replace"))
                return
            # ── ACK トピック ──────────────────────────────────
            try:
                data = json.loads(message.payload)
                req_id = data.get("id")
                logger.info(
                    "MQTT ACK on_message: topic=%s req_id=%s status=%s main_loop=%s",
                    topic, req_id, data.get("status"), _main_loop is not None,
                )
                if req_id and _main_loop:
                    event = _pending_acks.get(req_id)
                    logger.info(
                        "MQTT ACK lookup: req_id=%s event_found=%s pending_keys=%s",
                        req_id, event is not None, list(_pending_acks.keys()),
                    )
                    if event:
                        _main_loop.call_soon_threadsafe(event.set)
                        logger.info("MQTT ACK dispatched: requestId=%s", req_id)
            except Exception as e:
                logger.warning("MQTT ACK parse error: %s", e)

        client.on_connect = on_connect
        client.on_disconnect = on_disconnect
        client.on_message = on_message
        client.loop_start()
        client.connect(MQTT_BROKER, MQTT_PORT, keepalive=60)

        if not connected.wait(timeout=10):
            client.loop_stop()
            raise RuntimeError("MQTT connection timeout (no CONNACK within 10s)")

        return client

    def start(self) -> None:
        """起動時にバックグラウンドスレッドで即時接続する。失敗しても後で publish 時にリトライされる。"""
        def _try_connect():
            try:
                with self._lock:
                    if self._client is None or not self._client.is_connected():
                        self._client = self._connect()
            except Exception as e:
                logger.warning("MQTT eager connect failed (will retry on first publish): %s", e)
        threading.Thread(target=_try_connect, daemon=True, name="mqtt-eager-connect").start()

    def publish(self, topic: str, payload: str) -> None:
        for attempt in range(2):
            with self._lock:
                if self._client is None or not self._client.is_connected():
                    self._client = self._connect()
                client = self._client

            msg_info = client.publish(topic, payload, qos=MQTT_QOS)
            logger.info("MQTT publish queued: mid=%d", msg_info.mid)
            try:
                msg_info.wait_for_publish(timeout=10)
                logger.info("MQTT publish confirmed: topic=%s mid=%d payload=%s", topic, msg_info.mid, payload)
                return
            except Exception as e:
                logger.warning("MQTT publish attempt %d failed: %s", attempt + 1, e)
                with self._lock:
                    self._client = None  # force reconnect on next attempt

        raise RuntimeError("MQTT publish failed after retry")


_mqtt_conn = _MqttConnection()


def publish_speak(
    audio_url: str,
    audio_streaming_url: str | None,
    text: str,
    source: str,
    priority: str,
    request_id: str,
    expression: str = "neutral",
) -> None:
    """Publish MQTT speak event to Stack-chan."""
    topic = f"stackchan/{MQTT_DEVICE_ID}/speak"
    msg: dict = {
        "type": "speak",
        "audioUrl": audio_url,
        "text": text,
        "source": source,
        "priority": priority,
        "requestId": request_id,
        "expression": expression,
    }
    if audio_streaming_url:
        msg["audioStreamingUrl"] = audio_streaming_url
    payload = json.dumps(msg, ensure_ascii=False)
    _mqtt_conn.publish(topic, payload)


async def wait_for_ack(request_id: str, timeout: float = MQTT_ACK_TIMEOUT) -> bool:
    """stackchan/ack トピックで requestId に対応する ACK を待つ。

    publish_speak より前に _pending_acks に event を登録しておくと、
    ACK が先に届いた場合も取りこぼさない。
    Returns True if ACK received within timeout, False otherwise.
    """
    event = _pending_acks.get(request_id)
    if event is None:
        event = asyncio.Event()
        _pending_acks[request_id] = event
    try:
        await asyncio.wait_for(event.wait(), timeout=timeout)
        return True
    except asyncio.TimeoutError:
        logger.warning("MQTT ACK timeout: requestId=%s", request_id)
        return False
    finally:
        _pending_acks.pop(request_id, None)
