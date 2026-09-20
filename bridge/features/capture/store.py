"""撮ったものをファイルと DB に置く。

置き場所は `data/captures/YYYY-MM/{id}.jpg`。歌（data/songs）と同じく
実体はファイル、引き当ては SQLite という分け方にする。歌と違うのは、
**キャッシュではなく消してはいけないものが混ざる**点で、記憶に昇格した
ファイルを消さないために掃除は必ず `keep_until` を見る。
"""
import logging
import os
import uuid
from datetime import datetime, timedelta

from bridge.config import CAPTURE_DIR, CAPTURE_TEMP_DAYS, _JST
from bridge.core.db import (
    _fetch_expired_captures, _get_capture, _mark_capture_deleted, _save_capture,
    _set_capture_keep_until,
)

logger = logging.getLogger(__name__)

_EXT = {
    "image/jpeg": ".jpg",
    "image/png": ".png",
    "audio/wav": ".wav",
    "audio/x-wav": ".wav",
    "audio/mpeg": ".mp3",
}


class CaptureError(Exception):
    """保存できなかった（容量超過・未対応の形式など）。"""


def retention_deadline(retention: str, now: datetime | None = None) -> str | None:
    """保持期限を決める。記憶なら None（＝ずっと残す）。

    ここが「一時保存か記憶か」を決める唯一の場所。日数は設定で変えられるが、
    permanent は必ず None を返す（期限付きの記憶は作らない）。
    """
    if retention == "permanent":
        return None
    now = now or datetime.now(_JST)
    return (now + timedelta(days=CAPTURE_TEMP_DAYS)).isoformat()


def _relative_path(capture_id: str, content_type: str, now: datetime) -> str:
    ext = _EXT.get(content_type, ".bin")
    return os.path.join(now.strftime("%Y-%m"), f"{capture_id}{ext}")


def save(
    data: bytes,
    *,
    kind: str = "photo",
    content_type: str = "image/jpeg",
    retention: str = "temp",
    device_id: str = "",
    source: str = "",
    request_id: str = "",
    trigger: str = "",
) -> dict:
    """受け取ったバイト列を保存して、保存した行を返す。"""
    if not data:
        raise CaptureError("中身が空です")
    if content_type not in _EXT:
        raise CaptureError(f"未対応の形式です: {content_type}")

    now = datetime.now(_JST)
    capture_id = uuid.uuid4().hex
    rel = _relative_path(capture_id, content_type, now)
    abs_path = os.path.join(CAPTURE_DIR, rel)
    os.makedirs(os.path.dirname(abs_path), exist_ok=True)
    with open(abs_path, "wb") as f:
        f.write(data)

    keep_until = retention_deadline(retention, now)
    _save_capture(
        capture_id=capture_id, kind=kind, path=rel, content_type=content_type,
        size_bytes=len(data), captured_at=now.isoformat(), device_id=device_id,
        source=source, request_id=request_id, trigger=trigger, keep_until=keep_until,
    )
    logger.info(
        "キャプチャを保存: id=%s kind=%s bytes=%d source=%s keep_until=%s",
        capture_id, kind, len(data), source or "-", keep_until or "ずっと",
    )
    return _get_capture(capture_id) or {}


def abs_path(capture: dict) -> str:
    return os.path.join(CAPTURE_DIR, capture["path"])


def keep_forever(capture_id: str) -> bool:
    """記憶にする（保持期限を外す）。"""
    ok = _set_capture_keep_until(capture_id, None)
    if ok:
        logger.info("キャプチャを記憶にしました: id=%s", capture_id)
    return ok


def set_temporary(capture_id: str) -> bool:
    """記憶から一時保存に戻す（既定の日数で消えるようになる）。"""
    return _set_capture_keep_until(capture_id, retention_deadline("temp"))


def delete(capture_id: str) -> bool:
    """実体を消して、行には消した時刻を残す。"""
    capture = _get_capture(capture_id)
    if not capture or capture["deleted_at"]:
        return False
    _remove_file(capture)
    _mark_capture_deleted(capture_id)
    return True


def _remove_file(capture: dict) -> None:
    path = abs_path(capture)
    try:
        os.remove(path)
    except FileNotFoundError:
        logger.warning("キャプチャの実体が見つかりません（行だけ消します）: %s", path)
    except OSError as e:
        logger.error("キャプチャを消せませんでした: path=%s error=%s", path, e)
        raise


def cleanup_expired(now: datetime | None = None) -> int:
    """保持期限を過ぎた一時保存を消す。記憶（keep_until が NULL）は対象外。"""
    now = now or datetime.now(_JST)
    expired = _fetch_expired_captures(now.isoformat())
    removed = 0
    for capture in expired:
        try:
            _remove_file(capture)
        except OSError:
            continue  # 次の掃除でまた拾う
        _mark_capture_deleted(capture["id"])
        removed += 1
    if removed:
        logger.info("期限切れのキャプチャを %d 件消しました", removed)
    return removed
