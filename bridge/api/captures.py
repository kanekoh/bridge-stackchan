"""写真・音声の受け取りと一覧のエンドポイント。

`/ingest-photo` は Stack-chan（M5Stack CoreS3）から multipart で写真を受ける口。
`/ingest-audio` と同じ形にしてあるので、ファーム側は送り先とフィールド名を
変えるだけでよい。ファームがまだ対応していなくても、手元から curl で POST すれば
保存・保持期限・一覧まで通しで試せる。

**実体の配信は /api/captures/{id}/file だけ**。家族の顔が写るため、
歌のような認証なしの静的配信（StaticFiles）には置かない。
"""
import logging

from fastapi import APIRouter, File, Form, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse

from bridge.config import CAPTURE_MAX_BYTES, CAPTURE_TEMP_DAYS, MQTT_DEVICE_ID
from bridge.core.db import _count_captures, _fetch_captures, _get_capture
from bridge.features.capture import store

logger = logging.getLogger(__name__)
router = APIRouter()

_RETENTIONS = ("temp", "permanent")


def _public(capture: dict) -> dict:
    """UI に返す形。保存場所（path）は出さない。"""
    return {
        "id": capture["id"],
        "kind": capture["kind"],
        "deviceId": capture["device_id"],
        "contentType": capture["content_type"],
        "bytes": capture["bytes"],
        "capturedAt": capture["captured_at"],
        "source": capture["source"],
        "requestId": capture["request_id"],
        "trigger": capture["trigger"],
        "caption": capture["caption"],
        "keepUntil": capture["keep_until"],
        "keep": "temp" if capture["keep_until"] else "permanent",
        "memoryId": capture["memory_id"],
        "url": f"/api/captures/{capture['id']}/file",
    }


@router.post("/ingest-photo", status_code=201)
async def ingest_photo(
    file: UploadFile = File(...),
    request_id: str = Form(""),
    source: str = Form("stackchan"),
    trigger: str = Form(""),
    retention: str = Form("temp"),
    device_id: str = Form(""),
):
    """Stack-chan が撮った写真を受け取って保存する。

    retention は既定で temp（CAPTURE_TEMP_DAYS 日で自動削除）。撮る時点で
    残すと決まっているものだけ permanent を指定する。
    """
    if retention not in _RETENTIONS:
        raise HTTPException(status_code=400, detail=f"retention は {_RETENTIONS} のいずれかです")

    data = await file.read()
    if len(data) > CAPTURE_MAX_BYTES:
        raise HTTPException(
            status_code=413,
            detail=f"写真が大きすぎます（{len(data)} バイト、上限 {CAPTURE_MAX_BYTES}）",
        )

    content_type = (file.content_type or "image/jpeg").split(";")[0].strip()
    try:
        capture = store.save(
            data,
            kind="photo",
            content_type=content_type,
            retention=retention,
            device_id=device_id or MQTT_DEVICE_ID,
            source=source,
            request_id=request_id,
            trigger=trigger,
        )
    except store.CaptureError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return _public(capture)


@router.get("/api/captures")
def api_list_captures(
    limit: int = Query(default=60, le=200),
    kind: str = Query(default=""),
    keep: str = Query(default=""),
):
    captures = _fetch_captures(limit=limit, kind=kind, keep=keep)
    return {
        "captures": [_public(c) for c in captures],
        "stats": _count_captures(),
        "tempDays": CAPTURE_TEMP_DAYS,
    }


@router.get("/api/captures/{capture_id}/file")
def api_capture_file(capture_id: str):
    capture = _get_capture(capture_id)
    if not capture or capture["deleted_at"]:
        raise HTTPException(status_code=404, detail="その写真はありません")
    return FileResponse(store.abs_path(capture), media_type=capture["content_type"])


@router.post("/api/captures/{capture_id}/keep")
def api_keep_capture(capture_id: str):
    """記憶にする（保持期限を外して、ずっと残す）。"""
    if not store.keep_forever(capture_id):
        raise HTTPException(status_code=404, detail="その写真はありません")
    return _public(_get_capture(capture_id) or {})


@router.post("/api/captures/{capture_id}/unkeep")
def api_unkeep_capture(capture_id: str):
    """一時保存に戻す（既定の日数で消えるようになる）。"""
    if not store.set_temporary(capture_id):
        raise HTTPException(status_code=404, detail="その写真はありません")
    return _public(_get_capture(capture_id) or {})


@router.delete("/api/captures/{capture_id}", status_code=204)
def api_delete_capture(capture_id: str):
    if not store.delete(capture_id):
        raise HTTPException(status_code=404, detail="その写真はありません")
