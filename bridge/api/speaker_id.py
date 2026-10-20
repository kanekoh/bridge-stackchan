"""話者識別の記録を見るためのエンドポイント。

**判定には一切関与しない。** しきい値を決め直すための観測だけを担う。
実際の採否は bridge/integrations/stt.py が従来どおり行う。
"""
import json
import logging

from fastapi import APIRouter, HTTPException, Query
from pydantic import BaseModel

from bridge.core.db import (
    _fetch_speaker_id_log, _get_setting, _set_speaker_id_actual,
)
from bridge.config import SPEAKER_ID_THRESHOLD

logger = logging.getLogger(__name__)
router = APIRouter()


class ActualSpeakerUpdate(BaseModel):
    actual_speaker: str | None = None


def _public(row: dict) -> dict:
    return {
        "id": row["id"],
        "requestId": row["request_id"],
        "createdAt": row["created_at"],
        "candidate": row["candidate"],
        "score": row["score"],
        "secondName": row["second_name"],
        "secondScore": row["second_score"],
        "margin": row["margin"],
        "accepted": bool(row["accepted"]),
        "thresholdBridge": row["threshold_bridge"],
        "thresholdService": row["threshold_service"],
        "speechSec": row["speech_sec"],
        "elapsedMs": row["elapsed_ms"],
        "reason": row["reason"],
        "scores": json.loads(row["scores_json"]) if row["scores_json"] else [],
        "actualSpeaker": row["actual_speaker"],
    }


def _sweep(rows: list[dict]) -> list[dict]:
    """しきい値を動かしたら採否がどう変わるかを試算する。

    正解ラベル（actual_speaker）が付いている行については、正誤も数える。
    ラベルが無いうちは「何件通るか」だけが分かる。
    """
    out = []
    for step in range(10, 100, 5):
        th = step / 100
        passed = [r for r in rows if (r["score"] or 0) >= th]
        labeled = [r for r in passed if r["actual_speaker"]]
        correct = [r for r in labeled if r["candidate"] == r["actual_speaker"]]
        # しきい値で弾かれたが、本当は正解だったもの（取りこぼし）
        rejected_labeled = [
            r for r in rows
            if (r["score"] or 0) < th and r["actual_speaker"]
            and r["candidate"] == r["actual_speaker"]
        ]
        out.append({
            "threshold": th,
            "accepted": len(passed),
            "acceptedRate": round(len(passed) / len(rows) * 100, 1) if rows else 0.0,
            "labeled": len(labeled),
            "correct": len(correct),
            "wrong": len(labeled) - len(correct),
            "missed": len(rejected_labeled),
        })
    return out


@router.get("/api/speaker-id/log")
def api_speaker_id_log(
    days: int = Query(default=30, le=365),
    limit: int = Query(default=1000, le=5000),
):
    rows = _fetch_speaker_id_log(days=days, limit=limit)
    scored = [r for r in rows if r["score"] is not None]
    people = sorted({r["candidate"] for r in rows if r["candidate"]})
    return {
        "entries": [_public(r) for r in rows],
        "people": people,
        "thresholdBridge": float(_get_setting("speaker_id_threshold", str(SPEAKER_ID_THRESHOLD))),
        "stats": {
            "total": len(rows),
            "accepted": sum(1 for r in rows if r["accepted"]),
            "labeled": sum(1 for r in rows if r["actual_speaker"]),
        },
        "sweep": _sweep(scored),
    }


@router.put("/api/speaker-id/log/{log_id}/actual")
def api_set_actual_speaker(log_id: int, req: ActualSpeakerUpdate):
    """「実際は誰だったか」を記録する。これが無いと正誤は判定できない。"""
    name = (req.actual_speaker or "").strip() or None
    if not _set_speaker_id_actual(log_id, name):
        raise HTTPException(status_code=404, detail="その記録はありません")
    return {"id": log_id, "actualSpeaker": name}
