"""歌の一覧・試聴・再生成・トリガー設定のエンドポイント。"""
import logging
from datetime import datetime, timedelta

from fastapi import APIRouter, HTTPException
from pydantic import BaseModel

from bridge.config import SONG_PUBLIC_BASE_URL, _JST
import bridge.core.db as _db_mod
from bridge.core.db import _db_lock, _set_setting
from bridge.features import gatekeeper
from bridge.features.song import cache as _cache
from bridge.features.song import compose as _compose
from bridge.features.song import engine as _engine
from bridge.features.song import library, play, settings, triggers
from bridge.features.song.play import SongNotAllowed
from bridge.features.song.score import ScoreError, build_notes, cache_key, score_to_dict

logger = logging.getLogger(__name__)
router = APIRouter()


class SongSettingsUpdate(BaseModel):
    song_trigger_enabled:        bool | None = None
    song_trigger_lead_minutes:   int | None = None
    song_trigger_prep_minutes:   int | None = None
    song_trigger_travel_minutes: int | None = None
    song_trigger_song:           str | None = None
    song_quiet_start:            str | None = None
    song_quiet_end:              str | None = None
    song_cooldown_minutes:       int | None = None
    song_idle_enabled:           bool | None = None
    song_idle_min_hours:         float | None = None
    song_idle_chance:            float | None = None
    song_idle_start:             str | None = None
    song_idle_end:               str | None = None
    song_idle_mood:              str | None = None


class ComposeRequest(BaseModel):
    theme: str = ""
    mood: str = ""
    requested_by: str = ""
    play: bool = True


class OverrideUpdate(BaseModel):
    travel_minutes: int | None = None
    prep_minutes:   int | None = None
    song_id:        str | None = None
    enabled:        bool = True


@router.get("/api/songs")
async def api_list_songs():
    """曲の一覧。キャッシュ済みかどうかと、ENGINE の状態も返す。"""
    engine_info: dict | None = None
    engine_error = ""
    try:
        engine_info = await _engine.get_engine_info()
    except Exception as e:
        engine_error = str(e)

    stats = library.song_stats()
    items = []
    for score in library.all_scores():
        frame_rate = engine_info["frame_rate"] if engine_info else None
        cached_name = None
        key = ""
        if engine_info:
            key = cache_key(
                score,
                engine_version=engine_info["version"],
                frame_rate=engine_info["frame_rate"],
                audio_spec=_cache._audio_spec(),
            )
            cached_name = _cache.cached_name(score, key)
        items.append({
            "id": score.id,
            "title": score.title,
            "bpm": score.bpm,
            "transpose": score.transpose,
            "mood": score.mood,
            "noteCount": len(score.notes),
            "defaultLyric": score.default_lyric,
            "singStyleId": score.sing_style_id,
            "frameDecodeStyleId": score.frame_decode_style_id,
            "durationSec": round(score.duration_sec(frame_rate), 2) if frame_rate else None,
            "cached": cached_name is not None,
            "cacheKey": key,
            "url": play.song_url(cached_name) if (cached_name and SONG_PUBLIC_BASE_URL) else None,
            "credit": score.credit,
            # 出自（作った歌は「いつ・誰のために・何をお題に」が分かる）
            "source": score.source,
            "theme": score.theme,
            "createdBy": score.created_by,
            "createdAt": score.created_at,
            "playCount": stats.get(score.id, {}).get("play_count", 0),
            "lastPlayedAt": stats.get(score.id, {}).get("last_played_at"),
        })

    allowed, reason = play.check_allowed()
    return {
        "songs": items,
        "loadErrors": library.load_errors(),
        "engine": {
            "available": engine_info is not None,
            "url": _engine.VOICEVOX_SING_URL,
            "version": engine_info["version"] if engine_info else None,
            "frameRate": engine_info["frame_rate"] if engine_info else None,
            "error": engine_error,
        },
        "publicBaseUrl": SONG_PUBLIC_BASE_URL,
        "settings": settings.all_settings(),
        "gate": {"allowed": allowed, "reason": reason, "atHome": play.is_at_home()},
        "idle": dict(zip(("due", "reason"), triggers.idle_song_due())),
        "moods": library.all_moods(),
    }


@router.post("/api/songs/reload")
def api_reload_songs():
    """楽譜 YAML を読み直す（ファイルを編集したあと、再起動せずに反映する）。"""
    scores = library.load_scores()
    return {"count": len(scores), "songs": sorted(scores), "loadErrors": library.load_errors()}


@router.post("/api/songs/{song_id}/play")
async def api_play_song(song_id: str):
    """試聴。人が今まさに押した操作なので gatekeeper は通さない。"""
    score = library.get_score(song_id)
    if score is None:
        raise HTTPException(status_code=404, detail=f"曲が見つかりません: {song_id}")
    try:
        return await play.play_song(score, source="ui", bypass_gate=True)
    except SongNotAllowed as e:
        raise HTTPException(status_code=409, detail=str(e))
    except Exception as e:
        logger.error("試聴に失敗: song=%s error=%s", song_id, e)
        raise HTTPException(status_code=502, detail=str(e))


@router.post("/api/songs/{song_id}/rebuild")
async def api_rebuild_song(song_id: str):
    """キャッシュを作り直す（声色を変えた・楽譜を直したあとの確認用）。"""
    score = library.get_score(song_id)
    if score is None:
        raise HTTPException(status_code=404, detail=f"曲が見つかりません: {song_id}")
    try:
        name, key = await _cache.ensure_song(score, force=True)
    except Exception as e:
        logger.error("再生成に失敗: song=%s error=%s", song_id, e)
        raise HTTPException(status_code=502, detail=str(e))
    return {"songId": song_id, "file": name, "cacheKey": key}


@router.post("/api/songs/compose", status_code=201)
async def api_compose_song(req: ComposeRequest):
    """その場で1曲つくる。楽譜は LLM が書き、検証は手書きの楽譜と同じ関門を通す。"""
    try:
        score = await _compose.compose_and_build(
            theme=req.theme, mood=req.mood, requested_by=req.requested_by,
        )
    except ScoreError as e:
        raise HTTPException(status_code=422, detail=str(e))
    except Exception as e:
        logger.error("作曲に失敗: theme=%s error=%s", req.theme, e)
        raise HTTPException(status_code=502, detail=str(e))

    result: dict = {
        "songId": score.id, "title": score.title, "mood": score.mood,
        "theme": score.theme, "noteCount": len(score.notes), "bpm": score.bpm,
        "played": False,
    }
    if req.play:
        try:
            played = await play.play_song(score, source="ui_compose", bypass_gate=True)
            result["played"] = True
            result["durationSec"] = played["durationSec"]
        except Exception as e:
            result["playError"] = str(e)
    return result


@router.get("/api/songs/{song_id}/score")
async def api_song_score(song_id: str):
    """楽譜そのものと、ENGINE に渡す notes を返す（作った歌の中身を確かめる用）。"""
    score = library.get_score(song_id)
    if score is None:
        raise HTTPException(status_code=404, detail=f"曲が見つかりません: {song_id}")
    frame_rate = None
    try:
        frame_rate = (await _engine.get_engine_info())["frame_rate"]
    except Exception:
        pass
    notes = build_notes(score) if frame_rate is None else build_notes(score, frame_rate)
    return {
        "songId": score.id,
        "title": score.title,
        "source": score.source,
        "theme": score.theme,
        "createdBy": score.created_by,
        "createdAt": score.created_at,
        "score": score_to_dict(score),
        "notes": notes,
        "totalFrames": sum(n["frame_length"] for n in notes),
        "durationSec": round(score.duration_sec(frame_rate or 93.75), 2),
    }


@router.post("/api/songs/prebuild")
async def api_prebuild_songs():
    """全曲を事前生成する。"""
    return await _cache.prebuild_all(library.all_scores())


@router.get("/api/songs/history")
def api_song_history(limit: int = 50):
    return {"items": library.play_history(limit)}


@router.get("/api/songs/triggers")
def api_song_triggers():
    """いま出発リミットが近い予定と、直近の予定ごとの計算結果を返す。"""
    now = datetime.now(_JST)
    due = triggers.due_departures(now)
    upcoming = triggers._upcoming_events(now)
    overrides = triggers._overrides()
    default_travel = settings.get_int("song_trigger_travel_minutes")
    default_prep = settings.get_int("song_trigger_prep_minutes")
    rows = []
    for item in upcoming:
        ov = overrides.get(item["id"], {})
        travel = ov.get("travel_minutes") if ov.get("travel_minutes") is not None else default_travel
        prep = ov.get("prep_minutes") if ov.get("prep_minutes") is not None else default_prep
        try:
            start = datetime.fromisoformat(item["start_at"])
        except ValueError:
            continue
        depart_limit = start - timedelta(minutes=travel + prep)
        rows.append({
            **item,
            "travel_minutes": travel,
            "prep_minutes": prep,
            "overridden": bool(ov),
            "enabled": ov.get("enabled", True),
            "depart_limit": depart_limit.isoformat(),
            "minutes_to_limit": round((depart_limit - now).total_seconds() / 60, 1),
            "fired_at": gatekeeper.last_fired_at(play.GATE_KIND, item["id"]),
        })
    return {"due": due, "upcoming": rows, "enabled": settings.get_bool("song_trigger_enabled")}


@router.put("/api/songs/settings")
def api_update_song_settings(req: SongSettingsUpdate):
    values = req.model_dump(exclude_none=True)
    if not values:
        raise HTTPException(status_code=422, detail="更新するフィールドがありません")
    for key, value in values.items():
        if key in ("song_quiet_start", "song_quiet_end") and value:
            try:
                h, m = str(value).split(":")
                if not (0 <= int(h) <= 23 and 0 <= int(m) <= 59):
                    raise ValueError
            except ValueError:
                raise HTTPException(status_code=422, detail=f"{key} は HH:MM 形式で指定してください")
        if key == "song_trigger_song" and value and library.get_score(str(value)) is None:
            raise HTTPException(status_code=422, detail=f"曲が見つかりません: {value}")
        _set_setting(key, str(value).lower() if isinstance(value, bool) else str(value))
    return {"updated": list(values), "settings": settings.all_settings()}


@router.put("/api/songs/overrides/{item_id}")
def api_set_override(item_id: str, req: OverrideUpdate):
    """予定ごとの移動時間・準備バッファ・曲を上書きする。"""
    if req.song_id and library.get_score(req.song_id) is None:
        raise HTTPException(status_code=422, detail=f"曲が見つかりません: {req.song_id}")
    now = datetime.now(_JST).isoformat()
    with _db_lock:
        _db_mod._db_conn.execute(  # type: ignore[union-attr]
            "INSERT INTO song_trigger_overrides"
            " (item_id, travel_minutes, prep_minutes, song_id, enabled, updated_at)"
            " VALUES (?,?,?,?,?,?)"
            " ON CONFLICT(item_id) DO UPDATE SET"
            "  travel_minutes=excluded.travel_minutes, prep_minutes=excluded.prep_minutes,"
            "  song_id=excluded.song_id, enabled=excluded.enabled, updated_at=excluded.updated_at",
            (item_id, req.travel_minutes, req.prep_minutes, req.song_id, int(req.enabled), now),
        )
        _db_mod._db_conn.commit()  # type: ignore[union-attr]
    return {"item_id": item_id, **req.model_dump()}


@router.delete("/api/songs/overrides/{item_id}", status_code=204)
def api_delete_override(item_id: str):
    with _db_lock:
        _db_mod._db_conn.execute(  # type: ignore[union-attr]
            "DELETE FROM song_trigger_overrides WHERE item_id = ?", (item_id,)
        )
        _db_mod._db_conn.commit()  # type: ignore[union-attr]


@router.delete("/api/songs/gate")
def api_clear_gate(item_id: str = ""):
    """発火済み記録を消して、もう一度鳴らせるようにする。"""
    gatekeeper.clear(play.GATE_KIND, item_id)
    return {"cleared": item_id or "all"}


@router.delete("/api/songs/{song_id}", status_code=204)
def api_delete_song(song_id: str):
    """作った歌を消す（手書きの楽譜は消せない）。"""
    try:
        deleted = library.delete_song(song_id)
    except ValueError as e:
        raise HTTPException(status_code=409, detail=str(e))
    if not deleted:
        raise HTTPException(status_code=404, detail=f"曲が見つかりません: {song_id}")
