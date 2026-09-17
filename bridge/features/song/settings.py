"""歌まわりの設定。既定値は環境変数、実運用の切り替えは app_settings（UI から変更可）。

既存機能（天気・ISS・話者ID）と同じ流儀で、env を既定値・DB を上書きとして扱う。
"""
from bridge.config import (
    SONG_COOLDOWN_MINUTES, SONG_IDLE_CHANCE, SONG_IDLE_ENABLED, SONG_IDLE_END,
    SONG_IDLE_MIN_HOURS, SONG_IDLE_START, SONG_QUIET_END, SONG_QUIET_START,
    SONG_TRIGGER_ENABLED, SONG_TRIGGER_LEAD_MINUTES,
    SONG_TRIGGER_PREP_MINUTES, SONG_TRIGGER_TRAVEL_MINUTES,
)
from bridge.core.db import _get_setting

_DEFAULTS: dict[str, str] = {
    "song_trigger_enabled":         str(SONG_TRIGGER_ENABLED).lower(),
    "song_trigger_lead_minutes":    str(SONG_TRIGGER_LEAD_MINUTES),
    "song_trigger_prep_minutes":    str(SONG_TRIGGER_PREP_MINUTES),
    "song_trigger_travel_minutes":  str(SONG_TRIGGER_TRAVEL_MINUTES),
    "song_trigger_song":            "hurry",
    "song_quiet_start":             SONG_QUIET_START,
    "song_quiet_end":               SONG_QUIET_END,
    "song_cooldown_minutes":        str(SONG_COOLDOWN_MINUTES),
    "song_idle_enabled":            str(SONG_IDLE_ENABLED).lower(),
    "song_idle_min_hours":          str(SONG_IDLE_MIN_HOURS),
    "song_idle_chance":             str(SONG_IDLE_CHANCE),
    "song_idle_start":              SONG_IDLE_START,
    "song_idle_end":                SONG_IDLE_END,
    "song_idle_mood":               "",   # 空なら曲を限定しない
}


def get_str(key: str) -> str:
    return _get_setting(key, _DEFAULTS.get(key, ""))


def get_int(key: str) -> int:
    try:
        return int(float(get_str(key)))
    except (TypeError, ValueError):
        return int(float(_DEFAULTS.get(key, "0") or 0))


def get_float(key: str) -> float:
    try:
        return float(get_str(key))
    except (TypeError, ValueError):
        return float(_DEFAULTS.get(key, "0") or 0)


def get_bool(key: str) -> bool:
    return get_str(key).strip().lower() == "true"


def all_settings() -> dict[str, str]:
    return {key: get_str(key) for key in _DEFAULTS}


def defaults() -> dict[str, str]:
    return dict(_DEFAULTS)
