"""歌のライブラリ。曲を引く入口はここ1か所。

曲には2種類ある。
  builtin  … config/songs/*.yaml（人が書いた楽譜。config/expression_map.yaml と同じ位置づけ）
  composed … songs テーブル（スタックちゃんが作った歌）
どちらも同じ Score になるので、合成・再生・トリガーからは区別せずに扱える。
作った歌が残るので、次からは合成せずすぐ歌えるし「この前つくったあの歌」も引ける。
"""
import glob
import json
import logging
import os
import random
import re
import unicodedata
from datetime import datetime

import yaml

from bridge.config import SONG_CONFIG_DIR, _JST
import bridge.core.db as _db_mod
from bridge.core.db import _db_lock
from bridge.features.song.score import Score, ScoreError, parse_score, score_to_dict

logger = logging.getLogger(__name__)

_scores: dict[str, Score] = {}
_load_errors: list[dict] = []
# 「まだ読んでいない」と「読んだ結果0曲だった」は別物なので、空かどうかでは判断しない
_loaded = False


def _load_yaml_scores(directory: str) -> tuple[dict[str, Score], list[dict]]:
    """config/songs/*.yaml を読む。壊れた1ファイルで全曲を失わないよう曲ごとに握る。"""
    scores: dict[str, Score] = {}
    errors: list[dict] = []
    patterns = (os.path.join(directory, "*.yaml"), os.path.join(directory, "*.yml"))
    for path in sorted(p for pat in patterns for p in glob.glob(pat)):
        song_id = os.path.splitext(os.path.basename(path))[0]
        try:
            with open(path, encoding="utf-8") as f:
                data = yaml.safe_load(f)
            data = dict(data or {})
            data["source"] = "builtin"
            score = parse_score(data, song_id=song_id)
        except (ScoreError, yaml.YAMLError, OSError, TypeError) as e:
            logger.error("楽譜の読み込みに失敗: %s: %s", path, e)
            errors.append({"file": os.path.basename(path), "error": str(e)})
            continue
        if score.id in scores:
            logger.warning("楽譜の id が重複しています（後勝ち）: %s", score.id)
        scores[score.id] = score
    return scores, errors


def _load_db_scores() -> tuple[dict[str, Score], list[dict]]:
    """songs テーブルから作った歌を読む。"""
    scores: dict[str, Score] = {}
    errors: list[dict] = []
    try:
        with _db_lock:
            rows = _db_mod._db_conn.execute(  # type: ignore[union-attr]
                "SELECT id, title, score_json, mood, theme, created_by, created_at FROM songs ORDER BY id"
            ).fetchall()
    except Exception as e:
        logger.warning("songs テーブルの読み出しに失敗: %s", e)
        return scores, errors

    for song_id, title, score_json, mood, theme, created_by, created_at in rows:
        try:
            data = json.loads(score_json)
            data.update({
                "id": song_id, "title": title, "source": "composed",
                "mood": [m for m in (mood or "").split(",") if m],
                "theme": theme or "", "created_by": created_by or "",
                "created_at": created_at or "",
            })
            scores[song_id] = parse_score(data, song_id=song_id)
        except (ScoreError, json.JSONDecodeError, TypeError) as e:
            logger.error("作った歌の読み込みに失敗: id=%s: %s", song_id, e)
            errors.append({"file": f"songs/{song_id}", "error": str(e)})
    return scores, errors


def load_scores(directory: str | None = None) -> dict[str, Score]:
    """楽譜を読み直す（YAML と DB の両方）。

    id が衝突した場合は YAML を優先する。手書きの楽譜のほうが意図が明確で、
    作った歌は別 id で保存し直せるため。
    """
    global _scores, _load_errors
    directory = directory or SONG_CONFIG_DIR
    yaml_scores, yaml_errors = _load_yaml_scores(directory)
    db_scores, db_errors = _load_db_scores()

    global _loaded
    scores = {**db_scores, **yaml_scores}   # 衝突時は YAML が勝つ
    _scores, _load_errors = scores, yaml_errors + db_errors
    _loaded = True
    logger.info(
        "楽譜を読み込みました: 手書き%d曲 + 作った歌%d曲 = %d曲",
        len(yaml_scores), len(db_scores), len(scores),
    )
    return scores


def _ensure_loaded() -> None:
    if not _loaded:
        load_scores()


def all_scores() -> list[Score]:
    _ensure_loaded()
    return list(_scores.values())


def get_score(song_id: str) -> Score | None:
    _ensure_loaded()
    return _scores.get(song_id)


def load_errors() -> list[dict]:
    return list(_load_errors)


def pick_by_mood(mood: str) -> Score | None:
    """気分タグに合う曲を 1 つ選ぶ。最近歌った曲は避ける。"""
    candidates = [s for s in all_scores() if mood in s.mood]
    if not candidates:
        return None
    if len(candidates) == 1:
        return candidates[0]
    recent = set(recently_played_ids(limit=max(1, len(candidates) - 1)))
    fresh = [s for s in candidates if s.id not in recent]
    return random.choice(fresh or candidates)


def all_moods() -> list[str]:
    return sorted({m for s in all_scores() for m in s.mood})


def pick_for_idle() -> Score | None:
    """「ふと歌う」ときの選曲。しばらく歌っていない曲を優先する。

    同じ曲ばかり流れると飽きるので、直近に歌った曲は後回しにし、
    残った中からランダムに選ぶ。
    """
    scores = all_scores()
    if not scores:
        return None
    recent = recently_played_ids(limit=max(1, len(scores) - 1))
    fresh = [s for s in scores if s.id not in recent]
    return random.choice(fresh or scores)


# ── 作った歌の保存 ───────────────────────────────────────────────────────────

_SLUG_RE = re.compile(r"[^a-z0-9]+")


def make_song_id(title: str, *, taken: set[str] | None = None) -> str:
    """タイトルからファイル名にも使える id を作る。

    日本語のタイトルはローマ字化できないので、その場合は日付ベースの id にする
    （id は人が読むものではなく、タイトルのほうが表に出る）。
    """
    taken = taken if taken is not None else set(_scores)
    ascii_title = unicodedata.normalize("NFKD", title).encode("ascii", "ignore").decode()
    base = _SLUG_RE.sub("-", ascii_title.lower()).strip("-")[:24]
    if not base:
        base = "song-" + datetime.now(_JST).strftime("%m%d-%H%M")
    candidate = base
    i = 2
    while candidate in taken:
        candidate = f"{base}-{i}"
        i += 1
    return candidate


def save_composed_score(
    score: Score, *, theme: str = "", created_by: str = "", song_id: str | None = None
) -> Score:
    """作った歌を songs テーブルに保存し、ライブラリに載せた Score を返す。

    id は呼び出し側が決めなくてよい（タイトルから作り、衝突したら連番を足す）。
    """
    sid = song_id or make_song_id(score.title)
    now = datetime.now(_JST).isoformat()
    payload = score_to_dict(score)
    payload.pop("id", None)   # id は列で持つ
    with _db_lock:
        _db_mod._db_conn.execute(  # type: ignore[union-attr]
            "INSERT INTO songs (id, title, score_json, mood, theme, created_by, created_at, updated_at)"
            " VALUES (?,?,?,?,?,?,?,?)"
            " ON CONFLICT(id) DO UPDATE SET"
            "  title=excluded.title, score_json=excluded.score_json, mood=excluded.mood,"
            "  theme=excluded.theme, created_by=excluded.created_by, updated_at=excluded.updated_at",
            (sid, score.title, json.dumps(payload, ensure_ascii=False),
             ",".join(score.mood), theme, created_by, now, now),
        )
        _db_mod._db_conn.commit()  # type: ignore[union-attr]

    saved = Score(
        **{**score.__dict__, "id": sid, "source": "composed",
           "theme": theme, "created_by": created_by, "created_at": now}
    )
    _scores[sid] = saved
    logger.info("歌を保存しました: id=%s title=%s theme=%s", sid, score.title, theme)
    return saved


def delete_song(song_id: str) -> bool:
    """作った歌を消す。手書きの楽譜（YAML）は消せない。"""
    score = _scores.get(song_id)
    if score is not None and score.source != "composed":
        raise ValueError("手書きの楽譜は config/songs/ のファイルを消してください")
    with _db_lock:
        cur = _db_mod._db_conn.execute("DELETE FROM songs WHERE id = ?", (song_id,))  # type: ignore[union-attr]
        _db_mod._db_conn.commit()  # type: ignore[union-attr]
    _scores.pop(song_id, None)
    return cur.rowcount > 0


# ── 統計（「この前つくったあの歌」を引けるようにするため） ────────────────────

def song_stats() -> dict[str, dict]:
    """曲ごとの再生回数と最後に歌った日時を返す。"""
    try:
        with _db_lock:
            rows = _db_mod._db_conn.execute(  # type: ignore[union-attr]
                "SELECT song_id, COUNT(*), MAX(played_at) FROM song_play_log GROUP BY song_id"
            ).fetchall()
    except Exception:
        return {}
    return {r[0]: {"play_count": r[1], "last_played_at": r[2]} for r in rows}


def describe_songs() -> list[dict]:
    """LLM に渡す曲の一覧。作った歌は「いつ・誰と・何をお題に」まで含める。"""
    stats = song_stats()
    out = []
    for score in all_scores():
        st = stats.get(score.id, {})
        item = {
            "song_id": score.id,
            "title": score.title,
            "mood": score.mood,
            "source": score.source,
            "play_count": st.get("play_count", 0),
            "last_played_at": st.get("last_played_at"),
        }
        if score.source == "composed":
            item["theme"] = score.theme
            item["created_by"] = score.created_by
            item["created_at"] = (score.created_at or "")[:10]
        out.append(item)
    return out


# ── 再生履歴 ──────────────────────────────────────────────────────────────────
# 「この前歌ったばかりの曲をまた歌う」のを避けるため、および UI で
# いつ何を歌ったか見えるようにするために残す。

def record_play(song_id: str, *, source: str, trigger_key: str = "") -> None:
    now = datetime.now(_JST).isoformat()
    try:
        with _db_lock:
            _db_mod._db_conn.execute(  # type: ignore[union-attr]
                "INSERT INTO song_play_log (song_id, source, trigger_key, played_at) VALUES (?,?,?,?)",
                (song_id, source, trigger_key, now),
            )
            _db_mod._db_conn.execute(  # type: ignore[union-attr]
                "DELETE FROM song_play_log WHERE id NOT IN"
                " (SELECT id FROM song_play_log ORDER BY id DESC LIMIT 500)"
            )
            _db_mod._db_conn.commit()  # type: ignore[union-attr]
    except Exception as e:
        logger.warning("song_play_log の記録に失敗（再生自体は成功）: %s", e)


def recently_played_ids(limit: int = 5) -> list[str]:
    try:
        with _db_lock:
            rows = _db_mod._db_conn.execute(  # type: ignore[union-attr]
                "SELECT DISTINCT song_id FROM song_play_log ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
    except Exception:
        return []
    return [r[0] for r in rows]


def play_history(limit: int = 50) -> list[dict]:
    try:
        with _db_lock:
            rows = _db_mod._db_conn.execute(  # type: ignore[union-attr]
                "SELECT song_id, source, trigger_key, played_at FROM song_play_log"
                " ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
    except Exception:
        return []
    return [{"song_id": r[0], "source": r[1], "trigger_key": r[2], "played_at": r[3]} for r in rows]
