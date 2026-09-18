"""歌機能のユニットテスト。

主眼は楽譜 → notes の変換（bridge/features/song/score.py）。
・先頭が必ず無音になること
・合計フレーム数が楽譜の総拍数と一致し、丸め誤差が蓄積しないこと
VOICEVOX ENGINE の呼び出しはすべてモックする（起動不要）。

Run:
    pytest test_song.py -v
"""
import os
from unittest.mock import AsyncMock, patch

import pytest
import yaml

os.environ.setdefault("OPENAI_API_KEY", "sk-test")
os.environ.setdefault("DB_PATH", "/tmp/test-bridge-song.db")

import main  # noqa: E402,F401  作曲は sys.modules["main"].chat_with_llm を呼ぶため

from bridge.features.song.score import (  # noqa: E402
    DEFAULT_FRAME_RATE, NoteSpec, Score, ScoreError,
    build_notes, cache_key, is_single_mora, note_to_midi, parse_score, total_frames,
)


# 作った歌だけを見たいときに使う、楽譜ファイルが1つもないディレクトリ
_EMPTY_DIR = "/nonexistent-song-dir"


def _score(**kwargs) -> Score:
    defaults = dict(
        id="t", title="テスト", bpm=120,
        notes=[NoteSpec("C4", 1.0, "ら"), NoteSpec("E4", 1.0, "ら"), NoteSpec("G4", 2.0, "ら")],
    )
    defaults.update(kwargs)
    return Score(**defaults)


# ── 音名 ─────────────────────────────────────────────────────────────────────

@pytest.mark.parametrize("name,expected", [
    ("C4", 60), ("C0", 12), ("A4", 69), ("B4", 71), ("C5", 72),
    ("F#4", 66), ("Gb4", 66), ("c4", 60), (" C4 ", 60),
])
def test_note_to_midi(name, expected):
    assert note_to_midi(name) == expected


@pytest.mark.parametrize("name", ["rest", "r", "-", "休符", ""])
def test_note_to_midi_rest(name):
    assert note_to_midi(name) is None


@pytest.mark.parametrize("name", ["H4", "C", "4C", "C4#", "Z9", "C99"])
def test_note_to_midi_invalid(name):
    with pytest.raises(ScoreError):
        note_to_midi(name)


# ── 歌詞（1モーラ判定） ───────────────────────────────────────────────────────

@pytest.mark.parametrize("lyric,expected", [
    ("ん", True), ("ら", True), ("ラ", True), ("きゃ", True), ("ー", True),
    ("", False), ("らら", False), ("ゃ", False), ("きゃあ", False), ("a", False), ("歌", False),
])
def test_is_single_mora(lyric, expected):
    assert is_single_mora(lyric) is expected


# ── 変換の中核 ───────────────────────────────────────────────────────────────

def test_first_note_is_silent():
    """先頭ノートは必ず無音（key=None, lyric=""）。楽譜に書かなくても入る。"""
    notes = build_notes(_score())
    assert notes[0]["key"] is None
    assert notes[0]["lyric"] == ""
    assert notes[0]["frame_length"] >= 1
    # 2音目以降は楽譜どおり
    assert [n["key"] for n in notes[1:]] == [60, 64, 67]


def test_rest_becomes_silent_note():
    score = _score(notes=[NoteSpec("C4", 1.0, "ら"), NoteSpec("rest", 1.0, ""), NoteSpec("E4", 1.0, "ら")])
    notes = build_notes(score)
    assert [n["key"] for n in notes] == [None, 60, None, 64]
    assert notes[2]["lyric"] == ""


def test_total_frames_matches_tempo():
    """合計フレーム数が「総拍数 × 60/BPM × frame_rate」と一致する。"""
    score = _score()  # 先頭無音 0.5 + 1 + 1 + 2 = 4.5 拍
    notes = build_notes(score)
    assert score.total_beats == pytest.approx(4.5)
    expected = round(4.5 * (60.0 / 120) * DEFAULT_FRAME_RATE)
    assert sum(n["frame_length"] for n in notes) == expected
    assert total_frames(score) == expected


@pytest.mark.parametrize("bpm", [72, 100, 120, 132, 152, 168])
@pytest.mark.parametrize("beats", [0.25, 1 / 3, 0.5, 1.0])
def test_no_rounding_drift(bpm, beats):
    """丸め誤差が蓄積しないこと。

    1音ずつ round すると誤差が音数ぶん積み上がるが、累積フレームの差分で
    求めているので、何音つないでも合計は総拍数から求めた値と完全に一致する。
    """
    score = _score(bpm=bpm, notes=[NoteSpec("C4", beats, "ら") for _ in range(64)])
    notes = build_notes(score)
    assert sum(n["frame_length"] for n in notes) == total_frames(score)
    # 個別に丸めた場合との差（＝防いだドリフト）を確かめておく
    naive = round(score.lead_silence_beats * (60.0 / bpm) * DEFAULT_FRAME_RATE) + \
        64 * round(beats * (60.0 / bpm) * DEFAULT_FRAME_RATE)
    assert abs(naive - total_frames(score)) < 64  # 素朴実装はここまでずれ得る


def test_tempo_scales_duration():
    """BPM を倍にすると長さは半分になる。"""
    fast = _score(bpm=240)
    slow = _score(bpm=120)
    assert total_frames(slow) == pytest.approx(total_frames(fast) * 2, abs=1)


def test_frame_rate_is_respected():
    score = _score()
    assert total_frames(score, 48.0) == round(4.5 * 0.5 * 48.0)
    assert sum(n["frame_length"] for n in build_notes(score, 48.0)) == total_frames(score, 48.0)


def test_transpose_shifts_all_keys():
    notes = build_notes(_score(transpose=12))
    assert [n["key"] for n in notes] == [None, 72, 76, 79]


def test_transpose_out_of_range_raises():
    with pytest.raises(ScoreError, match="音域外"):
        build_notes(_score(transpose=24, notes=[NoteSpec("B8", 1.0, "ら")]))


def test_note_too_short_for_tempo_raises():
    """1フレーム未満になる音は黙って潰さずエラーにする。"""
    with pytest.raises(ScoreError, match="短すぎます"):
        build_notes(_score(bpm=400, notes=[NoteSpec("C4", 0.001, "ら")]))


def test_duration_sec():
    assert _score().duration_sec() == pytest.approx(2.25, abs=0.02)  # 4.5拍 @120BPM


# ── 楽譜のパース ─────────────────────────────────────────────────────────────

def test_parse_score_minimal():
    score = parse_score({"bpm": 120, "notes": [{"note": "C4", "beats": 1}]}, song_id="x")
    assert score.id == "x"
    assert score.title == "x"
    assert score.default_lyric == "ん"      # 既定は鼻歌
    assert score.notes[0].lyric == "ん"     # 省略時は default_lyric が入る
    assert score.sing_style_id == 6000
    assert score.frame_decode_style_id == 3003


def test_parse_score_full():
    score = parse_score({
        "id": "s", "title": "曲", "bpm": 150, "transpose": -2,
        "mood": "hurry", "default_lyric": "ら",
        "style": {"sing": 6000, "frame_decode": 3007},
        "notes": [{"note": "C4", "beats": 0.5}, {"note": "rest", "beats": 0.5, "lyric": "ら"}],
    })
    assert score.mood == ["hurry"]
    assert score.transpose == -2
    assert score.frame_decode_style_id == 3007
    assert score.notes[0].lyric == "ら"
    assert score.notes[1].lyric == ""  # 休符には歌詞が付かない


@pytest.mark.parametrize("data,message", [
    ({"bpm": 120}, "notes"),
    ({"bpm": 120, "notes": []}, "notes"),
    ({"bpm": 0, "notes": [{"note": "C4"}]}, "bpm"),
    ({"bpm": 500, "notes": [{"note": "C4"}]}, "bpm"),
    ({"bpm": 120, "notes": [{"note": "C4", "beats": 0}]}, "beats"),
    ({"bpm": 120, "notes": [{"note": "C4", "beats": -1}]}, "beats"),
    ({"bpm": 120, "notes": [{"note": "C4", "lyric": "らら"}]}, "モーラ"),
    ({"bpm": 120, "default_lyric": "ららら", "notes": [{"note": "C4"}]}, "default_lyric"),
    ({"bpm": 120, "transpose": 99, "notes": [{"note": "C4"}]}, "transpose"),
    ({"bpm": 120, "lead_silence_beats": 0, "notes": [{"note": "C4"}]}, "lead_silence_beats"),
])
def test_parse_score_invalid(data, message):
    with pytest.raises(ScoreError, match=message):
        parse_score(data, song_id="x")


def test_parse_score_requires_id():
    with pytest.raises(ScoreError, match="id"):
        parse_score({"bpm": 120, "notes": [{"note": "C4"}]})


# ── 同梱の楽譜 ───────────────────────────────────────────────────────────────

@pytest.mark.parametrize("song_id", ["hurry", "twinkle", "galop"])
def test_bundled_scores_are_valid(song_id):
    with open(f"config/songs/{song_id}.yaml", encoding="utf-8") as f:
        score = parse_score(yaml.safe_load(f), song_id=song_id)
    notes = build_notes(score)
    assert notes[0]["key"] is None                       # 先頭は無音
    assert sum(n["frame_length"] for n in notes) == total_frames(score)
    assert all(n["frame_length"] >= 1 for n in notes)
    assert 1 < score.duration_sec() < 60


def test_twinkle_melody():
    """きらきら星の最初の1フレーズが期待どおりの音程で並ぶ（変換の実地確認）。"""
    with open("config/songs/twinkle.yaml", encoding="utf-8") as f:
        score = parse_score(yaml.safe_load(f), song_id="twinkle")
    keys = [n["key"] for n in build_notes(score)][1:8]
    assert keys == [60, 60, 67, 67, 69, 69, 67]  # ド ド ソ ソ ラ ラ ソ


# ── キャッシュキー ───────────────────────────────────────────────────────────

def _key(score, **kw):
    args = dict(engine_version="0.25.2", frame_rate=DEFAULT_FRAME_RATE, audio_spec="mp3-16000-64k")
    args.update(kw)
    return cache_key(score, **args)


def test_cache_key_is_stable():
    assert _key(_score()) == _key(_score())


@pytest.mark.parametrize("change", [
    {"bpm": 121},
    {"transpose": 1},
    {"sing_style_id": 6001},
    {"frame_decode_style_id": 3001},
    {"notes": [NoteSpec("C4", 1.0, "ら")]},
    {"lead_silence_beats": 1.0},
])
def test_cache_key_changes_with_score(change):
    assert _key(_score()) != _key(_score(**change))


@pytest.mark.parametrize("change", [
    {"engine_version": "0.25.3"},
    {"frame_rate": 48.0},
    {"audio_spec": "mp3-24000-64k"},
])
def test_cache_key_changes_with_synthesis_conditions(change):
    assert _key(_score()) != _key(_score(), **change)


def test_cache_key_ignores_title():
    """タイトルだけの変更では作り直さない（音は変わらないため）。"""
    assert _key(_score()) == _key(_score(title="別のタイトル"))


# ── ENGINE 呼び出し（モック） ────────────────────────────────────────────────

@pytest.mark.asyncio
async def test_synthesize_posts_two_stages():
    """sing_frame_audio_query の結果をそのまま frame_synthesis に渡す2段階構成。"""
    from bridge.features.song import engine

    engine.reset_cache()
    score = _score()
    frame_query = {"f0": [1.0], "volume": [1.0], "phonemes": []}

    class _Resp:
        def __init__(self, json_data=None, content=b""):
            self._json, self.content = json_data, content
        def raise_for_status(self): pass
        def json(self): return self._json

    posts = []

    async def _post(url, params=None, json=None):
        posts.append((url, params, json))
        if url.endswith("/sing_frame_audio_query"):
            return _Resp(json_data=frame_query)
        return _Resp(content=b"RIFFfake")

    client = AsyncMock()
    client.post = AsyncMock(side_effect=_post)
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)

    with patch("bridge.features.song.engine.httpx.AsyncClient", return_value=client):
        wav = await engine.synthesize(score, DEFAULT_FRAME_RATE)

    assert wav == b"RIFFfake"
    assert posts[0][0].endswith("/sing_frame_audio_query")
    assert posts[0][1] == {"speaker": 6000}          # 音程は sing 側
    assert posts[0][2]["notes"][0]["key"] is None    # 先頭無音を送っている
    assert posts[1][0].endswith("/frame_synthesis")
    assert posts[1][1] == {"speaker": 3003}          # 声色は frame_decode 側
    assert posts[1][2] == frame_query                # 1 の結果をそのまま渡す
    engine.reset_cache()


@pytest.mark.asyncio
async def test_synthesize_wraps_errors():
    from bridge.features.song import engine

    engine.reset_cache()
    client = AsyncMock()
    client.post = AsyncMock(side_effect=RuntimeError("boom"))
    client.__aenter__ = AsyncMock(return_value=client)
    client.__aexit__ = AsyncMock(return_value=False)

    with patch("bridge.features.song.engine.httpx.AsyncClient", return_value=client):
        with pytest.raises(engine.SongEngineError):
            await engine.synthesize(_score(), DEFAULT_FRAME_RATE)
    engine.reset_cache()


# ── gatekeeper（時間帯判定は DB 非依存なのでそのまま試せる） ──────────────────

@pytest.mark.parametrize("now,expected", [
    ("23:30", True), ("03:00", True), ("06:59", True),
    ("07:00", False), ("12:00", False), ("21:59", False), ("22:00", True),
])
def test_quiet_hours_across_midnight(now, expected):
    from datetime import datetime
    from bridge.features.gatekeeper import in_quiet_hours

    h, m = map(int, now.split(":"))
    assert in_quiet_hours("22:00", "07:00", datetime(2026, 9, 17, h, m)) is expected


def test_quiet_hours_same_day_range():
    from datetime import datetime
    from bridge.features.gatekeeper import in_quiet_hours

    assert in_quiet_hours("13:00", "15:00", datetime(2026, 9, 17, 14, 0)) is True
    assert in_quiet_hours("13:00", "15:00", datetime(2026, 9, 17, 16, 0)) is False


def test_quiet_hours_disabled_when_unset():
    from bridge.features.gatekeeper import in_quiet_hours

    assert in_quiet_hours("", "") is False


# ── 出発リミットの計算 ───────────────────────────────────────────────────────

@pytest.fixture
def song_db(tmp_path, monkeypatch):
    """items / app_settings を持つ一時 DB を用意してトリガー計算を試せるようにする。"""
    import sqlite3
    import bridge.core.db as _db_mod

    monkeypatch.setattr(_db_mod, "DB_PATH", str(tmp_path / "song.db"))
    old_conn = _db_mod._db_conn
    _db_mod._db_conn = sqlite3.connect(str(tmp_path / "song.db"), check_same_thread=False)
    _db_mod._init_db()
    yield _db_mod._db_conn
    _db_mod._db_conn.close()
    _db_mod._db_conn = old_conn


def _add_event(conn, item_id, start_at, title="スイミング", person="しおり", all_day=0):
    conn.execute(
        "INSERT INTO items (id, type, source_id, person_name, notify, title,"
        " start_at, all_day, status, synced_at) VALUES (?, 'event', 'cal', ?, 1, ?, ?, ?, 'active', ?)",
        (item_id, person, title, start_at.isoformat(), all_day, start_at.isoformat()),
    )
    conn.commit()


def test_due_departures_fires_inside_lead_window(song_db):
    """出発リミットまで残り N 分を切ったら対象になる。"""
    from datetime import datetime, timedelta
    from bridge.config import _JST
    from bridge.features.song.triggers import due_departures

    now = datetime(2026, 9, 17, 15, 0, tzinfo=_JST)
    # 既定: 移動15分 + 準備10分 = リミットは開始の25分前、残り10分を切ったら発火。
    # 開始が 32 分後 → リミットまで残り 7 分 → 対象
    _add_event(song_db, "e1", now + timedelta(minutes=32))
    due = due_departures(now)
    assert [d["id"] for d in due] == ["e1"]
    assert due[0]["remaining_minutes"] == pytest.approx(7.0)
    assert due[0]["depart_limit"].startswith("2026-09-17T15:07")
    assert due[0]["song_id"] == "hurry"


def test_due_departures_ignores_far_future(song_db):
    from datetime import datetime, timedelta
    from bridge.config import _JST
    from bridge.features.song.triggers import due_departures

    now = datetime(2026, 9, 17, 15, 0, tzinfo=_JST)
    _add_event(song_db, "e1", now + timedelta(minutes=90))   # リミットまで65分
    _add_event(song_db, "e2", now + timedelta(hours=10))     # 先すぎて取得対象外
    assert due_departures(now) == []


def test_due_departures_keeps_short_grace_after_limit(song_db):
    """リミットを少し過ぎていても、猶予のあいだは取りこぼさない。"""
    from datetime import datetime, timedelta
    from bridge.config import _JST
    from bridge.features.song.triggers import LATE_GRACE_MINUTES, due_departures

    now = datetime(2026, 9, 17, 15, 0, tzinfo=_JST)
    _add_event(song_db, "late", now + timedelta(minutes=22))  # リミットは3分前
    due = due_departures(now)
    assert [d["id"] for d in due] == ["late"]
    assert -LATE_GRACE_MINUTES <= due[0]["remaining_minutes"] < 0

    _add_event(song_db, "toolate", now + timedelta(minutes=17))  # リミットは8分前
    assert "toolate" not in [d["id"] for d in due_departures(now)]


def test_due_departures_skips_all_day_events(song_db):
    """終日予定は開始時刻を持たないので出発リミットを作らない。"""
    from datetime import datetime, timedelta
    from bridge.config import _JST
    from bridge.features.song.triggers import due_departures

    now = datetime(2026, 9, 17, 15, 0, tzinfo=_JST)
    _add_event(song_db, "allday", now + timedelta(minutes=32), all_day=1)
    assert due_departures(now) == []


def test_due_departures_uses_per_item_override(song_db):
    """予定ごとの移動時間の上書きが効く。"""
    from datetime import datetime, timedelta
    from bridge.config import _JST
    from bridge.features.song.triggers import due_departures

    now = datetime(2026, 9, 17, 15, 0, tzinfo=_JST)
    _add_event(song_db, "far", now + timedelta(minutes=70))
    assert due_departures(now) == []  # 既定（移動15分）ではまだ早い

    song_db.execute(
        "INSERT INTO song_trigger_overrides (item_id, travel_minutes, prep_minutes, song_id, enabled, updated_at)"
        " VALUES ('far', 50, 10, 'galop', 1, '2026-09-17T15:00:00+09:00')"
    )
    song_db.commit()
    due = due_departures(now)  # リミットは開始の60分前 → 残り10分
    assert [d["id"] for d in due] == ["far"]
    assert due[0]["travel_minutes"] == 50
    assert due[0]["song_id"] == "galop"


def test_due_departures_respects_disabled_override(song_db):
    from datetime import datetime, timedelta
    from bridge.config import _JST
    from bridge.features.song.triggers import due_departures

    now = datetime(2026, 9, 17, 15, 0, tzinfo=_JST)
    _add_event(song_db, "e1", now + timedelta(minutes=32))
    song_db.execute(
        "INSERT INTO song_trigger_overrides (item_id, travel_minutes, prep_minutes, song_id, enabled, updated_at)"
        " VALUES ('e1', NULL, NULL, NULL, 0, '2026-09-17T15:00:00+09:00')"
    )
    song_db.commit()
    assert due_departures(now) == []


@pytest.mark.asyncio
async def test_check_departure_songs_fires_once_per_event(song_db):
    """同じ予定では2回鳴らさない（gatekeeper の once）。

    gatekeeper の判定は play.play_song の中にあるので、ここは
    ENGINE・キャッシュ・MQTT だけを差し替えて本物の経路を通す。
    """
    from datetime import datetime, timedelta
    from bridge.config import _JST
    from bridge.core.db import _set_setting
    from bridge.features.song import triggers

    _set_setting("song_trigger_enabled", "true")
    _set_setting("song_quiet_start", "")   # 時刻に左右されないよう深夜抑制は切る
    _set_setting("song_quiet_end", "")
    _set_setting("song_cooldown_minutes", "0")
    now = datetime(2026, 9, 17, 15, 0, tzinfo=_JST)
    _add_event(song_db, "e1", now + timedelta(minutes=32))

    published = []

    with (
        patch("bridge.features.song.triggers.due_departures", return_value=[{
            "id": "e1", "person_name": "しおり", "title": "スイミング",
            "remaining_minutes": 7.0, "song_id": "hurry",
        }]),
        patch("bridge.features.song.play.is_at_home", return_value=True),
        patch("bridge.features.song.play.SONG_PUBLIC_BASE_URL", "http://pi:8000"),
        patch("bridge.features.song.cache.ensure_song", new=AsyncMock(return_value=("hurry-abc.mp3", "abc"))),
        patch("bridge.features.song.play.get_engine_info",
              new=AsyncMock(return_value={"version": "0.25.2", "frame_rate": DEFAULT_FRAME_RATE})),
        patch("bridge.features.song.play.publish_speak",
              side_effect=lambda *a, **k: published.append(a)),
    ):
        await triggers.check_departure_songs()
        await triggers.check_departure_songs()   # 2回目は gatekeeper に止められる

    assert len(published) == 1
    assert published[0][0] == "http://pi:8000/songs/hurry-abc.mp3"  # audioUrl
    # audioStreamingUrl も必ず載せる（None だと publish_speak がフィールドごと
    # 落とし、デバイスは ACK を返すだけで取りに来ない）
    assert published[0][1] == "http://pi:8000/songs/hurry-abc.mp3"
    assert published[0][3] == "song_calendar"                        # source


@pytest.mark.asyncio
async def test_check_departure_songs_skips_when_away(song_db):
    from bridge.core.db import _set_setting
    from bridge.features.song import triggers

    _set_setting("song_trigger_enabled", "true")
    played = []

    with (
        patch("bridge.features.song.triggers.due_departures", return_value=[{
            "id": "e1", "person_name": "しおり", "title": "スイミング",
            "remaining_minutes": 7.0, "song_id": "hurry",
        }]),
        patch("bridge.features.song.play.is_at_home", return_value=False),
        patch("bridge.features.song.play.play_song", side_effect=lambda *a, **k: played.append(1)),
    ):
        await triggers.check_departure_songs()

    assert played == []


@pytest.mark.asyncio
async def test_check_departure_songs_disabled_by_setting(song_db):
    from bridge.core.db import _set_setting
    from bridge.features.song import triggers

    _set_setting("song_trigger_enabled", "false")
    with patch("bridge.features.song.triggers.due_departures") as due:
        await triggers.check_departure_songs()
    due.assert_not_called()


# ── キャッシュ（ffmpeg はモック。実機では data/songs に MP3 が置かれる） ─────

@pytest.fixture
def song_cache_dir(tmp_path, monkeypatch):
    from bridge.features.song import cache

    monkeypatch.setattr(cache, "SONG_DIR", str(tmp_path))
    return tmp_path


def _fake_ffmpeg(calls, out=b"ID3fake-mp3", returncode=0, stderr=b""):
    """asyncio.create_subprocess_exec の差し替え。呼ばれた引数を calls に記録する。

    本物の ffmpeg と同じく、出力はパイプではなくコマンド末尾の一時ファイルに書く
    （パイプだと Xing ヘッダが欠落するため、実装がファイル出力になっている）。
    """
    async def _exec(*cmd, **kwargs):
        calls.append(cmd)
        if returncode == 0 and out:
            with open(cmd[-1], "wb") as f:    # cmd[-1] が出力先のパス
                f.write(out)
        proc = AsyncMock()
        proc.communicate = AsyncMock(return_value=(b"", stderr))
        proc.returncode = returncode
        return proc
    return _exec


@pytest.mark.asyncio
async def test_ensure_song_builds_then_reuses_cache(song_cache_dir):
    """2回目は ENGINE にも ffmpeg にも触らない（事前生成＝毎回合成しない）。"""
    from bridge.features.song import cache

    calls = []
    synth = AsyncMock(return_value=b"RIFFwav")
    with (
        patch("bridge.features.song.engine.get_engine_info",
              new=AsyncMock(return_value={"version": "0.25.2", "frame_rate": DEFAULT_FRAME_RATE})),
        patch("bridge.features.song.engine.synthesize", new=synth),
        patch("asyncio.create_subprocess_exec", new=_fake_ffmpeg(calls)),
    ):
        name1, key1 = await cache.ensure_song(_score())
        name2, key2 = await cache.ensure_song(_score())

    assert (name1, key1) == (name2, key2)
    assert synth.await_count == 1        # 2回目は合成していない
    assert len(calls) == 1               # 2回目は ffmpeg も呼んでいない
    assert (song_cache_dir / name1).read_bytes() == b"ID3fake-mp3"
    assert not list(song_cache_dir.glob("*.tmp"))   # 一時ファイルが残らない


@pytest.mark.asyncio
async def test_ensure_song_ffmpeg_arguments(song_cache_dir):
    """発話と同じ 24kHz mono MP3 に変換し、ID3/Xing を付けない。"""
    from bridge.features.song import cache

    calls = []
    with (
        patch("bridge.features.song.engine.get_engine_info",
              new=AsyncMock(return_value={"version": "0.25.2", "frame_rate": DEFAULT_FRAME_RATE})),
        patch("bridge.features.song.engine.synthesize", new=AsyncMock(return_value=b"RIFFwav")),
        patch("asyncio.create_subprocess_exec", new=_fake_ffmpeg(calls)),
    ):
        await cache.ensure_song(_score())

    cmd = calls[0]
    # 発話（VOICEVOX Web 版）の MP3 と同じ形式に揃える。ここがずれると無音になる
    assert cmd[cmd.index("-ar") + 1] == "24000"
    assert cmd[cmd.index("-ac") + 1] == "1"
    assert "libmp3lame" in cmd
    # 先頭が MP3 フレームで始まるよう、ID3 タグも Xing フレームも付けない。
    # 付けるとデバイスのデコーダが同期できず、取得には成功するのに無音になる。
    assert cmd[cmd.index("-write_id3v2") + 1] == "0"
    assert cmd[cmd.index("-write_xing") + 1] == "0"
    assert cmd[-1].endswith(".mp3") and "pipe:" not in cmd[-1]


@pytest.mark.asyncio
async def test_ensure_song_purges_stale_cache(song_cache_dir):
    """楽譜を書き換えたら古い MP3 は消える（溜め込まない）。"""
    from bridge.features.song import cache

    calls = []
    patches = (
        patch("bridge.features.song.engine.get_engine_info",
              new=AsyncMock(return_value={"version": "0.25.2", "frame_rate": DEFAULT_FRAME_RATE})),
        patch("bridge.features.song.engine.synthesize", new=AsyncMock(return_value=b"RIFFwav")),
        patch("asyncio.create_subprocess_exec", new=_fake_ffmpeg(calls)),
    )
    with patches[0], patches[1], patches[2]:
        old_name, _ = await cache.ensure_song(_score())
        new_name, _ = await cache.ensure_song(_score(bpm=130))   # 楽譜を変更

    assert old_name != new_name
    assert [p.name for p in song_cache_dir.glob("*.mp3")] == [new_name]


@pytest.mark.asyncio
async def test_ensure_song_raises_when_ffmpeg_fails(song_cache_dir):
    from bridge.features.song import cache

    calls = []
    with (
        patch("bridge.features.song.engine.get_engine_info",
              new=AsyncMock(return_value={"version": "0.25.2", "frame_rate": DEFAULT_FRAME_RATE})),
        patch("bridge.features.song.engine.synthesize", new=AsyncMock(return_value=b"RIFFwav")),
        patch("asyncio.create_subprocess_exec",
              new=_fake_ffmpeg(calls, out=b"", returncode=1, stderr=b"broken")),
    ):
        with pytest.raises(cache.SongBuildError, match="ffmpeg"):
            await cache.ensure_song(_score())
    assert not list(song_cache_dir.glob("*.mp3"))   # 失敗したら壊れたファイルを残さない


@pytest.mark.asyncio
async def test_prebuild_all_survives_engine_down(song_cache_dir):
    """ENGINE が落ちていても起動を止めない（歌だけが無効になる）。"""
    from bridge.features.song import cache, engine

    with patch("bridge.features.song.engine.get_engine_info",
               new=AsyncMock(side_effect=engine.SongEngineError("接続できません"))):
        result = await cache.prebuild_all([_score()])

    assert result["built"] == [] and result["cached"] == []
    assert [f["id"] for f in result["failed"]] == ["t"]


# ── 作った歌の保存（songs テーブル） ─────────────────────────────────────────

_GOOD_SCORE_JSON = """{
  "title": "おふろのうた",
  "bpm": 110,
  "mood": ["happy"],
  "default_lyric": "ら",
  "style": {"sing": 6000, "frame_decode": 3001},
  "notes": [
    {"note": "C4", "beats": 1}, {"note": "E4", "beats": 1},
    {"note": "G4", "beats": 1}, {"note": "rest", "beats": 0.5},
    {"note": "G4", "beats": 0.5}, {"note": "E4", "beats": 1},
    {"note": "C4", "beats": 2}
  ]
}"""


@pytest.fixture
def song_library_db(song_db, tmp_path):
    """空の楽譜ディレクトリ + 一時 DB。作った歌だけを見たいときに使う。"""
    from bridge.features.song import library

    library.load_scores(_EMPTY_DIR)
    yield library
    library.load_scores()   # 他のテストに影響を残さない


def test_save_composed_score_roundtrips(song_library_db):
    """保存 → 読み直しで、同じ音の楽譜として戻ってくる。"""
    from bridge.features.song.score import build_notes

    original = _score(title="おふろのうた", bpm=110, transpose=-2)
    saved = song_library_db.save_composed_score(original, theme="おふろ", created_by="しおり")

    assert saved.source == "composed"
    assert saved.theme == "おふろ"
    assert saved.created_by == "しおり"

    song_library_db.load_scores(_EMPTY_DIR)   # DB から読み直す
    loaded = song_library_db.get_score(saved.id)
    assert loaded is not None
    assert loaded.title == "おふろのうた"
    assert loaded.bpm == 110
    assert loaded.transpose == -2
    assert loaded.source == "composed"
    assert loaded.theme == "おふろ"
    assert build_notes(loaded) == build_notes(original)   # 音が変わっていない


def test_saved_song_keeps_cache_key(song_library_db):
    """保存・読み直しでキャッシュキーが変わらない（作り直しが起きない）。"""
    original = _score(title="おふろのうた")
    saved = song_library_db.save_composed_score(original, theme="おふろ")
    song_library_db.load_scores(_EMPTY_DIR)
    loaded = song_library_db.get_score(saved.id)
    assert _key(loaded) == _key(saved)


def test_make_song_id_avoids_collision(song_library_db):
    a = song_library_db.save_composed_score(_score(title="ohuro"), theme="")
    b = song_library_db.save_composed_score(_score(title="ohuro"), theme="")
    assert a.id != b.id
    assert b.id.startswith(a.id)


def test_make_song_id_falls_back_for_japanese_title(song_library_db):
    saved = song_library_db.save_composed_score(_score(title="おふろのうた"), theme="")
    assert saved.id.startswith("song-")   # ローマ字化できないタイトルは日付ベース


def test_delete_song(song_library_db):
    saved = song_library_db.save_composed_score(_score(title="tmp"), theme="")
    assert song_library_db.delete_song(saved.id) is True
    assert song_library_db.get_score(saved.id) is None
    assert song_library_db.delete_song(saved.id) is False


def test_delete_builtin_song_is_refused(song_db):
    from bridge.features.song import library

    library.load_scores()   # 同梱の YAML を読む
    with pytest.raises(ValueError, match="手書き"):
        library.delete_song("twinkle")


def test_yaml_wins_over_db_on_id_conflict(song_db):
    """同じ id なら手書きの楽譜を優先する（意図が明確なほうを残す）。"""
    from bridge.features.song import library

    library.load_scores()
    library.save_composed_score(_score(title="にせもの"), theme="", song_id="twinkle")
    library.load_scores()
    assert library.get_score("twinkle").source == "builtin"
    assert library.get_score("twinkle").title == "きらきら星"


def test_describe_songs_includes_history(song_library_db):
    saved = song_library_db.save_composed_score(
        _score(title="uta"), theme="おふろ", created_by="しおり")
    song_library_db.record_play(saved.id, source="ui")
    song_library_db.record_play(saved.id, source="llm")

    described = {d["song_id"]: d for d in song_library_db.describe_songs()}
    assert described[saved.id]["play_count"] == 2
    assert described[saved.id]["theme"] == "おふろ"
    assert described[saved.id]["created_by"] == "しおり"
    assert described[saved.id]["last_played_at"] is not None


def test_pick_for_idle_avoids_recent(song_library_db):
    a = song_library_db.save_composed_score(_score(title="aaa"), theme="")
    b = song_library_db.save_composed_score(_score(title="bbb"), theme="")
    song_library_db.record_play(a.id, source="ui")
    picked = {song_library_db.pick_for_idle().id for _ in range(20)}
    assert picked == {b.id}   # 直近に歌った a は選ばれない


# ── 作曲（LLM はモック） ─────────────────────────────────────────────────────

@pytest.mark.parametrize("raw", [
    _GOOD_SCORE_JSON,
    "```json\n" + _GOOD_SCORE_JSON + "\n```",
    "はい、作りました！\n" + _GOOD_SCORE_JSON + "\nどうぞ！",
])
def test_extract_json_object_tolerates_wrapping(raw):
    from bridge.features.song.compose import extract_json_object

    assert extract_json_object(raw)["title"] == "おふろのうた"


@pytest.mark.parametrize("raw", ["", "ごめん、作れませんでした", "[1, 2, 3]"])
def test_extract_json_object_rejects_garbage(raw):
    from bridge.features.song.compose import extract_json_object

    with pytest.raises(ScoreError):
        extract_json_object(raw)


def _mock_llm(*replies):
    """main.chat_with_llm の差し替え。replies を順に返す。"""
    calls = []

    async def _chat(prompt, **kwargs):
        calls.append({"prompt": prompt, **kwargs})
        return replies[min(len(calls) - 1, len(replies) - 1)]
    return _chat, calls


@pytest.mark.asyncio
async def test_compose_score_saves_valid_song(song_library_db):
    import sys
    from bridge.features.song import compose

    chat, calls = _mock_llm(_GOOD_SCORE_JSON)
    with patch.object(sys.modules["main"], "chat_with_llm", new=chat):
        score = await compose.compose_score(theme="おふろ", mood="happy", requested_by="しおり")

    assert score.title == "おふろのうた"
    assert score.source == "composed"
    assert score.theme == "おふろ"
    assert score.frame_decode_style_id == 3001
    assert len(calls) == 1
    # 作曲は会話履歴に混ぜず、道具も使わせず、作曲用モデルを使う
    assert calls[0]["session_key"] == ""
    assert calls[0]["use_functions"] is False
    assert calls[0]["purpose"] == "compose"
    # お題・雰囲気・依頼者がプロンプトに入っている
    assert "おふろ" in calls[0]["prompt"] and "happy" in calls[0]["prompt"]
    assert "しおり" in calls[0]["prompt"]
    # 保存されてライブラリから引ける
    assert song_library_db.get_score(score.id) is not None


@pytest.mark.asyncio
async def test_compose_score_retries_with_error_message(song_library_db):
    """1回目の楽譜が不正なら、理由を添えて作り直させる。"""
    import sys
    from bridge.features.song import compose

    bad = '{"title": "だめな歌", "bpm": 120, "notes": [{"note": "H9", "beats": 1}]}'
    chat, calls = _mock_llm(bad, _GOOD_SCORE_JSON)
    with patch.object(sys.modules["main"], "chat_with_llm", new=chat):
        score = await compose.compose_score(theme="おふろ")

    assert score.title == "おふろのうた"
    assert len(calls) == 2
    assert "エラー" in calls[1]["prompt"]      # 2回目には理由が入っている
    assert "H9" in calls[1]["prompt"]


@pytest.mark.asyncio
async def test_compose_score_gives_up_after_retries(song_library_db):
    import sys
    from bridge.features.song import compose

    bad = '{"title": "だめ", "bpm": 120, "notes": []}'
    chat, calls = _mock_llm(bad, bad)
    with patch.object(sys.modules["main"], "chat_with_llm", new=chat):
        with pytest.raises(ScoreError, match="楽譜を作れませんでした"):
            await compose.compose_score()

    assert len(calls) == 2                       # 無限に投げ続けない
    assert song_library_db.all_scores() == []    # 失敗した歌は保存しない


@pytest.mark.asyncio
async def test_compose_and_build_rolls_back_when_synthesis_fails(song_library_db):
    """合成できない歌を一覧に残さない。"""
    import sys
    from bridge.features.song import compose

    chat, _ = _mock_llm(_GOOD_SCORE_JSON)
    with (
        patch.object(sys.modules["main"], "chat_with_llm", new=chat),
        patch("bridge.features.song.cache.ensure_song",
              new=AsyncMock(side_effect=RuntimeError("ENGINE が落ちています"))),
    ):
        with pytest.raises(RuntimeError, match="ENGINE"):
            await compose.compose_and_build(theme="おふろ")

    assert song_library_db.all_scores() == []


@pytest.mark.asyncio
async def test_compose_and_build_keeps_song_on_success(song_library_db):
    import sys
    from bridge.features.song import compose

    chat, _ = _mock_llm(_GOOD_SCORE_JSON)
    with (
        patch.object(sys.modules["main"], "chat_with_llm", new=chat),
        patch("bridge.features.song.cache.ensure_song",
              new=AsyncMock(return_value=("x.mp3", "key"))),
    ):
        score = await compose.compose_and_build(theme="おふろ")

    assert [s.id for s in song_library_db.all_scores()] == [score.id]


# ── ふと歌う ─────────────────────────────────────────────────────────────────

def _idle_settings(**over):
    from bridge.core.db import _set_setting

    base = {
        "song_idle_enabled": "true", "song_idle_min_hours": "6",
        "song_idle_chance": "1", "song_idle_start": "09:00", "song_idle_end": "20:00",
    }
    base.update({k: str(v) for k, v in over.items()})
    for k, v in base.items():
        _set_setting(k, v)


def test_idle_due_requires_enabled(song_db):
    from bridge.features.song.triggers import idle_song_due

    _idle_settings(song_idle_enabled="false")
    due, reason = idle_song_due()  # 無効なら時刻を見るまでもなく止まる
    assert due is False and reason == "無効"


@pytest.mark.parametrize("hour,expected", [(8, False), (9, True), (19, True), (20, False), (23, False)])
def test_idle_due_respects_time_window(song_db, hour, expected):
    from datetime import datetime
    from bridge.config import _JST
    from bridge.features.song.triggers import idle_song_due

    _idle_settings()
    with patch("bridge.features.song.play.is_at_home", return_value=True):
        due, _ = idle_song_due(datetime(2026, 9, 17, hour, 0, tzinfo=_JST))
    assert due is expected


def test_idle_due_waits_min_hours(song_db):
    from datetime import datetime, timedelta
    from bridge.config import _JST
    from bridge.features import gatekeeper
    from bridge.features.song import play
    from bridge.features.song.triggers import idle_song_due

    _idle_settings(song_idle_min_hours=6)
    now = datetime(2026, 9, 17, 15, 0, tzinfo=_JST)
    gatekeeper.clear(play.GATE_KIND)
    with patch("bridge.features.song.play.is_at_home", return_value=True):
        assert idle_song_due(now)[0] is True          # 一度も歌っていなければ歌える

        with patch("bridge.features.gatekeeper.last_fired_at",
                   return_value=(now - timedelta(hours=2)).isoformat()):
            due, reason = idle_song_due(now)
        assert due is False and "2.0時間" in reason

        with patch("bridge.features.gatekeeper.last_fired_at",
                   return_value=(now - timedelta(hours=7)).isoformat()):
            assert idle_song_due(now)[0] is True


def test_idle_due_requires_at_home(song_db):
    from bridge.features.song.triggers import idle_song_due

    from datetime import datetime
    from bridge.config import _JST

    _idle_settings()
    with patch("bridge.features.song.play.is_at_home", return_value=False):
        due, reason = idle_song_due(datetime(2026, 9, 17, 15, 0, tzinfo=_JST))
    assert due is False and reason == "在宅でない"


@pytest.mark.asyncio
async def test_check_idle_song_obeys_chance(song_db):
    """確率 0 なら条件を満たしても歌わない。"""
    from bridge.features.song import triggers

    _idle_settings(song_idle_chance=0)
    played = []
    with (
        patch("bridge.features.song.triggers.idle_song_due", return_value=(True, "ok")),
        patch("bridge.features.song.play.play_song", new=AsyncMock(side_effect=lambda *a, **k: played.append(1))),
    ):
        await triggers.check_idle_song()
    assert played == []


@pytest.mark.asyncio
async def test_check_idle_song_plays_when_due(song_db):
    from bridge.features.song import triggers

    _idle_settings(song_idle_chance=1)
    played = []

    async def _play(score, **kwargs):
        played.append((score.id, kwargs.get("source")))
        return {"songId": score.id, "title": score.title, "durationSec": 3.0}

    with (
        patch("bridge.features.song.triggers.idle_song_due", return_value=(True, "ok")),
        patch("bridge.features.song.play.play_song", side_effect=_play),
    ):
        await triggers.check_idle_song()

    assert len(played) == 1
    assert played[0][1] == "song_idle"


@pytest.mark.asyncio
async def test_check_idle_song_uses_mood_filter(song_db):
    from bridge.features.song import library, triggers

    library.load_scores()
    _idle_settings(song_idle_chance=1, song_idle_mood="sleepy")
    played = []

    async def _play(score, **kwargs):
        played.append(score.id)
        return {"songId": score.id, "title": score.title, "durationSec": 3.0}

    with (
        patch("bridge.features.song.triggers.idle_song_due", return_value=(True, "ok")),
        patch("bridge.features.song.play.play_song", side_effect=_play),
    ):
        await triggers.check_idle_song()

    assert played == ["twinkle"]   # mood に sleepy を持つのは twinkle だけ
