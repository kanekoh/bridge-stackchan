"""話者識別の記録のテスト。

いちばん大事なのは「記録が判定を壊さないこと」。識別は会話の入口にあるので、
記録まわりで例外が出ると会話そのものが止まる。記録は失敗しても黙って諦める。

しきい値の変更や、margin を使った判定はまだ入れていない（記録と可視化だけ）。

Run:
    pytest test_speaker_id.py -v
"""
import json
import os
import sqlite3

import pytest

os.environ.setdefault("OPENAI_API_KEY", "sk-test")
os.environ.setdefault("DB_PATH", "/tmp/test-bridge-speaker.db")

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import bridge.core.db as _db_mod  # noqa: E402
from bridge.core.db import (  # noqa: E402
    _fetch_speaker_id_log, _save_speaker_id_log, _set_speaker_id_actual,
)

# speaker-id サービスが返す形（本番の app.py が返すものに合わせる）
_RESPONSE = {
    "name": "ひろゆき",
    "kana": "ひろゆき",
    "score": 0.82,
    "threshold": 0.5,
    "speech_sec": 2.4,
    "reason": None,
    "scores": [
        {"name": "hiroyuki", "kana": "ひろゆき", "score": 0.82},
        {"name": "eri", "kana": "えり", "score": 0.61},
        {"name": "shiori", "kana": "しおり", "score": 0.44},
    ],
}


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setattr(_db_mod, "DB_PATH", str(tmp_path / "spk.db"))
    old = _db_mod._db_conn
    conn = sqlite3.connect(str(tmp_path / "spk.db"), check_same_thread=False)
    _db_mod._db_conn = conn
    _db_mod._init_db()
    # 接続は自前で持つ。テストが _db_conn を差し替えても後片付けできるようにする
    yield conn
    conn.close()
    _db_mod._db_conn = old


@pytest.fixture
def client(db, monkeypatch):
    from bridge.api import speaker_id as api

    monkeypatch.setattr(api, "_get_setting", lambda key, default="": "0.75")
    app = FastAPI()
    app.include_router(api.router)
    return TestClient(app)


def _record(**kw):
    base = dict(request_id="req-1", candidate="ひろゆき", score=0.82,
                second_name="えり", second_score=0.61, accepted=True,
                threshold_bridge=0.75, threshold_service=0.5, speech_sec=2.4,
                elapsed_ms=480, reason=None, scores_json=None)
    base.update(kw)
    _save_speaker_id_log(**base)


# ── 記録 ─────────────────────────────────────────────────────────────────────

def test_margin_is_computed_from_top_two(db):
    """2位との差は保存時に計算する（迷っている判定を後から探せるように）。"""
    _record(score=0.82, second_score=0.61)
    row = _fetch_speaker_id_log()[0]
    assert row["margin"] == pytest.approx(0.21)


def test_margin_is_none_when_there_is_no_second(db):
    """登録者が1人だけなら2位が無い。0 と区別する。"""
    _record(second_name=None, second_score=None)
    assert _fetch_speaker_id_log()[0]["margin"] is None


def test_rejected_identification_is_also_recorded(db):
    """弾かれた回こそ、しきい値を下げる判断の材料になる。"""
    _record(score=0.62, accepted=False, reason="below_threshold")
    row = _fetch_speaker_id_log()[0]
    assert row["accepted"] == 0
    assert row["score"] == 0.62
    assert row["reason"] == "below_threshold"


def test_both_thresholds_are_kept(db):
    """サービス側(0.5)と Bridge 側(0.75)の二重のしきい値を両方残す。"""
    row = _fetch_speaker_id_log()[0] if _fetch_speaker_id_log() else None
    _record()
    row = _fetch_speaker_id_log()[0]
    assert row["threshold_bridge"] == 0.75
    assert row["threshold_service"] == 0.5


def test_recording_failure_does_not_raise(db, monkeypatch):
    """記録が壊れても会話は続けること（識別は会話の入口にある）。"""
    monkeypatch.setattr(_db_mod, "_db_conn", None)
    _record()          # 例外が出ないこと


def test_actual_speaker_can_be_set_and_cleared(db):
    _record()
    log_id = _fetch_speaker_id_log()[0]["id"]
    assert _set_speaker_id_actual(log_id, "えり")
    assert _fetch_speaker_id_log()[0]["actual_speaker"] == "えり"
    assert _set_speaker_id_actual(log_id, None)
    assert _fetch_speaker_id_log()[0]["actual_speaker"] is None
    assert _set_speaker_id_actual(9999, "えり") is False


# ── 識別の呼び出しから記録まで ───────────────────────────────────────────────

@pytest.mark.asyncio
async def test_identify_speaker_records_what_the_service_returned(db, monkeypatch):
    from bridge.integrations import stt

    class _Resp:
        is_success = True
        def raise_for_status(self): pass
        def json(self): return _RESPONSE

    class _Client:
        async def post(self, *a, **kw): return _Resp()

    monkeypatch.setattr(stt, "_get_main_attr", lambda name, default=None: {
        "SPEAKER_ID_URL": "http://speaker-id:8082",
        "SPEAKER_ID_API_KEY": "",
        "SPEAKER_ID_THRESHOLD": 0.75,
        "_http_client": _Client(),
    }.get(name, default))

    name = await stt.identify_speaker(b"wav", request_id="req-9")
    assert name == "ひろゆき"

    row = _fetch_speaker_id_log()[0]
    assert row["request_id"] == "req-9"
    assert row["candidate"] == "ひろゆき"
    assert row["score"] == 0.82
    assert row["second_name"] == "えり"      # kana を優先して保存する
    assert row["accepted"] == 1
    assert row["speech_sec"] == 2.4
    assert json.loads(row["scores_json"])[0]["score"] == 0.82


@pytest.mark.asyncio
async def test_below_threshold_returns_none_but_still_records(db, monkeypatch):
    from bridge.integrations import stt

    low = dict(_RESPONSE, score=0.61, name=None, kana=None, reason="below_threshold")

    class _Resp:
        is_success = True
        def raise_for_status(self): pass
        def json(self): return low

    class _Client:
        async def post(self, *a, **kw): return _Resp()

    monkeypatch.setattr(stt, "_get_main_attr", lambda name, default=None: {
        "SPEAKER_ID_URL": "http://speaker-id:8082",
        "SPEAKER_ID_API_KEY": "",
        "SPEAKER_ID_THRESHOLD": 0.75,
        "_http_client": _Client(),
    }.get(name, default))

    assert await stt.identify_speaker(b"wav") is None
    row = _fetch_speaker_id_log()[0]
    assert row["accepted"] == 0
    assert row["score"] == 0.61


# ── 集計 ─────────────────────────────────────────────────────────────────────

def test_sweep_counts_how_many_pass_each_threshold(client, db):
    for score in (0.4, 0.6, 0.72, 0.8, 0.9):
        _record(score=score, accepted=score >= 0.75)
    body = client.get("/api/speaker-id/log").json()
    sweep = {round(s["threshold"], 2): s for s in body["sweep"]}
    assert sweep[0.70]["accepted"] == 3     # 0.72, 0.8, 0.9
    assert sweep[0.85]["accepted"] == 1     # 0.9 のみ
    assert body["stats"]["total"] == 5


def test_sweep_counts_errors_only_for_labeled_rows(client, db):
    """正解を付けた行だけ正誤を数える。付けていない行を正解扱いしない。"""
    _record(score=0.80, candidate="ひろゆき")           # ラベルなし
    _record(score=0.78, candidate="えり")               # 取り違え（実際はひろゆき）
    rows = _fetch_speaker_id_log()
    _set_speaker_id_actual(rows[0]["id"], "ひろゆき")
    body = client.get("/api/speaker-id/log").json()
    sweep = {round(s["threshold"], 2): s for s in body["sweep"]}
    assert sweep[0.75]["labeled"] == 1
    assert sweep[0.75]["wrong"] == 1
    assert sweep[0.75]["correct"] == 0
    assert body["stats"]["labeled"] == 1


def test_log_endpoint_exposes_people_for_labeling(client, db):
    _record(candidate="ひろゆき")
    _record(candidate="えり")
    body = client.get("/api/speaker-id/log").json()
    assert set(body["people"]) == {"ひろゆき", "えり"}
    assert body["thresholdBridge"] == 0.75


def test_actual_speaker_endpoint(client, db):
    _record()
    log_id = _fetch_speaker_id_log()[0]["id"]
    res = client.put(f"/api/speaker-id/log/{log_id}/actual", json={"actual_speaker": "しおり"})
    assert res.status_code == 200
    assert res.json()["actualSpeaker"] == "しおり"
    assert client.put("/api/speaker-id/log/9999/actual", json={"actual_speaker": "x"}).status_code == 404
