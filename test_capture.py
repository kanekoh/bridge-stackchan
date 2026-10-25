"""キャプチャ機能のユニットテスト。

主眼は「一時保存と記憶の取り違えが起きないこと」。
写真は消したら戻らないので、掃除が記憶（keep_until が NULL）に触れないことを
いちばん厚く確認する。MQTT も LLM も使わない範囲だけを見る。

Run:
    pytest test_capture.py -v
"""
import asyncio
import json
import os
import sqlite3
from datetime import datetime, timedelta

import pytest

os.environ.setdefault("OPENAI_API_KEY", "sk-test")
os.environ.setdefault("DB_PATH", "/tmp/test-bridge-capture.db")

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import bridge.core.db as _db_mod  # noqa: E402
from bridge.config import _JST  # noqa: E402
from bridge.core.db import (  # noqa: E402
    _count_captures, _fetch_captures, _get_capture, _set_capture_keep_until,
)

# 1x1 の JPEG（Pillow を使わずに済ませるための最小データ）
_JPEG = bytes.fromhex(
    "ffd8ffe000104a46494600010100000100010000ffdb004300ffffffffffffffffffffffffff"
    "ffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffffff"
    "ffffffffffffffffffffffffffffffffffffc00011080001000101011100ffc4001f00000105"
    "01010101010100000000000000000102030405060708090a0bffc400b5100002010303020403"
    "050504040000017d01020300041105122131410613516107227114328191a1082342b1c11552"
    "d1f02433627282090a161718191a25262728292a3435363738393a434445464748494a535455"
    "565758595a636465666768696a737475767778797a838485868788898a92939495969798999a"
    "a2a3a4a5a6a7a8a9aab2b3b4b5b6b7b8b9bac2c3c4c5c6c7c8c9cad2d3d4d5d6d7d8d9dae1e2"
    "e3e4e5e6e7e8e9eaf1f2f3f4f5f6f7f8f9faffda0008010100003f00fbfeffd9"
)


@pytest.fixture
def capture_env(tmp_path, monkeypatch):
    """一時 DB と一時の保存先を用意する。"""
    from bridge.features.capture import store

    monkeypatch.setattr(_db_mod, "DB_PATH", str(tmp_path / "capture.db"))
    old_conn = _db_mod._db_conn
    _db_mod._db_conn = sqlite3.connect(str(tmp_path / "capture.db"), check_same_thread=False)
    _db_mod._init_db()
    monkeypatch.setattr(store, "CAPTURE_DIR", str(tmp_path / "captures"))
    monkeypatch.setattr(store, "CAPTURE_TEMP_DAYS", 7)
    yield store
    _db_mod._db_conn.close()
    _db_mod._db_conn = old_conn


@pytest.fixture
def client(capture_env):
    from bridge.api import captures as _captures_api

    app = FastAPI()
    app.include_router(_captures_api.router)
    return TestClient(app)


def _expire(capture_id, days=1):
    """保持期限を過去にして、掃除の対象にする。"""
    _set_capture_keep_until(capture_id, (datetime.now(_JST) - timedelta(days=days)).isoformat())


# ── 一時保存と記憶の区別 ─────────────────────────────────────────────────────

def test_temp_has_deadline_permanent_does_not(capture_env):
    store = capture_env
    temp = store.save(_JPEG, retention="temp")
    perm = store.save(_JPEG, retention="permanent")
    assert temp["keep_until"] is not None
    assert perm["keep_until"] is None


def test_retention_deadline_is_temp_days_ahead(capture_env):
    store = capture_env
    now = datetime(2026, 9, 20, 12, 0, tzinfo=_JST)
    deadline = datetime.fromisoformat(store.retention_deadline("temp", now))
    assert deadline == now + timedelta(days=7)
    assert store.retention_deadline("permanent", now) is None


def test_unknown_retention_is_treated_as_temp(capture_env):
    """知らない値で永久保存になってしまうと、消えないゴミが貯まる。"""
    store = capture_env
    assert store.retention_deadline("あとで決める") is not None


# ── 掃除 ─────────────────────────────────────────────────────────────────────

def test_cleanup_removes_only_expired(capture_env):
    store = capture_env
    temp = store.save(_JPEG, retention="temp")
    assert store.cleanup_expired() == 0  # 期限前は消さない
    _expire(temp["id"])
    assert store.cleanup_expired() == 1
    assert not os.path.exists(store.abs_path(temp))
    assert _get_capture(temp["id"])["deleted_at"]


def test_cleanup_never_touches_memories(capture_env):
    """記憶は何度掃除しても消えない。ここが壊れると思い出が消える。"""
    store = capture_env
    perm = store.save(_JPEG, retention="permanent")
    for _ in range(3):
        store.cleanup_expired(datetime.now(_JST) + timedelta(days=3650))
    assert os.path.exists(store.abs_path(perm))
    assert _get_capture(perm["id"])["deleted_at"] is None


def test_kept_photo_survives_cleanup(capture_env):
    """期限切れでも、記憶に昇格していれば消えない。"""
    store = capture_env
    capture = store.save(_JPEG, retention="temp")
    _expire(capture["id"])
    store.keep_forever(capture["id"])
    assert store.cleanup_expired() == 0
    assert os.path.exists(store.abs_path(capture))


def test_cleanup_tolerates_missing_file(capture_env):
    """実体だけ先に消えていても、行は片付けて次に進む。"""
    store = capture_env
    capture = store.save(_JPEG, retention="temp")
    os.remove(store.abs_path(capture))
    _expire(capture["id"])
    assert store.cleanup_expired() == 1
    assert _get_capture(capture["id"])["deleted_at"]


# ── 昇格・降格・削除 ─────────────────────────────────────────────────────────

def test_keep_and_back_to_temp(capture_env):
    store = capture_env
    capture = store.save(_JPEG, retention="temp")
    assert store.keep_forever(capture["id"])
    assert _get_capture(capture["id"])["keep_until"] is None
    assert store.set_temporary(capture["id"])
    assert _get_capture(capture["id"])["keep_until"] is not None


def test_delete_keeps_the_row(capture_env):
    store = capture_env
    capture = store.save(_JPEG, retention="temp")
    assert store.delete(capture["id"])
    assert not os.path.exists(store.abs_path(capture))
    assert _get_capture(capture["id"])["deleted_at"]
    assert store.delete(capture["id"]) is False  # 二度目は何もしない


def test_deleted_is_hidden_from_list_and_stats(capture_env):
    store = capture_env
    capture = store.save(_JPEG, retention="temp")
    store.save(_JPEG, retention="permanent")
    store.delete(capture["id"])
    assert [c["id"] for c in _fetch_captures()] != [capture["id"]]
    assert _count_captures() == {
        "total": 1, "permanent": 1, "temp": 0, "bytes": len(_JPEG),
    }


def test_fetch_filters_by_keep(capture_env):
    store = capture_env
    store.save(_JPEG, retention="temp")
    store.save(_JPEG, retention="permanent")
    assert len(_fetch_captures(keep="temp")) == 1
    assert len(_fetch_captures(keep="permanent")) == 1
    assert len(_fetch_captures()) == 2


# ── エンドポイント ───────────────────────────────────────────────────────────

def test_ingest_photo_defaults_to_temp(client):
    res = client.post("/ingest-photo", files={"file": ("a.jpg", _JPEG, "image/jpeg")})
    assert res.status_code == 201
    body = res.json()
    assert body["keep"] == "temp"
    assert body["keepUntil"]
    assert body["url"] == f"/api/captures/{body['id']}/file"


def test_ingest_photo_rejects_unknown_retention(client):
    res = client.post(
        "/ingest-photo",
        files={"file": ("a.jpg", _JPEG, "image/jpeg")},
        data={"retention": "forever"},
    )
    assert res.status_code == 400


def test_ingest_photo_rejects_non_image(client):
    res = client.post("/ingest-photo", files={"file": ("a.txt", b"hello", "text/plain")})
    assert res.status_code == 400


def test_ingest_photo_rejects_too_large(client, monkeypatch):
    from bridge.api import captures as _captures_api

    monkeypatch.setattr(_captures_api, "CAPTURE_MAX_BYTES", 10)
    res = client.post("/ingest-photo", files={"file": ("a.jpg", _JPEG, "image/jpeg")})
    assert res.status_code == 413


def test_file_endpoint_serves_and_404s_after_delete(client):
    body = client.post("/ingest-photo", files={"file": ("a.jpg", _JPEG, "image/jpeg")}).json()
    res = client.get(body["url"])
    assert res.status_code == 200
    assert res.content == _JPEG
    assert client.delete(f"/api/captures/{body['id']}").status_code == 204
    assert client.get(body["url"]).status_code == 404


def test_list_does_not_expose_storage_path(client):
    """UI に保存場所を渡さない（外に出す情報を最小限にする）。"""
    client.post("/ingest-photo", files={"file": ("a.jpg", _JPEG, "image/jpeg")})
    body = client.get("/api/captures").json()
    assert body["captures"]
    assert "path" not in body["captures"][0]


def test_keep_endpoints_round_trip(client):
    body = client.post("/ingest-photo", files={"file": ("a.jpg", _JPEG, "image/jpeg")}).json()
    assert client.post(f"/api/captures/{body['id']}/keep").json()["keep"] == "permanent"
    assert client.post(f"/api/captures/{body['id']}/unkeep").json()["keep"] == "temp"
    assert client.post("/api/captures/nosuchid/keep").status_code == 404


# ── 撮影の依頼（MQTT → HTTP の往復） ─────────────────────────────────────────

@pytest.fixture
def mqtt_capture(monkeypatch):
    """MQTT の publish を差し替えて、送ったペイロードを覗けるようにする。"""
    import bridge.devices.mqtt as _mqtt

    sent = []

    class _Conn:
        def publish(self, topic, payload):
            sent.append((topic, json.loads(payload)))

    monkeypatch.setattr(_mqtt, "_mqtt_conn", _Conn())
    return sent


def test_publish_capture_payload(mqtt_capture):
    """依頼は1本のトピックに type を載せる（種類ごとに分けない）。"""
    from bridge.devices.mqtt import publish_capture

    topic = publish_capture(request_id="req-1", kind="photo", reason="llm")
    assert topic.endswith("/capture")
    sent_topic, msg = mqtt_capture[0]
    assert sent_topic == topic
    assert msg == {"type": "photo", "requestId": "req-1", "reason": "llm"}
    assert "audioUrl" not in msg and "image" not in msg   # 画像は MQTT に載せない


@pytest.mark.asyncio
async def test_request_capture_returns_the_arrived_photo(capture_env, mqtt_capture, monkeypatch):
    """依頼 → ACK → /ingest-photo の到着、までを結びつけられること。"""
    from bridge.features.capture import requests as capture_requests

    async def _ack(request_id, timeout=15.0):
        return True
    monkeypatch.setattr(capture_requests, "wait_for_ack", _ack)

    async def _deliver():
        await asyncio.sleep(0.05)
        request_id = mqtt_capture[0][1]["requestId"]
        capture_requests.notify_arrival(request_id, {"id": "abc", "keep": "temp"})

    task = asyncio.create_task(_deliver())
    result = await capture_requests.request_capture(reason="test", timeout=2.0)
    await task
    assert result["status"] == "ok"
    assert result["capture"]["id"] == "abc"
    assert capture_requests.pending_count() == 0


@pytest.mark.asyncio
async def test_request_capture_reports_no_ack(capture_env, mqtt_capture, monkeypatch):
    """応答が無いときは「届いていない」と分かる形で返す。"""
    from bridge.features.capture import requests as capture_requests

    async def _no_ack(request_id, timeout=15.0):
        return False
    monkeypatch.setattr(capture_requests, "wait_for_ack", _no_ack)

    result = await capture_requests.request_capture(timeout=0.2)
    assert result["status"] == "no_ack"


@pytest.mark.asyncio
async def test_request_capture_reports_timeout_after_ack(capture_env, mqtt_capture, monkeypatch):
    """ACK は来たのに写真が来ない＝撮影か送信の失敗。no_ack と区別する。"""
    from bridge.features.capture import requests as capture_requests

    async def _ack(request_id, timeout=15.0):
        return True
    monkeypatch.setattr(capture_requests, "wait_for_ack", _ack)

    result = await capture_requests.request_capture(timeout=0.2)
    assert result["status"] == "timeout"


@pytest.mark.asyncio
async def test_photo_arriving_before_ack_is_not_lost(capture_env, mqtt_capture, monkeypatch):
    """写真が ACK より先に届いても取りこぼさない（ACK 待ちで踏んだ失敗の再発防止）。"""
    from bridge.features.capture import requests as capture_requests

    async def _slow_ack(request_id, timeout=15.0):
        await asyncio.sleep(0.1)       # ACK は遅れて来る
        return True

    monkeypatch.setattr(capture_requests, "wait_for_ack", _slow_ack)

    async def _deliver_early():
        await asyncio.sleep(0.01)      # 先に写真が届く
        capture_requests.notify_arrival(mqtt_capture[0][1]["requestId"], {"id": "early"})

    task = asyncio.create_task(_deliver_early())
    result = await capture_requests.request_capture(timeout=2.0)
    await task
    assert result["status"] == "ok"
    assert result["capture"]["id"] == "early"


def test_notify_arrival_without_waiter_is_harmless():
    """デバイス起点のアップロード（誰も待っていない）でも落ちないこと。"""
    from bridge.features.capture import requests as capture_requests

    capture_requests.notify_arrival("nobody-waits", {"id": "x"})


def test_ingest_photo_wakes_the_waiter(client, capture_env, monkeypatch):
    """/ingest-photo が request_id つきで来たら、待っている依頼に結びつける。"""
    from bridge.features.capture import requests as capture_requests

    arrived = []
    monkeypatch.setattr(capture_requests, "notify_arrival",
                        lambda rid, cap: arrived.append((rid, cap)))
    client.post("/ingest-photo", files={"file": ("a.jpg", _JPEG, "image/jpeg")},
                data={"request_id": "req-42"})
    assert arrived and arrived[0][0] == "req-42"


def test_ingest_photo_without_request_id_does_not_notify(client, capture_env, monkeypatch):
    from bridge.features.capture import requests as capture_requests

    arrived = []
    monkeypatch.setattr(capture_requests, "notify_arrival",
                        lambda rid, cap: arrived.append(rid))
    client.post("/ingest-photo", files={"file": ("a.jpg", _JPEG, "image/jpeg")})
    assert arrived == []


# ── take_photo（LLM の道具） ─────────────────────────────────────────────────
# 撮って保存するだけ。**画像は外部に送らない**（家の中の写真を事業者に渡さない方針、
# 2026-10-25 決定）。スタックちゃん自身は写真を見られない。

@pytest.fixture
def photo_tool(monkeypatch, capture_env):
    from bridge.features.capture import requests as _requests
    from bridge.llm import tools as _tools

    state = {"capture_result": None}

    async def _request_capture(**kw):
        return state["capture_result"]

    monkeypatch.setattr(_requests, "request_capture", _request_capture)
    return _tools, state, capture_env


@pytest.mark.asyncio
async def test_take_photo_saves_without_looking(photo_tool):
    tools, state, store = photo_tool
    saved = store.save(_JPEG, retention="temp")
    state["capture_result"] = {"status": "ok", "capture": {"id": saved["id"]}}

    result = await tools._tool_take_photo({})
    assert result["status"] == "ok"
    assert result["photo_id"] == saved["id"]
    # 画像の中身にあたるものを LLM に返さない
    assert "seen" not in result and "caption" not in result and "image" not in result


@pytest.mark.asyncio
async def test_take_photo_reports_unreachable_device(photo_tool):
    """応答が無いときは、原因が分かる言い方で返す（LLM がそのまま伝える）。"""
    tools, state, _ = photo_tool
    state["capture_result"] = {"status": "no_ack"}
    result = await tools._tool_take_photo({})
    assert result["status"] == "error"
    assert "応答" in result["message"]


@pytest.mark.asyncio
async def test_take_photo_reports_missing_photo(photo_tool):
    tools, state, _ = photo_tool
    state["capture_result"] = {"status": "timeout"}
    result = await tools._tool_take_photo({})
    assert result["status"] == "error"
    assert "届きません" in result["message"]


@pytest.mark.asyncio
async def test_take_photo_has_no_question_parameter():
    """「写真を見て答える」道具に戻さないこと。見るには外部へ送る必要がある。"""
    from bridge.llm.tools import _CAPTURE_TOOLS

    take_photo = next(t for t in _CAPTURE_TOOLS if t["name"] == "take_photo")
    assert take_photo["parameters"]["properties"] == {}
    assert "見られない" in take_photo["description"]


# ── 写真を外部に送らない ─────────────────────────────────────────────────────

def test_capture_code_does_not_send_images_anywhere():
    """写真を外部 API に送るコードが入り込んでいないことを機械的に確かめる。

    2026-10-25 に vision.py（写真を OpenAI に送って説明させる機能）を削除した。
    家の中には子どもや来客が写る。撮られた本人の同意なく外部の事業者へ渡さない。
    """
    import glob

    suspicious = ("input_image", "image_url", "b64encode", "base64")
    for path in glob.glob(os.path.join(os.path.dirname(__file__), "bridge", "features", "capture", "*.py")):
        with open(path, encoding="utf-8") as f:
            body = f.read()
        for word in suspicious:
            assert word not in body, f"{os.path.basename(path)} に {word} がある（画像を外に送っていないか確認）"


def test_take_photo_tool_does_not_read_image_bytes():
    """道具の実装が画像ファイルを読んでいないこと（読む必要がない＝送らない）。"""
    import inspect

    from bridge.llm import tools as _tools

    source = inspect.getsource(_tools._tool_take_photo)
    assert "open(" not in source
    assert "b64" not in source and "image" not in source.replace("take_photo", "")
