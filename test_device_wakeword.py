"""ウェイクワード操作の口（HTTP 優先・MQTT 退避）のテスト。

デバイスの HTTP が使えるときは速い HTTP、駄目なときは MQTT に回る。
調整中に 1〜2 秒ごとに叩かれるため、届かないときも 200 で返して
UI のポーリングを止めないことを確かめる。

Run:
    pytest test_device_wakeword.py -v
"""
import os

import pytest

os.environ.setdefault("OPENAI_API_KEY", "sk-test")
os.environ.setdefault("DB_PATH", "/tmp/test-bridge-wakeword.db")

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

from bridge.api import devices as _devices  # noqa: E402


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(_devices, "_get_setting", lambda key, default="": default)
    app = FastAPI()
    app.include_router(_devices.router)
    return TestClient(app)


class _FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    def raise_for_status(self):
        pass

    def json(self):
        return self._payload


class _FakeClient:
    """httpx.AsyncClient の代わり。呼ばれた内容を calls に残す。"""

    def __init__(self, calls, payload=None, error=None):
        self._calls, self._payload, self._error = calls, payload, error

    async def __aenter__(self):
        return self

    async def __aexit__(self, *exc):
        return False

    async def get(self, url):
        self._calls.append(("GET", url, None))
        if self._error:
            raise self._error
        return _FakeResponse(self._payload)

    async def post(self, url, json=None):
        self._calls.append(("POST", url, json))
        if self._error:
            raise self._error
        return _FakeResponse({"ok": True})


def _patch_http(monkeypatch, calls, payload=None, error=None):
    monkeypatch.setattr(
        _devices.httpx, "AsyncClient",
        lambda **kwargs: _FakeClient(calls, payload, error),
    )


def _patch_device_url(monkeypatch, url="http://192.168.10.30"):
    monkeypatch.setattr(_devices, "_device_http_base", lambda: url)


def _patch_mqtt(monkeypatch, sent):
    monkeypatch.setattr(
        _devices, "publish_device_set",
        lambda device_id, **fields: sent.append((device_id, fields)),
    )


# ── アドレスの組み立て ───────────────────────────────────────────────────────

def test_device_url_gets_scheme_and_loses_trailing_slash(monkeypatch):
    monkeypatch.setattr(_devices, "_get_setting", lambda key, default="": "192.168.10.30/")
    assert _devices._device_http_base() == "http://192.168.10.30"


def test_setting_wins_over_env(monkeypatch):
    monkeypatch.setattr(_devices, "DEVICE_HTTP_URL", "http://from-env")
    monkeypatch.setattr(_devices, "_get_setting", lambda key, default="": "http://from-ui")
    assert _devices._device_http_base() == "http://from-ui"


# ── GET /api/device/live ─────────────────────────────────────────────────────

def test_live_returns_device_state(client, monkeypatch):
    calls = []
    _patch_device_url(monkeypatch)
    _patch_http(monkeypatch, calls, payload={"wakeWord": True, "wakeWordLastDistance": 128})

    body = client.get("/api/device/live").json()
    assert body["available"] is True
    assert body["state"]["wakeWordLastDistance"] == 128
    assert calls == [("GET", "http://192.168.10.30/device", None)]


def test_live_without_address_is_not_an_error(client, monkeypatch):
    """未設定でも 200。ポーリングのたびに例外を投げない。"""
    _patch_device_url(monkeypatch, "")
    res = client.get("/api/device/live")
    assert res.status_code == 200
    assert res.json()["available"] is False


def test_live_survives_unreachable_device(client, monkeypatch):
    _patch_device_url(monkeypatch)
    _patch_http(monkeypatch, [], error=OSError("timeout"))
    res = client.get("/api/device/live")
    assert res.status_code == 200
    assert res.json()["available"] is False
    assert "timeout" in res.json()["reason"]


# ── POST /api/device/wakeword ────────────────────────────────────────────────

def test_wakeword_prefers_http(client, monkeypatch):
    calls, sent = [], []
    _patch_device_url(monkeypatch)
    _patch_http(monkeypatch, calls)
    _patch_mqtt(monkeypatch, sent)

    body = client.post("/api/device/wakeword", json={"wakeWordThreshold": 180}).json()
    assert body["transport"] == "http"
    assert calls == [("POST", "http://192.168.10.30/device", {"wakeWordThreshold": 180})]
    assert sent == []  # HTTP で通ったら MQTT には流さない


def test_wakeword_falls_back_to_mqtt(client, monkeypatch):
    calls, sent = [], []
    _patch_device_url(monkeypatch)
    _patch_http(monkeypatch, calls, error=OSError("no route to host"))
    _patch_mqtt(monkeypatch, sent)

    body = client.post("/api/device/wakeword", json={"wakeWordRegister": True}).json()
    assert body["transport"] == "mqtt"
    assert sent == [("default", {"wakeWordRegister": True})]


def test_wakeword_off_is_sent_not_dropped(client, monkeypatch):
    """False は「変更なし」ではなく「止める」。落としてはいけない。"""
    sent = []
    _patch_device_url(monkeypatch, "")
    _patch_mqtt(monkeypatch, sent)

    client.post("/api/device/wakeword", json={"wakeWord": False})
    assert sent == [("default", {"wakeWord": False})]


def test_register_false_is_dropped(client, monkeypatch):
    """登録は「今から覚える」ための合図なので、false を送る意味がない。"""
    _patch_device_url(monkeypatch, "")
    res = client.post("/api/device/wakeword", json={"wakeWordRegister": False})
    assert res.status_code == 422


def test_empty_payload_is_rejected(client, monkeypatch):
    _patch_device_url(monkeypatch, "")
    assert client.post("/api/device/wakeword", json={}).status_code == 422


def test_threshold_out_of_range_is_rejected(client, monkeypatch):
    _patch_device_url(monkeypatch, "")
    assert client.post("/api/device/wakeword", json={"wakeWordThreshold": 5}).status_code == 422


# ── カメラ ───────────────────────────────────────────────────────────────────

def test_camera_settings_prefer_http(client, monkeypatch):
    calls, sent = [], []
    _patch_device_url(monkeypatch)
    _patch_http(monkeypatch, calls)
    _patch_mqtt(monkeypatch, sent)

    body = client.post("/api/device/camera", json={"cameraAeLevel": 2}).json()
    assert body["transport"] == "http"
    assert calls == [("POST", "http://192.168.10.30/device", {"cameraAeLevel": 2})]
    assert sent == []


def test_camera_settings_fall_back_to_mqtt(client, monkeypatch):
    calls, sent = [], []
    _patch_device_url(monkeypatch)
    _patch_http(monkeypatch, calls, error=OSError("unreachable"))
    _patch_mqtt(monkeypatch, sent)

    body = client.post("/api/device/camera", json={"cameraFrameSize": "VGA"}).json()
    assert body["transport"] == "mqtt"
    assert sent == [("default", {"cameraFrameSize": "VGA"})]


def test_camera_zero_values_are_sent(client, monkeypatch):
    """0 や -2 は「変更なし」ではない。落とすと暗いままになる。"""
    calls = []
    _patch_device_url(monkeypatch)
    _patch_http(monkeypatch, calls)

    client.post("/api/device/camera", json={"cameraAeLevel": 0, "cameraBrightness": -2,
                                            "cameraWarmupFrames": 0})
    assert calls[0][2] == {"cameraAeLevel": 0, "cameraBrightness": -2, "cameraWarmupFrames": 0}


def test_test_shot_goes_over_http(client, monkeypatch):
    calls = []
    _patch_device_url(monkeypatch)
    _patch_http(monkeypatch, calls)

    body = client.post("/api/device/camera", json={"capture": True}).json()
    assert body["transport"] == "http"
    assert calls[0][2] == {"capture": True}


def test_test_shot_without_device_url_is_refused(client, monkeypatch):
    """試し撮りは HTTP でしか送れない。MQTT に流して黙って失敗しないこと。"""
    sent = []
    _patch_device_url(monkeypatch, "")
    _patch_mqtt(monkeypatch, sent)

    res = client.post("/api/device/camera", json={"capture": True})
    assert res.status_code == 503
    assert sent == []


def test_camera_values_out_of_range_are_rejected(client, monkeypatch):
    _patch_device_url(monkeypatch, "")
    for payload in ({"cameraAeLevel": 3}, {"cameraBrightness": -3},
                    {"cameraWarmupFrames": 31}, {"cameraJpegQuality": 5},
                    {"cameraFrameSize": "UXGA"}):
        assert client.post("/api/device/camera", json=payload).status_code == 422, payload


def test_camera_empty_payload_is_rejected(client, monkeypatch):
    _patch_device_url(monkeypatch, "")
    assert client.post("/api/device/camera", json={}).status_code == 422


def test_settings_endpoint_also_carries_camera(client, monkeypatch):
    """既存のデバイス設定画面（MQTT 経由）からも送れること。"""
    sent = []
    _patch_mqtt(monkeypatch, sent)
    client.post("/api/device/settings", json={"cameraAeLevel": 1, "volume": 50})
    assert sent[0][1] == {"cameraAeLevel": 1, "volume": 50}


# ── 設定キーの登録 ───────────────────────────────────────────────────────────

def test_device_http_url_is_an_editable_setting():
    """UI のアドレス入力は /api/settings に保存する。

    _EDITABLE_SETTINGS に無いキーは 404 で弾かれるため、登録を忘れると
    「設定キー 'device_http_url' は存在しません」で保存できなくなる。
    """
    from bridge.api.settings import _EDITABLE_SETTINGS

    assert "device_http_url" in _EDITABLE_SETTINGS


# ── 既存の設定エンドポイント経由でも送れる ──────────────────────────────────

def test_settings_endpoint_also_carries_wakeword(client, monkeypatch):
    sent = []
    _patch_mqtt(monkeypatch, sent)
    client.post("/api/device/settings", json={"wakeWord": True, "volume": 60})
    assert sent[0][1] == {"wakeWord": True, "volume": 60}
