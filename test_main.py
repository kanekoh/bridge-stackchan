"""
pytest-based unit / integration tests for Bridge API (main.py).

External dependencies (VOICEVOX, MQTT, OpenAI, Speaker-ID) are all mocked
so no live services are required.

Run:
    pip install -r requirements-dev.txt
    pytest test_main.py -v
"""
import io
import os
import wave
from unittest.mock import MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# Ensure required env vars exist before importing main
os.environ.setdefault("OPENAI_API_KEY", "sk-test")

from main import (  # noqa: E402
    _build_datetime_context,
    app,
    chat_with_openclaw,
    identify_speaker,
    transcribe_audio,
)


# ── helpers ──────────────────────────────────────────────────────────────────

def _make_wav() -> bytes:
    """Return a minimal valid 16 kHz mono WAV (0.01 s of silence)."""
    buf = io.BytesIO()
    with wave.open(buf, "wb") as wf:
        wf.setnchannels(1)
        wf.setsampwidth(2)
        wf.setframerate(16000)
        wf.writeframes(b"\x00\x00" * 160)
    return buf.getvalue()


def _voicevox_web_mock(mp3_url: str = "https://example.com/audio/test.mp3",
                       streaming_url: str | None = "https://example.com/audio/test-stream.mp3"):
    """Build a mock response for the VOICEVOX Web API (GET /synthesis)."""
    mock = MagicMock()
    mock.raise_for_status = MagicMock()
    mock.json.return_value = {
        "success": True,
        "mp3DownloadUrl": mp3_url,
        "mp3StreamingUrl": streaming_url,
    }
    return mock


# ── fixtures ─────────────────────────────────────────────────────────────────

@pytest.fixture()
def client():
    return TestClient(app)


# ── /healthz ─────────────────────────────────────────────────────────────────

class TestHealthz:
    def test_returns_ok(self, client):
        resp = client.get("/healthz")
        assert resp.status_code == 200
        assert resp.json() == {"status": "ok"}


# ── /speak ────────────────────────────────────────────────────────────────────

class TestSpeak:
    def test_web_voicevox_success(self, client):
        """VOICEVOX Web API (API key set): returns mp3DownloadUrl and mp3StreamingUrl."""
        with (
            patch("main.VOICEVOX_API_KEY", "test-key"),
            patch("main.requests.get", return_value=_voicevox_web_mock()),
            patch("main.publish_speak") as mock_pub,
        ):
            resp = client.post("/speak", json={"text": "こんにちは", "source": "test"})

        assert resp.status_code == 200
        data = resp.json()
        assert data["audioUrl"] == "https://example.com/audio/test.mp3"
        assert data["audioStreamingUrl"] == "https://example.com/audio/test-stream.mp3"
        mock_pub.assert_called_once()

    def test_web_voicevox_no_streaming_url(self, client):
        """audioStreamingUrl is absent from response when not returned by VOICEVOX."""
        with (
            patch("main.VOICEVOX_API_KEY", "test-key"),
            patch("main.requests.get", return_value=_voicevox_web_mock(streaming_url=None)),
            patch("main.publish_speak"),
        ):
            resp = client.post("/speak", json={"text": "こんにちは"})

        assert resp.status_code == 200
        assert "audioStreamingUrl" not in resp.json()

    def test_voicevox_error_returns_502(self, client):
        with patch("main.requests.get", side_effect=RuntimeError("voicevox down")):
            resp = client.post("/speak", json={"text": "こんにちは"})

        assert resp.status_code == 502
        assert "VOICEVOX" in resp.json()["detail"]

    def test_mqtt_error_returns_502(self, client):
        with (
            patch("main.VOICEVOX_API_KEY", "test-key"),
            patch("main.requests.get", return_value=_voicevox_web_mock()),
            patch("main.publish_speak", side_effect=RuntimeError("MQTT down")),
        ):
            resp = client.post("/speak", json={"text": "こんにちは"})

        assert resp.status_code == 502
        assert "MQTT" in resp.json()["detail"]

    def test_request_id_preserved(self, client):
        """Caller-supplied request_id is echoed back."""
        with (
            patch("main.VOICEVOX_API_KEY", "test-key"),
            patch("main.requests.get", return_value=_voicevox_web_mock()),
            patch("main.publish_speak"),
        ):
            resp = client.post("/speak", json={"text": "テスト", "request_id": "my-req-123"})

        assert resp.json()["requestId"] == "my-req-123"


# ── /ingest-audio ─────────────────────────────────────────────────────────────

class TestIngestAudio:
    def test_success_full_pipeline(self, client):
        """Happy path async: STT → OpenClaw → VOICEVOX → MQTT, returns requestId only."""
        wav = _make_wav()

        with (
            patch("main.OPENAI_API_KEY", "sk-test"),
            patch("main.transcribe_audio", return_value="おはよう"),
            patch("main.identify_speaker", return_value="hiroyuki"),
            patch("main.chat_with_openclaw", return_value="おはよう！いい天気だね！"),
            patch("main.resolve_audio_url", return_value=("http://localhost:8000/audio/x.mp3", None)),
            patch("main.publish_speak") as mock_pub,
        ):
            resp = client.post(
                "/ingest-audio",
                files={"file": ("test.wav", wav, "audio/wav")},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert "requestId" in data
        assert "audioUrl" not in data
        assert "reply" not in data
        mock_pub.assert_called_once()

    def test_no_api_key_returns_503(self, client):
        wav = _make_wav()
        with patch("main.OPENAI_API_KEY", ""):
            resp = client.post(
                "/ingest-audio",
                files={"file": ("test.wav", wav, "audio/wav")},
            )
        assert resp.status_code == 503

    def test_stt_error_returns_502(self, client):
        wav = _make_wav()
        with (
            patch("main.OPENAI_API_KEY", "sk-test"),
            patch("main.transcribe_audio", side_effect=RuntimeError("whisper down")),
            patch("main.identify_speaker", return_value=None),
        ):
            resp = client.post(
                "/ingest-audio",
                files={"file": ("test.wav", wav, "audio/wav")},
            )
        assert resp.status_code == 502
        assert "STT" in resp.json()["detail"]

    def test_openclaw_error_returns_502(self, client):
        wav = _make_wav()
        with (
            patch("main.OPENAI_API_KEY", "sk-test"),
            patch("main.transcribe_audio", return_value="テスト"),
            patch("main.identify_speaker", return_value=None),
            patch("main.chat_with_openclaw", side_effect=RuntimeError("llm down")),
        ):
            resp = client.post(
                "/ingest-audio",
                files={"file": ("test.wav", wav, "audio/wav")},
            )
        assert resp.status_code == 502
        assert "OpenClaw" in resp.json()["detail"]

    def test_voicevox_error_returns_502(self, client):
        wav = _make_wav()
        with (
            patch("main.OPENAI_API_KEY", "sk-test"),
            patch("main.transcribe_audio", return_value="テスト"),
            patch("main.identify_speaker", return_value=None),
            patch("main.chat_with_openclaw", return_value="返事"),
            patch("main.resolve_audio_url", side_effect=RuntimeError("voicevox down")),
        ):
            resp = client.post(
                "/ingest-audio",
                files={"file": ("test.wav", wav, "audio/wav")},
            )
        assert resp.status_code == 502
        assert "VOICEVOX" in resp.json()["detail"]

    def test_system_prompt_append_passed_to_openclaw(self, client):
        """system_prompt_append form field is forwarded to chat_with_openclaw."""
        wav = _make_wav()
        with (
            patch("main.OPENAI_API_KEY", "sk-test"),
            patch("main.transcribe_audio", return_value="テスト"),
            patch("main.identify_speaker", return_value=None),
            patch("main.chat_with_openclaw", return_value="返事") as mock_chat,
            patch("main.resolve_audio_url", return_value=("http://localhost:8000/audio/x.mp3", None)),
            patch("main.publish_speak"),
        ):
            resp = client.post(
                "/ingest-audio",
                files={"file": ("test.wav", wav, "audio/wav")},
                data={"system_prompt_append": "追加指示テスト"},
            )

        assert resp.status_code == 200
        mock_chat.assert_called_once_with("テスト", None, "追加指示テスト")

    def test_unknown_speaker_when_not_identified(self, client):
        """speaker field is None when identify_speaker returns None (sync mode)."""
        wav = _make_wav()
        with (
            patch("main.OPENAI_API_KEY", "sk-test"),
            patch("main.transcribe_audio", return_value="テスト"),
            patch("main.identify_speaker", return_value=None),
            patch("main.chat_with_openclaw", return_value="返事"),
            patch("main.resolve_audio_url", return_value=("http://localhost:8000/audio/x.mp3", None)),
            patch("main.publish_speak"),
        ):
            resp = client.post(
                "/ingest-audio",
                files={"file": ("test.wav", wav, "audio/wav")},
                data={"mode": "sync"},
            )
        assert resp.status_code == 200
        assert resp.json()["speaker"] is None

    def test_sync_mode_skips_mqtt(self, client):
        """mode=sync: publish_speak is NOT called."""
        wav = _make_wav()
        with (
            patch("main.OPENAI_API_KEY", "sk-test"),
            patch("main.transcribe_audio", return_value="テスト"),
            patch("main.identify_speaker", return_value=None),
            patch("main.chat_with_openclaw", return_value="返事"),
            patch("main.resolve_audio_url", return_value=("http://example.com/audio.mp3", None)),
            patch("main.publish_speak") as mock_pub,
        ):
            resp = client.post(
                "/ingest-audio",
                files={"file": ("test.wav", wav, "audio/wav")},
                data={"mode": "sync"},
            )

        assert resp.status_code == 200
        mock_pub.assert_not_called()

    def test_sync_mode_returns_audio_url(self, client):
        """mode=sync: audioUrl is present in the response body."""
        wav = _make_wav()
        with (
            patch("main.OPENAI_API_KEY", "sk-test"),
            patch("main.transcribe_audio", return_value="テスト"),
            patch("main.identify_speaker", return_value=None),
            patch("main.chat_with_openclaw", return_value="返事"),
            patch("main.resolve_audio_url", return_value=("http://example.com/audio.mp3", "http://example.com/audio.mp3s")),
            patch("main.publish_speak"),
        ):
            resp = client.post(
                "/ingest-audio",
                files={"file": ("test.wav", wav, "audio/wav")},
                data={"mode": "sync"},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert data["audioUrl"] == "http://example.com/audio.mp3"
        assert data["audioStreamingUrl"] == "http://example.com/audio.mp3s"
        assert data["reply"] == "返事"

    def test_async_mode_publishes_mqtt(self, client):
        """mode=async (default): publish_speak is called, response contains only requestId."""
        wav = _make_wav()
        with (
            patch("main.OPENAI_API_KEY", "sk-test"),
            patch("main.transcribe_audio", return_value="テスト"),
            patch("main.identify_speaker", return_value=None),
            patch("main.chat_with_openclaw", return_value="返事"),
            patch("main.resolve_audio_url", return_value=("http://example.com/audio.mp3", None)),
            patch("main.publish_speak") as mock_pub,
        ):
            resp = client.post(
                "/ingest-audio",
                files={"file": ("test.wav", wav, "audio/wav")},
                data={"mode": "async"},
            )

        assert resp.status_code == 200
        data = resp.json()
        assert "requestId" in data
        assert "audioUrl" not in data
        mock_pub.assert_called_once()

    def test_sync_mode_mqtt_error_does_not_affect_response(self, client):
        """mode=sync: MQTT errors are irrelevant — publish is never attempted."""
        wav = _make_wav()
        with (
            patch("main.OPENAI_API_KEY", "sk-test"),
            patch("main.transcribe_audio", return_value="テスト"),
            patch("main.identify_speaker", return_value=None),
            patch("main.chat_with_openclaw", return_value="返事"),
            patch("main.resolve_audio_url", return_value=("http://example.com/audio.mp3", None)),
            patch("main.publish_speak", side_effect=RuntimeError("MQTT down")),
        ):
            resp = client.post(
                "/ingest-audio",
                files={"file": ("test.wav", wav, "audio/wav")},
                data={"mode": "sync"},
            )

        assert resp.status_code == 200


# ── unit: transcribe_audio ────────────────────────────────────────────────────

class TestTranscribeAudio:
    def test_calls_whisper_and_returns_text(self):
        mock_openai = MagicMock()
        mock_openai.audio.transcriptions.create.return_value = MagicMock(text="こんにちは")

        with patch("main.openai.OpenAI", return_value=mock_openai):
            result = transcribe_audio(b"fake-audio", "test.wav")

        assert result == "こんにちは"

    def test_uses_japanese_language(self):
        mock_openai = MagicMock()
        mock_openai.audio.transcriptions.create.return_value = MagicMock(text="テスト")

        with patch("main.openai.OpenAI", return_value=mock_openai):
            transcribe_audio(b"fake-audio", "test.wav")

        kwargs = mock_openai.audio.transcriptions.create.call_args.kwargs
        assert kwargs["model"] == "whisper-1"
        assert kwargs["language"] == "ja"

    def test_filename_used_as_buffer_name(self):
        """The filename is set on the BytesIO buffer (required by the OpenAI SDK)."""
        mock_openai = MagicMock()
        mock_openai.audio.transcriptions.create.return_value = MagicMock(text="テスト")
        captured = {}

        def capture(*args, **kwargs):
            captured["file"] = kwargs.get("file")
            return MagicMock(text="テスト")

        mock_openai.audio.transcriptions.create.side_effect = capture

        with patch("main.openai.OpenAI", return_value=mock_openai):
            transcribe_audio(b"fake-audio", "myfile.wav")

        assert captured["file"].name == "myfile.wav"


# ── unit: identify_speaker ────────────────────────────────────────────────────

class TestIdentifySpeaker:
    def test_returns_name_above_threshold(self):
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {"name": "hiroyuki", "kana": "ひろゆき", "score": 0.90}

        with (
            patch("main.SPEAKER_ID_URL", "http://localhost:8082"),
            patch("main.SPEAKER_ID_THRESHOLD", 0.75),
            patch("main.requests.post", return_value=mock_resp),
        ):
            result = identify_speaker(b"fake-audio")

        assert result == "ひろゆき"

    def test_returns_kana_over_name(self):
        """kana があれば name より優先して返す。"""
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {"name": "hiroyuki", "kana": "ひろゆき", "score": 0.90}

        with (
            patch("main.SPEAKER_ID_URL", "http://localhost:8082"),
            patch("main.SPEAKER_ID_THRESHOLD", 0.75),
            patch("main.requests.post", return_value=mock_resp),
        ):
            result = identify_speaker(b"fake-audio")

        assert result == "ひろゆき"

    def test_returns_none_below_threshold(self):
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {"name": "hiroyuki", "kana": "ひろゆき", "score": 0.50}

        with (
            patch("main.SPEAKER_ID_URL", "http://localhost:8082"),
            patch("main.SPEAKER_ID_THRESHOLD", 0.75),
            patch("main.requests.post", return_value=mock_resp),
        ):
            result = identify_speaker(b"fake-audio")

        assert result is None

    def test_returns_none_when_not_configured(self):
        with patch("main.SPEAKER_ID_URL", ""):
            result = identify_speaker(b"fake-audio")
        assert result is None

    def test_returns_none_on_service_error(self):
        """Service errors are non-fatal; identify_speaker returns None."""
        with (
            patch("main.SPEAKER_ID_URL", "http://localhost:8082"),
            patch("main.requests.post", side_effect=ConnectionError("unreachable")),
        ):
            result = identify_speaker(b"fake-audio")
        assert result is None

    def test_falls_back_to_name_when_no_kana(self):
        """kana がなければ name を返す。"""
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {"name": "hiroyuki", "score": 0.80}

        with (
            patch("main.SPEAKER_ID_URL", "http://localhost:8082"),
            patch("main.SPEAKER_ID_THRESHOLD", 0.75),
            patch("main.requests.post", return_value=mock_resp),
        ):
            result = identify_speaker(b"fake-audio")

        assert result == "hiroyuki"

    def test_sends_auth_header_when_api_key_set(self):
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {"name": "hiroyuki", "similarity": 0.90}

        with (
            patch("main.SPEAKER_ID_URL", "http://localhost:8082"),
            patch("main.SPEAKER_ID_API_KEY", "secret"),
            patch("main.SPEAKER_ID_THRESHOLD", 0.75),
            patch("main.requests.post", return_value=mock_resp) as mock_post,
        ):
            identify_speaker(b"fake-audio")

        headers = mock_post.call_args.kwargs.get("headers", {})
        assert headers.get("Authorization") == "Bearer secret"


# ── unit: chat_with_openclaw ──────────────────────────────────────────────────

class TestChatWithOpenclaw:
    def _mock_post(self, text: str) -> MagicMock:
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {"output_text": text}
        return mock_resp

    def test_returns_reply(self):
        with patch("main.requests.post", return_value=self._mock_post("おはよう！")):
            result = chat_with_openclaw("おはよう")
        assert result == "おはよう！"

    def test_speaker_prefixed_in_user_message(self):
        with patch("main.requests.post", return_value=self._mock_post("やあ！")) as mock_post:
            chat_with_openclaw("こんにちは", speaker="hiroyuki")
        payload = mock_post.call_args.kwargs["json"]
        assert "hiroyuki" in payload["input"]
        assert "こんにちは" in payload["input"]

    def test_no_speaker_prefix_when_none(self):
        with patch("main.requests.post", return_value=self._mock_post("返事")) as mock_post:
            chat_with_openclaw("テスト", speaker=None)
        payload = mock_post.call_args.kwargs["json"]
        assert payload["input"] == "テスト"

    def test_system_prompt_append_in_instructions(self):
        with patch("main.requests.post", return_value=self._mock_post("返事")) as mock_post:
            chat_with_openclaw("テスト", system_prompt_append="追加指示")
        payload = mock_post.call_args.kwargs["json"]
        assert "追加指示" in payload["instructions"]

    def test_session_key_header_when_set(self):
        with patch("main.OPENCLAW_SESSION_KEY", "agent:stackchan:slack:channel:C123"), \
             patch("main.requests.post", return_value=self._mock_post("返事")) as mock_post:
            chat_with_openclaw("テスト")
        headers = mock_post.call_args.kwargs["headers"]
        assert headers["x-openclaw-session-key"] == "agent:stackchan:slack:channel:C123"

    def test_session_key_header_absent_when_not_set(self):
        with patch("main.OPENCLAW_SESSION_KEY", ""), \
             patch("main.requests.post", return_value=self._mock_post("返事")) as mock_post:
            chat_with_openclaw("テスト")
        headers = mock_post.call_args.kwargs["headers"]
        assert "x-openclaw-session-key" not in headers

    def test_output_array_fallback(self):
        """output_text がない場合 output 配列から取得する。"""
        mock_resp = MagicMock()
        mock_resp.raise_for_status = MagicMock()
        mock_resp.json.return_value = {
            "output": [{"content": [{"type": "output_text", "text": "フォールバック"}]}]
        }
        with patch("main.requests.post", return_value=mock_resp):
            result = chat_with_openclaw("テスト")
        assert result == "フォールバック"

    def test_gateway_token_in_auth_header(self):
        with patch("main.OPENCLAW_GATEWAY_TOKEN", "secret-token"), \
             patch("main.requests.post", return_value=self._mock_post("返事")) as mock_post:
            chat_with_openclaw("テスト")
        headers = mock_post.call_args.kwargs["headers"]
        assert headers["Authorization"] == "Bearer secret-token"

    def test_scopes_header_always_set(self):
        with patch("main.requests.post", return_value=self._mock_post("返事")) as mock_post:
            chat_with_openclaw("テスト")
        headers = mock_post.call_args.kwargs["headers"]
        assert headers["x-openclaw-scopes"] == "operator.read,operator.write"


# ── unit: _build_datetime_context ─────────────────────────────────────────────

class TestBuildDatetimeContext:
    def test_contains_jst(self):
        result = _build_datetime_context()
        assert "JST" in result

    def test_contains_weekday(self):
        result = _build_datetime_context()
        assert any(d in result for d in ["月", "火", "水", "木", "金", "土", "日"])

    def test_contains_current_date_label(self):
        result = _build_datetime_context()
        assert "現在の日時" in result
