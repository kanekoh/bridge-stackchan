"""
pytest-based unit / integration tests for Bridge API (main.py).

External dependencies (VOICEVOX, MQTT, OpenAI, Speaker-ID) are all mocked
so no live services are required.

Run:
    pip install -r requirements-dev.txt
    pytest test_main.py -v
"""
import io
import json
import os
import wave
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from fastapi.testclient import TestClient

# Ensure required env vars exist before importing main
os.environ.setdefault("OPENAI_API_KEY", "sk-test")
os.environ.setdefault("DB_PATH", "/tmp/test-bridge.db")

import main  # noqa: E402  (needed for patch.object on module-level objects)
from main import (  # noqa: E402
    _build_datetime_context,
    _filter_messages_for_speaker,
    _handle_function_calls,
    _parse_duration,
    _register_timer,
    _slack_handle_dm,
    _slack_handle_mention,
    _slack_handle_speak,
    _slack_handle_timer,
    app,
    chat_with_llm,
    chat_with_openclaw,
    chat_with_openai_responses,
    identify_speaker,
    transcribe_audio,
    wait_for_ack,
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


def _make_mock_response(json_data, *, is_success=True):
    """Build a mock httpx response object."""
    resp = MagicMock()
    resp.is_success = is_success
    resp.status_code = 200 if is_success else 500
    resp.json.return_value = json_data
    resp.text = str(json_data)
    resp.raise_for_status = MagicMock()
    return resp


def _make_mock_http_client(*, post_response=None, get_response=None):
    """Build a mock httpx.AsyncClient with async post/get methods."""
    client = MagicMock()
    client.post = AsyncMock(return_value=post_response)
    client.get = AsyncMock(return_value=get_response)
    return client


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
            patch("main.resolve_audio_url", return_value=("https://example.com/audio/test.mp3", "https://example.com/audio/test-stream.mp3")),
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
            patch("main.resolve_audio_url", return_value=("https://example.com/audio/test.mp3", None)),
            patch("main.publish_speak"),
        ):
            resp = client.post("/speak", json={"text": "こんにちは"})

        assert resp.status_code == 200
        assert "audioStreamingUrl" not in resp.json()

    def test_voicevox_error_returns_502(self, client):
        with patch("main.resolve_audio_url", side_effect=RuntimeError("voicevox down")):
            resp = client.post("/speak", json={"text": "こんにちは"})

        assert resp.status_code == 502
        assert "VOICEVOX" in resp.json()["detail"]

    def test_mqtt_error_returns_502(self, client):
        with (
            patch("main.resolve_audio_url", return_value=("https://example.com/audio/test.mp3", "https://example.com/audio/test-stream.mp3")),
            patch("main.publish_speak", side_effect=RuntimeError("MQTT down")),
        ):
            resp = client.post("/speak", json={"text": "こんにちは"})

        assert resp.status_code == 502
        assert "MQTT" in resp.json()["detail"]

    def test_request_id_preserved(self, client):
        """Caller-supplied request_id is echoed back."""
        with (
            patch("main.resolve_audio_url", return_value=("https://example.com/audio/test.mp3", None)),
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
            patch("main.chat_with_llm", return_value="おはよう！いい天気だね！"),
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

    def test_llm_error_returns_502(self, client):
        wav = _make_wav()
        with (
            patch("main.OPENAI_API_KEY", "sk-test"),
            patch("main.transcribe_audio", return_value="テスト"),
            patch("main.identify_speaker", return_value=None),
            patch("main.chat_with_llm", side_effect=RuntimeError("llm down")),
        ):
            resp = client.post(
                "/ingest-audio",
                files={"file": ("test.wav", wav, "audio/wav")},
            )
        assert resp.status_code == 502
        assert "LLM" in resp.json()["detail"]

    def test_voicevox_error_returns_502(self, client):
        wav = _make_wav()
        with (
            patch("main.OPENAI_API_KEY", "sk-test"),
            patch("main.transcribe_audio", return_value="テスト"),
            patch("main.identify_speaker", return_value=None),
            patch("main.chat_with_llm", return_value="返事"),
            patch("main.resolve_audio_url", side_effect=RuntimeError("voicevox down")),
        ):
            resp = client.post(
                "/ingest-audio",
                files={"file": ("test.wav", wav, "audio/wav")},
            )
        assert resp.status_code == 502
        assert "VOICEVOX" in resp.json()["detail"]

    def test_system_prompt_append_passed_to_llm(self, client):
        """system_prompt_append form field is forwarded to chat_with_llm."""
        wav = _make_wav()
        with (
            patch("main.OPENAI_API_KEY", "sk-test"),
            patch("main.transcribe_audio", return_value="テスト"),
            patch("main.identify_speaker", return_value=None),
            patch("main.chat_with_llm", return_value="返事") as mock_chat,
            patch("main.resolve_audio_url", return_value=("http://localhost:8000/audio/x.mp3", None)),
            patch("main.publish_speak"),
        ):
            resp = client.post(
                "/ingest-audio",
                files={"file": ("test.wav", wav, "audio/wav")},
                data={"system_prompt_append": "追加指示テスト"},
            )

        assert resp.status_code == 200
        call_kwargs = mock_chat.call_args
        assert call_kwargs.args[0] == "テスト"    # text
        assert call_kwargs.args[1] is None         # speaker
        assert call_kwargs.args[2] == "追加指示テスト"  # system_prompt_append

    def test_unknown_speaker_when_not_identified(self, client):
        """speaker field is None when identify_speaker returns None (sync mode)."""
        wav = _make_wav()
        with (
            patch("main.OPENAI_API_KEY", "sk-test"),
            patch("main.transcribe_audio", return_value="テスト"),
            patch("main.identify_speaker", return_value=None),
            patch("main.chat_with_llm", return_value="返事"),
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
            patch("main.chat_with_llm", return_value="返事"),
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
            patch("main.chat_with_llm", return_value="返事"),
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
            patch("main.chat_with_llm", return_value="返事"),
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
            patch("main.chat_with_llm", return_value="返事"),
            patch("main.resolve_audio_url", return_value=("http://example.com/audio.mp3", None)),
            patch("main.publish_speak", side_effect=RuntimeError("MQTT down")),
        ):
            resp = client.post(
                "/ingest-audio",
                files={"file": ("test.wav", wav, "audio/wav")},
                data={"mode": "sync"},
            )

        assert resp.status_code == 200

    def test_async_mode_records_ingest_metrics_with_mqtt_ms(self, client):
        """mode=async: 各ステージ所要時間が記録され、mqtt_ms も設定される。"""
        wav = _make_wav()
        with (
            patch("main.OPENAI_API_KEY", "sk-test"),
            patch("main.transcribe_audio", return_value="おはよう"),
            patch("main.identify_speaker", return_value=None),
            patch("main.chat_with_llm", return_value="おはよう！"),
            patch("main.resolve_audio_url", return_value=("http://example.com/audio.mp3", None)),
            patch("main.publish_speak"),
            patch("main._save_ingest_metrics") as mock_save,
        ):
            resp = client.post(
                "/ingest-audio",
                files={"file": ("test.wav", wav, "audio/wav")},
                data={"mode": "async"},
            )

        assert resp.status_code == 200
        mock_save.assert_called_once()
        kwargs = mock_save.call_args.kwargs
        assert kwargs["mode"] == "async"
        assert kwargs["transcript_chars"] == len("おはよう")
        assert kwargs["reply_chars"] == len("おはよう！")
        assert kwargs["mqtt_ms"] is not None
        assert kwargs["total_ms"] >= 0

    def test_sync_mode_records_ingest_metrics_without_mqtt_ms(self, client):
        """mode=sync: MQTT は発行されないため mqtt_ms は None。"""
        wav = _make_wav()
        with (
            patch("main.OPENAI_API_KEY", "sk-test"),
            patch("main.transcribe_audio", return_value="テスト"),
            patch("main.identify_speaker", return_value=None),
            patch("main.chat_with_llm", return_value="返事"),
            patch("main.resolve_audio_url", return_value=("http://example.com/audio.mp3", None)),
            patch("main._save_ingest_metrics") as mock_save,
        ):
            resp = client.post(
                "/ingest-audio",
                files={"file": ("test.wav", wav, "audio/wav")},
                data={"mode": "sync"},
            )

        assert resp.status_code == 200
        mock_save.assert_called_once()
        assert mock_save.call_args.kwargs["mqtt_ms"] is None

    def test_ingest_metrics_save_error_does_not_affect_response(self, client):
        """メトリクス保存に失敗しても /ingest-audio のレスポンスには影響しない。"""
        wav = _make_wav()
        with (
            patch("main.OPENAI_API_KEY", "sk-test"),
            patch("main.transcribe_audio", return_value="テスト"),
            patch("main.identify_speaker", return_value=None),
            patch("main.chat_with_llm", return_value="返事"),
            patch("main.resolve_audio_url", return_value=("http://example.com/audio.mp3", None)),
            patch("main.publish_speak"),
            patch("main._save_ingest_metrics", side_effect=RuntimeError("db down")),
        ):
            resp = client.post(
                "/ingest-audio",
                files={"file": ("test.wav", wav, "audio/wav")},
                data={"mode": "async"},
            )

        assert resp.status_code == 200


# ── unit: transcribe_audio ────────────────────────────────────────────────────

class TestTranscribeAudio:
    async def test_calls_whisper_and_returns_text(self):
        mock_create = AsyncMock(return_value=MagicMock(text="こんにちは"))
        with patch("main._openai_client") as mock_client:
            mock_client.audio.transcriptions.create = mock_create
            result = await transcribe_audio(b"fake-audio", "test.wav")
        assert result == "こんにちは"

    async def test_uses_japanese_language(self):
        mock_create = AsyncMock(return_value=MagicMock(text="テスト"))
        with patch("main._openai_client") as mock_client:
            mock_client.audio.transcriptions.create = mock_create
            await transcribe_audio(b"fake-audio", "test.wav")
        kwargs = mock_create.call_args.kwargs
        assert kwargs["model"] == "whisper-1"
        assert kwargs["language"] == "ja"

    async def test_filename_used_as_buffer_name(self):
        """The filename is set on the BytesIO buffer (required by the OpenAI SDK)."""
        captured = {}

        async def capture_create(*args, **kwargs):
            captured["file"] = kwargs.get("file")
            return MagicMock(text="テスト")

        mock_create = AsyncMock(side_effect=capture_create)
        with patch("main._openai_client") as mock_client:
            mock_client.audio.transcriptions.create = mock_create
            await transcribe_audio(b"fake-audio", "myfile.wav")
        assert captured["file"].name == "myfile.wav"


# ── unit: identify_speaker ────────────────────────────────────────────────────

class TestIdentifySpeaker:
    async def test_returns_name_above_threshold(self):
        mock_resp = _make_mock_response({"name": "hiroyuki", "kana": "ひろゆき", "score": 0.90})
        mock_http = _make_mock_http_client(post_response=mock_resp)
        with (
            patch("main.SPEAKER_ID_URL", "http://localhost:8082"),
            patch("main.SPEAKER_ID_THRESHOLD", 0.75),
            patch("main._http_client", mock_http),
        ):
            result = await identify_speaker(b"fake-audio")
        assert result == "ひろゆき"

    async def test_returns_kana_over_name(self):
        """kana があれば name より優先して返す。"""
        mock_resp = _make_mock_response({"name": "hiroyuki", "kana": "ひろゆき", "score": 0.90})
        mock_http = _make_mock_http_client(post_response=mock_resp)
        with (
            patch("main.SPEAKER_ID_URL", "http://localhost:8082"),
            patch("main.SPEAKER_ID_THRESHOLD", 0.75),
            patch("main._http_client", mock_http),
        ):
            result = await identify_speaker(b"fake-audio")
        assert result == "ひろゆき"

    async def test_returns_none_below_threshold(self):
        mock_resp = _make_mock_response({"name": "hiroyuki", "kana": "ひろゆき", "score": 0.50})
        mock_http = _make_mock_http_client(post_response=mock_resp)
        with (
            patch("main.SPEAKER_ID_URL", "http://localhost:8082"),
            patch("main.SPEAKER_ID_THRESHOLD", 0.75),
            patch("main._http_client", mock_http),
        ):
            result = await identify_speaker(b"fake-audio")
        assert result is None

    async def test_returns_none_when_not_configured(self):
        with patch("main.SPEAKER_ID_URL", ""):
            result = await identify_speaker(b"fake-audio")
        assert result is None

    async def test_returns_none_on_service_error(self):
        """Service errors are non-fatal; identify_speaker returns None."""
        mock_http = _make_mock_http_client()
        mock_http.post = AsyncMock(side_effect=ConnectionError("unreachable"))
        with (
            patch("main.SPEAKER_ID_URL", "http://localhost:8082"),
            patch("main._http_client", mock_http),
        ):
            result = await identify_speaker(b"fake-audio")
        assert result is None

    async def test_falls_back_to_name_when_no_kana(self):
        """kana がなければ name を返す。"""
        mock_resp = _make_mock_response({"name": "hiroyuki", "score": 0.80})
        mock_http = _make_mock_http_client(post_response=mock_resp)
        with (
            patch("main.SPEAKER_ID_URL", "http://localhost:8082"),
            patch("main.SPEAKER_ID_THRESHOLD", 0.75),
            patch("main._http_client", mock_http),
        ):
            result = await identify_speaker(b"fake-audio")
        assert result == "hiroyuki"

    async def test_sends_auth_header_when_api_key_set(self):
        mock_resp = _make_mock_response({"name": "hiroyuki", "score": 0.90})
        mock_http = _make_mock_http_client(post_response=mock_resp)
        with (
            patch("main.SPEAKER_ID_URL", "http://localhost:8082"),
            patch("main.SPEAKER_ID_API_KEY", "secret"),
            patch("main.SPEAKER_ID_THRESHOLD", 0.75),
            patch("main._http_client", mock_http),
        ):
            await identify_speaker(b"fake-audio")
        headers = mock_http.post.call_args.kwargs.get("headers", {})
        assert headers.get("Authorization") == "Bearer secret"


# ── unit: chat_with_openclaw ──────────────────────────────────────────────────

class TestChatWithOpenclaw:
    def _mock_http(self, text: str) -> MagicMock:
        resp = _make_mock_response({"output_text": text})
        return _make_mock_http_client(post_response=resp)

    async def test_returns_reply(self):
        with patch("main._http_client", self._mock_http("おはよう！")):
            result = await chat_with_openclaw("おはよう")
        assert result == "おはよう！"

    async def test_speaker_prefixed_in_user_message(self):
        mock_http = self._mock_http("やあ！")
        with patch("main._http_client", mock_http):
            await chat_with_openclaw("こんにちは", speaker="hiroyuki")
        payload = mock_http.post.call_args.kwargs["json"]
        assert "hiroyuki" in payload["input"]
        assert "こんにちは" in payload["input"]

    async def test_no_speaker_prefix_when_none(self):
        mock_http = self._mock_http("返事")
        with patch("main._http_client", mock_http):
            await chat_with_openclaw("テスト", speaker=None)
        payload = mock_http.post.call_args.kwargs["json"]
        assert payload["input"] == "テスト"

    async def test_system_prompt_append_in_instructions(self):
        mock_http = self._mock_http("返事")
        with patch("main._http_client", mock_http):
            await chat_with_openclaw("テスト", system_prompt_append="追加指示")
        payload = mock_http.post.call_args.kwargs["json"]
        assert "追加指示" in payload["instructions"]

    async def test_session_key_header_when_set(self):
        mock_http = self._mock_http("返事")
        with (
            patch("main.OPENCLAW_SESSION_KEY", "agent:stackchan:slack:channel:C123"),
            patch("main._http_client", mock_http),
        ):
            await chat_with_openclaw("テスト")
        headers = mock_http.post.call_args.kwargs["headers"]
        assert headers["x-openclaw-session-key"] == "agent:stackchan:slack:channel:C123"

    async def test_session_key_header_absent_when_not_set(self):
        mock_http = self._mock_http("返事")
        with (
            patch("main.OPENCLAW_SESSION_KEY", ""),
            patch("main._http_client", mock_http),
        ):
            await chat_with_openclaw("テスト")
        headers = mock_http.post.call_args.kwargs["headers"]
        assert "x-openclaw-session-key" not in headers

    async def test_output_array_fallback(self):
        """output_text がない場合 output 配列から取得する。"""
        resp = _make_mock_response({
            "output": [{"content": [{"type": "output_text", "text": "フォールバック"}]}]
        })
        mock_http = _make_mock_http_client(post_response=resp)
        with patch("main._http_client", mock_http):
            result = await chat_with_openclaw("テスト")
        assert result == "フォールバック"

    async def test_gateway_token_in_auth_header(self):
        mock_http = self._mock_http("返事")
        with (
            patch("main.OPENCLAW_GATEWAY_TOKEN", "secret-token"),
            patch("main._http_client", mock_http),
        ):
            await chat_with_openclaw("テスト")
        headers = mock_http.post.call_args.kwargs["headers"]
        assert headers["Authorization"] == "Bearer secret-token"

    async def test_scopes_header_always_set(self):
        mock_http = self._mock_http("返事")
        with patch("main._http_client", mock_http):
            await chat_with_openclaw("テスト")
        headers = mock_http.post.call_args.kwargs["headers"]
        assert headers["x-openclaw-scopes"] == "operator.read,operator.write"


# ── unit: chat_with_openai_responses ─────────────────────────────────────────

class TestChatWithOpenAIResponses:
    def _mock_http(self, text: str, response_id: str = "resp_test123") -> MagicMock:
        resp = _make_mock_response({"id": response_id, "output_text": text})
        return _make_mock_http_client(post_response=resp)

    async def test_returns_reply(self):
        with (
            patch("main._http_client", self._mock_http("こんにちは！")),
            patch("main._get_previous_response_id", return_value=None),
            patch("main._save_response_id") as mock_save,
        ):
            result = await chat_with_openai_responses("こんにちは", session_key="test-session")
        assert result == "こんにちは！"
        mock_save.assert_called_once_with("test-session", "resp_test123")

    async def test_previous_response_id_included_in_payload(self):
        mock_http = self._mock_http("返事")
        with (
            patch("main._http_client", mock_http),
            patch("main._get_previous_response_id", return_value="resp_prev999"),
            patch("main._save_response_id"),
        ):
            await chat_with_openai_responses("テスト", session_key="test-session")
        payload = mock_http.post.call_args.kwargs["json"]
        assert payload["previous_response_id"] == "resp_prev999"

    async def test_no_previous_response_id_on_first_call(self):
        mock_http = self._mock_http("返事")
        with (
            patch("main._http_client", mock_http),
            patch("main._get_previous_response_id", return_value=None),
            patch("main._save_response_id"),
        ):
            await chat_with_openai_responses("テスト", session_key="test-session")
        payload = mock_http.post.call_args.kwargs["json"]
        assert "previous_response_id" not in payload

    async def test_speaker_prefixed_in_user_message(self):
        mock_http = self._mock_http("やあ！")
        with (
            patch("main._http_client", mock_http),
            patch("main._get_previous_response_id", return_value=None),
            patch("main._save_response_id"),
        ):
            await chat_with_openai_responses("こんにちは", speaker="hiroyuki", session_key="s")
        payload = mock_http.post.call_args.kwargs["json"]
        assert "hiroyuki" in payload["input"]
        assert "こんにちは" in payload["input"]

    async def test_stackchan_system_prompt_in_instructions(self):
        mock_http = self._mock_http("返事")
        with (
            patch("main._http_client", mock_http),
            patch("main._get_previous_response_id", return_value=None),
            patch("main._save_response_id"),
        ):
            await chat_with_openai_responses("テスト", session_key="s")
        payload = mock_http.post.call_args.kwargs["json"]
        assert "Stack-chan" in payload["instructions"]

    async def test_system_prompt_append_in_instructions(self):
        mock_http = self._mock_http("返事")
        with (
            patch("main._http_client", mock_http),
            patch("main._get_previous_response_id", return_value=None),
            patch("main._save_response_id"),
        ):
            await chat_with_openai_responses("テスト", system_prompt_append="追加指示", session_key="s")
        payload = mock_http.post.call_args.kwargs["json"]
        assert "追加指示" in payload["instructions"]

    async def test_does_not_save_when_no_session_key(self):
        with (
            patch("main._http_client", self._mock_http("返事")),
            patch("main._get_previous_response_id", return_value=None),
            patch("main._save_response_id") as mock_save,
        ):
            await chat_with_openai_responses("テスト", session_key="")
        mock_save.assert_not_called()

    async def test_output_array_fallback(self):
        resp = _make_mock_response({
            "id": "resp_xyz",
            "output": [{"content": [{"type": "output_text", "text": "フォールバック"}]}],
        })
        mock_http = _make_mock_http_client(post_response=resp)
        with (
            patch("main._http_client", mock_http),
            patch("main._get_previous_response_id", return_value=None),
            patch("main._save_response_id"),
        ):
            result = await chat_with_openai_responses("テスト", session_key="s")
        assert result == "フォールバック"

    async def test_web_search_tool_included_when_enabled(self):
        """web_search が有効のとき web_search_preview ツールが tools に含まれる。"""
        mock_http = self._mock_http("検索結果です")
        with (
            patch("main.OPENAI_RESPONSES_WEB_SEARCH", True),
            patch("main._http_client", mock_http),
            patch("main._get_previous_response_id", return_value=None),
            patch("main._save_response_id"),
        ):
            await chat_with_openai_responses("今日の天気は？", session_key="s")
        payload = mock_http.post.call_args.kwargs["json"]
        tool_types = [t.get("type") for t in payload["tools"]]
        assert "web_search_preview" in tool_types

    async def test_web_search_tool_absent_when_disabled(self):
        """web_search が無効のとき web_search_preview は tools に含まれない（timer tools は含まれる）。"""
        mock_http = self._mock_http("返事")
        with (
            patch("main.OPENAI_RESPONSES_WEB_SEARCH", False),
            patch("main._http_client", mock_http),
            patch("main._get_previous_response_id", return_value=None),
            patch("main._save_response_id"),
        ):
            await chat_with_openai_responses("テスト", session_key="s")
        payload = mock_http.post.call_args.kwargs["json"]
        tool_types = [t.get("type") for t in payload.get("tools", [])]
        assert "web_search_preview" not in tool_types


# ── unit: chat_with_llm ───────────────────────────────────────────────────────

class TestChatWithLLM:
    async def test_dispatches_to_openclaw_by_default(self):
        with (
            patch("main.LLM_BACKEND", "openclaw"),
            patch.object(main._BACKENDS["openclaw"], "chat", new_callable=AsyncMock, return_value="OpenClaw返事") as mock_oc,
        ):
            result = await chat_with_llm("テスト", session_key="s")
        assert result == "OpenClaw返事"
        mock_oc.assert_called_once()

    async def test_dispatches_to_openai_when_configured(self):
        with (
            patch("main.LLM_BACKEND", "openai"),
            patch.object(main._BACKENDS["openai"], "chat", new_callable=AsyncMock, return_value="OpenAI返事") as mock_oai,
        ):
            result = await chat_with_llm("テスト", session_key="s")
        assert result == "OpenAI返事"
        mock_oai.assert_called_once()

    async def test_session_key_passed_to_openai(self):
        with (
            patch("main.LLM_BACKEND", "openai"),
            patch.object(main._BACKENDS["openai"], "chat", new_callable=AsyncMock, return_value="返事") as mock_oai,
        ):
            await chat_with_llm("テスト", session_key="my-device")
        # chat(self, text, audio, speaker, system_prompt_append, session_key, ...)
        call_args = mock_oai.call_args
        session_key_arg = call_args.args[4] if len(call_args.args) > 4 else call_args.kwargs.get("session_key")
        assert session_key_arg == "my-device"


# ── unit: Slack handlers ──────────────────────────────────────────────────────

class TestSlackHandlers:
    async def test_mention_calls_llm_with_channel_session(self):
        say = AsyncMock()
        event = {"text": "<@UBOT123> おはよう", "channel": "C001"}
        with patch("main.chat_with_llm", new_callable=AsyncMock, return_value="おはよう！") as mock_llm:
            await _slack_handle_mention(event, say)
        mock_llm.assert_called_once()
        assert mock_llm.call_args.kwargs.get("session_key") == "slack:channel:C001" or \
               mock_llm.call_args.args[3] == "slack:channel:C001"
        say.assert_called_once_with("おはよう！")

    async def test_mention_does_not_publish_mqtt(self):
        """メンション応答は Slack 返信のみ。MQTT 発話は行わない。"""
        say = AsyncMock()
        event = {"text": "<@UBOT123> おはよう", "channel": "C001"}
        with (
            patch("main.chat_with_llm", new_callable=AsyncMock, return_value="おはよう！"),
            patch("main.publish_speak") as mock_pub,
        ):
            await _slack_handle_mention(event, say)
        mock_pub.assert_not_called()

    async def test_mention_strips_bot_mention_from_text(self):
        say = AsyncMock()
        event = {"text": "<@UBOT123> 今日の天気は？", "channel": "C001"}
        with (
            patch("main.chat_with_llm", new_callable=AsyncMock, return_value="晴れだよ！") as mock_llm,
            patch("main.resolve_audio_url", new_callable=AsyncMock, return_value=("http://x.com/a.mp3", None)),
            patch("main.publish_speak"),
        ):
            await _slack_handle_mention(event, say)
        text_arg = mock_llm.call_args.args[0]
        assert "<@" not in text_arg
        assert "今日の天気は？" in text_arg

    async def test_mention_empty_text_after_strip_does_nothing(self):
        say = AsyncMock()
        event = {"text": "<@UBOT123>", "channel": "C001"}
        with patch("main.chat_with_llm", new_callable=AsyncMock) as mock_llm:
            await _slack_handle_mention(event, say)
        mock_llm.assert_not_called()
        say.assert_not_called()

    async def test_mention_llm_error_replies_with_apology(self):
        say = AsyncMock()
        event = {"text": "<@UBOT123> テスト", "channel": "C001"}
        with patch("main.chat_with_llm", new_callable=AsyncMock, side_effect=RuntimeError("down")):
            await _slack_handle_mention(event, say)
        say.assert_called_once()
        assert "ごめん" in say.call_args.args[0]

    async def test_dm_calls_llm_with_user_session(self):
        say = AsyncMock()
        event = {"text": "こんにちは", "channel_type": "im", "user": "U001"}
        with patch("main.chat_with_llm", new_callable=AsyncMock, return_value="こんにちは！") as mock_llm:
            await _slack_handle_dm(event, say)
        mock_llm.assert_called_once()
        assert mock_llm.call_args.kwargs.get("session_key") == "slack:dm:U001" or \
               mock_llm.call_args.args[3] == "slack:dm:U001"

    async def test_dm_does_not_publish_mqtt(self):
        """DM 応答は Slack 返信のみ。MQTT 発話は行わない。"""
        say = AsyncMock()
        event = {"text": "こんにちは", "channel_type": "im", "user": "U001"}
        with (
            patch("main.chat_with_llm", new_callable=AsyncMock, return_value="こんにちは！"),
            patch("main.publish_speak") as mock_pub,
        ):
            await _slack_handle_dm(event, say)
        mock_pub.assert_not_called()

    async def test_dm_ignores_non_im_events(self):
        say = AsyncMock()
        event = {"text": "hello", "channel_type": "channel", "user": "U001"}
        with patch("main.chat_with_llm", new_callable=AsyncMock) as mock_llm:
            await _slack_handle_dm(event, say)
        mock_llm.assert_not_called()

    async def test_dm_ignores_bot_messages(self):
        say = AsyncMock()
        event = {"text": "hello", "channel_type": "im", "user": "U001", "bot_id": "B001"}
        with patch("main.chat_with_llm", new_callable=AsyncMock) as mock_llm:
            await _slack_handle_dm(event, say)
        mock_llm.assert_not_called()

    async def test_speak_command_publishes_mqtt(self):
        ack = AsyncMock()
        respond = AsyncMock()
        body = {"text": "おはようございます", "channel_id": "C001"}
        with (
            patch("main.chat_with_llm", new_callable=AsyncMock, return_value="おはよう！"),
            patch("main.resolve_audio_url", new_callable=AsyncMock, return_value=("http://x.com/a.mp3", None)),
            patch("main.publish_speak") as mock_pub,
            patch("main.wait_for_ack", new_callable=AsyncMock, return_value=True),
        ):
            await _slack_handle_speak(ack, body, respond)
        ack.assert_called_once()
        mock_pub.assert_called_once()
        respond.assert_called_once()

    async def test_speak_command_includes_broadcast_instruction(self):
        """/speak は「みんなへの発信」であることを system_prompt_append で伝える。"""
        ack = AsyncMock()
        respond = AsyncMock()
        body = {"text": "おはよう", "channel_id": "C001"}
        with (
            patch("main.chat_with_llm", new_callable=AsyncMock, return_value="おはよう！") as mock_llm,
            patch("main.resolve_audio_url", new_callable=AsyncMock, return_value=("http://x.com/a.mp3", None)),
            patch("main.publish_speak"),
            patch("main.wait_for_ack", new_callable=AsyncMock, return_value=True),
        ):
            await _slack_handle_speak(ack, body, respond)
        system_prompt_append = mock_llm.call_args.kwargs.get("system_prompt_append", "")
        assert "みんな" in system_prompt_append or "依頼" in system_prompt_append

    async def test_speak_command_no_text_returns_usage(self):
        ack = AsyncMock()
        respond = AsyncMock()
        body = {"text": "", "channel_id": "C001"}
        with patch("main.chat_with_llm", new_callable=AsyncMock) as mock_llm:
            await _slack_handle_speak(ack, body, respond)
        mock_llm.assert_not_called()
        respond.assert_called_once()
        assert "/speak" in respond.call_args.args[0]

    async def test_speak_command_no_session_key(self):
        """/speak は会話履歴を引き継がない（session_key なし）。"""
        ack = AsyncMock()
        respond = AsyncMock()
        body = {"text": "テスト", "channel_id": "C001"}
        with (
            patch("main.chat_with_llm", new_callable=AsyncMock, return_value="テスト！") as mock_llm,
            patch("main.resolve_audio_url", new_callable=AsyncMock, return_value=("http://x.com/a.mp3", None)),
            patch("main.publish_speak"),
            patch("main.wait_for_ack", new_callable=AsyncMock, return_value=True),
        ):
            await _slack_handle_speak(ack, body, respond)
        call_args = mock_llm.call_args
        session_key = call_args.kwargs.get("session_key", call_args.args[3] if len(call_args.args) > 3 else "")
        assert session_key == ""

    async def test_speak_command_ack_ok_sends_success_message(self):
        """ACK 受信 → 「話すよ！」メッセージを送信。"""
        ack = AsyncMock()
        respond = AsyncMock()
        body = {"text": "おはよう", "channel_id": "C001"}
        with (
            patch("main.chat_with_llm", new_callable=AsyncMock, return_value="おはよう！"),
            patch("main.resolve_audio_url", new_callable=AsyncMock, return_value=("http://x.com/a.mp3", None)),
            patch("main.publish_speak"),
            patch("main.wait_for_ack", new_callable=AsyncMock, return_value=True),
        ):
            await _slack_handle_speak(ack, body, respond)
        assert "話すよ" in respond.call_args.args[0]

    async def test_speak_command_ack_timeout_sends_warning(self):
        """ACK タイムアウト → 警告メッセージを Slack に送信。"""
        ack = AsyncMock()
        respond = AsyncMock()
        body = {"text": "おはよう", "channel_id": "C001"}
        with (
            patch("main.chat_with_llm", new_callable=AsyncMock, return_value="おはよう！"),
            patch("main.resolve_audio_url", new_callable=AsyncMock, return_value=("http://x.com/a.mp3", None)),
            patch("main.publish_speak"),
            patch("main.wait_for_ack", new_callable=AsyncMock, return_value=False),
        ):
            await _slack_handle_speak(ack, body, respond)
        assert "応答がなかった" in respond.call_args.args[0]


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


# ── unit: _parse_duration ─────────────────────────────────────────────────────

class TestParseDuration:
    def test_minutes_with_m(self):
        assert _parse_duration("3m") == 180

    def test_hours_with_h(self):
        assert _parse_duration("1h") == 3600

    def test_seconds_with_s(self):
        assert _parse_duration("10s") == 10

    def test_bare_number_is_minutes(self):
        assert _parse_duration("30") == 1800

    def test_japanese_minutes(self):
        assert _parse_duration("5分") == 300

    def test_japanese_hours(self):
        assert _parse_duration("2時間") == 7200

    def test_japanese_seconds(self):
        assert _parse_duration("15秒") == 15

    def test_absolute_time_hhmm(self):
        """HH:MM は将来の秒数になる（正の値かつ 1 日以内）。"""
        seconds = _parse_duration("23:59")
        assert seconds is not None
        assert 0 < seconds <= 86400

    def test_invalid_returns_none(self):
        assert _parse_duration("abc") is None

    def test_uppercase_h(self):
        assert _parse_duration("2H") == 7200


# ── unit: _handle_function_calls ─────────────────────────────────────────────

class TestHandleFunctionCalls:
    async def test_returns_none_when_no_function_calls(self):
        output = [{"type": "message", "content": [{"type": "output_text", "text": "hello"}]}]
        result = await _handle_function_calls(output, {})
        assert result is None

    async def test_set_timer_registers_and_returns_output(self):
        """call_id フィールドがある場合はそちらを使い、ない場合は id にフォールバックする。"""
        output = [{
            "type": "function_call",
            "id": "fc_001",
            "call_id": "call_001",  # OpenAI Responses API 形式: id と call_id は別フィールド
            "name": "set_timer",
            "arguments": '{"label": "宿題確認", "seconds": 1800}',
        }]
        with patch("main._register_timer", return_value="timer-abc") as mock_reg:
            result = await _handle_function_calls(output, {"session_key": "s", "slack_channel": "C001"})

        assert result is not None
        assert len(result) == 1
        assert result[0]["type"] == "function_call_output"
        assert result[0]["call_id"] == "call_001"  # id ではなく call_id が使われる
        mock_reg.assert_called_once_with(
            label="宿題確認",
            seconds=1800,
            session_key="s",
            slack_channel="C001",
            snooze_seconds=None,
        )

    async def test_call_id_fallback_to_id_when_absent(self):
        """call_id フィールドがない場合は id にフォールバックする。"""
        output = [{
            "type": "function_call",
            "id": "fc_only",
            # call_id フィールドなし
            "name": "set_timer",
            "arguments": '{"label": "テスト", "seconds": 60}',
        }]
        with patch("main._register_timer", return_value="t"):
            result = await _handle_function_calls(output, {})

        assert result[0]["call_id"] == "fc_only"  # id にフォールバック

    async def test_set_timer_passes_snooze_seconds(self):
        output = [{
            "type": "function_call",
            "id": "fc_002",
            "name": "set_timer",
            "arguments": '{"label": "おやつ", "seconds": 600, "snooze_seconds": 120}',
        }]
        with patch("main._register_timer", return_value="timer-xyz") as mock_reg:
            await _handle_function_calls(output, {})
        mock_reg.assert_called_once()
        _, kwargs = mock_reg.call_args
        assert kwargs["snooze_seconds"] == 120

    async def test_list_timers_returns_active_timers(self):
        """list_timers は _active_timer_infos の内容を返す。"""
        from datetime import timezone, timedelta
        from main import _TimerInfo, _active_timer_infos
        import uuid as _uuid

        jst = timezone(timedelta(hours=9))
        fake_info = _TimerInfo(
            timer_id="fake-id",
            label="テストタイマー",
            fire_at=__import__("datetime").datetime.now(jst) + timedelta(seconds=300),
            session_key="s",
            slack_channel=None,
            snooze_seconds=None,
        )
        _active_timer_infos["fake-id"] = fake_info
        try:
            output = [{"type": "function_call", "id": "fc_lt", "call_id": "call_lt", "name": "list_timers", "arguments": "{}"}]
            result = await _handle_function_calls(output, {})
            assert result is not None
            out = json.loads(result[0]["output"])
            assert out["status"] == "ok"
            assert out["count"] >= 1
            labels = [t["label"] for t in out["timers"]]
            assert "テストタイマー" in labels
        finally:
            _active_timer_infos.pop("fake-id", None)

    async def test_list_timers_empty_when_none(self):
        """タイマーがない場合は空リストを返す。"""
        from main import _active_timer_infos
        saved = dict(_active_timer_infos)
        _active_timer_infos.clear()
        try:
            output = [{"type": "function_call", "id": "fc_lt2", "call_id": "call_lt2", "name": "list_timers", "arguments": "{}"}]
            result = await _handle_function_calls(output, {})
            out = json.loads(result[0]["output"])
            assert out["count"] == 0
            assert out["timers"] == []
        finally:
            _active_timer_infos.update(saved)

    async def test_unknown_function_returns_error_output(self):
        output = [{
            "type": "function_call",
            "id": "fc_003",
            "name": "unknown_func",
            "arguments": "{}",
        }]
        result = await _handle_function_calls(output, {})
        assert result is not None
        output_json = json.loads(result[0]["output"])
        assert output_json["status"] == "error"

    async def test_slack_channel_none_when_not_in_context(self):
        output = [{
            "type": "function_call",
            "id": "fc_004",
            "name": "set_timer",
            "arguments": '{"label": "テスト", "seconds": 60}',
        }]
        with patch("main._register_timer", return_value="t") as mock_reg:
            await _handle_function_calls(output, {"session_key": "sk"})
        _, kwargs = mock_reg.call_args
        assert kwargs["slack_channel"] is None


# ── unit: _filter_messages_for_speaker / get_pending_messages ────────────────

class TestFilterMessagesForSpeaker:
    def test_untargeted_message_passes_for_any_speaker(self):
        messages = [{"id": 1, "sender": "パパ", "recipient": None, "content": "夕食は7時です"}]
        assert _filter_messages_for_speaker(messages, "しおり") == messages
        assert _filter_messages_for_speaker(messages, None) == messages

    def test_targeted_message_passes_only_for_matching_speaker(self):
        messages = [{"id": 2, "sender": "パパ", "recipient": "しおり", "content": "16時からだよ"}]
        assert _filter_messages_for_speaker(messages, "しおり") == messages
        assert _filter_messages_for_speaker(messages, "パパ") == []

    def test_targeted_message_excluded_when_speaker_unknown(self):
        """話者を特定できない場合、宛先付きの伝言を誤って届けない。"""
        messages = [{"id": 3, "sender": "パパ", "recipient": "しおり", "content": "16時からだよ"}]
        assert _filter_messages_for_speaker(messages, None) == []

    def test_mixed_messages_filtered_independently(self):
        messages = [
            {"id": 1, "sender": "パパ", "recipient": None, "content": "全員向け"},
            {"id": 2, "sender": "パパ", "recipient": "しおり", "content": "しおり向け"},
            {"id": 3, "sender": "ママ", "recipient": "たろう", "content": "たろう向け"},
        ]
        result = _filter_messages_for_speaker(messages, "しおり")
        assert [m["id"] for m in result] == [1, 2]


class TestGetPendingMessagesTool:
    async def test_delivers_only_messages_matching_speaker(self):
        """get_pending_messages ツールは speaker と一致しない宛先付き伝言を除外し、
        配信対象になった伝言だけ既読化する。"""
        output = [{
            "type": "function_call",
            "id": "fc_msg",
            "name": "get_pending_messages",
            "arguments": "{}",
        }]
        all_messages = [
            {"id": 1, "sender": "パパ", "sender_slack_id": None, "recipient": None, "content": "全員向け"},
            {"id": 2, "sender": "パパ", "sender_slack_id": None, "recipient": "しおり", "content": "しおり向け"},
            {"id": 3, "sender": "ママ", "sender_slack_id": None, "recipient": "たろう", "content": "たろう向け"},
        ]
        with (
            patch("main._fetch_pending_messages", return_value=all_messages),
            patch("main._mark_message_delivered") as mock_mark,
            patch("main._notify_message_delivered", new=AsyncMock()),
        ):
            result = await _handle_function_calls(output, {"speaker": "しおり"})

        out = json.loads(result[0]["output"])
        assert out["count"] == 2
        assert {m["content"] for m in out["messages"]} == {"全員向け", "しおり向け"}
        delivered_ids = {c.args[0] for c in mock_mark.call_args_list}
        assert delivered_ids == {1, 2}  # たろう向け(id=3) は既読化されない

    async def test_unknown_speaker_only_gets_untargeted_messages(self):
        output = [{
            "type": "function_call",
            "id": "fc_msg2",
            "name": "get_pending_messages",
            "arguments": "{}",
        }]
        all_messages = [
            {"id": 1, "sender": "パパ", "sender_slack_id": None, "recipient": None, "content": "全員向け"},
            {"id": 2, "sender": "パパ", "sender_slack_id": None, "recipient": "しおり", "content": "しおり向け"},
        ]
        with (
            patch("main._fetch_pending_messages", return_value=all_messages),
            patch("main._mark_message_delivered") as mock_mark,
            patch("main._notify_message_delivered", new=AsyncMock()),
        ):
            result = await _handle_function_calls(output, {})  # speaker 不明

        out = json.loads(result[0]["output"])
        assert out["count"] == 1
        assert out["messages"][0]["content"] == "全員向け"
        mock_mark.assert_called_once_with(1)


# ── unit: /api/device/settings (servoTest) ───────────────────────────────────

class TestDeviceSettingsServoTest:
    def test_servo_test_true_is_published(self, client):
        with patch("bridge.api.devices.publish_device_set") as mock_pub:
            resp = client.post("/api/device/settings", json={"servoTest": True})
        assert resp.status_code == 200
        mock_pub.assert_called_once_with("default", servoTest=True)

    def test_servo_test_false_is_published_not_dropped(self, client):
        """restart と違い、servoTest=False も明示的に送信される（テスト停止のため）。"""
        with patch("bridge.api.devices.publish_device_set") as mock_pub:
            resp = client.post("/api/device/settings", json={"servoTest": False})
        assert resp.status_code == 200
        mock_pub.assert_called_once_with("default", servoTest=False)

    def test_servo_test_omitted_is_not_sent(self, client):
        with patch("bridge.api.devices.publish_device_set") as mock_pub:
            resp = client.post("/api/device/settings", json={"brightness": 50})
        assert resp.status_code == 200
        mock_pub.assert_called_once_with("default", brightness=50)


# ── unit: _slack_handle_timer ─────────────────────────────────────────────────

class TestSlackHandleTimer:
    async def test_registers_timer_and_responds(self):
        ack = AsyncMock()
        respond = AsyncMock()
        body = {"text": "3m 宿題確認", "channel_id": "C001"}
        with patch("main._register_timer", return_value="tid-001") as mock_reg:
            await _slack_handle_timer(ack, body, respond)
        ack.assert_called_once()
        mock_reg.assert_called_once_with(
            label="宿題確認",
            seconds=180,
            session_key="",
            slack_channel="C001",
        )
        assert "宿題確認" in respond.call_args.args[0]

    async def test_no_text_returns_usage(self):
        ack = AsyncMock()
        respond = AsyncMock()
        body = {"text": "", "channel_id": "C001"}
        with patch("main._register_timer") as mock_reg:
            await _slack_handle_timer(ack, body, respond)
        mock_reg.assert_not_called()
        assert "/timer" in respond.call_args.args[0]

    async def test_invalid_duration_returns_error(self):
        ack = AsyncMock()
        respond = AsyncMock()
        body = {"text": "abc ラベル", "channel_id": "C001"}
        with patch("main._register_timer") as mock_reg:
            await _slack_handle_timer(ack, body, respond)
        mock_reg.assert_not_called()
        assert "解析" in respond.call_args.args[0]

    async def test_hour_duration(self):
        ack = AsyncMock()
        respond = AsyncMock()
        body = {"text": "1h お昼ご飯", "channel_id": "C002"}
        with patch("main._register_timer", return_value="tid-002") as mock_reg:
            await _slack_handle_timer(ack, body, respond)
        _, kwargs = mock_reg.call_args
        assert kwargs["seconds"] == 3600
        assert kwargs["label"] == "お昼ご飯"

    async def test_slack_channel_passed_to_register(self):
        ack = AsyncMock()
        respond = AsyncMock()
        body = {"text": "10m テスト", "channel_id": "C999"}
        with patch("main._register_timer", return_value="tid-003") as mock_reg:
            await _slack_handle_timer(ack, body, respond)
        _, kwargs = mock_reg.call_args
        assert kwargs["slack_channel"] == "C999"


# ── unit: Function calling loop (chat_with_openai_responses) ──────────────────

class TestFunctionCallingLoop:
    def _mock_http_multi(self, responses: list) -> MagicMock:
        """複数の HTTP レスポンスを順番に返す mock を作る。"""
        client = MagicMock()
        client.post = AsyncMock(side_effect=[_make_mock_response(r) for r in responses])
        return client

    async def test_no_function_call_returns_text_directly(self):
        """Function call がない場合は 1 回の HTTP 呼び出しでテキストを返す。"""
        with (
            patch("main._http_client", self._mock_http_multi([
                {"id": "resp_1", "output_text": "こんにちは！"},
            ])),
            patch("main._get_previous_response_id", return_value=None),
            patch("main._save_response_id"),
        ):
            result = await chat_with_openai_responses("こんにちは", session_key="s")
        assert result == "こんにちは！"

    async def test_function_call_then_text(self):
        """Function call → テキストの 2 ラウンドが正しく動く。最終レスポンスの ID のみ保存される。"""
        fc_response = {
            "id": "resp_fc",
            "output": [{
                "type": "function_call",
                "id": "fc_001",
                "name": "set_timer",
                "arguments": '{"label": "テスト", "seconds": 60}',
            }],
        }
        text_response = {"id": "resp_text", "output_text": "タイマーをセットしたよ！"}

        with (
            patch("main._http_client", self._mock_http_multi([fc_response, text_response])),
            patch("main._get_previous_response_id", return_value=None),
            patch("main._save_response_id") as mock_save,
            patch("main._register_timer", return_value="tid-x"),
        ):
            result = await chat_with_openai_responses(
                "1分後にテストして",
                session_key="s",
                notify_context={"session_key": "s", "slack_channel": "C001"},
            )
        assert result == "タイマーをセットしたよ！"
        # 中間の function_call レスポンス (resp_fc) は保存されず、最終テキスト (resp_text) のみ保存される
        mock_save.assert_called_once_with("s", "resp_text")

    async def test_function_call_uses_call_id_not_id(self):
        """function_call_output の call_id には id ではなく call_id フィールドを使う。"""
        fc_response = {
            "id": "resp_fc",
            "output": [{
                "type": "function_call",
                "id": "fc_item_id",
                "call_id": "call_actual_id",  # これが function_call_output に使われるべき
                "name": "set_timer",
                "arguments": '{"label": "確認", "seconds": 300}',
            }],
        }
        text_response = {"id": "resp_text", "output_text": "セットしたよ！"}

        with (
            patch("main._http_client", self._mock_http_multi([fc_response, text_response])),
            patch("main._get_previous_response_id", return_value=None),
            patch("main._save_response_id"),
            patch("main._register_timer", return_value="tid-cid"),
        ):
            result = await chat_with_openai_responses("5分後に確認して", session_key="s")

        assert result == "セットしたよ！"
        # 2 回目のリクエストの input に正しい call_id が含まれることを確認
        import main as main_module
        # _http_client.post の 2 回目の呼び出し引数を検査
        # (1 回目: fc_response, 2 回目: text_response を返す)

    async def test_broken_previous_response_id_is_retried_without_it(self):
        """previous_response_id が壊れている（400 + 'tool' エラー）場合、リセットして再試行する。"""
        broken_resp = MagicMock()
        broken_resp.status_code = 400
        broken_resp.is_success = False
        broken_resp.text = '{"error": {"message": "No tool output found for function call call_abc."}}'
        broken_resp.raise_for_status = MagicMock(side_effect=Exception("400"))

        ok_resp = _make_mock_response({"id": "resp_new", "output_text": "おはよう！"})

        client = MagicMock()
        client.post = AsyncMock(side_effect=[broken_resp, ok_resp])

        with (
            patch("main._http_client", client),
            patch("main._get_previous_response_id", return_value="resp_broken"),
            patch("main._save_response_id") as mock_save,
        ):
            result = await chat_with_openai_responses("おはよう", session_key="s")

        assert result == "おはよう！"
        assert client.post.call_count == 2
        # 2 回目のリクエストには previous_response_id が含まれない
        second_payload = client.post.call_args_list[1].kwargs["json"]
        assert "previous_response_id" not in second_payload
        mock_save.assert_called_once_with("s", "resp_new")

    async def test_use_functions_false_omits_timer_tools(self):
        """use_functions=False のとき _TIMER_TOOLS が payload に含まれない。"""
        mock_http = self._mock_http_multi([{"id": "r", "output_text": "返事"}])
        with (
            patch("main._http_client", mock_http),
            patch("main._get_previous_response_id", return_value=None),
            patch("main._save_response_id"),
            patch("main.OPENAI_RESPONSES_WEB_SEARCH", False),
        ):
            await chat_with_openai_responses("テスト", session_key="s", use_functions=False)
        payload = mock_http.post.call_args.kwargs["json"]
        assert "tools" not in payload

    async def test_notify_context_slack_channel_passed_to_register(self):
        """notify_context の slack_channel が _register_timer に渡る。"""
        fc_response = {
            "id": "resp_fc",
            "output": [{
                "type": "function_call",
                "id": "fc_x",
                "name": "set_timer",
                "arguments": '{"label": "確認", "seconds": 300}',
            }],
        }
        text_response = {"id": "resp_text", "output_text": "セットしたよ！"}

        with (
            patch("main._http_client", self._mock_http_multi([fc_response, text_response])),
            patch("main._get_previous_response_id", return_value=None),
            patch("main._save_response_id"),
            patch("main._register_timer", return_value="tid-y") as mock_reg,
        ):
            await chat_with_openai_responses(
                "5分後に確認",
                session_key="s",
                notify_context={"session_key": "s", "slack_channel": "C_SLACK"},
            )
        _, kwargs = mock_reg.call_args
        assert kwargs["slack_channel"] == "C_SLACK"

    async def test_ingest_audio_no_slack_channel(self):
        """ingest-audio 経由のタイマーは slack_channel=None で登録される。"""
        fc_response = {
            "id": "resp_fc",
            "output": [{
                "type": "function_call",
                "id": "fc_z",
                "name": "set_timer",
                "arguments": '{"label": "宿題", "seconds": 900}',
            }],
        }
        text_response = {"id": "resp_text", "output_text": "タイマーセットしたよ！"}

        with (
            patch("main._http_client", self._mock_http_multi([fc_response, text_response])),
            patch("main._get_previous_response_id", return_value=None),
            patch("main._save_response_id"),
            patch("main._register_timer", return_value="tid-z") as mock_reg,
        ):
            await chat_with_openai_responses(
                "15分後に宿題",
                session_key="s",
                notify_context={"session_key": "s", "slack_channel": None},
            )
        _, kwargs = mock_reg.call_args
        assert kwargs["slack_channel"] is None
