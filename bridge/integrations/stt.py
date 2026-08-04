import io
import logging
import sys
import httpx
import openai
from bridge.config import OPENAI_API_KEY, STT_MODEL
from bridge.core.db import _get_setting

logger = logging.getLogger(__name__)


def _get_main_attr(name: str, default=None):
    """Look up an attribute from the main module at call time.

    This allows tests to patch main.X and have that patch observed by
    functions that were moved out of main.py into this module.
    """
    main_mod = sys.modules.get("main")
    if main_mod is not None:
        return getattr(main_mod, name, default)
    return default


async def transcribe_audio(audio_bytes: bytes, filename: str) -> str:
    """Transcribe audio bytes using OpenAI Whisper API."""
    buf = io.BytesIO(audio_bytes)
    buf.name = filename or "audio.wav"
    openai_client = _get_main_attr("_openai_client") or openai.AsyncOpenAI(api_key=OPENAI_API_KEY)
    # 設定画面の値を優先し、未設定なら env のデフォルトを使う
    stt_model = _get_setting("stt_model", "") or _get_main_attr("STT_MODEL", STT_MODEL)
    result = await openai_client.audio.transcriptions.create(
        model=stt_model,
        file=buf,
        language="ja",
    )
    return result.text


async def identify_speaker(audio_bytes: bytes) -> str | None:
    """Identify speaker via speaker-id service. Returns display name or None.

    Non-fatal: returns None on any error or when SPEAKER_ID_URL is not configured.
    """
    from bridge.config import SPEAKER_ID_URL as _DEFAULT_URL, SPEAKER_ID_API_KEY as _DEFAULT_KEY, SPEAKER_ID_THRESHOLD as _DEFAULT_THRESHOLD
    speaker_id_url = _get_main_attr("SPEAKER_ID_URL", _DEFAULT_URL)
    speaker_id_api_key = _get_main_attr("SPEAKER_ID_API_KEY", _DEFAULT_KEY)
    speaker_id_threshold = _get_main_attr("SPEAKER_ID_THRESHOLD", _DEFAULT_THRESHOLD)

    if not speaker_id_url:
        return None
    try:
        http_client = _get_main_attr("_http_client")
        headers = {}
        if speaker_id_api_key:
            headers["Authorization"] = f"Bearer {speaker_id_api_key}"
        resp = await http_client.post(
            f"{speaker_id_url}/identify",
            files={"audio": ("audio.wav", audio_bytes, "audio/wav")},
            headers=headers,
        )
        if not resp.is_success:
            logger.warning("Speaker ID HTTP %d: body=%s", resp.status_code, resp.text[:200])
        resp.raise_for_status()
        data = resp.json()
        score = float(data.get("score", 0))
        if score >= speaker_id_threshold:
            name = data.get("kana") or data.get("name")
            logger.info("Speaker identified: name=%s score=%.3f", name, score)
            return name
        logger.info("Speaker below threshold: score=%.3f threshold=%.3f", score, speaker_id_threshold)
        return None
    except Exception as e:
        logger.warning("Speaker identification failed (non-fatal): %s", e)
        return None
