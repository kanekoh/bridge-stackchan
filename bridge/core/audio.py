import logging
import httpx
from bridge.config import VOICEVOX_URL, VOICEVOX_SPEAKER, VOICEVOX_API_KEY

logger = logging.getLogger(__name__)

# Shared httpx.AsyncClient — set by main.py during lifespan startup.
# This module re-uses the client created in main.py to avoid creating a
# second connection pool.  main.py imports this variable and writes to it:
#   import bridge.core.audio as _audio_mod; _audio_mod._http_client = client
_http_client: httpx.AsyncClient = None  # type: ignore


async def get_audio_url_web(text: str, speaker_id: int | None = None) -> tuple[str, str | None]:
    """Get MP3 URLs from VOICEVOX Web高速版 (api.tts.quest) without downloading.
    Returns (mp3DownloadUrl, mp3StreamingUrl).
    """
    resp = await _http_client.get(
        f"{VOICEVOX_URL}/synthesis",
        params={"speaker": speaker_id if speaker_id is not None else VOICEVOX_SPEAKER, "text": text, "key": VOICEVOX_API_KEY},
    )
    resp.raise_for_status()
    data = resp.json()

    if not data.get("success"):
        raise RuntimeError(f"VOICEVOX Web API error: {data}")

    mp3_url = data.get("mp3DownloadUrl")
    if not mp3_url:
        raise RuntimeError(f"No mp3DownloadUrl in response: {data}")

    return mp3_url, data.get("mp3StreamingUrl")


async def resolve_audio_url(text: str, speaker_id: int | None = None) -> tuple[str, str | None]:
    """Return (audioUrl, audioStreamingUrl) for the given text."""
    return await get_audio_url_web(text, speaker_id)
