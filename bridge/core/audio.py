import logging
import struct
import httpx
from bridge.config import VOICEVOX_URL, VOICEVOX_SPEAKER, VOICEVOX_API_KEY

logger = logging.getLogger(__name__)


def pcm_to_wav(pcm: bytes, sample_rate: int = 16000, channels: int = 1, bits: int = 16) -> bytes:
    """生 PCM に 44 バイトの WAV ヘッダを付けて返す。

    WAV ヘッダは先頭にデータ長を持つため、録音開始時点では確定できない。
    ストリーミング受信では生 PCM を受け取り、全チャンクが揃ってから
    ここでヘッダを組み立てる。
    """
    byte_rate = sample_rate * channels * bits // 8
    block_align = channels * bits // 8
    header = b"RIFF" + struct.pack("<I", 36 + len(pcm)) + b"WAVE"
    header += b"fmt " + struct.pack("<IHHIIHH", 16, 1, channels, sample_rate, byte_rate, block_align, bits)
    header += b"data" + struct.pack("<I", len(pcm))
    return header + pcm


def looks_like_wav(data: bytes) -> bool:
    """先頭が RIFF/WAVE なら WAV とみなす。"""
    return len(data) >= 12 and data[:4] == b"RIFF" and data[8:12] == b"WAVE"

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
