"""VOICEVOX ENGINE（歌声合成）クライアント。

発話側（bridge/core/audio.py）が使っている Web 高速版 api.tts.quest には歌唱 API が
ないため、歌だけはローカルの VOICEVOX ENGINE 0.25.2 を叩く。常駐している前提
（systemd 等）で、起動していなければ歌機能だけが無効になり他機能には影響しない。

合成は 2 段階:
  1. POST /sing_frame_audio_query?speaker=<sing用style_id>   … 音程・タイミングを決める
  2. POST /frame_synthesis?speaker=<frame_decode用style_id>  … 声色を決めて WAV を得る
"""
import logging

import httpx

from bridge.config import VOICEVOX_SING_URL, VOICEVOX_SING_TIMEOUT
from bridge.features.song.score import DEFAULT_FRAME_RATE, Score, build_notes

logger = logging.getLogger(__name__)


class SongEngineError(RuntimeError):
    """ENGINE が使えない・合成に失敗した。"""


# /version と /engine_manifest は起動中に変わらないのでプロセス内にキャッシュする。
_engine_info: dict | None = None


def reset_cache() -> None:
    """ENGINE の再起動やバージョン差し替えのあとに呼ぶ。"""
    global _engine_info
    _engine_info = None


async def get_engine_info() -> dict:
    """{"version": ..., "frame_rate": ...} を返す。ENGINE 未起動なら SongEngineError。"""
    global _engine_info
    if _engine_info is not None:
        return _engine_info
    base = VOICEVOX_SING_URL.rstrip("/")
    try:
        async with httpx.AsyncClient(timeout=10) as client:
            ver_resp = await client.get(f"{base}/version")
            ver_resp.raise_for_status()
            # /version は JSON 文字列（"0.25.2"）を返す。念のため素のテキストにも備える。
            try:
                version = str(ver_resp.json())
            except ValueError:
                version = ver_resp.text.strip().strip('"')

            frame_rate = DEFAULT_FRAME_RATE
            try:
                man_resp = await client.get(f"{base}/engine_manifest")
                man_resp.raise_for_status()
                frame_rate = float(man_resp.json().get("frame_rate", DEFAULT_FRAME_RATE))
            except Exception as e:
                # frame_rate は既定値 93.75 で十分動くので、取得できなくても止めない。
                logger.warning("engine_manifest 取得に失敗（frame_rate=%s を使用）: %s", DEFAULT_FRAME_RATE, e)
    except Exception as e:
        raise SongEngineError(f"VOICEVOX ENGINE に接続できません ({base}): {e}") from e

    _engine_info = {"version": version, "frame_rate": frame_rate}
    logger.info("VOICEVOX ENGINE: version=%s frame_rate=%s url=%s", version, frame_rate, base)
    return _engine_info


async def synthesize(score: Score, frame_rate: float | None = None) -> bytes:
    """楽譜を歌声 WAV（24kHz / mono / 16bit）にして返す。"""
    if frame_rate is None:
        frame_rate = (await get_engine_info())["frame_rate"]
    notes = build_notes(score, frame_rate)
    base = VOICEVOX_SING_URL.rstrip("/")

    try:
        async with httpx.AsyncClient(timeout=VOICEVOX_SING_TIMEOUT) as client:
            query_resp = await client.post(
                f"{base}/sing_frame_audio_query",
                params={"speaker": score.sing_style_id},
                json={"notes": notes},
            )
            query_resp.raise_for_status()
            frame_query = query_resp.json()

            synth_resp = await client.post(
                f"{base}/frame_synthesis",
                params={"speaker": score.frame_decode_style_id},
                json=frame_query,
            )
            synth_resp.raise_for_status()
            wav = synth_resp.content
    except httpx.HTTPStatusError as e:
        raise SongEngineError(
            f"歌声合成に失敗しました: HTTP {e.response.status_code} {e.response.text[:200]}"
        ) from e
    except Exception as e:
        raise SongEngineError(f"歌声合成に失敗しました: {e}") from e

    if not wav:
        raise SongEngineError("歌声合成の応答が空でした")
    logger.info(
        "歌声合成: song=%s notes=%d frames=%d bytes=%d",
        score.id, len(notes), sum(n["frame_length"] for n in notes), len(wav),
    )
    return wav
