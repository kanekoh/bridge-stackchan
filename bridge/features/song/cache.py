"""歌の事前生成とキャッシュ。

リアルタイム合成はしない。起動時に全曲を合成して data/songs/ に置き、
楽譜・style_id・ENGINE バージョン・出力フォーマットのどれかが変わったときだけ
作り直す（判定は score.cache_key）。

M5Stack は発話と同じく HTTP で audioUrl を取りに来るので、ENGINE が返す
24kHz WAV を ffmpeg で 16kHz mono MP3 に落としてから配信する。
"""
import asyncio
import glob
import logging
import os
import tempfile

from bridge.config import (
    FFMPEG_BIN, SONG_BITRATE, SONG_DIR, SONG_SAMPLE_RATE,
)
from bridge.features.song import engine as _engine
from bridge.features.song.score import Score, cache_key

logger = logging.getLogger(__name__)


class SongBuildError(RuntimeError):
    """合成または変換に失敗した。"""


# 出力の作り方を変えたらこの値を上げる。サンプルレートやビットレートと違い、
# ID3/Xing の有無のような「同じパラメータでも中身が変わる」変更はキャッシュキーに
# 現れないため、明示的に印を付けないと古いファイルが使われ続ける。
_AUDIO_FORMAT_REV = "2-noid3"


def _audio_spec() -> str:
    """出力フォーマットの識別子。変えるとキャッシュキーが変わる。"""
    return f"mp3-{SONG_SAMPLE_RATE}-{SONG_BITRATE}-{_AUDIO_FORMAT_REV}"


def filename_for(score: Score, key: str) -> str:
    return f"{score.id}-{key}.mp3"


def path_for(score: Score, key: str) -> str:
    return os.path.join(SONG_DIR, filename_for(score, key))


async def _to_mp3(wav: bytes) -> bytes:
    """ENGINE の WAV を M5Stack 向け MP3 に変換する。

    デバイスは audioStreamingUrl を「MP3 フレームの生の列」として読み、受け取った
    バイトをそのままデコーダへ流し込む。発話で使われている tts.quest の
    ストリーミング配信（.mp3s）も、先頭からいきなり MP3 フレームで始まっている。

    そのため ID3v2 タグと Xing/Info フレームは付けない。ID3 タグを先頭に置くと
    デコーダが同期できず、HTTP の取得には成功するのにエラーも出さず無音になる
    （実際にこれで鳴らなかった）。

    サンプルレートも発話と同じ 24kHz に揃える。VOICEVOX ENGINE の出力が 24kHz
    なので再サンプリングも起きない。
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        out_path = os.path.join(tmpdir, "out.mp3")
        cmd = [
            FFMPEG_BIN, "-hide_banner", "-loglevel", "error", "-y",
            "-f", "wav", "-i", "pipe:0",
            "-ar", str(SONG_SAMPLE_RATE), "-ac", "1",
            "-codec:a", "libmp3lame", "-b:a", SONG_BITRATE,
            # 先頭を MP3 フレームで始める（デバイスのデコーダが同期できるように）
            "-write_id3v2", "0", "-id3v2_version", "0", "-write_xing", "0",
            out_path,
        ]
        try:
            proc = await asyncio.create_subprocess_exec(
                *cmd,
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
        except FileNotFoundError as e:
            raise SongBuildError(f"{FFMPEG_BIN} が見つかりません（FFMPEG_BIN を確認してください）") from e
        _stdout, stderr = await proc.communicate(wav)
        if proc.returncode != 0 or not os.path.exists(out_path):
            raise SongBuildError(f"ffmpeg 変換に失敗しました: {stderr.decode('utf-8', 'replace')[:300]}")
        with open(out_path, "rb") as f:
            mp3 = f.read()
    if not mp3:
        raise SongBuildError("ffmpeg の出力が空でした")
    return mp3


def _purge_stale(score: Score, keep: str) -> None:
    """同じ曲の古いキャッシュを消す（楽譜を書き換えるたびに溜まるのを防ぐ）。"""
    for path in glob.glob(os.path.join(SONG_DIR, f"{score.id}-*.mp3")):
        if os.path.basename(path) == keep:
            continue
        try:
            os.remove(path)
            logger.info("古い歌キャッシュを削除: %s", os.path.basename(path))
        except OSError as e:
            logger.warning("歌キャッシュの削除に失敗: %s: %s", path, e)


async def ensure_song(score: Score, *, force: bool = False) -> tuple[str, str]:
    """曲のキャッシュを用意して (ファイル名, キャッシュキー) を返す。

    既にあれば ENGINE には一切触れない。force=True で強制的に作り直す。
    """
    info = await _engine.get_engine_info()  # ENGINE 未起動ならここで SongEngineError
    key = cache_key(
        score,
        engine_version=info["version"],
        frame_rate=info["frame_rate"],
        audio_spec=_audio_spec(),
    )
    name = filename_for(score, key)
    path = path_for(score, key)

    if not force and os.path.exists(path) and os.path.getsize(path) > 0:
        return name, key

    os.makedirs(SONG_DIR, exist_ok=True)
    wav = await _engine.synthesize(score, info["frame_rate"])
    mp3 = await _to_mp3(wav)

    # 生成途中のファイルを配信してしまわないよう、一時ファイルに書いてから置き換える。
    tmp = path + ".tmp"
    with open(tmp, "wb") as f:
        f.write(mp3)
    os.replace(tmp, path)
    logger.info("歌を生成: song=%s key=%s bytes=%d", score.id, key, len(mp3))

    _purge_stale(score, name)
    return name, key


def cached_name(score: Score, key: str) -> str | None:
    """キャッシュ済みならファイル名、なければ None（ENGINE に触らない）。"""
    path = path_for(score, key)
    return filename_for(score, key) if os.path.exists(path) and os.path.getsize(path) > 0 else None


async def prebuild_all(scores: list[Score]) -> dict:
    """全曲を事前生成する。ENGINE が落ちていても例外にはせず結果を返す。"""
    result: dict = {"built": [], "cached": [], "failed": []}
    if not scores:
        return result
    try:
        await _engine.get_engine_info()
    except Exception as e:
        logger.warning("歌の事前生成をスキップ（ENGINE 未起動）: %s", e)
        result["failed"] = [{"id": s.id, "error": str(e)} for s in scores]
        return result

    for score in scores:
        try:
            existed = False
            info = await _engine.get_engine_info()
            key = cache_key(
                score,
                engine_version=info["version"],
                frame_rate=info["frame_rate"],
                audio_spec=_audio_spec(),
            )
            existed = cached_name(score, key) is not None
            await ensure_song(score)
            (result["cached"] if existed else result["built"]).append(score.id)
        except Exception as e:
            logger.error("歌の生成に失敗: song=%s error=%s", score.id, e)
            result["failed"].append({"id": score.id, "error": str(e)})

    logger.info(
        "歌の事前生成: 生成=%d 既存=%d 失敗=%d",
        len(result["built"]), len(result["cached"]), len(result["failed"]),
    )
    return result
