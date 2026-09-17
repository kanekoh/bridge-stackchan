"""LLM に歌を作らせる。

作らせるのは「楽譜（JSON）」だけで、音にするのはここから先の既存の経路
（score.parse_score → engine.synthesize → cache.ensure_song）をそのまま使う。
つまり LLM が出すのは人が config/songs/*.yaml に書くのと同じものなので、
おかしな楽譜は parse_score が弾く。検証をプロンプトの「お願い」に頼らず、
手書きの楽譜と同じ関門を通すのがこのモジュールの肝。

できた歌は songs テーブルに残るので、次からは合成せずすぐ歌えるし、
「この前つくったあの歌」として呼び戻せる。
"""
import json
import logging
import sys

from bridge.features.song import cache as _cache
from bridge.features.song import library
from bridge.features.song.score import Score, ScoreError, parse_score

logger = logging.getLogger(__name__)

# 作り直しを含めて LLM を呼ぶ回数の上限。
# 1回目で弾かれてもエラー文を添えて作り直させると、たいてい2回目で通る。
_MAX_ATTEMPTS = 2

_COMPOSE_PROMPT = """あなたはロボット「スタックちゃん」です。これから短い歌を作ります。
楽譜だけを JSON で返してください。説明文・コードフェンスは不要です。

{request}

## 楽譜の形式

{{
  "title": "曲の名前（日本語。短く、かわいく）",
  "bpm": 120,
  "mood": ["happy"],
  "default_lyric": "ん",
  "style": {{"sing": 6000, "frame_decode": 3003}},
  "notes": [
    {{"note": "C4", "beats": 1}},
    {{"note": "E4", "beats": 0.5, "lyric": "ら"}},
    {{"note": "rest", "beats": 0.5}}
  ]
}}

## 決まりごと

- note は音名。C4 が真ん中のド。使ってよいのは C3〜C6 の範囲
  （例: C4 D4 E4 F4 G4 A4 B4 C5、半音は F#4 や Bb4 のように書く）
- 休符は "rest"
- beats は拍数。4分音符=1、8分音符=0.5、16分音符=0.25、付点4分=1.5、2分音符=2
  使ってよいのは 0.25 / 0.5 / 0.75 / 1 / 1.5 / 2 / 3 / 4 のいずれか
- bpm は 60〜180
- notes は 16〜48 個。短くまとまった歌にしてください
- 先頭の無音は書かないでください（こちらで自動的に付けます）
- lyric を書く場合は、ひらがな・カタカナ1モーラだけ（「ら」「な」「きゃ」など）。
  「ゃ」「ゅ」「ょ」「ぁ」などの小書きのかなを単独で置かないでください
  （「きゃ」のように直前のかなとセットなら1モーラとして使えます）。
  鼻歌にするなら default_lyric を "ん"、はっきり歌わせたいなら "ら" にして、
  各 note の lyric は省略してかまいません
- mood は曲の雰囲気を表す短い英単語の配列（happy / calm / hurry / sleepy / silly など）
- style.sing は必ず 6000。style.frame_decode は声色で、
  3003=ふつう / 3001=あまあま / 3007=ツンツン / 3076=なみだめ から曲に合うものを選ぶ

## 曲づくりのこつ

- 同じ音が延々と続かないようにし、上がり下がりをつけてください
- 最後は主音（曲の調のド）で終わると落ち着いて聞こえます
- 2〜4小節のフレーズを作り、少し変えて繰り返すと歌らしくなります
"""

_RETRY_SUFFIX = """

さきほどの楽譜は次の理由で使えませんでした。直してもう一度 JSON だけを返してください。
エラー: {error}
"""


def _build_request(theme: str, mood: str, requested_by: str) -> str:
    parts = []
    if theme:
        parts.append(f"お題: {theme}")
    if mood:
        parts.append(f"雰囲気: {mood}")
    if requested_by:
        parts.append(f"{requested_by}のために作ります")
    if not parts:
        parts.append("お題は自由です。いま作りたい歌を作ってください。")
    return "\n".join(parts)


def extract_json_object(raw: str) -> dict:
    """LLM の返答から JSON オブジェクトを取り出す。

    コードフェンスや前置きが付いていても拾えるようにする
    （記憶抽出の _parse_items と同じ考え方）。
    """
    text = (raw or "").strip()
    if text.startswith("```"):
        text = text.split("```")[1] if "```" in text[3:] else text
        text = text.lstrip("json").strip()
    start, end = text.find("{"), text.rfind("}")
    if start == -1 or end == -1:
        raise ScoreError("返答に JSON が含まれていません")
    try:
        data = json.loads(text[start:end + 1])
    except json.JSONDecodeError as e:
        raise ScoreError(f"JSON として読めません: {e}")
    if not isinstance(data, dict):
        raise ScoreError("JSON がオブジェクトではありません")
    return data


async def compose_score(
    *, theme: str = "", mood: str = "", requested_by: str = "", song_id: str | None = None
) -> Score:
    """LLM に楽譜を作らせ、検証して songs テーブルに保存した Score を返す。

    音の合成まではここでは行わない（呼び出し側が ensure_song / play_song を呼ぶ）。
    """
    prompt = _COMPOSE_PROMPT.format(request=_build_request(theme, mood, requested_by))
    last_error: Exception | None = None

    for attempt in range(1, _MAX_ATTEMPTS + 1):
        try:
            raw = await sys.modules["main"].chat_with_llm(
                prompt,
                session_key="",        # 作曲は会話履歴に混ぜない
                use_functions=False,   # 道具は使わせない（楽譜だけ返させる）
                purpose="compose",     # 作曲用モデル（未設定なら会話モデル）
            )
        except Exception as e:
            logger.error("作曲の LLM 呼び出しに失敗: %s: %s", type(e).__name__, e)
            raise

        try:
            data = extract_json_object(raw)
            data.pop("id", None)  # id はこちらで決める
            score = parse_score(data, song_id=song_id or "composed")
        except ScoreError as e:
            last_error = e
            logger.warning("作曲された楽譜が不正（%d回目）: %s", attempt, e)
            if attempt < _MAX_ATTEMPTS:
                prompt = (
                    _COMPOSE_PROMPT.format(request=_build_request(theme, mood, requested_by))
                    + _RETRY_SUFFIX.format(error=e)
                )
                continue
            raise ScoreError(f"楽譜を作れませんでした: {e}") from e
        else:
            logger.info(
                "作曲: title=%s bpm=%s notes=%d mood=%s theme=%s",
                score.title, score.bpm, len(score.notes), score.mood, theme,
            )
            return library.save_composed_score(
                score, theme=theme, created_by=requested_by, song_id=song_id
            )

    raise ScoreError(f"楽譜を作れませんでした: {last_error}")


async def compose_and_build(
    *, theme: str = "", mood: str = "", requested_by: str = ""
) -> Score:
    """作曲してから音も用意する。

    合成に失敗したら歌えないので、保存した歌は消して元の状態に戻す
    （歌えない曲が一覧に残り続けるのを避ける）。
    """
    score = await compose_score(theme=theme, mood=mood, requested_by=requested_by)
    try:
        await _cache.ensure_song(score)
    except Exception:
        logger.exception("作った歌の合成に失敗したので保存を取り消します: id=%s", score.id)
        library.delete_song(score.id)
        raise
    return score
