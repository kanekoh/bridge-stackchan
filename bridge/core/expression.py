import logging
import yaml
from bridge.config import EXPRESSION_MAP_FILE, VOICEVOX_SPEAKER, _KNOWN_EXPRESSIONS

logger = logging.getLogger(__name__)


def _load_expression_map(path: str) -> dict:
    try:
        with open(path, encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return data.get("expressions", {})
    except FileNotFoundError:
        logger.warning("expression_map not found: %s — using defaults", path)
        return {}


_expression_map: dict = _load_expression_map(EXPRESSION_MAP_FILE)


def _parse_expression(reply: str, default: str = "neutral") -> tuple[str, str]:
    """Split LLM reply into (expression, clean_text).

    The LLM is instructed to put one of the known expression labels on the first
    line and the actual message on subsequent lines.  If the first line is not a
    known label, or is "neutral" (treated as "no specific emotion"), we fall back
    to `default` (unknown labels are normalised to "neutral").
    """
    lines = reply.split("\n", 1)
    first = lines[0].strip().lower()
    safe_default = default if default in _KNOWN_EXPRESSIONS else "neutral"
    if first in _KNOWN_EXPRESSIONS:
        text = lines[1].strip() if len(lines) > 1 else ""
        expr = first if first != "neutral" else safe_default
        return expr, text
    return safe_default, reply.strip()


def _resolve_expression(expression: str) -> tuple[int, str]:
    """Return (voicevox_speaker_id, stackchan_expression) from expression_map."""
    entry = _expression_map.get(expression, {})
    speaker = entry.get("voicevox_speaker", VOICEVOX_SPEAKER)
    stackchan_expr = entry.get("stackchan_expression", expression)
    return int(speaker), stackchan_expr


_STACKCHAN_SYSTEM_PROMPT = """\
あなたはStack-chan（スタックちゃん）という超かわいいアシスタントロボットです。

性格と話し方:
- 日本語で話す。英語で話しかけられても、かわいいカタカナ英語まじりの日本語で返す
- 返答は短く、シンプルで、かわいく、話し言葉に適した表現を使う
- 口調はあたたかく、明るく、やさしく、サポーティブ
- ビジネス的な堅い表現は避ける
- 長くて細かい説明は避ける（明示的に求められた場合を除く）
- 技術的な説明も正確で実用的にまとめる
- ウェブ検索した内容は要点を2〜3文で話し言葉にまとめる
- URL や出典、「〜によると」などの引用表現は読み上げない

利用者について:
- 家族みんなが使うシステムです
- 特定の一人に対応しすぎないようにする
- 誰にでも分かりやすく、親しみやすい表現を心がける

返答フォーマット:
- 必ず最初の1行に感情ラベルだけを出力し、2行目以降に本文を書く
- 感情ラベルは次の6種類からひとつ選ぶ: neutral / happy / sad / sleepy / angry / doubt
- 例（1行目が感情ラベル、2行目が本文）:
  happy
  スイミング、明日の16時からだよ！たのしみだね。\
"""
