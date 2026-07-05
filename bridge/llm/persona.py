import logging
from datetime import datetime

from bridge.config import _JST
from bridge.core.db import _get_setting

logger = logging.getLogger(__name__)


def _build_datetime_context() -> str:
    """Return current JST datetime as a context string for the system prompt."""
    now = datetime.now(_JST)
    weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    weekday = weekdays[now.weekday()]
    return f"【現在の日時】{now.year}年{now.month}月{now.day}日（{weekday}）{now.hour:02d}:{now.minute:02d} JST"


def _build_location_context() -> str:
    """設置場所が設定されていればシステムプロンプト用の文字列を返す。未設定なら空文字。"""
    title = _get_setting("location_title", "")
    pref  = _get_setting("location_pref", "")  # noqa: F841
    if not title:
        return ""
    return (
        f"【あなたの設置場所】あなたは {title} に置かれています。"
        "「どこにいる？」「ここはどこ？」などの質問にはこの場所名を答えること。"
        "天気・地域の話題・距離感もこの場所を基準にすること。"
    )


def _build_birthday_context() -> str:
    """今日がスタックちゃんの誕生日（設定が MM-DD 形式）と一致する日だけ文脈を返す。
    誕生日ではない日は空文字にして、普段の会話で毎回意識させないようにする。
    """
    birthday = _get_setting("stackchan_birthday", "").strip()
    if not birthday:
        return ""
    today_md = datetime.now(_JST).strftime("%m-%d")
    if birthday != today_md:
        return ""
    return (
        "【今日はあなたの誕生日です】今日はスタックちゃんの誕生日です。"
        "話しかけられた流れの中で自然に触れてよいですが、無理に毎回言う必要はありません。"
        "お祝いされたら素直に喜んでください。"
    )
