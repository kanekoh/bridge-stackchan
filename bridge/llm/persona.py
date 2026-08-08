import logging
from datetime import datetime

from bridge.config import _JST
from bridge.core.db import (
    _get_setting, _get_all_family_members, _fetch_memories_for_speaker,
    _get_display_tz,
)

logger = logging.getLogger(__name__)


def _build_family_context() -> str:
    """登録済みの家族メンバーをシステムプロンプト用の文字列で返す。未登録なら空文字。

    「家族に誰がいる？」に確実に答えられるようにするための常時注入。
    件数が少なく変化も稀なので、検索せず全件そのまま渡す。
    """
    try:
        members = _get_all_family_members()
    except Exception as e:
        logger.warning("family context build failed (non-fatal): %s", e)
        return ""
    names = [m["name"] for m in members if m.get("name")]
    if not names:
        return ""
    return (
        "【あなたの家族】" + "、".join(names) + "\n"
        "これがあなたと一緒に暮らしている家族です。"
        "「家族に誰がいる？」と聞かれたらこの人たちを答えること。"
    )


def _build_datetime_context() -> str:
    """現在の日時を、スタックちゃんが置かれている場所のタイムゾーンで返す。

    設置場所を登録すると Open-Meteo からタイムゾーンを取得して location_timezone に
    保存されるので、それを使う。日本以外に置かれた場合や旅行に持ち出した場合でも
    その土地の日付・時刻で答えられる。未設定なら JST にフォールバックする。
    """
    tz = _get_display_tz()
    now = datetime.now(tz)
    weekdays = ["月", "火", "水", "木", "金", "土", "日"]
    weekday = weekdays[now.weekday()]
    tz_label = now.strftime("%Z") or str(tz)
    return (
        f"【現在の日時】{now.year}年{now.month}月{now.day}日（{weekday}）"
        f"{now.hour:02d}:{now.minute:02d} {tz_label}\n"
        "日付や時刻を聞かれたら、この場所の時間で答えること。"
    )


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


def _build_memory_context(speaker: str | None) -> str:
    """話者に見せてよい記憶を数件、プロンプト用の文字列にして返す。

    埋め込み検索はせず DB を読むだけ（1ms 未満）なので、応答が遅くならない。
    ここに入った記憶は、聞かれなくても自然と返答ににじむ。
    """
    try:
        memories = _fetch_memories_for_speaker(speaker, limit=8)
    except Exception as e:
        logger.warning("memory context build failed (non-fatal): %s", e)
        return ""
    if not memories:
        return ""
    lines = [f"・{m['content']}" for m in memories]
    return (
        "【覚えていること】\n" + "\n".join(lines) + "\n"
        "会話の流れで自然に触れてよいですが、関係ないときは無理に持ち出さないこと。"
    )
