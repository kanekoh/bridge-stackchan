import logging
from datetime import datetime

from bridge.config import _JST
from bridge.core.db import (
    _get_setting, _get_all_family_members, _fetch_family_relations,
    _fetch_memories_for_speaker, _get_display_tz,
)
from bridge.features.family import household_terms_text

logger = logging.getLogger(__name__)


_GENDER_LABELS = {"male": "男性", "female": "女性"}


def _is_stackchan(name: str) -> bool:
    """家族メンバーのうち、スタックちゃん自身として登録された人か。

    関係図ではスタックちゃんも1人として登録されている（きょうだいの線つき）。
    LLM に「これはあなた自身」と分かるように渡すため、名前で見分ける。
    """
    n = name.replace(" ", "").replace("　", "").lower()
    return "スタック" in n or "すたっく" in n or "stackchan" in n or "stack-chan" in n


def _relation_phrases(member: dict, relations: list[dict], by_id: dict) -> list[str]:
    """その人の立場から見た関係を「えりの夫」「しおりの父」の形で並べる。

    親子・祖父母と孫は向きを取り違えやすいので、LLM に向きを解釈させず、
    ここで本人から見た呼び名に直してから渡す。相手が同じ呼び名なら
    「ひろゆき・えりの子」とまとめる。
    """
    me, gender = member["id"], member.get("gender")
    grouped: dict[str, list[str]] = {}

    def add(label: str, other_id: int) -> None:
        other = by_id.get(other_id)
        if other is None:
            return
        name = "あなた" if _is_stackchan(other["name"]) else other["name"]
        grouped.setdefault(label, []).append(name)

    for r in relations:
        rel, a, b = r["relation"], r["from_id"], r["to_id"]
        if me not in (a, b):
            continue
        other = b if me == a else a
        if rel == "parent":
            if me == a:
                add({"male": "父", "female": "母"}.get(gender, "親"), other)
            else:
                add({"male": "息子", "female": "娘"}.get(gender, "子"), other)
        elif rel == "grandparent":
            if me == a:
                add({"male": "祖父", "female": "祖母"}.get(gender, "祖父母"), other)
            else:
                add("孫", other)
        elif rel == "spouse":
            add({"male": "夫", "female": "妻"}.get(gender, "配偶者"), other)
        elif rel == "sibling":
            add("きょうだい", other)
        elif rel == "relative":
            add("親戚", other)
    return [f"{'・'.join(names)}の{label}" for label, names in grouped.items()]


def _build_family_context() -> str:
    """登録済みの家族メンバーと関係をシステムプロンプト用の文字列で返す。未登録なら空文字。

    「家族に誰がいる？」「しおりのママはだれ？」に確実に答えられるようにするための常時注入。
    件数が少なく変化も稀なので、検索せず全件そのまま渡す。
    関係図（/ui/family）の線と性別もここで渡す。名前だけだと誰が親で誰が子かを
    知らないまま話すことになり、家族構成を取り違えていた。
    """
    try:
        members = [m for m in _get_all_family_members() if m.get("name")]
        relations = _fetch_family_relations()
    except Exception as e:
        logger.warning("family context build failed (non-fatal): %s", e)
        return ""
    if not members:
        return ""
    by_id = {m["id"]: m for m in members}
    lines = []
    for m in members:
        tags = []
        if _is_stackchan(m["name"]):
            tags.append("あなた自身")
        elif m.get("gender") in _GENDER_LABELS:
            tags.append(_GENDER_LABELS[m["gender"]])
        head = m["name"] + (f"（{'・'.join(tags)}）" if tags else "")
        phrases = _relation_phrases(m, relations, by_id)
        lines.append(f"・{head}" + (f"：{'、'.join(phrases)}" if phrases else ""))
    text = (
        "【あなたの家族】\n" + "\n".join(lines) + "\n"
        "これがあなたと一緒に暮らしている家族です。"
        "「家族に誰がいる？」と聞かれたらこの人たちを答えること。"
    )
    if relations:
        text += (
            "\n家族の関係はこの一覧が正しい情報です。"
            "「A の B」は「A から見て B にあたる人」という意味です"
            "（「しおりの父」なら、しおりのお父さん）。"
            "一覧に書かれていない関係は推測で決めつけないこと。"
        )
    terms = household_terms_text(members, relations)
    if terms:
        text += (
            f"\n【この家での呼び方】{terms}\n"
            "家の中の呼び方は子どもから見た呼び方です。誰が言っても"
            "（たとえば妻が「おとうさん」と言っても）この人のことを指します。"
            "話している人自身の親のことだと受け取らないこと。"
        )
    return text


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
