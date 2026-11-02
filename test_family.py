"""家での呼び方（おとうさん・ママ）の解決と、スタックちゃんから Slack への送信のテスト。

家の中の呼び方は子どもから見た呼び方にそろう。えりが「おとうさん」と言っても
えりの父親ではなく、ひろゆきのこと。

Run:
    pytest test_family.py -v
"""
import os
import sys
import types
from unittest.mock import AsyncMock

import pytest

os.environ.setdefault("OPENAI_API_KEY", "sk-test")

from bridge.features.family import (  # noqa: E402
    household_roles, household_terms_text, normalize, resolve_member,
)

HIRO, ERI, SHIORI, STACK, YOSHIKO, KAZUO = 1, 2, 3, 4, 5, 6


def _member(i, name, gender=None, slack=None):
    return {"id": i, "name": name, "gender": gender, "slack_user_id": slack}


def _rel(a, b, relation):
    return {"from_id": a, "to_id": b, "relation": relation}


MEMBERS = [
    _member(HIRO, "ひろゆき", "male", "U_HIRO"),
    _member(ERI, "えり", "female", "U_ERI"),
    _member(SHIORI, "しおり", "female"),
    _member(STACK, "スタックちゃん"),
    _member(YOSHIKO, "よしこ", "female"),      # えりの母
]
RELATIONS = [
    _rel(HIRO, ERI, "spouse"),
    _rel(HIRO, SHIORI, "parent"),
    _rel(ERI, SHIORI, "parent"),
    _rel(SHIORI, STACK, "sibling"),
    _rel(YOSHIKO, ERI, "parent"),
]


def _resolve(query, members=MEMBERS, relations=RELATIONS):
    return resolve_member(query, members, relations)


# ── 呼び方の解決 ─────────────────────────────────────────────────────────────

@pytest.mark.parametrize("query", ["おとうさん", "お父さん", "パパ", "ぱぱ", "とうさん"])
def test_father_is_the_childs_father(query):
    r = _resolve(query)
    assert r["status"] == "ok" and r["member"]["name"] == "ひろゆき" and r["via"] == "father"


@pytest.mark.parametrize("query", ["おかあさん", "ママ", "お母さん"])
def test_mother_is_the_childs_mother_not_the_wifes_mother(query):
    """よしこはえりの母だが、家の中で「おかあさん」と言えばえりのこと。"""
    r = _resolve(query)
    assert r["status"] == "ok" and r["member"]["name"] == "えり"


def test_parent_of_a_parent_is_a_grandparent():
    """祖父母の線を引いていなくても、親の親は「おばあちゃん」。"""
    r = _resolve("おばあちゃん")
    assert r["status"] == "ok" and r["member"]["name"] == "よしこ"


def test_two_grandmothers_are_ambiguous():
    members = MEMBERS + [_member(KAZUO, "かずこ", "female")]
    relations = RELATIONS + [_rel(KAZUO, SHIORI, "grandparent")]
    r = _resolve("おばあちゃん", members, relations)
    assert r["status"] == "ambiguous"
    assert {m["name"] for m in r["candidates"]} == {"よしこ", "かずこ"}


def test_parent_without_gender_is_not_guessed():
    members = [dict(m, gender=None) if m["id"] == HIRO else m for m in MEMBERS]
    assert _resolve("おとうさん", members)["status"] == "not_found"


def test_no_relations_means_no_kin_terms():
    assert _resolve("パパ", MEMBERS, [])["status"] == "not_found"


@pytest.mark.parametrize("query,name", [
    ("えり", "えり"), ("エリ", "えり"), ("えりちゃん", "えり"),
    ("ひろゆきさん", "ひろゆき"), ("ヒロユキ", "ひろゆき"),
])
def test_names_ignore_kana_and_honorifics(query, name):
    r = _resolve(query)
    assert r["status"] == "ok" and r["member"]["name"] == name and r["via"] is None


def test_unknown_name_is_not_found():
    assert _resolve("たろう")["status"] == "not_found"


def test_normalize_katakana_to_hiragana():
    assert normalize("スタック ちゃん") == "すたっくちゃん"


def test_household_terms_text():
    assert household_terms_text(MEMBERS, RELATIONS) == (
        "おとうさん・パパ＝ひろゆき／おかあさん・ママ＝えり／おばあちゃん＝よしこ")


def test_roles_skip_people_without_children_in_the_house():
    assert household_terms_text(MEMBERS, [_rel(HIRO, ERI, "spouse")]) == ""
    assert household_roles(MEMBERS, []) == {}


# ── Slack への送信（send_slack_message） ─────────────────────────────────────

@pytest.fixture
def slack(monkeypatch):
    """main の _slack_app と DB 読み出しを差し替える。"""
    from bridge.core import db as _db

    client = types.SimpleNamespace(chat_postMessage=AsyncMock())
    fake_main = types.SimpleNamespace(_slack_app=types.SimpleNamespace(client=client))
    monkeypatch.setitem(sys.modules, "main", fake_main)
    monkeypatch.setattr(_db, "_get_all_family_members", lambda: MEMBERS)
    monkeypatch.setattr(_db, "_fetch_family_relations", lambda: RELATIONS)
    return client


async def _send(to, message="牛乳かってきて", speaker="えり"):
    from bridge.llm.tools import _execute_tool
    return await _execute_tool("send_slack_message", {"to": to, "message": message},
                               {"speaker": speaker})


@pytest.mark.asyncio
async def test_wife_saying_father_sends_to_husband(slack):
    """えりが「おとうさんに送って」と言ったら、ひろゆきに届く。"""
    result = await _send("おとうさん", speaker="えり")
    assert result == {"status": "ok", "sent_to": "ひろゆき", "asked_as": "おとうさん"}
    kwargs = slack.chat_postMessage.await_args.kwargs
    assert kwargs["channel"] == "U_HIRO"
    assert kwargs["text"] == "🤖 スタックちゃんから（えりより）\n牛乳かってきて"


@pytest.mark.asyncio
async def test_send_by_name(slack):
    result = await _send("エリ", speaker="ひろゆき")
    assert result["sent_to"] == "えり"
    assert slack.chat_postMessage.await_args.kwargs["channel"] == "U_ERI"


@pytest.mark.asyncio
async def test_unknown_speaker_has_no_sender_line(slack):
    await _send("パパ", speaker=None)
    assert slack.chat_postMessage.await_args.kwargs["text"] == "🤖 スタックちゃんから\n牛乳かってきて"


@pytest.mark.asyncio
async def test_member_without_slack_is_not_sent(slack):
    result = await _send("しおり")
    assert result["status"] == "error" and "/register" in result["message"]
    slack.chat_postMessage.assert_not_awaited()


@pytest.mark.asyncio
async def test_unknown_recipient_is_not_sent(slack):
    result = await _send("たろう")
    assert result["status"] == "error"
    slack.chat_postMessage.assert_not_awaited()


@pytest.mark.asyncio
async def test_empty_message_is_rejected(slack):
    result = await _send("パパ", message=" ")
    assert result["status"] == "error"
    slack.chat_postMessage.assert_not_awaited()


@pytest.mark.asyncio
async def test_without_slack_connection(monkeypatch):
    monkeypatch.setitem(sys.modules, "main", types.SimpleNamespace(_slack_app=None))
    result = await _send("パパ")
    assert result["status"] == "error" and "Slack" in result["message"]
