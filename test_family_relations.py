"""家族の関係図のテスト。

**家族に閉じる**という方針（2026-10-25）を、データの持ち方で守る。
友達やよその家の子は family_members に入れないため、関係も家族どうしだけになる。

Run:
    pytest test_family_relations.py -v
"""
import os
import sqlite3

import pytest

os.environ.setdefault("OPENAI_API_KEY", "sk-test")
os.environ.setdefault("DB_PATH", "/tmp/test-bridge-family.db")

from fastapi import FastAPI  # noqa: E402
from fastapi.testclient import TestClient  # noqa: E402

import bridge.core.db as _db_mod  # noqa: E402
from bridge.core.db import (  # noqa: E402
    _delete_family_relation, _fetch_family_relations, _save_family_relation,
)


@pytest.fixture
def db(tmp_path, monkeypatch):
    monkeypatch.setattr(_db_mod, "DB_PATH", str(tmp_path / "fam.db"))
    old = _db_mod._db_conn
    conn = sqlite3.connect(str(tmp_path / "fam.db"), check_same_thread=False)
    _db_mod._db_conn = conn
    _db_mod._init_db()
    now = "2026-10-25T12:00:00+09:00"
    for name in ("ひろゆき", "えり", "しおり"):
        conn.execute("INSERT INTO family_members (name, created_at, updated_at) VALUES (?,?,?)",
                     (name, now, now))
    conn.commit()
    yield conn
    conn.close()
    _db_mod._db_conn = old


@pytest.fixture
def client(db):
    from bridge.api import devices as api

    app = FastAPI()
    app.include_router(api.router)
    return TestClient(app)


def _ids(conn):
    return {r[1]: r[0] for r in conn.execute("SELECT id, name FROM family_members").fetchall()}


# ── 保存 ─────────────────────────────────────────────────────────────────────

def test_parent_relation_keeps_its_direction(db):
    """親子は向きが意味を持つ。入れ替えてはいけない。"""
    ids = _ids(db)
    _save_family_relation(ids["ひろゆき"], ids["しおり"], "parent")
    rel = _fetch_family_relations()[0]
    assert rel["from_name"] == "ひろゆき"
    assert rel["to_name"] == "しおり"


def test_spouse_order_is_normalized(db):
    """夫婦に向きは無い。逆順で入れても同じ1件として扱う。"""
    ids = _ids(db)
    assert _save_family_relation(ids["えり"], ids["ひろゆき"], "spouse") is not None
    assert _save_family_relation(ids["ひろゆき"], ids["えり"], "spouse") is None
    assert len(_fetch_family_relations()) == 1


def test_duplicate_parent_relation_is_ignored(db):
    ids = _ids(db)
    assert _save_family_relation(ids["えり"], ids["しおり"], "parent") is not None
    assert _save_family_relation(ids["えり"], ids["しおり"], "parent") is None


def test_reverse_parent_relation_is_a_different_row(db):
    """親子は逆向きが別の意味になる（誤って正規化しないこと）。"""
    ids = _ids(db)
    _save_family_relation(ids["えり"], ids["しおり"], "parent")
    assert _save_family_relation(ids["しおり"], ids["えり"], "parent") is not None


def test_self_relation_is_rejected(db):
    ids = _ids(db)
    with pytest.raises(ValueError, match="同じ人"):
        _save_family_relation(ids["えり"], ids["えり"], "spouse")


def test_unknown_relation_is_rejected(db):
    ids = _ids(db)
    with pytest.raises(ValueError):
        _save_family_relation(ids["えり"], ids["しおり"], "友達")


def test_delete_relation(db):
    ids = _ids(db)
    rel_id = _save_family_relation(ids["えり"], ids["しおり"], "parent")
    assert _delete_family_relation(rel_id) is True
    assert _fetch_family_relations() == []
    assert _delete_family_relation(rel_id) is False


# ── 性別によるラベルの出し分け ───────────────────────────────────────────────
# 性別は「父/母」「祖父/祖母」の出し分け（関係図のラベルと LLM に渡す家族構成）に使う。
# 未設定でも図は成立する（「親」「祖父母」と出る）。

def _set_gender(conn, name, gender):
    conn.execute("UPDATE family_members SET gender=? WHERE name=?", (gender, name))
    conn.commit()


@pytest.mark.parametrize("gender,expected", [
    ("male", "父"), ("female", "母"), (None, "親"),
])
def test_parent_label_follows_gender(client, db, gender, expected):
    ids = _ids(db)
    _set_gender(db, "ひろゆき", gender)
    _save_family_relation(ids["ひろゆき"], ids["しおり"], "parent")
    body = client.get("/api/family-relations").json()
    assert body["relations"][0]["label"] == expected


@pytest.mark.parametrize("gender,expected", [
    ("male", "祖父"), ("female", "祖母"), (None, "祖父母"),
])
def test_grandparent_label_follows_gender(client, db, gender, expected):
    ids = _ids(db)
    _set_gender(db, "えり", gender)
    _save_family_relation(ids["えり"], ids["しおり"], "grandparent")
    body = client.get("/api/family-relations").json()
    assert body["relations"][0]["label"] == expected


def test_undirected_labels_do_not_use_gender(client, db):
    """夫婦・きょうだい・親戚は、どちらから見ても同じ呼び名。"""
    ids = _ids(db)
    _set_gender(db, "ひろゆき", "male")
    _save_family_relation(ids["ひろゆき"], ids["えり"], "spouse")
    body = client.get("/api/family-relations").json()
    assert body["relations"][0]["label"] == "夫婦"


def test_member_gender_can_be_set_and_cleared(client, db):
    res = client.post("/api/family-members", data={"name": "たろう", "gender": "male"})
    assert res.status_code == 201
    member_id = res.json()["id"]
    members = {m["name"]: m for m in client.get("/api/family-members").json()}
    assert members["たろう"]["gender"] == "male"

    client.put(f"/api/family-members/{member_id}", data={"name": "たろう", "gender": ""})
    members = {m["name"]: m for m in client.get("/api/family-members").json()}
    assert members["たろう"]["gender"] is None


def test_unknown_gender_is_rejected(client, db):
    res = client.post("/api/family-members", data={"name": "はなこ", "gender": "その他"})
    assert res.status_code == 422


def test_parent_label_is_about_the_from_side(client, db):
    """ラベルは「from から見た呼び名」。向きを逆に入れると意味が変わる。

    実際に「しおり → えり parent」と逆向きに登録され、しおり（女性）の性別で
    「母」と出る取り違えが起きた（2026-10-25）。入力欄の見出しを
    「親はだれ／子はだれ」に変えたのはこのため。
    """
    ids = _ids(db)
    _set_gender(db, "ひろゆき", "male")
    _set_gender(db, "しおり", "female")

    _save_family_relation(ids["ひろゆき"], ids["しおり"], "parent")   # 正しい向き
    _save_family_relation(ids["しおり"], ids["ひろゆき"], "parent")   # 逆向き
    labels = [r["label"] for r in client.get("/api/family-relations").json()["relations"]]
    assert labels == ["父", "母"]


# ── エンドポイント ───────────────────────────────────────────────────────────

def test_api_returns_members_and_relations(client, db):
    ids = _ids(db)
    _save_family_relation(ids["ひろゆき"], ids["しおり"], "parent")
    body = client.get("/api/family-relations").json()
    assert {m["name"] for m in body["members"]} == {"ひろゆき", "えり", "しおり"}
    # 矢印の上に出る呼び名。性別が未設定なら「親」（設定済みなら「父」「母」）
    assert body["relations"][0]["label"] == "親"
    assert {t["value"] for t in body["relationTypes"]} == {
        "parent", "grandparent", "spouse", "sibling", "relative"}


@pytest.mark.parametrize("relation", ["grandparent", "relative"])
def test_added_relation_types_are_accepted(client, db, relation):
    ids = _ids(db)
    res = client.post("/api/family-relations",
                      data={"from_id": ids["ひろゆき"], "to_id": ids["しおり"], "relation": relation})
    assert res.status_code == 201


def test_grandparent_keeps_its_direction(db):
    """祖父母→孫は向きが意味を持つ。逆向きは別の行。"""
    ids = _ids(db)
    _save_family_relation(ids["ひろゆき"], ids["しおり"], "grandparent")
    assert _save_family_relation(ids["しおり"], ids["ひろゆき"], "grandparent") is not None
    assert _fetch_family_relations()[0]["from_name"] == "ひろゆき"


def test_relative_order_is_normalized(db):
    """親戚に向きは無い。逆順で入れても同じ1件。"""
    ids = _ids(db)
    assert _save_family_relation(ids["えり"], ids["ひろゆき"], "relative") is not None
    assert _save_family_relation(ids["ひろゆき"], ids["えり"], "relative") is None


def test_api_create_and_delete(client, db):
    ids = _ids(db)
    res = client.post("/api/family-relations",
                      data={"from_id": ids["ひろゆき"], "to_id": ids["えり"], "relation": "spouse"})
    assert res.status_code == 201
    rel_id = res.json()["id"]
    assert client.delete(f"/api/family-relations/{rel_id}").status_code == 204
    assert client.delete(f"/api/family-relations/{rel_id}").status_code == 404


def test_api_rejects_duplicate(client, db):
    ids = _ids(db)
    data = {"from_id": ids["ひろゆき"], "to_id": ids["えり"], "relation": "spouse"}
    assert client.post("/api/family-relations", data=data).status_code == 201
    assert client.post("/api/family-relations", data=data).status_code == 409


def test_api_rejects_unknown_relation(client, db):
    ids = _ids(db)
    res = client.post("/api/family-relations",
                      data={"from_id": ids["えり"], "to_id": ids["しおり"], "relation": "友達"})
    assert res.status_code == 422


def test_api_rejects_self_relation(client, db):
    ids = _ids(db)
    res = client.post("/api/family-relations",
                      data={"from_id": ids["えり"], "to_id": ids["えり"], "relation": "sibling"})
    assert res.status_code == 422


# ── LLM に渡す家族構成 ───────────────────────────────────────────────────────
# 以前は名前を並べるだけで、誰が親で誰が子かをスタックちゃんは知らなかった。
# 向きは LLM に解釈させず、一人ひとりの立場から見た呼び名に直して渡す。

def _family_context():
    from bridge.llm.persona import _build_family_context
    return _build_family_context()


def _line_of(text, name):
    return next(line for line in text.splitlines() if line.startswith(f"・{name}"))


def test_context_lists_names_without_relations(db):
    text = _family_context()
    assert "・ひろゆき" in text and "・えり" in text and "・しおり" in text
    assert "推測で決めつけない" not in text      # 関係がなければ関係の説明もしない


def test_context_shows_parent_from_both_sides(db):
    ids = _ids(db)
    _set_gender(db, "ひろゆき", "male")
    _set_gender(db, "しおり", "female")
    _save_family_relation(ids["ひろゆき"], ids["しおり"], "parent")
    text = _family_context()
    assert _line_of(text, "ひろゆき") == "・ひろゆき（男性）：しおりの父"
    assert _line_of(text, "しおり") == "・しおり（女性）：ひろゆきの娘"


def test_context_groups_same_label(db):
    """両親とも登録されていれば「ひろゆき・えりの子」とまとめる。"""
    ids = _ids(db)
    _save_family_relation(ids["ひろゆき"], ids["しおり"], "parent")
    _save_family_relation(ids["えり"], ids["しおり"], "parent")
    assert _line_of(_family_context(), "しおり") == "・しおり：ひろゆき・えりの子"


def test_context_spouse_follows_own_gender(db):
    ids = _ids(db)
    _set_gender(db, "ひろゆき", "male")
    _set_gender(db, "えり", "female")
    _save_family_relation(ids["えり"], ids["ひろゆき"], "spouse")
    text = _family_context()
    assert _line_of(text, "ひろゆき").endswith("えりの夫")
    assert _line_of(text, "えり").endswith("ひろゆきの妻")


def test_context_grandparent_and_grandchild(db):
    ids = _ids(db)
    _set_gender(db, "えり", "female")
    _save_family_relation(ids["えり"], ids["しおり"], "grandparent")
    text = _family_context()
    assert _line_of(text, "えり").endswith("しおりの祖母")
    assert _line_of(text, "しおり").endswith("えりの孫")


def test_context_marks_stackchan_as_self(db):
    """スタックちゃんも家族として登録されている。LLM には「あなた自身」と分かるように渡す。"""
    db.execute("INSERT INTO family_members (name, created_at, updated_at) VALUES (?,?,?)",
               ("スタックちゃん", "2026-10-25T12:00:00+09:00", "2026-10-25T12:00:00+09:00"))
    db.commit()
    ids = _ids(db)
    _save_family_relation(ids["しおり"], ids["スタックちゃん"], "sibling")
    text = _family_context()
    assert _line_of(text, "スタックちゃん") == "・スタックちゃん（あなた自身）：しおりのきょうだい"
    assert _line_of(text, "しおり").endswith("あなたのきょうだい")


def test_context_shows_household_terms(db):
    """家の中の呼び方は子どもから見たもの。えりが「おとうさん」と言ってもひろゆきのこと。"""
    ids = _ids(db)
    _set_gender(db, "ひろゆき", "male")
    _set_gender(db, "えり", "female")
    _save_family_relation(ids["ひろゆき"], ids["しおり"], "parent")
    _save_family_relation(ids["えり"], ids["しおり"], "parent")
    text = _family_context()
    assert "【この家での呼び方】おとうさん・パパ＝ひろゆき／おかあさん・ママ＝えり" in text
    assert "話している人自身の親のことだと受け取らないこと" in text
