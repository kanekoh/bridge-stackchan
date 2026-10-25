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
# 性別は関係図のラベルを「父/母」「祖父/祖母」と出し分けるためだけに持つ。
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
