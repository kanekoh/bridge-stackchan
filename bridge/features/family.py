"""家族の呼び方（おとうさん・ママ・おばあちゃん）を、関係図から実在の家族に結びつける。

日本の家庭では、家の中の呼び方は**いちばん下の世代（子ども）から見た呼び方**にそろう。
えりが夫のことを「おとうさん」と呼ぶのが普通で、えり自身の父親のことではない。
そのため、話している人の立場では解かず、家の子どもから見た続柄で解く。

純関数のみ（DB には触らない）。メンバーと関係は呼び出し側が渡す。
"""
from __future__ import annotations

# 呼び方 → 子どもから見た続柄。表記ゆれはひらがなにそろえてから引く。
_KIN_TERMS: dict[str, tuple[str, ...]] = {
    "father": ("おとうさん", "おとーさん", "とうさん", "おとん", "ぱぱ", "ちち",
               "お父さん", "父さん", "父"),
    "mother": ("おかあさん", "おかーさん", "かあさん", "おかん", "まま", "はは",
               "お母さん", "母さん", "母"),
    "grandfather": ("おじいちゃん", "おじいさん", "じいじ", "じいちゃん",
                    "お祖父さん", "祖父"),
    "grandmother": ("おばあちゃん", "おばあさん", "ばあば", "ばあちゃん",
                    "お祖母さん", "祖母"),
}
# プロンプトに出すときの代表的な呼び方
_KIN_DISPLAY = {
    "father": "おとうさん・パパ",
    "mother": "おかあさん・ママ",
    "grandfather": "おじいちゃん",
    "grandmother": "おばあちゃん",
}
_HONORIFICS = ("さん", "ちゃん", "くん", "様", "さま")


def normalize(text: str) -> str:
    """カタカナをひらがなに、空白を除いてそろえる（「エリ」と「えり」を同じに扱う）。"""
    out = []
    for c in text.strip():
        if c in " 　":
            continue
        if "ァ" <= c <= "ヶ":
            c = chr(ord(c) - 0x60)
        out.append(c)
    return "".join(out).lower()


def _children(relations: list[dict]) -> set[int]:
    """家の子ども＝親（祖父母）の線の子側にいて、自分は誰の親でもない人。"""
    parents = {r["from_id"] for r in relations if r["relation"] == "parent"}
    return {
        r["to_id"] for r in relations
        if r["relation"] in ("parent", "grandparent") and r["to_id"] not in parents
    }


def household_roles(members: list[dict], relations: list[dict]) -> dict[str, list[dict]]:
    """子どもから見た続柄ごとに、当てはまる家族を返す。

    性別が未設定の人は「おとうさん／おかあさん」に振り分けられないので入らない。
    同じ続柄に2人いる（祖母が2人など）ときは両方入る。どちらかは呼び出し側で聞き返す。
    """
    by_id = {m["id"]: m for m in members}
    kids = _children(relations)
    parents = {r["from_id"] for r in relations
               if r["relation"] == "parent" and r["to_id"] in kids}
    grandparents = {r["from_id"] for r in relations
                    if r["relation"] == "grandparent" and r["to_id"] in kids}
    # 祖父母の線が引かれていなくても、親の親は祖父母
    grandparents |= {r["from_id"] for r in relations
                     if r["relation"] == "parent" and r["to_id"] in parents}

    roles: dict[str, list[dict]] = {k: [] for k in _KIN_TERMS}
    for ids, male, female in ((parents, "father", "mother"),
                              (grandparents, "grandfather", "grandmother")):
        for i in sorted(ids):
            m = by_id.get(i)
            if m is None:
                continue
            if m.get("gender") == "male":
                roles[male].append(m)
            elif m.get("gender") == "female":
                roles[female].append(m)
    return {k: v for k, v in roles.items() if v}


def household_terms_text(members: list[dict], relations: list[dict]) -> str:
    """「おとうさん・パパ＝ひろゆき／おかあさん・ママ＝えり」の形。当てはまる人がいなければ空文字。"""
    roles = household_roles(members, relations)
    parts = [f"{_KIN_DISPLAY[k]}＝{'または'.join(m['name'] for m in roles[k])}"
             for k in _KIN_TERMS if k in roles]
    return "／".join(parts)


def resolve_member(query: str, members: list[dict], relations: list[dict]) -> dict:
    """「えり」「エリちゃん」「おとうさん」「パパ」を家族の1人に結びつける。

    戻り値の status:
      ok        … member に1人入る。via は呼び方で解いたときの続柄（名前で引いたら None）
      ambiguous … candidates に複数（「おばあちゃん」が2人いるなど）
      not_found … 該当なし
    """
    q = normalize(query)
    if not q:
        return {"status": "not_found"}

    # 名前で引く（「えりちゃん」「ひろゆきさん」も通す）
    stripped = q
    for h in _HONORIFICS:
        if stripped.endswith(h) and len(stripped) > len(h):
            stripped = stripped[: -len(h)]
            break
    by_name = [m for m in members if normalize(m["name"]) in (q, stripped)]
    if len(by_name) == 1:
        return {"status": "ok", "member": by_name[0], "via": None}

    # 呼び方で引く（子どもから見た続柄）
    for kin, terms in _KIN_TERMS.items():
        if q in {normalize(t) for t in terms}:
            found = household_roles(members, relations).get(kin, [])
            if len(found) == 1:
                return {"status": "ok", "member": found[0], "via": kin}
            if found:
                return {"status": "ambiguous", "candidates": found}
            return {"status": "not_found"}
    if by_name:
        return {"status": "ambiguous", "candidates": by_name}
    return {"status": "not_found"}
