"""地図タイルの参照先のテスト。

2026-10、CARTO の basemaps が API キー必須になり、鍵なしの要求に対して
HTTP 200 のまま「API KEY REQUIRED」と書かれた画像を返すようになった。
失敗が 4xx ではなく画像で返るため、ログにもエラーが出ず、画面を見るまで
気づけなかった。鍵の要る配信元に戻さないようここで縛る。

Run:
    pytest test_ui_maps.py -v
"""
import glob
import os
import re

import pytest

_TEMPLATES = sorted(glob.glob(os.path.join(os.path.dirname(__file__), "templates", "*.html")))

# 鍵が必要・無料では使えない配信元
_NEEDS_KEY = ("cartocdn.com", "api.mapbox.com", "maps.googleapis.com", "api.maptiler.com")

_TILE_RE = re.compile(r"L\.tileLayer\(\s*'([^']+)'")


def _templates_with_tiles():
    found = []
    for path in _TEMPLATES:
        with open(path, encoding="utf-8") as f:
            text = f.read()
        for url in _TILE_RE.findall(text):
            found.append((os.path.basename(path), url, text))
    return found


def test_there_is_at_least_one_map():
    assert _templates_with_tiles(), "地図が1つも無い（テストの前提が崩れている）"


@pytest.mark.parametrize("name,url,_text", _templates_with_tiles())
def test_tiles_do_not_need_an_api_key(name, url, _text):
    for host in _NEEDS_KEY:
        assert host not in url, f"{name}: {host} は鍵が必要（鍵なしだと API KEY REQUIRED の画像が返る）"


@pytest.mark.parametrize("name,url,_text", _templates_with_tiles())
def test_tile_urls_carry_no_credentials(name, url, _text):
    """URL に鍵を直接書かない（リポジトリに載るため）。"""
    assert "key=" not in url and "token=" not in url, name


@pytest.mark.parametrize("name,url,text", _templates_with_tiles())
def test_each_map_has_attribution(name, url, text):
    """地理院タイルも OSM も出典表示が利用の条件。"""
    assert "attribution" in text, name
