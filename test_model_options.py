"""モデル選択肢と、モデル名で分岐する判定のテスト。

モデルを入れ替えるとき、選択肢だけ直してコード側の接頭辞判定を忘れると、
reasoning.effort が送られず API 既定の medium で動く（遅くなり、隠れ推論
トークンのぶん高くなる）。黙って起きるので、ここで縛っておく。

Run:
    pytest test_model_options.py -v
"""
import os

os.environ.setdefault("OPENAI_API_KEY", "sk-test")
os.environ.setdefault("DB_PATH", "/tmp/test-bridge-models.db")

from bridge.api.settings import _LLM_MODEL_OPTIONS, _STT_MODEL_OPTIONS  # noqa: E402
from bridge.config import OPENAI_RESPONSES_MODEL  # noqa: E402
from bridge.llm.backends import (  # noqa: E402
    _supports_reasoning_effort, _supports_web_search,
)

# 2026-10-23 以降に停止するモデル。選択肢に残っていると、ある日突然 400 になる。
_RETIRED_SOON = ("gpt-4.1-nano", "gpt-5-nano", "o1", "o3", "o3-mini", "o4-mini")


def test_every_option_accepts_reasoning_effort():
    """選択肢のモデルは全部 effort を送れること（送れないと既定の medium で動く）。"""
    for option in _LLM_MODEL_OPTIONS:
        assert _supports_reasoning_effort(option["value"]), option["value"]


def test_every_option_supports_web_search():
    """Web 検索に非対応のモデルを選べると、検索を有効にしても黙って使われない。"""
    for option in _LLM_MODEL_OPTIONS:
        assert _supports_web_search(option["value"]), option["value"]


def test_no_retired_models_in_options():
    for option in _LLM_MODEL_OPTIONS:
        assert option["value"] not in _RETIRED_SOON, option["value"]


def test_default_model_is_in_the_options():
    """既定値が一覧に無いと、UI でどれも選ばれていない状態になる。"""
    assert OPENAI_RESPONSES_MODEL in [o["value"] for o in _LLM_MODEL_OPTIONS]


def test_options_have_prices_and_notes():
    for option in _LLM_MODEL_OPTIONS:
        assert option["price_in"] > 0 and option["price_out"] > 0, option["value"]
        assert option["note"], option["value"]
        # 未計測は None（0 や欠損にしない。UI が「—」と出し分ける）
        assert "latency_ms" in option and "tool_ms" in option


def test_default_is_the_fastest_measured_option():
    """会話の既定は「計測した中でいちばん速いもの」。

    費用ではなく速さで選ぶ。声のやり取りでは1〜2秒の差がそのまま待ち時間になり、
    会話の費用はもともと月数十円の規模だから（2026-10-20 の実測で方針を決めた）。
    """
    measured = [o for o in _LLM_MODEL_OPTIONS if o["latency_ms"] is not None]
    assert measured, "レイテンシを計測した選択肢が無い"
    fastest = min(measured, key=lambda o: o["latency_ms"])
    assert fastest["value"] == OPENAI_RESPONSES_MODEL


def test_a_cheaper_option_exists_for_background_work():
    """通知・記憶・作曲に回せる、会話の既定より安い選択肢を残しておく。"""
    default = next(o for o in _LLM_MODEL_OPTIONS if o["value"] == OPENAI_RESPONSES_MODEL)
    cheaper = [o for o in _LLM_MODEL_OPTIONS if o["price_out"] < default["price_out"]]
    assert cheaper, "安いほうの選択肢が無くなっている"


def test_stt_options_have_prices():
    for option in _STT_MODEL_OPTIONS:
        assert option["price_min"] > 0 and option["latency_ms"] > 0, option["value"]
