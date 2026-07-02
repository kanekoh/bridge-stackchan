import asyncio
import io
import math
import os
import re
import socket
import sqlite3
import uuid
import json
import logging
import threading
from contextlib import asynccontextmanager
from dataclasses import dataclass
from html.parser import HTMLParser
from typing import Protocol
from datetime import datetime, timezone, timedelta
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError

import aiohttp
import ephem
from PIL import Image
import httpx
import openai
import paho.mqtt.client as mqtt
import yaml
from fastapi import FastAPI, Form, HTTPException, Query, Request, UploadFile, File
from fastapi.responses import HTMLResponse, RedirectResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

VOICEVOX_URL = os.getenv("VOICEVOX_URL", "http://localhost:50021")
VOICEVOX_SPEAKER = int(os.getenv("VOICEVOX_SPEAKER", "1"))
VOICEVOX_API_KEY = os.getenv("VOICEVOX_API_KEY", "")

MQTT_BROKER = os.getenv("MQTT_BROKER", "localhost")
MQTT_PORT = int(os.getenv("MQTT_PORT", "1883"))
MQTT_USERNAME = os.getenv("MQTT_USERNAME", "")
MQTT_PASSWORD = os.getenv("MQTT_PASSWORD", "")
MQTT_TLS = os.getenv("MQTT_TLS", "false").lower() == "true"
MQTT_DEVICE_ID = os.getenv("MQTT_DEVICE_ID", "default")
MQTT_QOS = int(os.getenv("MQTT_QOS", "1"))
MQTT_ACK_TIMEOUT = float(os.getenv("MQTT_ACK_TIMEOUT", "15.0"))

OPENAI_API_KEY = os.getenv("OPENAI_API_KEY", "")
OPENCLAW_BASE_URL = os.getenv("OPENCLAW_BASE_URL", "http://localhost:18789/v1")
OPENCLAW_MODEL = os.getenv("OPENCLAW_MODEL", "openclaw")
OPENCLAW_GATEWAY_TOKEN = os.getenv("OPENCLAW_GATEWAY_TOKEN", "")
OPENCLAW_SESSION_KEY = os.getenv("OPENCLAW_SESSION_KEY", "")
_raw = os.getenv("OPENCLAW_MAX_OUTPUT_TOKENS", "")
OPENCLAW_MAX_OUTPUT_TOKENS: int | None = int(_raw) if _raw.strip() else None

SPEAKER_ID_URL = os.getenv("SPEAKER_ID_URL", "")
SPEAKER_ID_API_KEY = os.getenv("SPEAKER_ID_API_KEY", "")
SPEAKER_ID_THRESHOLD = float(os.getenv("SPEAKER_ID_THRESHOLD", "0.75"))
SPEAKER_ID_BROWSER_URL = os.getenv("SPEAKER_ID_BROWSER_URL", "")  # ブラウザからアクセスする URL（例: http://raspberrypi:8082）
STT_MODEL = os.getenv("STT_MODEL", "whisper-1")

# LLM バックエンド切り替え
LLM_BACKEND = os.getenv("LLM_BACKEND", "openclaw")  # "openclaw" or "openai"
OPENAI_RESPONSES_BASE_URL = os.getenv("OPENAI_RESPONSES_BASE_URL", "https://api.openai.com/v1")
OPENAI_RESPONSES_MODEL = os.getenv("OPENAI_RESPONSES_MODEL", "gpt-4o-mini")
_raw_or = os.getenv("OPENAI_RESPONSES_MAX_OUTPUT_TOKENS", "")
OPENAI_RESPONSES_MAX_OUTPUT_TOKENS: int | None = int(_raw_or) if _raw_or.strip() else None
OPENAI_RESPONSES_WEB_SEARCH = os.getenv("OPENAI_RESPONSES_WEB_SEARCH", "false").lower() == "true"
OPENAI_RESPONSES_WEB_SEARCH_TOOL = os.getenv("OPENAI_RESPONSES_WEB_SEARCH_TOOL", "web_search_preview")
# 実験: True にすると Pass 1 では request_web_search のみ提示し、LLM が必要と判断したときだけ
# Pass 2 で web_search_preview を有効化する。雑談ターンの平均レイテンシを短縮できる。
OPENAI_RESPONSES_WEB_SEARCH_ON_DEMAND = os.getenv("OPENAI_RESPONSES_WEB_SEARCH_ON_DEMAND", "false").lower() == "true"
# 切り分け用フラグ (デフォルト false = 通常動作)
DISABLE_SESSION_HISTORY = os.getenv("DISABLE_SESSION_HISTORY", "false").lower() == "true"
DISABLE_TOOLS = os.getenv("DISABLE_TOOLS", "false").lower() == "true"
# 会話サマリ: 合計文字数がこの閾値を超えたら会話を要約してリセットする
SESSION_SUMMARY_THRESHOLD = int(os.getenv("SESSION_SUMMARY_THRESHOLD", "3000"))
SESSION_SUMMARY_MAX_TOKENS = int(os.getenv("SESSION_SUMMARY_MAX_TOKENS", "500"))

DB_PATH = os.getenv("DB_PATH", "data/bridge.db")

# Google Calendar / Tasks
CALENDAR_ENABLED = os.getenv("CALENDAR_ENABLED", "false").lower() == "true"

# Google Geolocation API（Stack-chan からの位置更新）
GOOGLE_GEOLOCATION_API_KEY = os.getenv("GOOGLE_GEOLOCATION_API_KEY", "")

# 天気通知
WEATHER_NOTIFY_RAIN     = os.getenv("WEATHER_NOTIFY_RAIN", "false")
WEATHER_CHECK_INTERVAL  = int(os.getenv("WEATHER_CHECK_INTERVAL", "900"))   # 15分
WEATHER_RAIN_THRESHOLD  = float(os.getenv("WEATHER_RAIN_THRESHOLD", "0.3")) # mm/15min で雨とみなす
WEATHER_RAIN_SUDDEN_MUL = 5.0  # 閾値の何倍以上で「急な雨」とみなすか

# ─── JMA ナウキャスト設定 ───────────────────────────────────────────────────
_NOWCAST_ZOOM = 9
_NOWCAST_COLOR_MAP: list[tuple[tuple[int, int, int], float]] = [
    ((242, 242, 255), 0.3),   # 微雨
    ((160, 210, 255), 0.7),   # 小雨
    ((  0, 150, 255), 3.0),   # 雨
    ((  0,  65, 255), 7.0),   # 強雨
    ((250, 245,   0), 15.0),  # 激しい雨
    ((255, 153,   0), 25.0),  # 非常に激しい雨
    ((255,  40,   0), 40.0),  # 猛烈な雨
    ((180,   0, 104), 65.0),  # 猛烈な雨+
]

# P2P地震情報 WebSocket
P2PQUAKE_ENABLED = os.getenv("P2PQUAKE_ENABLED", "false").lower() == "true"
P2PQUAKE_WS_URL = os.getenv("P2PQUAKE_WS_URL", "wss://api.p2pquake.net/v2/ws")
P2PQUAKE_MIN_SCALE = int(os.getenv("P2PQUAKE_MIN_SCALE", "30"))  # 震度3以上で通知
P2PQUAKE_TSUNAMI_TARGET_AREAS: set[str] = set(
    os.getenv("P2PQUAKE_TSUNAMI_TARGET_AREAS", "相模湾・三浦半島,神奈川県,伊豆諸島").split(",")
)
# ISS 通過通知
ISS_NOTIFY_ENABLED  = os.getenv("ISS_NOTIFY_ENABLED", "false").lower() == "true"
ISS_MIN_ELEVATION   = float(os.getenv("ISS_MIN_ELEVATION", "30"))   # 最大仰角が何度以上のパスを通知するか
ISS_NOTIFY_AHEAD    = int(os.getenv("ISS_NOTIFY_AHEAD", "5"))       # 何分前に通知するか
ISS_TLE_URL         = "https://celestrak.org/NORAD/elements/gp.php?NAME=ISS%20(ZARYA)&FORMAT=TLE"

GOOGLE_CREDENTIALS_FILE = os.getenv("GOOGLE_CREDENTIALS_FILE", "secrets/credentials.json")
GOOGLE_TOKEN_DIR = os.getenv("GOOGLE_TOKEN_DIR", "secrets")
CALENDAR_SYNC_INTERVAL_MINUTES = int(os.getenv("CALENDAR_SYNC_INTERVAL_MINUTES", "30"))
CALENDAR_DEFAULT_NOTIFY_MINUTES = int(os.getenv("CALENDAR_DEFAULT_NOTIFY_MINUTES", "15"))
CALENDAR_SYNC_DAYS_AHEAD = int(os.getenv("CALENDAR_SYNC_DAYS_AHEAD", "7"))
CALENDAR_NOTIFY_CHECK_INTERVAL = int(os.getenv("CALENDAR_NOTIFY_CHECK_INTERVAL", "60"))
CALENDAR_NOTIFY_GRACE_MINUTES = int(os.getenv("CALENDAR_NOTIFY_GRACE_MINUTES", "60"))

EXPRESSION_MAP_FILE = os.getenv("EXPRESSION_MAP_FILE", "config/expression_map.yaml")

# Slack (Socket Mode — 両方設定されている場合のみ有効)
SLACK_BOT_TOKEN = os.getenv("SLACK_BOT_TOKEN", "")
SLACK_APP_TOKEN = os.getenv("SLACK_APP_TOKEN", "")

_JST = timezone(timedelta(hours=9))

_KNOWN_EXPRESSIONS = {"neutral", "happy", "sad", "sleepy", "angry", "doubt"}

# Explicitly export underscore-prefixed names so `from bridge.config import *` works
__all__ = [
    # stdlib / third-party re-exports (needed by main.py at module level)
    "asyncio", "io", "math", "os", "re", "socket", "sqlite3", "uuid", "json",
    "logging", "threading", "asynccontextmanager", "dataclass", "HTMLParser",
    "Protocol", "datetime", "timezone", "timedelta", "ZoneInfo", "ZoneInfoNotFoundError",
    "aiohttp", "ephem", "Image", "httpx", "openai", "mqtt", "yaml",
    "FastAPI", "Form", "HTTPException", "Query", "Request", "UploadFile", "File",
    "HTMLResponse", "RedirectResponse", "Jinja2Templates", "BaseModel", "load_dotenv",
    # config constants
    "VOICEVOX_URL", "VOICEVOX_SPEAKER", "VOICEVOX_API_KEY",
    "MQTT_BROKER", "MQTT_PORT", "MQTT_USERNAME", "MQTT_PASSWORD", "MQTT_TLS",
    "MQTT_DEVICE_ID", "MQTT_QOS", "MQTT_ACK_TIMEOUT",
    "OPENAI_API_KEY", "OPENCLAW_BASE_URL", "OPENCLAW_MODEL", "OPENCLAW_GATEWAY_TOKEN",
    "OPENCLAW_SESSION_KEY", "OPENCLAW_MAX_OUTPUT_TOKENS",
    "SPEAKER_ID_URL", "SPEAKER_ID_API_KEY", "SPEAKER_ID_THRESHOLD",
    "SPEAKER_ID_BROWSER_URL", "STT_MODEL",
    "LLM_BACKEND", "OPENAI_RESPONSES_BASE_URL", "OPENAI_RESPONSES_MODEL",
    "OPENAI_RESPONSES_MAX_OUTPUT_TOKENS", "OPENAI_RESPONSES_WEB_SEARCH",
    "OPENAI_RESPONSES_WEB_SEARCH_TOOL", "OPENAI_RESPONSES_WEB_SEARCH_ON_DEMAND",
    "DISABLE_SESSION_HISTORY", "DISABLE_TOOLS",
    "SESSION_SUMMARY_THRESHOLD", "SESSION_SUMMARY_MAX_TOKENS",
    "DB_PATH", "CALENDAR_ENABLED", "GOOGLE_GEOLOCATION_API_KEY",
    "WEATHER_NOTIFY_RAIN", "WEATHER_CHECK_INTERVAL", "WEATHER_RAIN_THRESHOLD",
    "WEATHER_RAIN_SUDDEN_MUL",
    "_NOWCAST_ZOOM", "_NOWCAST_COLOR_MAP",
    "P2PQUAKE_ENABLED", "P2PQUAKE_WS_URL", "P2PQUAKE_MIN_SCALE",
    "P2PQUAKE_TSUNAMI_TARGET_AREAS",
    "ISS_NOTIFY_ENABLED", "ISS_MIN_ELEVATION", "ISS_NOTIFY_AHEAD", "ISS_TLE_URL",
    "GOOGLE_CREDENTIALS_FILE", "GOOGLE_TOKEN_DIR",
    "CALENDAR_SYNC_INTERVAL_MINUTES", "CALENDAR_DEFAULT_NOTIFY_MINUTES",
    "CALENDAR_SYNC_DAYS_AHEAD", "CALENDAR_NOTIFY_CHECK_INTERVAL",
    "CALENDAR_NOTIFY_GRACE_MINUTES",
    "EXPRESSION_MAP_FILE", "SLACK_BOT_TOKEN", "SLACK_APP_TOKEN",
    "_JST", "_KNOWN_EXPRESSIONS",
]
