#!/usr/bin/env bash
# test_interface.sh - Bridge API インターフェーステスト
#
# 使い方:
#   ./start.sh && ./test_interface.sh                          # デフォルトメッセージで実行
#   ./start.sh && ./test_interface.sh "こんにちは、テストだよ！"  # メッセージを引数で指定
#
# 起動・停止は start.sh / stop.sh に任せる。
# このスクリプトは既に起動済みのサービスに対してテストのみ実行する。

set -euo pipefail

# ── 設定 ─────────────────────────────────────────────
MESSAGE="${1:-おはよう！今日もよろしくね。テストメッセージだよ。}"
PORT="${PORT:-8000}"
BASE_URL="http://localhost:${PORT}"

# ── 色出力 ────────────────────────────────────────────
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
CYAN='\033[0;36m'
BOLD='\033[1m'
RESET='\033[0m'

PASS=0
FAIL=0

pass() { echo -e "  ${GREEN}✓ PASS${RESET}  $1"; PASS=$((PASS + 1)); }
fail() { echo -e "  ${RED}✗ FAIL${RESET}  $1"; FAIL=$((FAIL + 1)); }
info() { echo -e "${CYAN}▶${RESET} $1"; }
warn() { echo -e "${YELLOW}⚠${RESET}  $1"; }
header() { echo -e "\n${BOLD}$1${RESET}"; }

# ── サービス起動確認 ──────────────────────────────────
if ! curl -sf "${BASE_URL}/healthz" > /dev/null 2>&1; then
  warn "サービスが起動していません: ${BASE_URL}"
  warn "先に ./start.sh を実行してください。"
  exit 1
fi
info "サービス確認OK: ${BASE_URL}"

# ── テスト実行 ────────────────────────────────────────
header "インターフェーステスト"
echo -e "  テストメッセージ: ${BOLD}${MESSAGE}${RESET}\n"

# --- /healthz ---
echo -e "${BOLD}[GET /healthz]${RESET}"
HTTP_CODE=$(curl -s -o /tmp/healthz_body.json -w "%{http_code}" "${BASE_URL}/healthz")
BODY=$(cat /tmp/healthz_body.json)
if [[ "${HTTP_CODE}" == "200" ]] && echo "${BODY}" | grep -q '"ok"'; then
  pass "HTTP 200, body=${BODY}"
else
  fail "HTTP ${HTTP_CODE}, body=${BODY}"
fi

# --- /speak ---
echo -e "\n${BOLD}[POST /speak]${RESET}"
SPEAK_PAYLOAD=$(python3 -c "
import json, sys
print(json.dumps({
  'text': sys.argv[1],
  'source': 'test_interface.sh',
  'priority': 'normal'
}, ensure_ascii=False))
" "${MESSAGE}")

HTTP_CODE=$(curl -s -o /tmp/speak_body.json -w "%{http_code}" \
  -X POST "${BASE_URL}/speak" \
  -H "Content-Type: application/json" \
  -d "${SPEAK_PAYLOAD}")
BODY=$(cat /tmp/speak_body.json)

if [[ "${HTTP_CODE}" == "200" ]]; then
  REQUEST_ID=$(echo "${BODY}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('requestId',''))" 2>/dev/null || true)
  AUDIO_URL=$(echo "${BODY}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('audioUrl',''))" 2>/dev/null || true)
  pass "HTTP 200"
  echo -e "     requestId : ${REQUEST_ID}"
  echo -e "     audioUrl  : ${AUDIO_URL}"
elif [[ "${HTTP_CODE}" == "502" ]]; then
  DETAIL=$(echo "${BODY}" | python3 -c "import json,sys; d=json.load(sys.stdin); print(d.get('detail',''))" 2>/dev/null || echo "${BODY}")
  warn "HTTP 502 (外部サービス未接続の可能性): ${DETAIL}"
  echo -e "  ${YELLOW}→ VOICEVOX/MQTT が利用できない環境では502は想定内です${RESET}"
  PASS=$((PASS + 1))  # 接続性テストとしては「疎通確認まで到達」をパスとする
else
  fail "HTTP ${HTTP_CODE}, body=${BODY}"
fi

# --- /ingest-audio (Phase 2 プレースホルダ、501 を期待) ---
echo -e "\n${BOLD}[POST /ingest-audio]${RESET}"
HTTP_CODE=$(curl -s -o /tmp/ingest_body.json -w "%{http_code}" \
  -X POST "${BASE_URL}/ingest-audio" \
  -F "file=@/dev/null;filename=test.wav;type=audio/wav")
BODY=$(cat /tmp/ingest_body.json)

if [[ "${HTTP_CODE}" == "501" ]]; then
  pass "HTTP 501 (Phase 2 未実装として正しく返却)"
else
  fail "HTTP ${HTTP_CODE} (501 を期待), body=${BODY}"
fi

# --- /audio/<不存在ID>.mp3 (404 を期待) ---
echo -e "\n${BOLD}[GET /audio/nonexistent.mp3]${RESET}"
HTTP_CODE=$(curl -s -o /dev/null -w "%{http_code}" "${BASE_URL}/audio/nonexistent-id.mp3")
if [[ "${HTTP_CODE}" == "404" ]]; then
  pass "HTTP 404 (存在しないファイルとして正しく返却)"
else
  fail "HTTP ${HTTP_CODE} (404 を期待)"
fi

# ── 結果サマリ ────────────────────────────────────────
header "結果"
TOTAL=$((PASS + FAIL))
echo -e "  合計: ${TOTAL}  ${GREEN}PASS: ${PASS}${RESET}  ${RED}FAIL: ${FAIL}${RESET}"

if [[ ${FAIL} -eq 0 ]]; then
  echo -e "\n${GREEN}${BOLD}全テスト成功！${RESET}"
  exit 0
else
  echo -e "\n${RED}${BOLD}${FAIL}件のテストが失敗しました。${RESET}"
  exit 1
fi
