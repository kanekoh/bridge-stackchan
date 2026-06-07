#!/bin/bash
# CRONジョブの失敗をSlackに通知するラッパースクリプト
# 使い方: cron_notify_failure.sh <ジョブ名> <コマンド> [引数...]
#
# crontab 例:
#   0 2 * * * /home/hiroyuki/bridge-stackchan/cron_notify_failure.sh "バックアップ" /home/hiroyuki/backup.sh

set -euo pipefail

ENV_FILE="$(dirname "$0")/.env"
SLACK_CHANNEL="#general"

if [ "$#" -lt 2 ]; then
    echo "Usage: $0 <job-name> <command> [args...]" >&2
    exit 1
fi

JOB_NAME="$1"
shift

# ジョブ実行（終了コードを保持するため set -e の外で実行）
set +e
"$@"
EXIT_CODE=$?
set -e

if [ "$EXIT_CODE" -ne 0 ]; then
    SLACK_BOT_TOKEN="$(grep '^SLACK_BOT_TOKEN=' "$ENV_FILE" | cut -d= -f2 | tr -d '[:space:]')"

    if [ -z "$SLACK_BOT_TOKEN" ]; then
        echo "ERROR: SLACK_BOT_TOKEN not found in $ENV_FILE" >&2
        exit "$EXIT_CODE"
    fi

    TIMESTAMP="$(date '+%Y-%m-%d %H:%M:%S')"

    curl -s -X POST https://slack.com/api/chat.postMessage \
      -H "Authorization: Bearer $SLACK_BOT_TOKEN" \
      -H "Content-Type: application/json" \
      -d "{
        \"channel\": \"$SLACK_CHANNEL\",
        \"text\": \":warning: *CRONジョブ失敗*\nジョブ名: ${JOB_NAME}\n終了コード: ${EXIT_CODE}\n時刻: ${TIMESTAMP}\"
      }" > /dev/null
fi

exit "$EXIT_CODE"
