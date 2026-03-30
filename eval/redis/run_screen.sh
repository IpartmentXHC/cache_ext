#!/bin/bash
# Run Redis eval script in a detached screen session.
set -eu -o pipefail

SCRIPT_PATH=$(realpath $0)
BASE_DIR=$(realpath "$(dirname $SCRIPT_PATH)/../../")
RUN_SCRIPT="$BASE_DIR/eval/redis/run.sh"
RESULTS_PATH="$BASE_DIR/results"

SESSION_NAME="${SESSION_NAME:-redis_eval}"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
LOG_FILE="${LOG_FILE:-$RESULTS_PATH/redis_eval_${TIMESTAMP}.log}"

if ! command -v screen >/dev/null 2>&1; then
	echo "screen is not installed. Install it first: sudo apt install screen"
	exit 1
fi

if [[ ! -x "$RUN_SCRIPT" ]]; then
	echo "Run script not found or not executable: $RUN_SCRIPT"
	exit 1
fi

mkdir -p "$RESULTS_PATH"

if screen -list | grep -q "[.]${SESSION_NAME}[[:space:]]"; then
	echo "A screen session named '${SESSION_NAME}' is already running."
	echo "Use another name: SESSION_NAME=redis_eval_2 bash eval/redis/run_screen.sh"
	exit 1
fi

ENV_ARGS=()
for var in ITERATIONS CPU WORKLOADS NR_OP NR_WARMUP_OP REDIS_SERVERS REDIS_PORT RESULTS_SUFFIX POLICIES_OVERRIDE; do
	if [[ -n "${!var:-}" ]]; then
		ENV_ARGS+=("${var}=${!var}")
	fi
done

screen -dmS "$SESSION_NAME" env "${ENV_ARGS[@]}" bash -lc "cd '$BASE_DIR' && bash '$RUN_SCRIPT' > '$LOG_FILE' 2>&1"

echo "Started detached screen session: $SESSION_NAME"
echo "Log file: $LOG_FILE"
echo "Check status: screen -ls"
echo "Attach: screen -r $SESSION_NAME"
echo "Detach after attach: Ctrl+A then D"
