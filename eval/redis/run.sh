#!/bin/bash
# Redis run script (paper-level multi-policy workflow)
#
# Usage examples:
#   bash eval/redis/run.sh
#   ITERATIONS=1 WORKLOADS=uniform bash eval/redis/run.sh
#   POLICIES_OVERRIDE=cache_ext_mru,cache_ext_fifo RESULTS_SUFFIX=quick bash eval/redis/run.sh
set -eu -o pipefail

if ! uname -r | grep -q "cache-ext"; then
	echo "This script is intended to be run on a cache_ext kernel."
	echo "Please switch to the cache_ext kernel and try again."
	exit 1
fi

SCRIPT_PATH=$(realpath $0)
BASE_DIR=$(realpath "$(dirname $SCRIPT_PATH)/../../")
BENCH_PATH="$BASE_DIR/bench"
POLICY_PATH="$BASE_DIR/policies"
YCSB_PATH="$BASE_DIR/My-YCSB"
RESULTS_PATH="$BASE_DIR/results"

ITERATIONS="${ITERATIONS:-3}"
CPU="${CPU:-8}"
WORKLOADS="${WORKLOADS:-uniform,zipfian}"
NR_OP="${NR_OP:-5000000}"
NR_WARMUP_OP="${NR_WARMUP_OP:-1000000}"
REDIS_SERVERS="${REDIS_SERVERS:-1}"
REDIS_PORT="${REDIS_PORT:-6380}"
RESULTS_SUFFIX="${RESULTS_SUFFIX:-}"

POLICIES=(
	"cache_ext_lhd"
	"cache_ext_s3fifo"
	"cache_ext_sampling"
	"cache_ext_fifo"
	"cache_ext_mru"
	"cache_ext_mglru"
)

if [[ -n "${POLICIES_OVERRIDE:-}" ]]; then
	IFS=',' read -r -a POLICIES <<< "$POLICIES_OVERRIDE"
fi

RESULTS_FILE="$RESULTS_PATH/redis_results.json"
RESULTS_FILE_MGLRU="$RESULTS_PATH/redis_results_mglru.json"
if [[ -n "$RESULTS_SUFFIX" ]]; then
	RESULTS_FILE="$RESULTS_PATH/redis_results_${RESULTS_SUFFIX}.json"
	RESULTS_FILE_MGLRU="$RESULTS_PATH/redis_results_mglru_${RESULTS_SUFFIX}.json"
fi

mkdir -p "$RESULTS_PATH"

# Build run_redis
cd "$YCSB_PATH/build"
make -j run_redis

cd -

cleanup() {
	if ! "$BASE_DIR/utils/disable-mglru.sh"; then
		echo "Warning: failed to disable MGLRU during cleanup"
	fi
}

trap cleanup EXIT

# Disable MGLRU for cache_ext baseline comparisons
if ! "$BASE_DIR/utils/disable-mglru.sh"; then
	echo "Failed to disable MGLRU. Please check the script."
	exit 1
fi

for POLICY in "${POLICIES[@]}"; do
	echo "Running Redis policies on cache_ext: ${POLICY}"
	python3 "$BENCH_PATH/bench_redis.py" \
		--cpu "$CPU" \
		--policy-loader "$POLICY_PATH/${POLICY}.out" \
		--results-file "$RESULTS_FILE" \
		--bench-binary-dir "$YCSB_PATH/build" \
		--benchmark "$WORKLOADS" \
		--iterations "$ITERATIONS" \
		--redis-servers "$REDIS_SERVERS" \
		--redis-port "$REDIS_PORT" \
		--nr-op "$NR_OP" \
		--nr-warmup-op "$NR_WARMUP_OP"
done

# Enable MGLRU and run default-only baseline
if ! "$BASE_DIR/utils/enable-mglru.sh"; then
	echo "Failed to enable MGLRU. Please check the script."
	exit 1
fi

echo "Running Redis baseline on kernel MGLRU"
python3 "$BENCH_PATH/bench_redis.py" \
	--cpu "$CPU" \
	--policy-loader "$POLICY_PATH/${POLICIES[0]}.out" \
	--results-file "$RESULTS_FILE_MGLRU" \
	--bench-binary-dir "$YCSB_PATH/build" \
	--benchmark "$WORKLOADS" \
	--iterations "$ITERATIONS" \
	--redis-servers "$REDIS_SERVERS" \
	--redis-port "$REDIS_PORT" \
	--nr-op "$NR_OP" \
	--nr-warmup-op "$NR_WARMUP_OP" \
	--default-only

if ! "$BASE_DIR/utils/disable-mglru.sh"; then
	echo "Failed to disable MGLRU. Please check the script."
	exit 1
fi

echo "Redis benchmark completed."
echo "Main results: $RESULTS_FILE"
echo "MGLRU results: $RESULTS_FILE_MGLRU"
