#!/bin/bash
# MySQL + YCSB-JDBC run script for cache_ext policies.
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
YCSB_JDBC_PATH="$BASE_DIR/ycsb-jdbc"
RESULTS_PATH="$BASE_DIR/results"

ITERATIONS="${ITERATIONS:-3}"
CPU="${CPU:-8}"
WORKLOADS="${WORKLOADS:-workloada,workloadb,workloadc,workloadd,workloade,workloadf}"
THREADS="${THREADS:-10}"
RECORDCOUNT="${RECORDCOUNT:-100000}"
OPERATIONCOUNT="${OPERATIONCOUNT:-100000}"
TARGET="${TARGET:-0}"
PLOT="${PLOT:-0}"

MYSQL_HOST="${MYSQL_HOST:-127.0.0.1}"
MYSQL_PORT="${MYSQL_PORT:-3307}"
MYSQL_DATA_DIR="${MYSQL_DATA_DIR:-/var/lib/mysql/cacheext}"
MYSQL_RUNTIME_DIR="${MYSQL_RUNTIME_DIR:-/tmp/cache_ext_mysql_runtime}"
MYSQLD_RUN_USER="${MYSQLD_RUN_USER:-mysql}"
MYSQL_DB="${MYSQL_DB:-ycsb}"
MYSQL_TABLE="${MYSQL_TABLE:-usertable}"
MYSQL_ADMIN_USER="${MYSQL_ADMIN_USER:-root}"
MYSQL_ADMIN_PASSWD="${MYSQL_ADMIN_PASSWD:-}"
MYSQL_USER="${MYSQL_USER:-ycsb}"
MYSQL_PASSWD="${MYSQL_PASSWD:-ycsb-pass}"

JDBC_DRIVER_JAR="${JDBC_DRIVER_JAR:-$YCSB_JDBC_PATH/lib/mysql-connector-java-8.0.28.jar}"
JDBC_PROPS_TEMPLATE="${JDBC_PROPS_TEMPLATE:-$YCSB_JDBC_PATH/conf/mysql_cacheext.properties.sample}"
RESULTS_SUFFIX="${RESULTS_SUFFIX:-}"
PLOT_OUTPUT_DIR="${PLOT_OUTPUT_DIR:-$BASE_DIR/figures}"

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

RESULTS_FILE="$RESULTS_PATH/mysql_ycsb_results.json"
RESULTS_FILE_MGLRU="$RESULTS_PATH/mysql_ycsb_results_mglru.json"
if [[ -n "$RESULTS_SUFFIX" ]]; then
    RESULTS_FILE="$RESULTS_PATH/mysql_ycsb_results_${RESULTS_SUFFIX}.json"
    RESULTS_FILE_MGLRU="$RESULTS_PATH/mysql_ycsb_results_mglru_${RESULTS_SUFFIX}.json"
fi

mkdir -p "$RESULTS_PATH"

echo "MySQL YCSB config:"
echo "  workloads: $WORKLOADS"
echo "  policies: ${POLICIES[*]}"
echo "  iterations: $ITERATIONS"
echo "  cpu: $CPU"
echo "  plot: $PLOT"

if [[ ! -f "$JDBC_DRIVER_JAR" ]]; then
    echo "JDBC driver jar not found: $JDBC_DRIVER_JAR"
    exit 1
fi

if [[ ! -f "$JDBC_PROPS_TEMPLATE" ]]; then
    echo "JDBC properties template not found: $JDBC_PROPS_TEMPLATE"
    exit 1
fi

cleanup() {
    if ! "$BASE_DIR/utils/disable-mglru.sh"; then
        echo "Warning: failed to disable MGLRU during cleanup"
    fi
}

trap cleanup EXIT

if ! "$BASE_DIR/utils/disable-mglru.sh"; then
    echo "Failed to disable MGLRU."
    exit 1
fi

for POLICY in "${POLICIES[@]}"; do
    echo "Running MySQL YCSB on cache_ext policy: ${POLICY}"
    python3 "$BENCH_PATH/bench_mysql_jdbc.py" \
        --cpu "$CPU" \
        --policy-loader "$POLICY_PATH/${POLICY}.out" \
        --results-file "$RESULTS_FILE" \
        --iterations "$ITERATIONS" \
        --ycsb-root "$YCSB_JDBC_PATH" \
        --benchmark "$WORKLOADS" \
        --jdbc-driver-jar "$JDBC_DRIVER_JAR" \
        --jdbc-props-template "$JDBC_PROPS_TEMPLATE" \
        --threads "$THREADS" \
        --recordcount "$RECORDCOUNT" \
        --operationcount "$OPERATIONCOUNT" \
        --target "$TARGET" \
        --mysql-host "$MYSQL_HOST" \
        --mysql-port "$MYSQL_PORT" \
        --mysql-data-dir "$MYSQL_DATA_DIR" \
        --mysql-runtime-dir "$MYSQL_RUNTIME_DIR" \
        --mysqld-run-user "$MYSQLD_RUN_USER" \
        --mysql-db "$MYSQL_DB" \
        --mysql-table "$MYSQL_TABLE" \
        --mysql-admin-user "$MYSQL_ADMIN_USER" \
        --mysql-admin-passwd "$MYSQL_ADMIN_PASSWD" \
        --mysql-user "$MYSQL_USER" \
        --mysql-passwd "$MYSQL_PASSWD"
done

if ! "$BASE_DIR/utils/enable-mglru.sh"; then
    echo "Failed to enable MGLRU."
    exit 1
fi

echo "Running MySQL baseline on kernel MGLRU"
python3 "$BENCH_PATH/bench_mysql_jdbc.py" \
    --cpu "$CPU" \
    --policy-loader "$POLICY_PATH/${POLICIES[0]}.out" \
    --results-file "$RESULTS_FILE_MGLRU" \
    --iterations "$ITERATIONS" \
    --default-only \
    --ycsb-root "$YCSB_JDBC_PATH" \
    --benchmark "$WORKLOADS" \
    --jdbc-driver-jar "$JDBC_DRIVER_JAR" \
    --jdbc-props-template "$JDBC_PROPS_TEMPLATE" \
    --threads "$THREADS" \
    --recordcount "$RECORDCOUNT" \
    --operationcount "$OPERATIONCOUNT" \
    --target "$TARGET" \
    --mysql-host "$MYSQL_HOST" \
    --mysql-port "$MYSQL_PORT" \
    --mysql-data-dir "$MYSQL_DATA_DIR" \
    --mysql-runtime-dir "$MYSQL_RUNTIME_DIR" \
    --mysqld-run-user "$MYSQLD_RUN_USER" \
    --mysql-db "$MYSQL_DB" \
    --mysql-table "$MYSQL_TABLE" \
    --mysql-admin-user "$MYSQL_ADMIN_USER" \
    --mysql-admin-passwd "$MYSQL_ADMIN_PASSWD" \
    --mysql-user "$MYSQL_USER" \
    --mysql-passwd "$MYSQL_PASSWD"

if ! "$BASE_DIR/utils/disable-mglru.sh"; then
    echo "Failed to disable MGLRU."
    exit 1
fi

echo "MySQL YCSB benchmark completed."
echo "Main results: $RESULTS_FILE"
echo "MGLRU results: $RESULTS_FILE_MGLRU"

if [[ "$PLOT" == "1" ]]; then
    PLOT_PREFIX="mysql"
    if [[ -n "$RESULTS_SUFFIX" ]]; then
        PLOT_PREFIX="mysql_${RESULTS_SUFFIX}"
    fi

    echo "Generating MySQL figures..."
    python3 "$BENCH_PATH/bench_plot_mysql.py" \
        --results-file "$RESULTS_FILE" \
        --mglru-results-file "$RESULTS_FILE_MGLRU" \
        --output-dir "$PLOT_OUTPUT_DIR" \
        --prefix "$PLOT_PREFIX" \
        --benchmarks "$WORKLOADS"
fi
