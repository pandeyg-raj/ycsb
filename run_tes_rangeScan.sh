#!/bin/bash

# ============================================================
# Workload E — Quick Test Script (JPEG, no warmup, full memory)
#
# Asks once: hard reset + load, or skip straight to scan.
# No cache size sweep. No warmup. Normal Cassandra startup.
# ============================================================

# === Config ===
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql
MEASURE_OPS=10000000
FIELD_LENGTH=10000
RECORD_COUNT=7000000

MIN_SCAN_LENGTH=1
MAX_SCAN_LENGTH=100

POOL_FILE=/mydata/compressData/values_pool_jpeg.txt

SSH_USER=rzp5412
CASS_DIR=/mydata/cassandra

# =====================================================================
# hard_restart_cluster — kill all, wipe all, start sequentially
# =====================================================================
hard_restart_cluster() {
    local nodes
    if [ "$NUM_NODES" = "3" ]; then nodes=(2 3 4); else nodes=(2 3 4 5 6); fi

    echo ""
    echo "=== HARD restart: nodes ${nodes[*]} ==="

    echo "  [1/3] Killing all nodes in parallel..."
    for node in "${nodes[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill 2>/dev/null; true" &
    done
    wait

    for node in "${nodes[@]}"; do
        local ip="10.10.1.$node"
        local attempts=0
        while ssh ${SSH_USER}@${ip} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' > /dev/null 2>&1"; do
            sleep 10
            attempts=$((attempts + 1))
            if [ "$attempts" -ge 6 ]; then
                ssh ${SSH_USER}@${ip} \
                    "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill -9 2>/dev/null; true"
                sleep 5; break
            fi
        done
        echo "    ${ip} stopped"
    done

    echo "  [2/3] Wiping data on all nodes in parallel..."
    for node in "${nodes[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} "rm -rf ${CASS_DIR}/data/" &
    done
    wait
    echo "  [2/3] All nodes wiped"

    echo "  [3/3] Starting nodes sequentially (seeds first)..."
    for node in "${nodes[@]}"; do
        local ip="10.10.1.$node"
        echo "    Starting ${ip}..."
        ssh ${SSH_USER}@${ip} \
            "cd ${CASS_DIR} && bin/cassandra > /dev/null 2>&1"

        local start_attempts=0
        until ssh ${SSH_USER}@${ip} \
            "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep '${ip}' | grep -q 'UN'"; do
            sleep 10
            start_attempts=$((start_attempts + 1))
            echo "    Waiting for ${ip} UN... (${start_attempts}/30)"
            if [ "$start_attempts" -ge 30 ]; then
                echo "    ERROR: ${ip} not UN after 5 min."
                exit 1
            fi
        done
        echo "    ${ip} is UN"
    done

    echo "  Creating YCSB table via /mydata/${CREATE_TABLE_BIN} ..."
    /mydata/${CREATE_TABLE_BIN}
    echo "  Table created"
    echo "=== HARD restart complete ==="
}

# =====================================================================
# Setup — asked once
# =====================================================================
echo "Is this EC or REP?"
read EXP_LABEL

read -p "How many Cassandra nodes? (3 or 5): " NUM_NODES

echo "Is compression ON or OFF? (on/off)"
read COMPRESSION
if echo "$EXP_LABEL" | grep -qi "rep"; then
    if echo "$COMPRESSION" | grep -qi "on"; then
        CREATE_TABLE_BIN="create_table_rep_compr_on"
    else
        CREATE_TABLE_BIN="create_table_rep_compr_off"
    fi
else
    if echo "$COMPRESSION" | grep -qi "on"; then
        CREATE_TABLE_BIN="create_table_ec_compr_on"
    else
        CREATE_TABLE_BIN="create_table_ec_compr_off"
    fi
fi
echo "Using /mydata/${CREATE_TABLE_BIN}"

echo "How many load/write threads?"
read WTHREADS

echo "How many scan threads? (recommend 4-8)"
read SCAN_THREADS

OUT_DIR="result_test_workloadE_jpeg"
mkdir -p "$OUT_DIR"
LOG="${OUT_DIR}/${EXP_LABEL}_jpeg_scanE_test.log"

# =====================================================================
# Hard reset + load — optional
# =====================================================================
read -p "Hard reset cluster and load data? (y/n): " DO_RESET

if echo "$DO_RESET" | grep -qi "y"; then
    hard_restart_cluster

    LOAD_FILE="${OUT_DIR}/${EXP_LABEL}_jpeg_Load_test.scr"
    echo ""
    echo "=== Loading ${RECORD_COUNT} records x ${FIELD_LENGTH}B from JPEG pool ==="
    $YCSB_DIR load $DB -threads $WTHREADS \
        -p recordcount=${RECORD_COUNT} \
        -p fieldlength=${FIELD_LENGTH} \
        -p valuepool.file=${POOL_FILE} \
        -p measurement.raw.output_file="$LOAD_FILE" \
        -P commonworkload \
        -s >> "$LOG" 2>&1
    echo "=== Load done ==="
else
    echo "Skipping reset and load — using existing data."
fi

# =====================================================================
# Workload E — no warmup, straight to scan
# =====================================================================
MEASURE_FILE="${OUT_DIR}/${EXP_LABEL}_jpeg_workloadE_test.scr"

echo ""
echo "=== Running Workload E (${MEASURE_OPS} ops, scans ${MIN_SCAN_LENGTH}-${MAX_SCAN_LENGTH} records) ==="
echo "    Log: ${LOG}"

$YCSB_DIR run $DB -threads $SCAN_THREADS \
    -p operationcount=$MEASURE_OPS \
    -p scanproportion=0.95 \
    -p insertproportion=0.05 \
    -p readproportion=0 \
    -p updateproportion=0 \
    -p requestdistribution=uniform \
    -p minscanlength=${MIN_SCAN_LENGTH} \
    -p maxscanlength=${MAX_SCAN_LENGTH} \
    -p recordcount=${RECORD_COUNT} \
    -p fieldlength=${FIELD_LENGTH} \
    -p valuepool.file=${POOL_FILE} \
    -p measurement.raw.output_file="$MEASURE_FILE" \
    -p cassandra.writeconsistencylevel=QUORUM \
    -p cassandra.readconsistencylevel=QUORUM \
    -P commonworkload \
    -s >> "$LOG" 2>&1

echo "=== Workload E done ==="
echo "Results in: ${LOG}"
echo "Raw ops in: ${MEASURE_FILE}"
