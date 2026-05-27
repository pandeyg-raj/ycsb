#!/bin/bash
# =============================================================================
# run_workloadC.sh
#
# Simplified flow:
#   ask: rep/ec, compression on/off, nodes, load or skip
#   if load=y:  hard_restart (stop + wipe + start) → YCSB load → wait compaction
#   if load=n:  verify cluster is UN, do nothing else
#   then:       reset breakdown → workload C (100% read) → collect breakdown
# =============================================================================

# ── Config ───────────────────────────────────────────────────────────────────
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql
MEASURE_OPS=10000000
FIELD_LENGTH=10000
RECORD_COUNT=7000000
SSH_USER=rzp5412
CASS_DIR=/mydata/cassandra
POOL_DIR=/mydata/compressData

# =============================================================================
# stop_cluster
# =============================================================================
stop_cluster() {
    echo "=== Stopping Cassandra on all nodes ==="
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node"
        echo "  Stopping ${ip}..."
        ssh ${SSH_USER}@${ip} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill 2>/dev/null; true"
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
        echo "  ${ip} stopped"
    done
}

# =============================================================================
# hard_restart_cluster — kill (parallel) → wipe (parallel) → start (sequential)
# =============================================================================
hard_restart_cluster() {
    echo "=== HARD restart: nodes ${BD_NODES[*]} ==="

    # Phase 1 — kill all in parallel
    echo "  [1/3] Killing all nodes in parallel..."
    for node in "${BD_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill 2>/dev/null; true" &
    done
    wait
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node"
        local attempts=0
        while ssh ${SSH_USER}@${ip} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' > /dev/null 2>&1"; do
            sleep 10; attempts=$((attempts + 1))
            if [ "$attempts" -ge 6 ]; then
                ssh ${SSH_USER}@${ip} \
                    "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill -9 2>/dev/null; true"
                sleep 5; break
            fi
        done
        echo "  ${ip} stopped"
    done

    # Phase 2 — wipe all in parallel
    echo "  [2/3] Wiping data on all nodes in parallel..."
    for node in "${BD_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} "rm -rf ${CASS_DIR}/data/" &
    done
    wait
    echo "  [2/3] All data wiped"

    # Phase 3 — start sequentially, seeds first
    echo "  [3/3] Starting nodes sequentially (seeds first)..."
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node"
        ssh ${SSH_USER}@${ip} "cd ${CASS_DIR} && bin/cassandra > /dev/null 2>&1"
        local attempts=0
        until ssh ${SSH_USER}@${ip} \
            "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep '${ip}' | grep -q 'UN'"; do
            sleep 10; attempts=$((attempts + 1))
            echo "  Waiting for ${ip} UN... (${attempts}/30)"
            if [ "$attempts" -ge 30 ]; then
                echo "  ERROR: ${ip} not UN after 5 min."; exit 1
            fi
        done
        echo "  ${ip} is UN"
    done

    echo "  Creating YCSB table via /mydata/${CREATE_TABLE_BIN}..."
    /mydata/${CREATE_TABLE_BIN}
    echo "=== HARD restart complete. ==="
}

# =============================================================================
# verify_cluster_up — used when skipping load
# =============================================================================
verify_cluster_up() {
    echo "=== Verifying cluster is UN (load skipped) ==="
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node"
        if ! ssh ${SSH_USER}@${ip} \
            "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep '${ip}' | grep -q 'UN'"; then
            echo "ERROR: ${ip} is not UN. Start the cluster or re-run with load=y."
            exit 1
        fi
        echo "  ${ip} is UN"
    done
    echo "=== Cluster ready ==="
}

# =============================================================================
# Interactive setup
# =============================================================================
read -p "EC or REP? " EXP_LABEL
read -p "Compression on or off? " COMPRESSION
read -p "How many Cassandra nodes? (3 or 5): " NUM_NODES
if [ "$NUM_NODES" = "3" ]; then
    BD_NODES=(2 3 4)
elif [ "$NUM_NODES" = "5" ]; then
    BD_NODES=(2 3 4 5 6)
else
    echo "ERROR: must be 3 or 5"; exit 1
fi
read -p "Load fresh data? (y/n): " DO_LOAD
read -p "Read threads (for workload C): " THREADS

# Determine create-table binary
if echo "$EXP_LABEL" | grep -qi "rep"; then
    CREATE_TABLE_BIN="create_table_rep_compr_${COMPRESSION}"
else
    CREATE_TABLE_BIN="create_table_ec_compr_${COMPRESSION}"
fi

if [ ! -x "/mydata/${CREATE_TABLE_BIN}" ]; then
    echo "ERROR: /mydata/${CREATE_TABLE_BIN} missing or not executable"
    exit 1
fi

# Output dir
OUT_DIR="result_${EXP_LABEL}_compr_${COMPRESSION}"
mkdir -p "$OUT_DIR"
LOG="${OUT_DIR}/run.log"
BREAKDOWN_FILE="${OUT_DIR}/breakdown.txt"
touch "$BREAKDOWN_FILE"

# =============================================================================
# Load (only if requested)
# =============================================================================
if [ "$DO_LOAD" = "y" ] || [ "$DO_LOAD" = "Y" ]; then
    read -p "Dataset for load (jpeg/wiki/hdfs): " DATASET
    read -p "Write threads (for load): " WTHREADS
    POOL_FILE="${POOL_DIR}/values_pool_${DATASET}.txt"

    if [ ! -f "$POOL_FILE" ]; then
        echo "ERROR: pool file ${POOL_FILE} not found"
        exit 1
    fi

    hard_restart_cluster

    LOAD_FILE="${OUT_DIR}/load.scr"
    echo "--- Loading ${RECORD_COUNT} records x ${FIELD_LENGTH}B from ${DATASET} pool ---"
    $YCSB_DIR load $DB -threads $WTHREADS \
        -p recordcount=${RECORD_COUNT} \
        -p fieldlength=${FIELD_LENGTH} \
        -p valuepool.file=${POOL_FILE} \
        -p measurement.raw.output_file="$LOAD_FILE" \
        -P commonworkload \
        -s >> "$LOG" 2>&1
    echo "--- Load done ---"

    # Wait for post-load compaction to settle
    echo "--- Waiting for compaction to settle on all nodes ---"
    for node in "${BD_NODES[@]}"; do
        ip="10.10.1.$node"
        echo "  Waiting on ${ip}..."
        while ssh ${SSH_USER}@${ip} \
            "${CASS_DIR}/bin/nodetool compactionstats 2>/dev/null | grep -q 'pending tasks: [^0]'"; do
            sleep 30
            echo "  Compaction still running on ${ip}..."
        done
        echo "  ${ip} compaction settled"
    done
    echo "--- Compaction settled ---"
else
    verify_cluster_up
fi

# =============================================================================
# Workload C — 100% read
# =============================================================================
MEASURE_FILE="${OUT_DIR}/workloadC.scr"
echo ""
echo "=== Workload C: 100% read, ${MEASURE_OPS} ops, ${THREADS} threads ==="

# Reset breakdown counters
echo "--- Resetting breakdown counters ---"
for node in "${BD_NODES[@]}"; do
    ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool breakdown --reset"
done

$YCSB_DIR run $DB -threads $THREADS \
    -p operationcount=$MEASURE_OPS \
    -p readproportion=1.0 -p updateproportion=0.0 -p insertproportion=0.0 \
    -p recordcount=${RECORD_COUNT} \
    -p measurement.raw.output_file="$MEASURE_FILE" \
    -p cassandra.writeconsistencylevel=QUORUM \
    -p cassandra.readconsistencylevel=QUORUM \
    -P commonworkload \
    -s >> "$LOG" 2>&1

echo "=== Workload C done ==="

# Collect breakdown
echo "run for ${EXP_LABEL} compr=${COMPRESSION} workloadC" >> "$BREAKDOWN_FILE"
for node in "${BD_NODES[@]}"; do
    echo "-- node 10.10.1.$node --" >> "$BREAKDOWN_FILE"
    ssh ${SSH_USER}@10.10.1.$node \
        "${CASS_DIR}/bin/nodetool breakdown | grep -E 'keyspace|ycsb'" >> "$BREAKDOWN_FILE"
done

echo ""
echo "Experiment complete. Results in ${OUT_DIR}/"
