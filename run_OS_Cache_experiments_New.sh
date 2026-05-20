#!/bin/bash
# =============================================================================
# run_OS_Cache_experiments_New.sh
#
# Flow per object size:
#   hard_restart (wipe + full memory) → YCSB load
#   for each cache_size:
#       restart_cluster(cache_size) → ONE warmup → all workloads back-to-back
#   next object size → hard_restart again
# =============================================================================

# ── Config ────────────────────────────────────────────────────────────────────
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql
MEASURE_OPS=10000000
WARMUP_OPS=3000000

# Total target database size (~100 GB). RECORD_COUNT = TOTAL_DB_BYTES / FIELD_LENGTH
TOTAL_DB_BYTES=100000000000

# Workloads — C (100% read) first to keep cache stable, then A (50/50)
WORKLOAD_LABELS=("workloadC" "workloadA")
READ_PROPORTIONS=(
    "readproportion=1.0  -p updateproportion=0.0  -p insertproportion=0"   # C: 100% read
    "readproportion=0.5  -p updateproportion=0.5  -p insertproportion=0"   # A: 50/50
)

CACHE_SIZES=("16GB" "28GB" "40GB" "52GB" "64GB")

# Object sizes — biggest first so disk/compaction issues surface early
OBJECT_SIZE_LABELS=("100KB" "10KB" "1KB")
FIELD_LENGTHS=(50000 10000 1000)

SSH_USER=rzp5412
CASS_DIR=/mydata/cassandra

# =============================================================================
# log_banner
# =============================================================================
log_banner() {
    local log=$1 label=$2 objsize=$3 cache=$4 workload=$5 outfile=$6
    {
        echo ""
        echo "########################################################"
        echo "# LABEL    : ${label}"
        echo "# OBJ SIZE : ${objsize}"
        echo "# CACHE    : ${cache}"
        echo "# WORKLOAD : ${workload}"
        echo "# OUTPUT   : $(basename ${outfile})"
        echo "# TIME     : $(date)"
        echo "########################################################"
    } >> "$log"
}

# =============================================================================
# stop_cluster — kill Cassandra on all nodes, wait for clean stop
# =============================================================================
stop_cluster() {
    local nodes=("${NODE_LIST[@]}")
    echo ""
    echo "=== Stopping Cassandra on all nodes ==="
    for node in "${nodes[@]}"; do
        local ip="10.10.1.$node"
        ssh ${SSH_USER}@${ip} \
            "ps -ef | grep '[j]ava' | grep -i cassandra | awk '{print \$2}' | xargs kill 2>/dev/null; true"
        local attempts=0
        while ssh ${SSH_USER}@${ip} \
            "ps -ef | grep '[j]ava' | grep -i cassandra > /dev/null 2>&1"; do
            sleep 10
            attempts=$((attempts+1))
            if [ "$attempts" -ge 6 ]; then
                ssh ${SSH_USER}@${ip} \
                    "ps -ef | grep '[j]ava' | grep -i cassandra | awk '{print \$2}' | xargs kill -9 2>/dev/null; true"
                sleep 5; break
            fi
        done
        echo "  ${ip} stopped"
    done
    echo "=== All nodes stopped ==="
}

# =============================================================================
# restart_cluster <cache_size>
#   Soft restart — keeps data, applies cgroup memory limit, evicts page cache.
#   One node at a time, seeds first.
# =============================================================================
restart_cluster() {
    local cache_size=$1
    local cache_gb="${cache_size//GB/}"
    local mem_bytes=$((cache_gb * 1024 * 1024 * 1024))

    echo ""
    echo "=== Soft restart: cache=${cache_size} (${mem_bytes} bytes) ==="
    for node in "${NODE_LIST[@]}"; do
        local ip="10.10.1.$node"
        echo ""
        echo "  --- ${ip} ---"

        # Stop
        ssh ${SSH_USER}@${ip} \
            "ps -ef | grep '[j]ava' | grep -i cassandra | awk '{print \$2}' | xargs kill 2>/dev/null; true"
        local attempts=0
        while ssh ${SSH_USER}@${ip} \
            "ps -ef | grep '[j]ava' | grep -i cassandra > /dev/null 2>&1"; do
            sleep 10; attempts=$((attempts+1))
            if [ "$attempts" -ge 6 ]; then
                ssh ${SSH_USER}@${ip} \
                    "ps -ef | grep '[j]ava' | grep -i cassandra | awk '{print \$2}' | xargs kill -9 2>/dev/null; true"
                sleep 5; break
            fi
        done

        # Set cgroup limit, evict page cache, start Cassandra
        ssh ${SSH_USER}@${ip} \
            "cd ${CASS_DIR} && \
             echo ${mem_bytes} | sudo tee /sys/fs/cgroup/mylimitedgroup/memory.max && \
             echo \$\$ | sudo tee /sys/fs/cgroup/mylimitedgroup/cgroup.procs && \
             vmtouch -e data/ > /dev/null 2>&1 && \
             bin/cassandra > /dev/null 2>&1"

        # Wait for UN
        local start_attempts=0
        until ssh ${SSH_USER}@${ip} \
            "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep '${ip}' | grep -q 'UN'"; do
            sleep 10; start_attempts=$((start_attempts+1))
            echo "    Waiting for UN... (${start_attempts}/30)"
            if [ "$start_attempts" -ge 30 ]; then
                echo "  ERROR: ${ip} not UN after 5 min."
                exit 1
            fi
        done
        echo "  ${ip} is UN"
    done
    echo "=== Soft restart complete. Cluster ready with ${cache_size} cache. ==="
}

# =============================================================================
# hard_restart_cluster
#   Phase 1: kill ALL nodes in parallel
#   Phase 2: wipe ALL data in parallel
#   Phase 3: start nodes sequentially (seeds first), create YCSB table
#   Fix: all must be dead before any node starts to avoid UUID gossip collision
# =============================================================================
hard_restart_cluster() {
    echo ""
    echo "=== HARD restart: wipe + full memory ==="

    # Phase 1 — kill all in parallel
    echo "  [1/3] Killing all nodes..."
    for node in "${NODE_LIST[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} \
            "ps -ef | grep '[j]ava' | grep -i cassandra | awk '{print \$2}' | xargs kill 2>/dev/null; true" &
    done
    wait
    for node in "${NODE_LIST[@]}"; do
        local ip="10.10.1.$node"
        local attempts=0
        while ssh ${SSH_USER}@${ip} \
            "ps -ef | grep '[j]ava' | grep -i cassandra > /dev/null 2>&1"; do
            sleep 10; attempts=$((attempts+1))
            if [ "$attempts" -ge 6 ]; then
                ssh ${SSH_USER}@${ip} \
                    "ps -ef | grep '[j]ava' | grep -i cassandra | awk '{print \$2}' | xargs kill -9 2>/dev/null; true"
                sleep 5; break
            fi
        done
        echo "  ${ip} stopped"
    done

    # Phase 2 — wipe all in parallel
    echo "  [2/3] Wiping data on all nodes..."
    for node in "${NODE_LIST[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} "rm -rf ${CASS_DIR}/data/" &
    done
    wait
    echo "  [2/3] All data wiped"

    # Phase 3 — start sequentially, seeds first
    echo "  [3/3] Starting nodes sequentially (seeds first)..."
    for node in "${NODE_LIST[@]}"; do
        local ip="10.10.1.$node"
        ssh ${SSH_USER}@${ip} \
            "cd ${CASS_DIR} && bin/cassandra > /dev/null 2>&1"
        local attempts=0
        until ssh ${SSH_USER}@${ip} \
            "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep '${ip}' | grep -q 'UN'"; do
            sleep 10; attempts=$((attempts+1))
            echo "    Waiting for ${ip} UN... (${attempts}/30)"
            if [ "$attempts" -ge 30 ]; then
                echo "  ERROR: ${ip} not UN after 5 min."
                exit 1
            fi
        done
        echo "  ${ip} is UN"
    done

    echo "  Creating YCSB table via /mydata/${CREATE_TABLE_BIN}..."
    /mydata/${CREATE_TABLE_BIN}
    echo "  Table created"
    echo "=== HARD restart complete. ==="
}

# =============================================================================
# Pre-flight checks
# =============================================================================
echo "Checking create-table binaries..."
for bin in create_table_ec_compr_on create_table_ec_compr_off \
           create_table_rep_compr_on create_table_rep_compr_off; do
    if [ ! -x "/mydata/${bin}" ]; then
        echo "ERROR: /mydata/${bin} missing or not executable. Aborting."
        exit 1
    fi
done
echo "All binaries OK."
echo ""

# =============================================================================
# Interactive setup
# =============================================================================
read -p "Is this EC or REP? " EXP_LABEL

read -p "How many Cassandra nodes? (3 or 5): " NUM_NODES
if [ "$NUM_NODES" = "3" ]; then
    NODE_LIST=(2 3 4)
elif [ "$NUM_NODES" = "5" ]; then
    NODE_LIST=(2 3 4 5 6)
else
    echo "ERROR: must be 3 or 5"; exit 1
fi
echo "Nodes: ${NODE_LIST[*]}"

read -p "Compression ON or OFF? (on/off): " COMPRESSION
if echo "$EXP_LABEL" | grep -qi "rep"; then
    CREATE_TABLE_BIN="create_table_rep_compr_$(echo $COMPRESSION | tr '[:upper:]' '[:lower:]')"
else
    CREATE_TABLE_BIN="create_table_ec_compr_$(echo $COMPRESSION | tr '[:upper:]' '[:lower:]')"
fi
echo "Using: /mydata/${CREATE_TABLE_BIN}"

read -p "Write threads (for YCSB load): " WTHREADS
read -p "Read threads (for YCSB run):   " THREADS

# =============================================================================
# MAIN EXPERIMENT LOOP
#
# For each object size:
#   hard_restart → load
#   for each cache_size:
#     restart_cluster → ONE warmup → all workloads (no restart between them)
# =============================================================================
for size_idx in "${!OBJECT_SIZE_LABELS[@]}"; do
    OBJECT_SIZE_LABEL="${OBJECT_SIZE_LABELS[$size_idx]}"
    FIELD_LENGTH="${FIELD_LENGTHS[$size_idx]}"
    RECORD_COUNT=$((TOTAL_DB_BYTES / FIELD_LENGTH))

    echo ""
    echo "============================================================"
    echo ">>> Object size  : ${OBJECT_SIZE_LABEL} (${FIELD_LENGTH} bytes)"
    echo ">>> Record count : ${RECORD_COUNT}"
    echo ">>> Total size   : ~$((TOTAL_DB_BYTES / 1000000000)) GB"
    echo "============================================================"

    BASE_OUT_DIR="result_OS_CacheObjectSize_${OBJECT_SIZE_LABEL}"
    mkdir -p "$BASE_OUT_DIR"
    LOG="${BASE_OUT_DIR}/${EXP_LABEL}_${OBJECT_SIZE_LABEL}_run${FIELD_LENGTH}Bytes.log"

    # ── Hard restart and load data ────────────────────────────────────
    hard_restart_cluster

    LOAD_FILE="${BASE_OUT_DIR}/${EXP_LABEL}_${OBJECT_SIZE_LABEL}_Load${FIELD_LENGTH}Bytes.scr"
    log_banner "$LOG" "$EXP_LABEL" "$OBJECT_SIZE_LABEL" "FULL_MEM" "LOAD" "$LOAD_FILE"
    echo "--- Loading ${RECORD_COUNT} records × ${FIELD_LENGTH} B ---"
    $YCSB_DIR load $DB -threads $WTHREADS \
        -p recordcount=${RECORD_COUNT} \
        -p fieldlength=${FIELD_LENGTH} \
        -p measurement.raw.output_file="$LOAD_FILE" \
        -P commonworkload \
        -s >> "$LOG" 2>&1
    echo "--- Load done ---"

    # Wait for post-load compaction to settle before starting measurements
    echo "--- Waiting for compaction to settle on all nodes ---"
    for node in "${NODE_LIST[@]}"; do
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

    # ── Cache size loop ───────────────────────────────────────────────
    for cache_size in "${CACHE_SIZES[@]}"; do
        echo ""
        echo ">>> Cache: ${cache_size}  |  Object size: ${OBJECT_SIZE_LABEL}"

        CACHE_OUT_DIR="${BASE_OUT_DIR}_${cache_size}"
        mkdir -p "$CACHE_OUT_DIR"

        # Soft restart with this cache size (evicts page cache, applies cgroup)
        restart_cluster "$cache_size"

        # ONE warmup per cache size — 100% read, populates OS cache
        WARMUP_FILE="${CACHE_OUT_DIR}/${EXP_LABEL}_${OBJECT_SIZE_LABEL}_${cache_size}_Warmup.scr"
        log_banner "$LOG" "$EXP_LABEL" "$OBJECT_SIZE_LABEL" "$cache_size" "WARMUP" "$WARMUP_FILE"
        echo "--- Warmup (${cache_size}, 100% read) ---"
        $YCSB_DIR run $DB -threads $THREADS \
            -p operationcount=$WARMUP_OPS \
            -p readproportion=1.0 -p updateproportion=0.0 -p insertproportion=0.0 \
            -p recordcount=${RECORD_COUNT} \
            -p measurement.raw.output_file="$WARMUP_FILE" \
            -p cassandra.writeconsistencylevel=QUORUM \
            -p cassandra.readconsistencylevel=QUORUM \
            -P commonworkload \
            -s >> "$LOG" 2>&1
        echo "--- Warmup done ---"

        # All workloads back-to-back — NO restart, NO extra warmup between them
        for i in "${!WORKLOAD_LABELS[@]}"; do
            workload="${WORKLOAD_LABELS[$i]}"
            READ_PCT="${READ_PROPORTIONS[$i]}"

            MEASURE_FILE="${CACHE_OUT_DIR}/${EXP_LABEL}_${OBJECT_SIZE_LABEL}_${cache_size}_${workload}Run${FIELD_LENGTH}Bytes.scr"
            log_banner "$LOG" "$EXP_LABEL" "$OBJECT_SIZE_LABEL" "$cache_size" "$workload" "$MEASURE_FILE"
            echo "=== ${workload} | ${cache_size} | ${OBJECT_SIZE_LABEL} ==="
            $YCSB_DIR run $DB -threads $THREADS \
                -p operationcount=$MEASURE_OPS \
                -p ${READ_PCT} \
                -p recordcount=${RECORD_COUNT} \
                -p measurement.raw.output_file="$MEASURE_FILE" \
                -p cassandra.writeconsistencylevel=QUORUM \
                -p cassandra.readconsistencylevel=QUORUM \
                -P commonworkload \
                -s >> "$LOG" 2>&1
            echo "=== Done: ${workload} | ${cache_size} | ${OBJECT_SIZE_LABEL} ==="
        done

        echo ">>> All workloads done for cache=${cache_size}, objsize=${OBJECT_SIZE_LABEL}"
    done

    echo ""
    echo ">>> Completed all cache sizes for ${OBJECT_SIZE_LABEL}"
done

echo ""
echo "All experiments completed successfully."
