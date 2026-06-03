#!/bin/bash
# =============================================================================
# run_coding_params.sh  —  §6.5 Effect of Coding Parameters
#
# Fixed N=8, sweeps K ∈ {2, 4, 6}.
# 8-node cluster: storage on 10.10.1.{2..9}, client driver on 10.10.1.1.
# Default YCSB high-entropy data (NO compression pool), 7M × 10KB ≈ 70 GB.
# Compression ON throughout. 32 GB per-node cgroup memory cap
# (≈24 GB OS page cache after the 8 GB JVM heap).
# Measurement workload: 50/50 read/update (YCSB workloadA semantics).
#
# Flow per K:
#   stop_all → wipe_data → PAUSE for user to update K in ecconfig (prompt
#   asks for the K value, used in folder naming) → start cluster with FULL
#   memory (no cgroup, fast load) → create table → load → wait compaction →
#   drain → stop → restart with 32 GB cgroup cap + page-cache evict →
#   warmup (100% read, sized to fill ~24 GB cache for this K) →
#   measure (50/50 R/U) → capture tablestats for per-node storage.
# =============================================================================

set -u

# ── Config ───────────────────────────────────────────────────────────────────
SSH_USER=rzp5412
CASS_DIR=/mydata/cassandra
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql

NODES=(2 3 4 5 6 7 8 9)            # 8 storage nodes -- N=8 placement
CACHE_GB=32                         # per-node cgroup memory.max
RECORD_COUNT=7000000
FIELD_LENGTH=10000                  # 10 KB cells, default YCSB high-entropy
MEASURE_OPS=10000000
THREADS=16
WTHREADS=16

# 50/50 read/update -- workloadA semantics
READ_PROPORTION=0.5
UPDATE_PROPORTION=0.5
INSERT_PROPORTION=0

CREATE_TABLE_BIN=/mydata/create_table_ec_compr_on
K_LIST=(2 4 6)

# ── Helpers ──────────────────────────────────────────────────────────────────
log_banner() {
    local log=$1 label=$2 k=$3 cache=$4 workload=$5 outfile=$6
    {
        echo ""
        echo "########################################################"
        echo "# LABEL    : ${label}"
        echo "# K        : ${k}"
        echo "# CACHE    : ${cache}"
        echo "# WORKLOAD : ${workload}"
        echo "# OUTPUT   : $(basename ${outfile})"
        echo "# TIME     : $(date)"
        echo "########################################################"
    } >> "$log"
}

stop_all_parallel() {
    echo ""
    echo "=== Stopping Cassandra on all nodes (parallel) ==="
    for node in "${NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill 2>/dev/null; true" &
    done
    wait
    for node in "${NODES[@]}"; do
        local ip="10.10.1.${node}"
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

wipe_data_parallel() {
    echo ""
    echo "=== Wiping data/ on all nodes (parallel) ==="
    for node in "${NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} "rm -rf ${CASS_DIR}/data/" &
    done
    wait
    echo "=== All data wiped ==="
}

start_cluster_no_cgroup() {
    # Full memory, sequential start (seeds first). Used for the LOAD phase.
    echo ""
    echo "=== Starting cluster (no cgroup; full memory for LOAD) ==="
    for node in "${NODES[@]}"; do
        local ip="10.10.1.${node}"
        ssh ${SSH_USER}@${ip} "cd ${CASS_DIR} && bin/cassandra > /dev/null 2>&1"
        local attempts=0
        until ssh ${SSH_USER}@${ip} \
            "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep '${ip}' | grep -q 'UN'"; do
            sleep 10
            attempts=$((attempts + 1))
            echo "  Waiting for ${ip} UN... (${attempts}/30)"
            if [ "$attempts" -ge 30 ]; then
                echo "  ERROR: ${ip} not UN after 5 min. Check ${CASS_DIR}/logs/system.log"
                exit 1
            fi
        done
        echo "  ${ip} is UN"
    done
}

start_cluster_with_cgroup() {
    # Apply ${1}GB cgroup memory.max, evict page cache for data/, then start
    # cassandra inheriting cgroup membership from the parent shell.
    local cache_gb=$1
    local mem_bytes=$((cache_gb * 1024 * 1024 * 1024))
    echo ""
    echo "=== Restart with cgroup cap = ${cache_gb}GB (${mem_bytes} bytes), evicting cache ==="
    for node in "${NODES[@]}"; do
        local ip="10.10.1.${node}"

        ssh ${SSH_USER}@${ip} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill 2>/dev/null; true"
        local k=0
        while ssh ${SSH_USER}@${ip} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' > /dev/null 2>&1"; do
            sleep 10; k=$((k + 1))
            if [ "$k" -ge 6 ]; then
                ssh ${SSH_USER}@${ip} \
                    "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill -9 2>/dev/null; true"
                sleep 5; break
            fi
        done

        ssh ${SSH_USER}@${ip} \
            "cd ${CASS_DIR} && \
             echo ${mem_bytes} | sudo tee /sys/fs/cgroup/mylimitedgroup/memory.max && \
             echo \$\$ | sudo tee /sys/fs/cgroup/mylimitedgroup/cgroup.procs && \
             vmtouch -e data/ > /dev/null 2>&1 && \
             bin/cassandra > /dev/null 2>&1"

        local sa=0
        until ssh ${SSH_USER}@${ip} \
            "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep '${ip}' | grep -q 'UN'"; do
            sleep 10; sa=$((sa + 1))
            echo "  Waiting for ${ip} UN... (${sa}/30)"
            if [ "$sa" -ge 30 ]; then
                echo "  ERROR: ${ip} not UN after 5 min"; exit 1
            fi
        done
        echo "  ${ip} is UP and NORMAL"
    done
}

wait_compaction_settled() {
    echo ""
    echo "=== Waiting for compaction to settle on all nodes ==="
    for node in "${NODES[@]}"; do
        local ip="10.10.1.${node}"
        echo "  Waiting on ${ip}..."
        while ssh ${SSH_USER}@${ip} \
            "${CASS_DIR}/bin/nodetool compactionstats 2>/dev/null | grep -q 'pending tasks: [^0]'"; do
            sleep 30
            echo "  Compaction still running on ${ip}..."
        done
        echo "  ${ip} compaction settled"
    done
}

drain_all() {
    echo ""
    echo "=== Draining all nodes (flushing memtables) ==="
    for node in "${NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} "${CASS_DIR}/bin/nodetool drain" &
    done
    wait
    echo "=== Drain complete ==="
}

# ── Pre-flight ───────────────────────────────────────────────────────────────
if [ ! -x "${CREATE_TABLE_BIN}" ]; then
    echo "ERROR: ${CREATE_TABLE_BIN} missing or not executable. Aborting."
    exit 1
fi
echo "Pre-flight OK."
echo "  Cluster nodes: 10.10.1.{${NODES[*]}}"
echo "  Client implied on 10.10.1.1"
echo "  Cache cap: ${CACHE_GB} GB per node"
echo "  Sweep: K = ${K_LIST[*]} at fixed N=8"
echo ""

# ── Main loop: one iteration per K value ─────────────────────────────────────
for K_TARGET in "${K_LIST[@]}"; do

    echo ""
    echo "################################################################"
    echo ">>> NEXT TARGET: K=${K_TARGET}  (N=8)"
    echo "################################################################"

    # Phase 1: stop + wipe. We wipe BEFORE the pause so the user updates
    # ecconfig on a known-empty cluster, no risk of stale state.
    stop_all_parallel
    wipe_data_parallel

    # Phase 2: PAUSE. User updates K in ecconfig on every node now.
    echo ""
    echo "================================================================"
    echo "  Cluster stopped, data/ wiped on all ${#NODES[@]} nodes."
    echo "  >>> Now update K in ecconfig (target K=${K_TARGET}) on every node."
    echo "  >>> Restart any EC-Service / re-sync configs as needed."
    echo "================================================================"
    read -p "Enter K value you set (e.g., ${K_TARGET}): " K_VALUE

    # Validate
    if [[ ! "$K_VALUE" =~ ^[0-9]+$ ]]; then
        echo "ERROR: K must be a positive integer."
        exit 1
    fi
    if [ "$K_VALUE" -lt 1 ] || [ "$K_VALUE" -ge 8 ]; then
        echo "ERROR: K must satisfy 1 ≤ K < N=8."
        exit 1
    fi

    OUT_DIR="result_codingparams_k${K_VALUE}"
    mkdir -p "${OUT_DIR}"
    LOG="${OUT_DIR}/codingparams_k${K_VALUE}_run${FIELD_LENGTH}Bytes.log"
    echo "  >>> Output directory: ${OUT_DIR}"
    echo "  >>> Log file:         ${LOG}"

    # Phase 3: start cluster (no cgroup), create table
    start_cluster_no_cgroup

    echo ""
    echo "=== Creating YCSB table via ${CREATE_TABLE_BIN} ==="
    ${CREATE_TABLE_BIN}
    echo "=== Table created ==="

    # Phase 4: LOAD 70 GB with full memory (fast)
    LOAD_FILE="${OUT_DIR}/codingparams_k${K_VALUE}_Load${FIELD_LENGTH}Bytes.scr"
    log_banner "$LOG" "codingparams" "${K_VALUE}" "FULL_MEM" "LOAD" "$LOAD_FILE"
    echo ""
    echo "=== Loading ${RECORD_COUNT} records × ${FIELD_LENGTH}B (full memory) ==="
    $YCSB_DIR load $DB -threads ${WTHREADS} \
        -p recordcount=${RECORD_COUNT} \
        -p fieldlength=${FIELD_LENGTH} \
        -p measurement.raw.output_file="${LOAD_FILE}" \
        -P commonworkload \
        -s >> "$LOG" 2>&1
    echo "=== Load complete ==="

    wait_compaction_settled
    drain_all
    stop_all_parallel

    # Phase 5: restart with 32 GB cgroup cap, page cache evicted
    start_cluster_with_cgroup ${CACHE_GB}

    # Phase 6: WARMUP -- 100% read, sized to fill the ~24 GB OS cache once.
    # per-node fragment ≈ FIELD_LENGTH / K bytes (each of the 8 nodes stores
    # one fragment per cell, all fragments equal-sized under RS(N,K)).
    AVAILABLE_BYTES=$(( (CACHE_GB - 8) * 1024 * 1024 * 1024 ))
    SHARD_SIZE=$(( FIELD_LENGTH / K_VALUE ))
    OBJECTS_FIT=$(( AVAILABLE_BYTES / SHARD_SIZE ))
    if [ "${OBJECTS_FIT}" -lt "${RECORD_COUNT}" ]; then
        WARMUP_OPS=${OBJECTS_FIT}
    else
        WARMUP_OPS=${RECORD_COUNT}
    fi
    if [ "${WARMUP_OPS}" -lt 1000000 ]; then WARMUP_OPS=1000000; fi
    echo ""
    echo ">>> K=${K_VALUE}, shard_size=${SHARD_SIZE}B, objects_that_fit=${OBJECTS_FIT}, warmup_ops=${WARMUP_OPS}"

    WARMUP_FILE="${OUT_DIR}/codingparams_k${K_VALUE}_${CACHE_GB}GB_Warmup.scr"
    log_banner "$LOG" "codingparams" "${K_VALUE}" "${CACHE_GB}GB" "WARMUP" "$WARMUP_FILE"
    echo ""
    echo "=== Warmup (100% read, ${WARMUP_OPS} ops, K=${K_VALUE}) ==="
    $YCSB_DIR run $DB -threads ${THREADS} \
        -p operationcount=${WARMUP_OPS} \
        -p readproportion=1.0 -p updateproportion=0.0 -p insertproportion=0.0 \
        -p recordcount=${RECORD_COUNT} \
        -p fieldlength=${FIELD_LENGTH} \
        -p measurement.raw.output_file="${WARMUP_FILE}" \
        -p cassandra.writeconsistencylevel=QUORUM \
        -p cassandra.readconsistencylevel=QUORUM \
        -P commonworkload \
        -s >> "$LOG" 2>&1
    echo "=== Warmup done ==="

    # Phase 7: MEASURE 50/50 R/U
    MEASURE_FILE="${OUT_DIR}/codingparams_k${K_VALUE}_${CACHE_GB}GB_workloadA_Run${FIELD_LENGTH}Bytes.scr"
    log_banner "$LOG" "codingparams" "${K_VALUE}" "${CACHE_GB}GB" "workloadA" "$MEASURE_FILE"
    echo ""
    echo "=== Measure (50/50 R/U, ${MEASURE_OPS} ops) -- K=${K_VALUE} ==="
    $YCSB_DIR run $DB -threads ${THREADS} \
        -p operationcount=${MEASURE_OPS} \
        -p readproportion=${READ_PROPORTION} \
        -p updateproportion=${UPDATE_PROPORTION} \
        -p insertproportion=${INSERT_PROPORTION} \
        -p recordcount=${RECORD_COUNT} \
        -p fieldlength=${FIELD_LENGTH} \
        -p measurement.raw.output_file="${MEASURE_FILE}" \
        -p cassandra.writeconsistencylevel=QUORUM \
        -p cassandra.readconsistencylevel=QUORUM \
        -P commonworkload \
        -s >> "$LOG" 2>&1
    echo "=== Measure done for K=${K_VALUE} ==="

    # Phase 8: capture per-node tablestats so storage usage can be plotted
    # alongside latency (Fig. 12b in the paper -- "Effect of K on storage").
    STORAGE_FILE="${OUT_DIR}/codingparams_k${K_VALUE}_tablestats.txt"
    {
        echo "=== Per-node tablestats for K=${K_VALUE} ==="
        echo "=== Captured at $(date) ==="
    } > "${STORAGE_FILE}"
    for node in "${NODES[@]}"; do
        echo "" >> "${STORAGE_FILE}"
        echo "-- 10.10.1.${node} --" >> "${STORAGE_FILE}"
        ssh ${SSH_USER}@10.10.1.${node} "${CASS_DIR}/bin/nodetool tablestats ycsb" \
            >> "${STORAGE_FILE}" 2>&1
    done
    echo "=== Tablestats saved to ${STORAGE_FILE} ==="

    echo ""
    echo ">>> K=${K_VALUE} complete. Logs under ${OUT_DIR}/"
done

# ── Summary ──────────────────────────────────────────────────────────────────
echo ""
echo "################################################################"
echo "  ALL K ITERATIONS COMPLETE"
echo "################################################################"
echo "  Output directories:"
for K in "${K_LIST[@]}"; do
    echo "    result_codingparams_k${K}/"
done
echo ""
echo "  Files per K:"
echo "    codingparams_k<K>_run10000Bytes.log       -- YCSB stdout + banners"
echo "    codingparams_k<K>_Load10000Bytes.scr      -- raw load latencies"
echo "    codingparams_k<K>_32GB_Warmup.scr         -- raw warmup latencies"
echo "    codingparams_k<K>_32GB_workloadA_Run*.scr -- raw measurement latencies"
echo "    codingparams_k<K>_tablestats.txt          -- per-node storage usage"
echo ""
