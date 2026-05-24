#!/bin/bash
# =============================================================================
# run_OS_Cache_experiments_Compress.sh
#
# Flow per compression dataset (jpeg, wiki, hdfs):
#   hard_restart (wipe + full memory) → YCSB load → wait compaction →
#   drain → stop → take_snapshot
#   for each cache_size:
#       stop_cluster → restore_from_snapshot → restart_cluster(cache_size)
#       ONE warmup (ops scaled to fill cache 100%)
#       for each workload (C→B→D→A):
#           reset breakdown → measure → collect breakdown
#   delete_snapshot
# =============================================================================

# ── Config ────────────────────────────────────────────────────────────────────
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql
MEASURE_OPS=10000000
# WARMUP_OPS computed dynamically per cache size to fill cache 100%
# formula: (cache_gb - 8) * 1GB / FIELD_LENGTH, floor 1M
FIELD_LENGTH=10000
RECORD_COUNT=7000000

WORKLOAD_LABELS=("workloadC" "workloadB" "workloadD" "workloadA")
READ_PROPORTIONS=(
    "readproportion=1.0  -p updateproportion=0.0 -p insertproportion=0"                             # C: read only
    "readproportion=0.95 -p updateproportion=0.05 -p insertproportion=0"                            # B: read mostly
    "readproportion=0.95 -p updateproportion=0.0 -p insertproportion=0.05 -p requestdistrib=latest" # D: read latest
    "readproportion=0.5  -p updateproportion=0.5 -p insertproportion=0"                             # A: 50/50 (worst case)
)

CACHE_SIZES=("16GB" "28GB" "40GB" "52GB" "64GB")

POOL_DIR=/mydata/compressData
COMPRESS_LABELS=("jpeg" "wiki" "hdfs")
POOL_FILES=("values_pool_jpeg.txt" "values_pool_wiki.txt" "values_pool_hdfs.txt")

SSH_USER=rzp5412
CASS_DIR=/mydata/cassandra

# =============================================================================
# log_banner
# =============================================================================
log_banner() {
    local log=$1 label=$2 dataset=$3 cache=$4 workload=$5 outfile=$6
    {
        echo ""
        echo "########################################################"
        echo "# LABEL    : ${label}"
        echo "# DATASET  : ${dataset}"
        echo "# CACHE    : ${cache}"
        echo "# WORKLOAD : ${workload}"
        echo "# OUTPUT   : $(basename ${outfile})"
        echo "# TIME     : $(date)"
        echo "########################################################"
    } >> "$log"
}

# =============================================================================
# stop_cluster
# =============================================================================
stop_cluster() {
    local nodes
    if [ "$NUM_NODES" = "3" ]; then nodes=(2 3 4); else nodes=(2 3 4 5 6); fi

    echo ""
    echo "=== Stopping Cassandra on all nodes ==="
    for node in "${nodes[@]}"; do
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
    echo "=== All nodes stopped ==="
}

# =============================================================================
# take_snapshot
#   Hard-links data/ → data_snapshot/ on each node.
#   Zero extra disk space. Cassandra must be stopped before calling.
# =============================================================================
take_snapshot() {
    local nodes
    if [ "$NUM_NODES" = "3" ]; then nodes=(2 3 4); else nodes=(2 3 4 5 6); fi

    echo ""
    echo "=== Taking snapshot (hard links, zero extra disk) ==="
    for node in "${nodes[@]}"; do
        local ip="10.10.1.$node"
        echo "  Snapshotting ${ip}..."
        ssh ${SSH_USER}@${ip} \
            "rm -rf ${CASS_DIR}/data_snapshot && \
             cp -rl ${CASS_DIR}/data ${CASS_DIR}/data_snapshot"
        echo "  Snapshot done on ${ip}"
    done
    echo "=== Snapshot complete ==="
}

# =============================================================================
# restore_from_snapshot
#   Replaces data/ with a fresh hard-link copy from data_snapshot/.
#   Near-instant. Cassandra must be stopped before calling.
# =============================================================================
restore_from_snapshot() {
    local nodes
    if [ "$NUM_NODES" = "3" ]; then nodes=(2 3 4); else nodes=(2 3 4 5 6); fi

    echo ""
    echo "=== Restoring from snapshot (instant) ==="
    for node in "${nodes[@]}"; do
        local ip="10.10.1.$node"
        echo "  Restoring ${ip}..."
        ssh ${SSH_USER}@${ip} \
            "rm -rf ${CASS_DIR}/data && \
             cp -rl ${CASS_DIR}/data_snapshot ${CASS_DIR}/data"
        echo "  Restored ${ip}"
    done
    echo "=== Restore complete ==="
}

# =============================================================================
# delete_snapshot
#   Removes data_snapshot/ on all nodes after dataset is fully done.
#   Frees disk before next dataset loads.
# =============================================================================
delete_snapshot() {
    local nodes
    if [ "$NUM_NODES" = "3" ]; then nodes=(2 3 4); else nodes=(2 3 4 5 6); fi

    echo ""
    echo "=== Deleting snapshot on all nodes ==="
    for node in "${nodes[@]}"; do
        local ip="10.10.1.$node"
        ssh ${SSH_USER}@${ip} "rm -rf ${CASS_DIR}/data_snapshot/"
        echo "  Snapshot deleted on ${ip}"
    done
    echo "=== Snapshot deleted ==="
}

# =============================================================================
# restart_cluster <cache_size>
#   Soft restart — keeps data, applies cgroup memory limit, evicts page cache.
#   One node at a time, seeds first.
# =============================================================================
restart_cluster() {
    local cache_size=$1
    local nodes
    if [ "$NUM_NODES" = "3" ]; then nodes=(2 3 4); else nodes=(2 3 4 5 6); fi

    local cache_gb="${cache_size//GB/}"
    local mem_bytes=$((cache_gb * 1024 * 1024 * 1024))

    echo ""
    echo "=== Soft restart: nodes ${nodes[*]}, cache=${cache_size} (${mem_bytes} bytes) ==="
    for node in "${nodes[@]}"; do
        local ip="10.10.1.$node"
        echo ""
        echo "  --- ${ip} ---"

        ssh ${SSH_USER}@${ip} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill 2>/dev/null; true"
        local kill_attempts=0
        while ssh ${SSH_USER}@${ip} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' > /dev/null 2>&1"; do
            sleep 10
            kill_attempts=$((kill_attempts + 1))
            echo "  Waiting for stop... (${kill_attempts}/6)"
            if [ "$kill_attempts" -ge 6 ]; then
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

        local start_attempts=0
        until ssh ${SSH_USER}@${ip} \
            "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep '${ip}' | grep -q 'UN'"; do
            sleep 10
            start_attempts=$((start_attempts + 1))
            echo "  Waiting for UN... (${start_attempts}/30)"
            if [ "$start_attempts" -ge 30 ]; then
                echo "  ERROR: ${ip} not UN after 5 min. Check ${CASS_DIR}/logs/system.log"
                exit 1
            fi
        done
        echo "  ${ip} is UP and NORMAL (UN)"
    done
    echo ""
    echo "=== Soft restart complete. Cluster ready with ${cache_size} cache. ==="
}

# =============================================================================
# hard_restart_cluster
#   Phase 1: kill ALL nodes in parallel
#   Phase 2: wipe ALL data in parallel
#   Phase 3: start sequentially (seeds first), create YCSB table
# =============================================================================
hard_restart_cluster() {
    local nodes
    if [ "$NUM_NODES" = "3" ]; then nodes=(2 3 4); else nodes=(2 3 4 5 6); fi

    echo ""
    echo "=== HARD restart: nodes ${nodes[*]} ==="

    # Phase 1 — kill all in parallel
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
    for node in "${nodes[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} "rm -rf ${CASS_DIR}/data/" &
    done
    wait
    echo "  [2/3] All data wiped"

    # Phase 3 — start sequentially, seeds first
    echo "  [3/3] Starting nodes sequentially (seeds first)..."
    for node in "${nodes[@]}"; do
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
echo "Is this EC or REP?"
read EXP_LABEL

read -p "How many Cassandra nodes? (3 or 5): " NUM_NODES
if [ "$NUM_NODES" = "3" ]; then
    BD_NODES=(2 3 4)
elif [ "$NUM_NODES" = "5" ]; then
    BD_NODES=(2 3 4 5 6)
else
    echo "ERROR: must be 3 or 5"; exit 1
fi
echo "Nodes: ${BD_NODES[*]}"

echo "How many write threads?"
read WTHREADS
echo "How many read threads?"
read THREADS

# =============================================================================
# MAIN EXPERIMENT LOOP
#
# For each compression state (on → off):
#   Set correct CREATE_TABLE_BIN for this state
#   For each dataset (jpeg, wiki, hdfs):
#     hard_restart → load → wait compaction → drain → stop → take_snapshot
#     for each cache_size:
#         stop → restore → restart → warmup → all workloads
#         per workload: reset breakdown → measure → collect breakdown
#     delete_snapshot
#   Between on/off: hard_restart of next iteration wipes everything clean
# =============================================================================
for COMPRESSION in "on" "off"; do

    # Set correct binary for this compression state
    if echo "$EXP_LABEL" | grep -qi "rep"; then
        CREATE_TABLE_BIN="create_table_rep_compr_${COMPRESSION}"
    else
        CREATE_TABLE_BIN="create_table_ec_compr_${COMPRESSION}"
    fi

    echo ""
    echo "################################################################"
    echo ">>> ${EXP_LABEL^^} — Compression ${COMPRESSION^^}"
    echo ">>> Binary: /mydata/${CREATE_TABLE_BIN}"
    echo "################################################################"

    for compress_idx in "${!COMPRESS_LABELS[@]}"; do
    COMPRESS_LABEL="${COMPRESS_LABELS[$compress_idx]}"
    POOL_FILE="${POOL_DIR}/${POOL_FILES[$compress_idx]}"

    # POOL_PARAMS: passed to all run commands — ensures updates/inserts use
    # correct 10KB pool values so reads always return consistent object size
    POOL_PARAMS="-p fieldlength=${FIELD_LENGTH} -p valuepool.file=${POOL_FILE}"

    echo ""
    echo "============================================================"
    echo ">>> Compression dataset : ${COMPRESS_LABEL}"
    echo ">>> Pool file           : ${POOL_FILE}"
    echo "============================================================"

    BASE_OUT_DIR="result_OS_CacheCompress_${EXP_LABEL}_${COMPRESSION}_${COMPRESS_LABEL}"
    mkdir -p "$BASE_OUT_DIR"
    LOG="${BASE_OUT_DIR}/${EXP_LABEL}_${COMPRESSION}_${COMPRESS_LABEL}_run${FIELD_LENGTH}Bytes.log"
    BREAKDOWN_FILE="${BASE_OUT_DIR}/${EXP_LABEL}_${COMPRESSION}_${COMPRESS_LABEL}_breakdown.txt"
    touch "$BREAKDOWN_FILE"

    # ── Hard restart and load ─────────────────────────────────────────
    hard_restart_cluster

    LOAD_FILE="${BASE_OUT_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_Load${FIELD_LENGTH}Bytes_run.scr"
    log_banner "$LOG" "$EXP_LABEL" "$COMPRESS_LABEL" "FULL_MEM" "LOAD" "$LOAD_FILE"
    echo "--- Loading ${RECORD_COUNT} records x ${FIELD_LENGTH}B from ${COMPRESS_LABEL} pool ---"
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

    # Drain all nodes — flush memtables to SSTables before snapshot
    # Ensures snapshot captures fully persisted clean state
    echo "--- Draining all nodes (flushing memtables) ---"
    for node in "${BD_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool drain" &
    done
    wait
    echo "--- Drain complete ---"

    # Stop cluster, take snapshot of clean loaded state
    stop_cluster
    take_snapshot

    # ── Cache size loop ───────────────────────────────────────────────
    for cache_size in "${CACHE_SIZES[@]}"; do
        echo ""
        echo ">>> Cache: ${cache_size}  |  Dataset: ${COMPRESS_LABEL}"

        CACHE_OUT_DIR="${BASE_OUT_DIR}_${cache_size}"
        mkdir -p "$CACHE_OUT_DIR"

        # Restore clean snapshot state before each cache size
        # stop_cluster first in case Cassandra is still running from previous run
        stop_cluster
        restore_from_snapshot

        # Dynamic warmup ops: fills available page cache exactly once
        # available = cgroup_limit - 8GB JVM heap
        cache_gb="${cache_size//GB/}"
        available_bytes=$(( (cache_gb - 8) * 1024 * 1024 * 1024 ))
        if echo "$EXP_LABEL" | grep -qi "ec"; then
            shard_size=$(( FIELD_LENGTH / 3 ))
        else
            shard_size=$FIELD_LENGTH
        fi
        objects_that_fit=$(( available_bytes / shard_size ))
        WARMUP_OPS=$(( objects_that_fit < RECORD_COUNT ? objects_that_fit : RECORD_COUNT ))
        if [ "$WARMUP_OPS" -lt 1000000 ]; then WARMUP_OPS=1000000; fi
        echo ">>> Warmup ops: ${WARMUP_OPS} (fills ${cache_size} cache for ${FIELD_LENGTH}B objects)"

        # Soft restart: evict page cache, apply cgroup limit
        restart_cluster "$cache_size"

        # ONE warmup per cache size — 100% read, populates OS cache
        WARMUP_FILE="${CACHE_OUT_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_${cache_size}_Warmup.scr"
        log_banner "$LOG" "$EXP_LABEL" "$COMPRESS_LABEL" "$cache_size" "WARMUP" "$WARMUP_FILE"
        echo "--- Warmup (${cache_size}, 100% read, ${WARMUP_OPS} ops) ---"
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

        # All workloads back-to-back (C→B→D→A)
        # breakdown: reset before each workload, collect after
        for i in "${!WORKLOAD_LABELS[@]}"; do
            workload="${WORKLOAD_LABELS[$i]}"
            READ_PCT="${READ_PROPORTIONS[$i]}"

            MEASURE_FILE="${CACHE_OUT_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_${cache_size}_${workload}Run${FIELD_LENGTH}Bytes.scr"
            log_banner "$LOG" "$EXP_LABEL" "$COMPRESS_LABEL" "$cache_size" "$workload" "$MEASURE_FILE"
            echo "=== ${workload} | ${cache_size} | ${COMPRESS_LABEL} ==="

            # Reset breakdown counters before this workload
            echo "--- Resetting breakdown (${workload} | ${cache_size}) ---"
            for node in "${BD_NODES[@]}"; do
                ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool breakdown --reset"
            done

            $YCSB_DIR run $DB -threads $THREADS \
                -p operationcount=$MEASURE_OPS \
                -p ${READ_PCT} \
                -p recordcount=${RECORD_COUNT} \
                ${POOL_PARAMS} \
                -p measurement.raw.output_file="$MEASURE_FILE" \
                -p cassandra.writeconsistencylevel=QUORUM \
                -p cassandra.readconsistencylevel=QUORUM \
                -P commonworkload \
                -s >> "$LOG" 2>&1
            echo "=== Done: ${workload} | ${cache_size} | ${COMPRESS_LABEL} ==="

            # Collect breakdown after this workload
            echo "run for ${EXP_LABEL} ${cache_size} ${workload} dataset=${COMPRESS_LABEL}" >> "$BREAKDOWN_FILE"
            for node in "${BD_NODES[@]}"; do
                echo "-- node 10.10.1.$node --" >> "$BREAKDOWN_FILE"
                ssh ${SSH_USER}@10.10.1.$node \
                    "${CASS_DIR}/bin/nodetool breakdown | grep -E 'keyspace|ycsb'" >> "$BREAKDOWN_FILE"
            done

        done

        echo ">>> All workloads done for cache=${cache_size} | dataset=${COMPRESS_LABEL}"
    done

    # Delete snapshot after all cache sizes complete — free disk for next dataset
    delete_snapshot

    echo ">>> Completed all cache sizes for ${COMPRESS_LABEL}"
    done
    # ── End of dataset loop ──────────────────────────────────────────

    echo ""
    echo ">>> Completed ${EXP_LABEL^^} compression ${COMPRESSION^^} for all datasets"

done
# ── End of compression loop ──────────────────────────────────────────────────

echo ""
echo "All experiments completed successfully."
