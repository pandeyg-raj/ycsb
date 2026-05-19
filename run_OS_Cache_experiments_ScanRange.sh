#!/bin/bash

# ============================================================
# Workload E (Scan) — Compression Sensitivity Experiment
#
# Built on top of run_OS_Cache_experiments_Compress.sh.
# All infrastructure (SSH functions, snapshot mechanism,
# hard/soft restart, CREATE_TABLE_BIN selection) is identical.
#
# Workload E specific changes vs the main script:
#   - WORKLOAD_LABELS = ("workloadE") only
#   - scanproportion=0.95, insertproportion=0.05, readproportion=0
#   - requestdistribution=uniform (scan start key)
#   - MIN_SCAN_LENGTH / MAX_SCAN_LENGTH control scan fan-out
#   - Separate SCAN_THREADS prompt — scans are heavier than point reads
#   - Warmup stays 100% read (same as main script — fills page cache
#     with the data that will subsequently be range-scanned)
#   - Output metric tag in log: [SCAN] not [READ]
#   - Output dir/log use _scanE suffix to stay separate from main runs
# ============================================================

# === Config ===
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql
MEASURE_OPS=10000000
WARMUP_OPS=3000000
FIELD_LENGTH=10000
RECORD_COUNT=7000000

# --- Scan length range ---
# At FIELD_LENGTH=10000 (10 KB):
#   MAX_SCAN_LENGTH=100  → up to 1 MB per scan   (recommended starting point)
#   MAX_SCAN_LENGTH=10   → up to 100 KB per scan  (lighter)
#   MAX_SCAN_LENGTH=1000 → up to 10 MB per scan   (heavy stress)
MIN_SCAN_LENGTH=1
MAX_SCAN_LENGTH=100

# --- Workload E definition ---
# Single entry: 95% scans, 5% inserts, no point reads.
WORKLOAD_LABELS=("workloadE")
READ_PROPORTIONS=(
    "scanproportion=0.95 -p insertproportion=0.05 -p readproportion=0 -p updateproportion=0 -p requestdistribution=uniform -p minscanlength=${MIN_SCAN_LENGTH} -p maxscanlength=${MAX_SCAN_LENGTH}"
)

#CACHE_SIZES=("16GB" "28GB" "40GB" "52GB" "64GB")
CACHE_SIZES=("64GB")

POOL_DIR=/mydata/compressData
COMPRESS_LABELS=("jpeg" "wiki" "hdfs")
POOL_FILES=("values_pool_jpeg.txt" "values_pool_wiki.txt" "values_pool_hdfs.txt")

SSH_USER=rzp5412
CASS_DIR=/mydata/cassandra

HARD_RESTART_PER_WORKLOAD=1

# =====================================================================
# log_banner — identical to main script
# =====================================================================
log_banner() {
    local log=$1 label=$2 dataset=$3 cache=$4 workload=$5 outfile=$6
    echo ""                                                       >> "$log"
    echo "########################################################" >> "$log"
    echo "# LABEL    : ${label}"                                  >> "$log"
    echo "# DATASET  : ${dataset}"                                >> "$log"
    echo "# CACHE    : ${cache}"                                  >> "$log"
    echo "# WORKLOAD : ${workload}"                               >> "$log"
    echo "# OUTPUT   : $(basename ${outfile})"                    >> "$log"
    echo "# TIME     : $(date)"                                   >> "$log"
    echo "########################################################" >> "$log"
}

# =====================================================================
# stop_cluster — identical to main script
# =====================================================================
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

# =====================================================================
# take_snapshot — identical to main script
# =====================================================================
take_snapshot() {
    local nodes
    if [ "$NUM_NODES" = "3" ]; then nodes=(2 3 4); else nodes=(2 3 4 5 6); fi

    echo ""
    echo "=== Taking data snapshot (hard links, zero extra disk) ==="
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

# =====================================================================
# restore_from_snapshot — identical to main script
# =====================================================================
restore_from_snapshot() {
    local nodes
    if [ "$NUM_NODES" = "3" ]; then nodes=(2 3 4); else nodes=(2 3 4 5 6); fi

    echo ""
    echo "=== Restoring data from snapshot (instant) ==="
    for node in "${nodes[@]}"; do
        local ip="10.10.1.$node"
        echo "  Restoring ${ip}..."
        ssh ${SSH_USER}@${ip} \
            "rm -rf ${CASS_DIR}/data && \
             cp -rl ${CASS_DIR}/data_snapshot ${CASS_DIR}/data"
        echo "  Restore done on ${ip}"
    done
    echo "=== Restore complete ==="
}

# =====================================================================
# delete_snapshot — identical to main script
# =====================================================================
delete_snapshot() {
    local nodes
    if [ "$NUM_NODES" = "3" ]; then nodes=(2 3 4); else nodes=(2 3 4 5 6); fi

    echo ""
    echo "=== Deleting snapshot on all nodes (freeing disk space) ==="
    for node in "${nodes[@]}"; do
        local ip="10.10.1.$node"
        ssh ${SSH_USER}@${ip} "rm -rf ${CASS_DIR}/data_snapshot/"
        echo "  Snapshot deleted on ${ip}"
    done
    echo "=== Snapshot deleted ==="
}

# =====================================================================
# restart_cluster — identical to main script
# =====================================================================
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
        echo "--- $ip ---"

        echo "  [1/3] Stopping Cassandra..."
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
        echo "  [1/3] Stopped"

        echo "  [2/3] Setting ${cache_size} limit, evicting cache, starting Cassandra..."
        ssh ${SSH_USER}@${ip} \
            "cd ${CASS_DIR} && \
             echo ${mem_bytes} | sudo tee /sys/fs/cgroup/mylimitedgroup/memory.max && \
             echo \$\$ | sudo tee /sys/fs/cgroup/mylimitedgroup/cgroup.procs && \
             vmtouch -e data/ > /dev/null 2>&1 && \
             bin/cassandra > /dev/null 2>&1"

        echo "  [3/3] Waiting for ${ip} to be UN..."
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
        echo "  [3/3] ${ip} is UP and NORMAL (UN)"
    done

    echo ""
    echo "=== Soft restart complete. Cluster ready with ${cache_size} cache. ==="
}

# =====================================================================
# hard_restart_cluster — identical to main script
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
    echo "  [2/3] All nodes stopped and wiped"

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
                echo "    ERROR: ${ip} not UN after 5 min. Check ${CASS_DIR}/logs/system.log"
                exit 1
            fi
        done
        echo "    ${ip} is UN"
    done

    echo "  Creating YCSB table via /mydata/${CREATE_TABLE_BIN} ..."
    /mydata/${CREATE_TABLE_BIN}
    echo "  Table created"
    echo ""
    echo "=== HARD restart complete. Cluster wiped and ready at full memory. ==="
}

# =====================================================================
# Experiment setup — asked once before all loops
# =====================================================================

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

echo "Is this EC or REP?"
read EXP_LABEL

read -p "How many Cassandra nodes? (3 or 5): " NUM_NODES
echo "Confirmed: using ${NUM_NODES} nodes."

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
echo "Confirmed: using /mydata/${CREATE_TABLE_BIN}"

echo "How many write/load threads?"
read WTHREADS

# Scan threads kept separate from read threads — each scan touches up
# to MAX_SCAN_LENGTH records, making individual ops much heavier.
# Recommended: 4–8 (vs. 16–32 for point-read workloads).
echo "How many scan threads? (recommend 4-8, fewer than point-read threads)"
read SCAN_THREADS

# =====================================================================
# Outermost loop: compression datasets
# =====================================================================
for compress_idx in "${!COMPRESS_LABELS[@]}"; do
    COMPRESS_LABEL="${COMPRESS_LABELS[$compress_idx]}"
    POOL_FILE="${POOL_DIR}/${POOL_FILES[$compress_idx]}"

    echo ""
    echo "============================================================"
    echo ">>> Compression dataset : ${COMPRESS_LABEL}  [Workload E / Scan]"
    echo ">>> Pool file           : ${POOL_FILE}"
    echo ">>> Scan length range   : ${MIN_SCAN_LENGTH}–${MAX_SCAN_LENGTH} records"
    echo "============================================================"

    # _scanE suffix keeps output dirs and logs separate from main script
    BASE_OUT_DIR="result_OS_CacheCompress_${COMPRESS_LABEL}_scanE"
    mkdir -p "$BASE_OUT_DIR"
    LOG="${BASE_OUT_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_scanE_run${FIELD_LENGTH}Bytes.log"

    POOL_PARAMS="-p fieldlength=${FIELD_LENGTH} -p valuepool.file=${POOL_FILE}"

    if [ "$HARD_RESTART_PER_WORKLOAD" = "0" ]; then
        # ============================================================
        # NORMAL FLOW: load once, restart per cache size
        # ============================================================
        read -p "Wipe Cassandra data + restart in full memory, CREATE TABLE, then press Enter..."

        LOAD_FILE="${BASE_OUT_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_Load${FIELD_LENGTH}Bytes_run.scr"
        log_banner "$LOG" "$EXP_LABEL" "$COMPRESS_LABEL" "FULL_MEM" "LOAD" "$LOAD_FILE"
        echo "Load: $RECORD_COUNT records x ${FIELD_LENGTH}B from ${COMPRESS_LABEL} pool..."
        $YCSB_DIR load $DB -threads $WTHREADS \
            -p recordcount=${RECORD_COUNT} \
            -p fieldlength=${FIELD_LENGTH} \
            -p valuepool.file=${POOL_FILE} \
            -p measurement.raw.output_file="$LOAD_FILE" \
            -P commonworkload \
            -s >> "$LOG" 2>&1
        echo "Load done."

        for cache_size in "${CACHE_SIZES[@]}"; do
            echo ""
            echo ">>> Cache: ${cache_size}  Dataset: ${COMPRESS_LABEL}  [Workload E]"

            WARMUP_DIR="${BASE_OUT_DIR}_${cache_size}"
            mkdir -p "$WARMUP_DIR"

            restart_cluster "$cache_size"

            WARMUP_FILE="${WARMUP_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_${cache_size}_workloadE_Warmup.scr"
            log_banner "$LOG" "$EXP_LABEL" "$COMPRESS_LABEL" "$cache_size" "workloadE_WARMUP" "$WARMUP_FILE"
            echo "--- Warmup (${cache_size}, 100% read) ---"
            $YCSB_DIR run $DB -threads $SCAN_THREADS \
                -p operationcount=$WARMUP_OPS \
                -p readproportion=1.0 -p updateproportion=0.0 -p insertproportion=0.0 \
                -p recordcount=${RECORD_COUNT} \
                -p measurement.raw.output_file="$WARMUP_FILE" \
                -p cassandra.writeconsistencylevel=QUORUM \
                -p cassandra.readconsistencylevel=QUORUM \
                -P commonworkload \
                -s >> "$LOG" 2>&1

            # Measurement — output tag in log will be [SCAN] not [READ]
            MEASURE_FILE="${WARMUP_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_${cache_size}_workloadERun${FIELD_LENGTH}Bytes.scr"
            log_banner "$LOG" "$EXP_LABEL" "$COMPRESS_LABEL" "$cache_size" "workloadE" "$MEASURE_FILE"
            echo "=== workloadE | ${cache_size} | ${COMPRESS_LABEL} ==="
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
                ${POOL_PARAMS} \
                -p measurement.raw.output_file="$MEASURE_FILE" \
                -p cassandra.writeconsistencylevel=QUORUM \
                -p cassandra.readconsistencylevel=QUORUM \
                -P commonworkload \
                -s >> "$LOG" 2>&1
            echo "=== Done: workloadE | ${cache_size} | ${COMPRESS_LABEL} ==="
        done

    else
        # ============================================================
        # HARD RESTART FLOW: snapshot optimisation
        #
        # With only one workload (E), the snapshot is taken once per
        # dataset after the first YCSB load, then restored for each
        # subsequent cache size — identical logic to the main script.
        # ============================================================

        SNAPSHOT_READY=0

        for cache_size in "${CACHE_SIZES[@]}"; do
            echo ""
            echo ">>> Cache: ${cache_size}  Dataset: ${COMPRESS_LABEL}  [Workload E]"

            WARMUP_DIR="${BASE_OUT_DIR}_${cache_size}"
            mkdir -p "$WARMUP_DIR"

            for i in "${!WORKLOAD_LABELS[@]}"; do
                workload="${WORKLOAD_LABELS[$i]}"
                READ_PCT="${READ_PROPORTIONS[$i]}"

                echo ""
                echo "--- ${workload} | ${cache_size} | ${COMPRESS_LABEL} ---"

                if [ "$SNAPSHOT_READY" = "0" ]; then
                    echo "First run for ${COMPRESS_LABEL}: full load + snapshot"

                    hard_restart_cluster

                    LOAD_FILE="${BASE_OUT_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_Load${FIELD_LENGTH}Bytes_run.scr"
                    log_banner "$LOG" "$EXP_LABEL" "$COMPRESS_LABEL" "FULL_MEM" "LOAD" "$LOAD_FILE"
                    echo "--- Loading ${RECORD_COUNT} records x ${FIELD_LENGTH}B ---"
                    $YCSB_DIR load $DB -threads $WTHREADS \
                        -p recordcount=${RECORD_COUNT} \
                        -p fieldlength=${FIELD_LENGTH} \
                        -p valuepool.file=${POOL_FILE} \
                        -p measurement.raw.output_file="$LOAD_FILE" \
                        -P commonworkload \
                        -s >> "$LOG" 2>&1
                    echo "--- Load done ---"

                    echo "--- Waiting for compaction to settle on all nodes ---"
                    if [ "$NUM_NODES" = "3" ]; then snap_nodes=(2 3 4); else snap_nodes=(2 3 4 5 6); fi
                    for node in "${snap_nodes[@]}"; do
                        ip="10.10.1.$node"
                        echo "  Waiting on ${ip}..."
                        while ssh ${SSH_USER}@${ip} \
                            "${CASS_DIR}/bin/nodetool compactionstats 2>/dev/null | grep -q 'pending tasks: [^0]'"; do
                            sleep 30
                            echo "  Compaction still running on ${ip}..."
                        done
                        echo "  ${ip} compaction settled"
                    done
                    echo "--- Compaction settled on all nodes ---"

                    echo "--- Draining all nodes (flushing memtables) ---"
                    if [ "$NUM_NODES" = "3" ]; then snap_nodes=(2 3 4); else snap_nodes=(2 3 4 5 6); fi
                    for node in "${snap_nodes[@]}"; do
                        ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool drain" &
                    done
                    wait
                    echo "--- Drain complete ---"

                    stop_cluster
                    take_snapshot
                    SNAPSHOT_READY=1

                else
                    echo "Restoring from snapshot (instant, no reload needed)..."
                    stop_cluster
                    restore_from_snapshot
                fi

                restart_cluster "$cache_size"

                WARMUP_FILE="${WARMUP_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_${cache_size}_${workload}_Warmup.scr"
                log_banner "$LOG" "$EXP_LABEL" "$COMPRESS_LABEL" "$cache_size" "${workload}_WARMUP" "$WARMUP_FILE"
                echo "--- Warmup (${cache_size}, 100% read) ---"
                $YCSB_DIR run $DB -threads $SCAN_THREADS \
                    -p operationcount=$WARMUP_OPS \
                    -p readproportion=1.0 -p updateproportion=0.0 -p insertproportion=0.0 \
                    -p recordcount=${RECORD_COUNT} \
                    -p measurement.raw.output_file="$WARMUP_FILE" \
                    -p cassandra.writeconsistencylevel=QUORUM \
                    -p cassandra.readconsistencylevel=QUORUM \
                    -P commonworkload \
                    -s >> "$LOG" 2>&1

                # Measurement — output tag in log will be [SCAN] not [READ]
                MEASURE_FILE="${WARMUP_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_${cache_size}_${workload}Run${FIELD_LENGTH}Bytes.scr"
                log_banner "$LOG" "$EXP_LABEL" "$COMPRESS_LABEL" "$cache_size" "$workload" "$MEASURE_FILE"
                echo "=== ${workload} | ${cache_size} | ${COMPRESS_LABEL} ==="
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
                    ${POOL_PARAMS} \
                    -p measurement.raw.output_file="$MEASURE_FILE" \
                    -p cassandra.writeconsistencylevel=QUORUM \
                    -p cassandra.readconsistencylevel=QUORUM \
                    -P commonworkload \
                    -s >> "$LOG" 2>&1
                echo "=== Done: ${workload} | ${cache_size} | ${COMPRESS_LABEL} ==="
            done
        done
    fi

    if [ "$HARD_RESTART_PER_WORKLOAD" = "1" ] && [ "$SNAPSHOT_READY" = "1" ]; then
        delete_snapshot
    fi

    echo ">>> Completed all cache sizes for ${COMPRESS_LABEL} [Workload E]"
done

echo "All Workload E experiments completed."
