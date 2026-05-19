#!/bin/bash
# === Config ===
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql
MEASURE_OPS=10000000
WARMUP_OPS=3000000

# Total target database size in bytes (~70GB). RECORD_COUNT per object size
# is computed below as TOTAL_DB_BYTES / FIELD_LENGTH.
TOTAL_DB_BYTES=70000000000

# Standard YCSB workloads A, B, C, D — worst case (A) first for early failure detection
WORKLOAD_LABELS=("workloadA" "workloadC")
READ_PROPORTIONS=(
    "readproportion=0.5  -p updateproportion=0.5  -p insertproportion=0"                           # A: 50% update (worst case)
    "readproportion=1.0  -p updateproportion=0.0  -p insertproportion=0"                           # C: read only
)

CACHE_SIZES=("16GB" "28GB" "40GB" "52GB" "64GB")

# Object size axis — biggest first so disk-pressure / compaction issues surface early.
#   100KB → 700,000  records
#    10KB → 7,000,000 records
#     1KB → 70,000,000 records
OBJECT_SIZE_LABELS=("100KB" "10KB" "1KB")
FIELD_LENGTHS=(100000 10000 1000)

SSH_USER=rzp5412
CASS_DIR=/mydata/cassandra

# ---------------------------------------------------------------
# HARD_RESTART_PER_WORKLOAD=1 → each workload starts from clean cluster.
#   Uses snapshot after first load to avoid reloading from YCSB each time.
#   First workload: YCSB load → snapshot. Subsequent: instant restore.
# HARD_RESTART_PER_WORKLOAD=0 → load once, restart per cache size.
HARD_RESTART_PER_WORKLOAD=1
# ---------------------------------------------------------------

# =====================================================================
# log_banner — writes a labeled section header to the log file
# =====================================================================
log_banner() {
    local log=$1 label=$2 dataset=$3 cache=$4 workload=$5 outfile=$6
    echo ""                                                       >> "$log"
    echo "########################################################" >> "$log"
    echo "# LABEL    : ${label}"                                  >> "$log"
    echo "# OBJ SIZE : ${dataset}"                                >> "$log"
    echo "# CACHE    : ${cache}"                                  >> "$log"
    echo "# WORKLOAD : ${workload}"                               >> "$log"
    echo "# OUTPUT   : $(basename ${outfile})"                    >> "$log"
    echo "# TIME     : $(date)"                                   >> "$log"
    echo "########################################################" >> "$log"
}

# =====================================================================
# stop_cluster
#   Stops Cassandra on all nodes. No wipe, no restart.
#   Used before taking snapshot to ensure data is fully flushed to disk.
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
# take_snapshot
#   Saves a hard-link copy of data/ as data_snapshot/ on each node.
#   Hard links = zero extra disk space, instant to create.
#   Cassandra must be stopped before calling this.
#   Call once per dataset after the first YCSB load.
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
# restore_from_snapshot
#   Replaces current data/ with a fresh hard-link copy from data_snapshot/.
#   Near-instant — recreates directory entries, no actual data copying.
#   Cassandra must be stopped before calling this.
#   Call for all workloads after the first (instead of YCSB load).
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
# delete_snapshot
#   Removes data_snapshot/ on all nodes after a dataset is fully done.
#   Frees the disk space held by the snapshot before loading next dataset.
#   Call after all cache_sizes x workloads for a dataset complete.
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
# restart_cluster <cache_size>
#   Rolling restart — one node at a time, seeds first.
#   Evicts page cache and starts Cassandra under cgroup memory limit.
#   Does NOT wipe data.
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
# hard_restart_cluster
#   Wipes ALL data on every node, restarts with FULL memory (no cgroup),
#   then creates the YCSB table using the pre-selected binary.
#
#   FIX: kill ALL nodes first, wipe ALL nodes, then start sequentially.
#   Prevents "already exists" UUID gossip collision that occurs when a
#   running node with old UUID sees a restarted node with new UUID.
# =====================================================================
hard_restart_cluster() {
    local nodes
    if [ "$NUM_NODES" = "3" ]; then nodes=(2 3 4); else nodes=(2 3 4 5 6); fi

    echo ""
    echo "=== HARD restart: nodes ${nodes[*]} ==="

    # ── Phase 1: Kill ALL nodes in parallel ──────────────────────────
    # All must be killed BEFORE any node is wiped+restarted.
    # A running node with old UUID causes gossip collision on the
    # freshly-wiped node that gets a new UUID → "already exists" error.
    echo "  [1/3] Killing all nodes in parallel..."
    for node in "${nodes[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill 2>/dev/null; true" &
    done
    wait

    # Wait for all to fully stop
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

    # ── Phase 2: Wipe ALL nodes in parallel ──────────────────────────
    echo "  [2/3] Wiping data on all nodes in parallel..."
    for node in "${nodes[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} "rm -rf ${CASS_DIR}/data/" &
    done
    wait
    echo "  [2/3] All nodes stopped and wiped"

    # ── Phase 3: Start nodes sequentially, seeds (2,3) first ─────────
    # Sequential start ensures each node fully joins (UN) before the
    # next starts, preventing gossip race conditions.
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

# Pre-flight: verify all create-table binaries exist and are executable
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

if echo "$EXP_LABEL" | grep -qi "rep"; then
    EXPECTED_NODES=5
else
    EXPECTED_NODES=5
fi
read -p "How many Cassandra nodes? (3 or 5): " NUM_NODES
if [ "$NUM_NODES" != "$EXPECTED_NODES" ]; then
    echo "WARNING: '$EXP_LABEL' suggests ${EXPECTED_NODES} nodes but you entered ${NUM_NODES}."
    read -p "Re-enter node count (3 or 5): " NUM_NODES
fi
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

echo "How many write threads?"
read WTHREADS
echo "How many read threads?"
read THREADS

# =====================================================================
# Outermost loop: object size datasets
#   Each iteration uses a different FIELD_LENGTH; RECORD_COUNT is
#   recomputed so the total dataset is always ~TOTAL_DB_BYTES (~70GB).
# =====================================================================
for size_idx in "${!OBJECT_SIZE_LABELS[@]}"; do
    OBJECT_SIZE_LABEL="${OBJECT_SIZE_LABELS[$size_idx]}"
    FIELD_LENGTH="${FIELD_LENGTHS[$size_idx]}"
    RECORD_COUNT=$((TOTAL_DB_BYTES / FIELD_LENGTH))

    echo ""
    echo "============================================================"
    echo ">>> Object size  : ${OBJECT_SIZE_LABEL} (${FIELD_LENGTH} bytes)"
    echo ">>> Record count : ${RECORD_COUNT}"
    echo ">>> Total size   : ~$((TOTAL_DB_BYTES / 1000000000))GB"
    echo ">>> Mode         : $([ "$HARD_RESTART_PER_WORKLOAD" = "1" ] && echo 'HARD restart per workload (snapshot)' || echo 'Normal restart per cache size')"
    echo "============================================================"

    BASE_OUT_DIR="result_OS_CacheObjectSize_${OBJECT_SIZE_LABEL}"
    mkdir -p "$BASE_OUT_DIR"
    LOG="${BASE_OUT_DIR}/${EXP_LABEL}_${OBJECT_SIZE_LABEL}_run${FIELD_LENGTH}Bytes.log"

    # Params for measurement runs.
    # No value pool — YCSB generates default (random) values from fieldlength.
    POOL_PARAMS="-p fieldlength=${FIELD_LENGTH}"

    if [ "$HARD_RESTART_PER_WORKLOAD" = "0" ]; then
        # ============================================================
        # NORMAL FLOW: load once, restart per cache size
        # ============================================================
        read -p "Wipe Cassandra data + restart in full memory, CREATE TABLE, then press Enter..."

        LOAD_FILE="${BASE_OUT_DIR}/${EXP_LABEL}_${OBJECT_SIZE_LABEL}_Load${FIELD_LENGTH}Bytes_run.scr"
        log_banner "$LOG" "$EXP_LABEL" "$OBJECT_SIZE_LABEL" "FULL_MEM" "LOAD" "$LOAD_FILE"
        echo "Load: $RECORD_COUNT records x ${FIELD_LENGTH}B (YCSB default values)..."
        $YCSB_DIR load $DB -threads $WTHREADS \
            -p recordcount=${RECORD_COUNT} \
            -p fieldlength=${FIELD_LENGTH} \
            -p measurement.raw.output_file="$LOAD_FILE" \
            -P commonworkload \
            -s >> "$LOG" 2>&1
        echo "Load done."

        for cache_size in "${CACHE_SIZES[@]}"; do
            echo ""
            echo ">>> Cache: ${cache_size}  Object size: ${OBJECT_SIZE_LABEL}"

            WARMUP_DIR="${BASE_OUT_DIR}_${cache_size}"
            mkdir -p "$WARMUP_DIR"

            restart_cluster "$cache_size"

            WARMUP_FILE="${WARMUP_DIR}/${EXP_LABEL}_${OBJECT_SIZE_LABEL}_${cache_size}_Warmup${FIELD_LENGTH}Bytes_run.scr"
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

            for i in "${!WORKLOAD_LABELS[@]}"; do
                workload="${WORKLOAD_LABELS[$i]}"
                READ_PCT="${READ_PROPORTIONS[$i]}"
                MEASURE_FILE="${WARMUP_DIR}/${EXP_LABEL}_${OBJECT_SIZE_LABEL}_${cache_size}_${workload}Run${FIELD_LENGTH}Bytes.scr"
                log_banner "$LOG" "$EXP_LABEL" "$OBJECT_SIZE_LABEL" "$cache_size" "$workload" "$MEASURE_FILE"
                echo "=== ${workload} | ${cache_size} | ${OBJECT_SIZE_LABEL} ==="
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
                echo "=== Done: ${workload} | ${cache_size} | ${OBJECT_SIZE_LABEL} ==="
            done
        done

    else
        # ============================================================
        # HARD RESTART FLOW: per workload with snapshot optimisation
        #
        # First workload of each dataset:
        #   hard_restart → YCSB load → wait compaction → drain →
        #   stop → take_snapshot → restart(size) → warmup → measure
        #
        # All subsequent workloads:
        #   stop → restore_from_snapshot → restart(size) → warmup → measure
        #
        # Snapshot uses hard links → zero extra disk, near-instant restore.
        # After all workloads for dataset complete: delete_snapshot.
        # ============================================================

        SNAPSHOT_READY=0   # reset for each new dataset

        for cache_size in "${CACHE_SIZES[@]}"; do
            echo ""
            echo ">>> Cache: ${cache_size}  Object size: ${OBJECT_SIZE_LABEL}"

            WARMUP_DIR="${BASE_OUT_DIR}_${cache_size}"
            mkdir -p "$WARMUP_DIR"

            for i in "${!WORKLOAD_LABELS[@]}"; do
                workload="${WORKLOAD_LABELS[$i]}"
                READ_PCT="${READ_PROPORTIONS[$i]}"

                echo ""
                echo "--- ${workload} | ${cache_size} | ${OBJECT_SIZE_LABEL} ---"

                if [ "$SNAPSHOT_READY" = "0" ]; then
                    # ------------------------------------------------
                    # First workload of this dataset:
                    # Full setup — load via YCSB, then take snapshot
                    # ------------------------------------------------
                    echo "First workload for ${OBJECT_SIZE_LABEL}: full load + snapshot"

                    hard_restart_cluster   # kill all + wipe all + start sequentially + create table

                    LOAD_FILE="${BASE_OUT_DIR}/${EXP_LABEL}_${OBJECT_SIZE_LABEL}_Load${FIELD_LENGTH}Bytes_run.scr"
                    log_banner "$LOG" "$EXP_LABEL" "$OBJECT_SIZE_LABEL" "FULL_MEM" "LOAD" "$LOAD_FILE"
                    echo "--- Loading ${RECORD_COUNT} records x ${FIELD_LENGTH}B (YCSB default values) ---"
                    $YCSB_DIR load $DB -threads $WTHREADS \
                        -p recordcount=${RECORD_COUNT} \
                        -p fieldlength=${FIELD_LENGTH} \
                        -p measurement.raw.output_file="$LOAD_FILE" \
                        -P commonworkload \
                        -s >> "$LOG" 2>&1
                    echo "--- Load done ---"

                    # Wait for compaction to settle before snapshot
                    # Ensures minimal SSTables → no compaction storm after restore
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

                    # Drain all nodes in parallel — flush memtable, close commitlog
                    echo "--- Draining all nodes (flushing memtables) ---"
                    if [ "$NUM_NODES" = "3" ]; then snap_nodes=(2 3 4); else snap_nodes=(2 3 4 5 6); fi
                    for node in "${snap_nodes[@]}"; do
                        ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool drain" &
                    done
                    wait
                    echo "--- Drain complete ---"

                    # Stop Cassandra, then snapshot
                    stop_cluster
                    take_snapshot
                    SNAPSHOT_READY=1

                else
                    # ------------------------------------------------
                    # Subsequent workloads: instant restore from snapshot
                    # No YCSB load needed — snapshot has all the data
                    # ------------------------------------------------
                    echo "Restoring from snapshot (instant, no reload needed)..."
                    stop_cluster              # stop Cassandra from previous run
                    restore_from_snapshot     # rm data + cp -rl snapshot → data
                fi

                # Apply cache size limit, evict page cache, start Cassandra
                restart_cluster "$cache_size"

                # Warmup: 100% read — populates OS cache, no disk growth
                WARMUP_FILE="${WARMUP_DIR}/${EXP_LABEL}_${OBJECT_SIZE_LABEL}_${cache_size}_${workload}_Warmup.scr"
                log_banner "$LOG" "$EXP_LABEL" "$OBJECT_SIZE_LABEL" "$cache_size" "${workload}_WARMUP" "$WARMUP_FILE"
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

                # Measurement
                MEASURE_FILE="${WARMUP_DIR}/${EXP_LABEL}_${OBJECT_SIZE_LABEL}_${cache_size}_${workload}Run${FIELD_LENGTH}Bytes.scr"
                log_banner "$LOG" "$EXP_LABEL" "$OBJECT_SIZE_LABEL" "$cache_size" "$workload" "$MEASURE_FILE"
                echo "=== ${workload} | ${cache_size} | ${OBJECT_SIZE_LABEL} ==="
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
                echo "=== Done: ${workload} | ${cache_size} | ${OBJECT_SIZE_LABEL} ==="
            done
        done
    fi

    # Delete snapshot after all runs for this object size complete.
    # Frees disk before next object size loads its own data.
    if [ "$HARD_RESTART_PER_WORKLOAD" = "1" ] && [ "$SNAPSHOT_READY" = "1" ]; then
        delete_snapshot
    fi

    echo ">>> Completed all runs for ${OBJECT_SIZE_LABEL}"
done

echo "All experiments completed successfully."
