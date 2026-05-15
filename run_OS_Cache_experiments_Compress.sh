#!/bin/bash
# === Config ===
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql
MEASURE_OPS=5000000
WARMUP_OPS=5000000
FIELD_LENGTH=10000
RECORD_COUNT=10000000

# Standard YCSB workloads A, B, C using updateproportion (updates existing keys)
# updateproportion → writes go to existing keys → disk stays flat, correct cache behavior
WORKLOAD_LABELS=("workloadC" "workloadB" "workloadA" "workloadD")
READ_PROPORTIONS=(
    "readproportion=1.0  -p updateproportion=0.0 -p insertproportion=0"                          # C: read only
    "readproportion=0.95 -p updateproportion=0.05 -p insertproportion=0"                          # B: read mostly
    "readproportion=0.5  -p updateproportion=0.5  -p insertproportion=0"                          # A: update heavy
    "readproportion=0.95 -p updateproportion=0.0  -p insertproportion=0.05 -p requestdistrib=latest"  # D: read latest
)

CACHE_SIZES=("16GB" "28GB" "40GB" "52GB" "64GB")

POOL_DIR=/mydata/compressData
COMPRESS_LABELS=("jpeg" "wiki" "hdfs")
POOL_FILES=("values_pool_jpeg.txt" "values_pool_wiki.txt" "values_pool_hdfs.txt")

SSH_USER=rzp5412
CASS_DIR=/mydata/cassandra

# ---------------------------------------------------------------
# HARD_RESTART_PER_WORKLOAD controls experiment flow:
#   0 → Normal:  load once per dataset, restart per cache size,
#                run all workloads on the same data
#   1 → Hard:    before each workload, wipe cluster + reload +
#                warmup (saves disk space, each workload starts fresh)
HARD_RESTART_PER_WORKLOAD=0
# ---------------------------------------------------------------

# =====================================================================
# log_banner — writes a labeled section header to the log file
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
    echo "=== Rolling restart: nodes ${nodes[*]}, cache=${cache_size} (${mem_bytes} bytes) ==="

    for node in "${nodes[@]}"; do
        local ip="10.10.1.$node"
        echo ""
        echo "--- $ip ---"

        # [1/3] Kill — verified: SIGTERM, exits 0
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
                echo "  Still running — force killing..."
                ssh ${SSH_USER}@${ip} \
                    "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill -9 2>/dev/null; true"
                sleep 5
                break
            fi
        done
        echo "  [1/3] Stopped"

        # [2/3] Set cgroup + evict page cache + start
        echo "  [2/3] Setting ${cache_size} limit, evicting cache, starting Cassandra..."
        ssh ${SSH_USER}@${ip} \
            "cd ${CASS_DIR} && \
             echo ${mem_bytes} | sudo tee /sys/fs/cgroup/mylimitedgroup/memory.max && \
             echo \$\$ | sudo tee /sys/fs/cgroup/mylimitedgroup/cgroup.procs && \
             vmtouch -e data/ > /dev/null 2>&1 && \
             bin/cassandra > /dev/null 2>&1"

        # [3/3] Wait for UN
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
    echo "=== All nodes up. Cluster ready with ${cache_size} cache. ==="
}

# =====================================================================
# hard_restart_cluster <cache_size>
#   Wipes ALL data on every node, restarts under cgroup memory limit,
#   then creates the YCSB table using the pre-selected binary.
#   Does NOT load data — caller is responsible for loading after.
#
#   Uses CREATE_TABLE_BIN set at startup based on EC/REP + compression.
#   No vmtouch needed — data was deleted so nothing to evict.
# =====================================================================
hard_restart_cluster() {
    local cache_size=$1
    local nodes
    if [ "$NUM_NODES" = "3" ]; then nodes=(2 3 4); else nodes=(2 3 4 5 6); fi

    local cache_gb="${cache_size//GB/}"
    local mem_bytes=$((cache_gb * 1024 * 1024 * 1024))

    echo ""
    echo "=== HARD restart: wipe + restart nodes ${nodes[*]}, cache=${cache_size} ==="

    for node in "${nodes[@]}"; do
        local ip="10.10.1.$node"
        echo ""
        echo "--- $ip ---"

        # [1/4] Kill
        echo "  [1/4] Stopping Cassandra..."
        ssh ${SSH_USER}@${ip} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill 2>/dev/null; true"

        local kill_attempts=0
        while ssh ${SSH_USER}@${ip} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' > /dev/null 2>&1"; do
            sleep 10
            kill_attempts=$((kill_attempts + 1))
            echo "  Waiting for stop... (${kill_attempts}/6)"
            if [ "$kill_attempts" -ge 6 ]; then
                echo "  Still running — force killing..."
                ssh ${SSH_USER}@${ip} \
                    "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill -9 2>/dev/null; true"
                sleep 5
                break
            fi
        done
        echo "  [1/4] Stopped"

        # [2/4] Wipe data — rm -rf data/ covers all Cassandra data on this node
        echo "  [2/4] Wiping data on ${ip}..."
        ssh ${SSH_USER}@${ip} "rm -rf ${CASS_DIR}/data/"
        echo "  [2/4] Data wiped"

        # [3/4] Set cgroup + start (no vmtouch — nothing to evict after wipe)
        echo "  [3/4] Setting ${cache_size} limit and starting Cassandra..."
        ssh ${SSH_USER}@${ip} \
            "cd ${CASS_DIR} && \
             echo ${mem_bytes} | sudo tee /sys/fs/cgroup/mylimitedgroup/memory.max && \
             echo \$\$ | sudo tee /sys/fs/cgroup/mylimitedgroup/cgroup.procs && \
             bin/cassandra > /dev/null 2>&1"

        # [4/4] Wait for UN
        echo "  [4/4] Waiting for ${ip} to be UN..."
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
        echo "  [4/4] ${ip} is UP and NORMAL (UN)"
    done

    # [5] Create YCSB table using the binary selected at startup
    # Binary connects internally to all nodes — no arguments needed
    echo ""
    echo "  [5] Creating YCSB table via /mydata/${CREATE_TABLE_BIN} ..."
    /mydata/${CREATE_TABLE_BIN}
    echo "  [5] Table created"

    echo ""
    echo "=== HARD restart complete. Cluster wiped and ready with ${cache_size} cache. ==="
    echo "=== Remember to load data before running workloads. ==="
}

# =====================================================================
# Experiment setup — asked once before all loops
# =====================================================================
echo "Is this EC or REP?"
read EXP_LABEL

# Node count double-check
if echo "$EXP_LABEL" | grep -qi "rep"; then
    EXPECTED_NODES=3
else
    EXPECTED_NODES=5
fi
read -p "How many Cassandra nodes? (3 or 5): " NUM_NODES
if [ "$NUM_NODES" != "$EXPECTED_NODES" ]; then
    echo "WARNING: '$EXP_LABEL' suggests ${EXPECTED_NODES} nodes but you entered ${NUM_NODES}."
    read -p "Re-enter node count (3 or 5): " NUM_NODES
fi
echo "Confirmed: using ${NUM_NODES} nodes."

# Compression — determines which create-table binary to use
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
# Outermost loop: compression datasets
# =====================================================================
for compress_idx in "${!COMPRESS_LABELS[@]}"; do
    COMPRESS_LABEL="${COMPRESS_LABELS[$compress_idx]}"
    POOL_FILE="${POOL_DIR}/${POOL_FILES[$compress_idx]}"

    echo ""
    echo "============================================================"
    echo ">>> Compression dataset : ${COMPRESS_LABEL}"
    echo ">>> Pool file           : ${POOL_FILE}"
    echo ">>> Mode                : $([ "$HARD_RESTART_PER_WORKLOAD" = "1" ] && echo 'HARD restart per workload' || echo 'Normal restart per cache size')"
    echo "============================================================"

    BASE_OUT_DIR="result_OS_CacheCompress_${COMPRESS_LABEL}"
    mkdir -p "$BASE_OUT_DIR"
    LOG="${BASE_OUT_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_run${FIELD_LENGTH}Bytes.log"

    # ---------------------------------------------------------------
    # Pool params for measurement runs.
    # Comment line 1 and uncomment line 2 to disable if disk is tight.
    POOL_PARAMS="-p fieldlength=${FIELD_LENGTH} -p valuepool.file=${POOL_FILE}"
    # POOL_PARAMS=""
    # ---------------------------------------------------------------

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
            echo ">>> Cache: ${cache_size}  Dataset: ${COMPRESS_LABEL}"

            WARMUP_DIR="${BASE_OUT_DIR}_${cache_size}"
            mkdir -p "$WARMUP_DIR"

            restart_cluster "$cache_size"

            # Warmup: 100% read — populates OS cache, no disk growth
            WARMUP_FILE="${WARMUP_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_${cache_size}_Warmup${FIELD_LENGTH}Bytes_run.scr"
            log_banner "$LOG" "$EXP_LABEL" "$COMPRESS_LABEL" "$cache_size" "WARMUP" "$WARMUP_FILE"
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

            # Measurement: one run per workload
            for i in "${!WORKLOAD_LABELS[@]}"; do
                workload="${WORKLOAD_LABELS[$i]}"
                READ_PCT="${READ_PROPORTIONS[$i]}"

                MEASURE_FILE="${WARMUP_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_${cache_size}_${workload}Run${FIELD_LENGTH}Bytes.scr"
                log_banner "$LOG" "$EXP_LABEL" "$COMPRESS_LABEL" "$cache_size" "$workload" "$MEASURE_FILE"
                echo "=== ${workload} | ${cache_size} | ${COMPRESS_LABEL} ==="
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
            done
        done

    else
        # ============================================================
        # HARD RESTART FLOW: per workload — wipe + reload + warmup + measure
        # Each workload starts from a clean cluster to save disk space.
        # ============================================================
        for cache_size in "${CACHE_SIZES[@]}"; do
            echo ""
            echo ">>> Cache: ${cache_size}  Dataset: ${COMPRESS_LABEL}"

            WARMUP_DIR="${BASE_OUT_DIR}_${cache_size}"
            mkdir -p "$WARMUP_DIR"

            for i in "${!WORKLOAD_LABELS[@]}"; do
                workload="${WORKLOAD_LABELS[$i]}"
                READ_PCT="${READ_PROPORTIONS[$i]}"

                echo ""
                echo "--- Hard restart for: ${workload} | ${cache_size} | ${COMPRESS_LABEL} ---"

                # Wipe cluster, restart with cache limit, create table
                hard_restart_cluster "$cache_size"

                # Reload data for this dataset
                RELOAD_FILE="${WARMUP_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_${cache_size}_${workload}_Load.scr"
                log_banner "$LOG" "$EXP_LABEL" "$COMPRESS_LABEL" "$cache_size" "${workload}_LOAD" "$RELOAD_FILE"
                echo "--- Loading data for ${workload} ---"
                $YCSB_DIR load $DB -threads $WTHREADS \
                    -p recordcount=${RECORD_COUNT} \
                    -p fieldlength=${FIELD_LENGTH} \
                    -p valuepool.file=${POOL_FILE} \
                    -p measurement.raw.output_file="$RELOAD_FILE" \
                    -P commonworkload \
                    -s >> "$LOG" 2>&1
                echo "--- Load done ---"

                # Warmup: 100% read — populates OS cache, no disk growth
                WARMUP_FILE="${WARMUP_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_${cache_size}_${workload}_Warmup.scr"
                log_banner "$LOG" "$EXP_LABEL" "$COMPRESS_LABEL" "$cache_size" "${workload}_WARMUP" "$WARMUP_FILE"
                echo "--- Warmup for ${workload} ---"
                $YCSB_DIR run $DB -threads $THREADS \
                    -p operationcount=$WARMUP_OPS \
                    -p readproportion=1.0 -p updateproportion=0.0 -p insertproportion=0.0 \
                    -p recordcount=${RECORD_COUNT} \
                    -p measurement.raw.output_file="$WARMUP_FILE" \
                    -p cassandra.writeconsistencylevel=QUORUM \
                    -p cassandra.readconsistencylevel=QUORUM \
                    -P commonworkload \
                    -s >> "$LOG" 2>&1

                # Measure
                MEASURE_FILE="${WARMUP_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_${cache_size}_${workload}Run${FIELD_LENGTH}Bytes.scr"
                log_banner "$LOG" "$EXP_LABEL" "$COMPRESS_LABEL" "$cache_size" "$workload" "$MEASURE_FILE"
                echo "=== ${workload} | ${cache_size} | ${COMPRESS_LABEL} ==="
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
            done
        done
    fi

    echo ">>> Completed all runs for ${COMPRESS_LABEL}"
done

echo "All experiments completed successfully."
