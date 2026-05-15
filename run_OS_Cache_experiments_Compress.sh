#!/bin/bash
# === Config ===
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql
MEASURE_OPS=5000000
WARMUP_OPS=5000000
REPEAT=5
FIELD_LENGTH=10000
RECORD_COUNT=10000000

WORKLOAD_LABELS=("read90" "read50")
READ_PROPORTIONS=("readproportion=0.9 -p insertproportion=0.1" \
                  "readproportion=0.5 -p insertproportion=0.5")

CACHE_SIZES=("16GB" "28GB" "40GB" "52GB" "64GB")

POOL_DIR=/mydata/compressData
COMPRESS_LABELS=("jpeg" "wiki" "hdfs")
POOL_FILES=("values_pool_jpeg.txt" "values_pool_wiki.txt" "values_pool_hdfs.txt")

SSH_USER=rzp5412
CASS_DIR=/mydata/cassandra

# =====================================================================
# restart_cluster <cache_size>
#   Rolling restart — one node at a time, seeds first.
#   Uses NUM_NODES confirmed by user at startup.
# =====================================================================
restart_cluster() {
    local cache_size=$1

    local nodes
    if [ "$NUM_NODES" = "3" ]; then
        nodes=(2 3 4)
    else
        nodes=(2 3 4 5 6)
    fi

    local cache_gb="${cache_size//GB/}"
    local mem_bytes=$((cache_gb * 1024 * 1024 * 1024))

    echo ""
    echo "=== Rolling restart: nodes ${nodes[*]}, cache=${cache_size} (${mem_bytes} bytes) ==="

    for node in "${nodes[@]}"; do
        local ip="10.10.1.$node"
        echo ""
        echo "--- $ip ---"

        # [1/3] Kill — your tested command
        echo "  [1/3] Stopping Cassandra..."
        ssh ${SSH_USER}@${ip} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill 2>/dev/null; true"

        # Wait for death — exit 0 = still running, exit 1 = stopped
        # Verified: ps -a | grep java returns 0 when running, 1 when stopped
        local kill_attempts=0
        while ssh ${SSH_USER}@${ip} "ps -a | grep java > /dev/null 2>&1"; do
            sleep 10
            kill_attempts=$((kill_attempts + 1))
            echo "  Waiting for Cassandra to stop... (${kill_attempts})"
            if [ "$kill_attempts" -ge 6 ]; then
                echo "  Still running after 1 min — force killing..."
                ssh ${SSH_USER}@${ip} \
                    "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill -9 2>/dev/null; true"
                sleep 5
                break
            fi
        done
        echo "  [1/3] Stopped"

        # [2/3] Set cgroup + evict + start — your tested command
        # cd sets working dir so bin/cassandra and data/ resolve correctly
        # \$\$ → remote shell PID → Cassandra child inherits cgroup
        # ${mem_bytes} → already a number, expands locally → correct on remote
        echo "  [2/3] Setting ${cache_size} limit, evicting cache, starting Cassandra..."
        ssh ${SSH_USER}@${ip} \
            "cd ${CASS_DIR} && \
             echo ${mem_bytes} | sudo tee /sys/fs/cgroup/mylimitedgroup/memory.max && \
             echo \$\$ | sudo tee /sys/fs/cgroup/mylimitedgroup/cgroup.procs && \
             vmtouch -e data/ > /dev/null 2>&1 && \
             bin/cassandra > /dev/null 2>&1"

        # [3/3] Wait for this node to be UN
        # Verified: nodetool status | grep IP | grep -q UN returns 0 when UN
        echo "  [3/3] Waiting for ${ip} to be UN..."
        local start_attempts=0
        until ssh ${SSH_USER}@${ip} \
            "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep '${ip}' | grep -q 'UN'"; do
            sleep 10
            start_attempts=$((start_attempts + 1))
            echo "  Waiting for UN... (${start_attempts})"
            if [ "$start_attempts" -ge 30 ]; then
                echo "  ERROR: ${ip} did not become UN within 5 minutes."
                echo "  Check ${CASS_DIR}/logs/system.log on ${ip}."
                exit 1
            fi
        done
        echo "  [3/3] ${ip} is UP and NORMAL (UN)"
    done

    echo ""
    echo "=== All nodes up. Cluster ready with ${cache_size} cache. ==="
}

# =====================================================================
# Experiment setup — asked once before all loops
# =====================================================================
echo "Is this EC or REP?"
read EXP_LABEL

# Determine expected node count from label, then double-check with user
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
    echo "============================================================"
    read -p "Wipe Cassandra data + restart in full memory, CREATE TABLE, then press Enter..."

    BASE_OUT_DIR="result_OS_CacheCompress_${COMPRESS_LABEL}"
    mkdir -p "$BASE_OUT_DIR"

    # Load phase — once per compression dataset, full memory, no cgroup limit
    LOAD_FILE="${BASE_OUT_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_Load${FIELD_LENGTH}Bytes_run.scr"
    echo "Load: $RECORD_COUNT records x ${FIELD_LENGTH}B from ${COMPRESS_LABEL} pool..."
    $YCSB_DIR load $DB -threads $WTHREADS \
        -p recordcount=${RECORD_COUNT} \
        -p fieldlength=${FIELD_LENGTH} \
        -p valuepool.file=${POOL_FILE} \
        -p measurement.raw.output_file="$LOAD_FILE" \
        -P commonworkload \
        -s >> "${BASE_OUT_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_run${FIELD_LENGTH}Bytes.log" 2>&1
    echo "Load done."

    # Cache size sweep
    for cache_size in "${CACHE_SIZES[@]}"; do
        echo ""
        echo ">>> Cache: ${cache_size}  Dataset: ${COMPRESS_LABEL}"

        # Automated rolling restart with new memory limit
        restart_cluster "$cache_size"

        # Warmup — pool file included: warmup writes establish realistic data in cache
        WARMUP_DIR="${BASE_OUT_DIR}_${cache_size}"
        mkdir -p "$WARMUP_DIR"
        WARMUP_FILE="${WARMUP_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_${cache_size}_Warmup${FIELD_LENGTH}Bytes_run.scr"
        echo "--- Warmup (${cache_size}) ---"
        $YCSB_DIR run $DB -threads $THREADS \
            -p operationcount=$WARMUP_OPS \
            -p ${READ_PROPORTIONS[1]} \
            -p recordcount=${RECORD_COUNT} \
            -p fieldlength=${FIELD_LENGTH} \
            -p valuepool.file=${POOL_FILE} \
            -p measurement.raw.output_file="$WARMUP_FILE" \
            -p cassandra.writeconsistencylevel=QUORUM \
            -p cassandra.readconsistencylevel=QUORUM \
            -P commonworkload \
            -s >> "${BASE_OUT_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_run${FIELD_LENGTH}Bytes.log" 2>&1

        # Measurement runs
        # No pool file or fieldlength: measurement inserts go to throwaway keys
        # beyond recordcount, never read back — matches original working design.
        for i in "${!WORKLOAD_LABELS[@]}"; do
            workload="${WORKLOAD_LABELS[$i]}"
            READ_PCT="${READ_PROPORTIONS[$i]}"
            read_ratio=$(echo "$workload" | grep -o '[0-9]*')
            echo "=== ${workload} | ${cache_size} | ${COMPRESS_LABEL} ==="

            for iter in $(seq 1 $REPEAT); do
                MEASURE_FILE="${WARMUP_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_${cache_size}_iter_${iter}Run${FIELD_LENGTH}Bytes_read${read_ratio}run.scr"
                echo "--- Run ${iter}/${REPEAT} ---"
                $YCSB_DIR run $DB -threads $THREADS \
                    -p operationcount=$MEASURE_OPS \
                    -p ${READ_PCT} \
                    -p recordcount=${RECORD_COUNT} \
                    -p measurement.raw.output_file="$MEASURE_FILE" \
                    -p cassandra.writeconsistencylevel=QUORUM \
                    -p cassandra.readconsistencylevel=QUORUM \
                    -P commonworkload \
                    -s >> "${BASE_OUT_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_run${FIELD_LENGTH}Bytes.log" 2>&1
            done
        done
    done

    echo ">>> Completed all runs for ${COMPRESS_LABEL}"
done

echo "All experiments completed successfully."
