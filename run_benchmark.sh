#!/bin/bash

YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql

REPEAT=1

WORKLOAD_LABELS=("read50")

echo "Is this ec or rep"
read EXP_LABEL

echo "How Many Write threads (for load phase)"
read WTHREADS

echo "How Many Read/Run threads"
read THREADS

mkdir -p benchmark
OUT_DIR=benchmark

NODES=("10.10.1.2" "10.10.1.3" "10.10.1.4" "10.10.1.5" "10.10.1.6")
SSH_USER="rzp5412"
COLLECTOR_SCRIPT="/mydata/collector.sh"
LABEL="${EXP_LABEL}_$(date +%Y%m%d_%H%M%S)"
STOPFLAG_REMOTE="/mydata/cassandra_metrics/${LABEL}.stop"
RESULTS_DIR="/mydata/results/${LABEL}"
mkdir -p "$RESULTS_DIR"

# ============================================================
# Load phase — as fast as possible, consistency ALL
# (commonworkload already sets writeconsistencylevel=ALL)
# ============================================================
echo "=== Load phase: trace-accurate value sizes, consistency ALL, threads=${WTHREADS} ==="
RAW_FILE="${OUT_DIR}/${EXP_LABEL}_Load_run.scr"

$YCSB_DIR load $DB -threads $WTHREADS \
    -P commonworkload \
    -p trace.load.valuesize=trace \
    -p measurement.raw.output_file="$RAW_FILE" \
    -s >> "${OUT_DIR}/${EXP_LABEL}_load.log" 2>&1

echo "=== Load phase done ==="

# ============================================================
# Start collectors on all nodes after load
# ============================================================
echo "=== Starting collectors on replica nodes ==="
for NODE in "${NODES[@]}"; do
    ssh "${SSH_USER}@${NODE}" \
        "nohup bash ${COLLECTOR_SCRIPT} ${LABEL} > /mydata/collector_${LABEL}.log 2>&1 &"
    echo "  [OK] collector started on $NODE"
done
sleep 3

# ============================================================
# Run phase — trace timing + sizes, consistency QUORUM
# ============================================================
for i in "${!WORKLOAD_LABELS[@]}"; do
    workload="${WORKLOAD_LABELS[$i]}"

    for rep in $(seq 1 $REPEAT); do
        echo "--- Run ${rep} of ${REPEAT}: workload=${workload}, threads=${THREADS} ---"

        RAW_FILE="${OUT_DIR}/${EXP_LABEL}_iter_${rep}_${workload}_run.scr"

        echo "------ Run phase: trace replay, valuesize=trace, QUORUM"
        $YCSB_DIR run $DB -threads $THREADS \
            -P commonworkload \
            -p trace.valuesize=trace \
            -p cassandra.writeconsistencylevel=QUORUM \
            -p cassandra.readconsistencylevel=QUORUM \
            -p measurement.raw.output_file="$RAW_FILE" \
            -s >> "${OUT_DIR}/${EXP_LABEL}_run_${workload}.log" 2>&1
        echo "------ Run phase done"
    done

    # --- Write stop flag on every node ---
    for NODE in "${NODES[@]}"; do
        ssh "${SSH_USER}@${NODE}" "touch ${STOPFLAG_REMOTE}"
        echo "  [OK] stop flag written on $NODE"
    done

    # Give collectors a moment to finish current snapshot and flush
    sleep 15

    # --- Pull results from all nodes ---
    echo "=== Pulling results from nodes ==="
    for NODE in "${NODES[@]}"; do
        mkdir -p "${RESULTS_DIR}/${NODE}"
        scp -r "${SSH_USER}@${NODE}:/mydata/cassandra_metrics/${LABEL}/" \
            "${RESULTS_DIR}/${NODE}/"
        echo "  [OK] pulled from $NODE"
    done

    echo ""
    echo "=== All results saved to: ${RESULTS_DIR} ==="
    echo "=== Run: bash analyze.sh ${RESULTS_DIR} ==="

done
