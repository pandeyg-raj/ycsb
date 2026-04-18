#!/bin/bash

YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql

# --- Fixed thread count for load and warmup phases ---
LOAD_THREADS=16
WARMUP_THREADS=16

# --- Thread counts to sweep during the run phase ---
# Increase these until you see read latency start to climb
SWEEP_THREADS=(4 8 12 16 20 24 28 32)

# --- Sleep between thread sweep runs to let queues drain ---
QUEUE_DRAIN_SLEEP=60   # seconds

WARMUP_OPS=5000000
MEASURE_OPS=25000000

REPEAT=1

FIELD_LENGTH=10000
RECORD_COUNT=10000000

WORKLOAD_LABELS=("read50")
READ_PROPORTIONS=("readproportion=0.5 -p insertproportion=0.5")

echo "Is this ec or rep"
read EXP_LABEL

mkdir -p benchmark
OUT_DIR=benchmark

# ============================================================
# Load phase — runs once with fixed LOAD_THREADS
# ============================================================
echo "=== Load phase: $RECORD_COUNT records, field=${FIELD_LENGTH} bytes, threads=${LOAD_THREADS} ==="
RAW_FILE="${OUT_DIR}/${EXP_LABEL}_Load${FIELD_LENGTH}Bytes.scr"
$YCSB_DIR load $DB -threads $LOAD_THREADS \
    -p recordcount=${RECORD_COUNT} \
    -p fieldlength=${FIELD_LENGTH} \
    -p measurement.raw.output_file="$RAW_FILE" \
    -P commonworkload \
    -s >> "${OUT_DIR}/${EXP_LABEL}_load.log" 2>&1
echo "=== Load phase done ==="

# ============================================================
# Warmup phase — runs once with fixed WARMUP_THREADS
# ============================================================
echo "=== Warmup phase: $WARMUP_OPS ops, threads=${WARMUP_THREADS} ==="
RAW_FILE="${OUT_DIR}/${EXP_LABEL}_Warmup${FIELD_LENGTH}Bytes.scr"
$YCSB_DIR run $DB -threads $WARMUP_THREADS \
    -p operationcount=$WARMUP_OPS \
    -p readproportion=0.5 -p insertproportion=0.5 \
    -p recordcount=${RECORD_COUNT} \
    -p measurement.raw.output_file="$RAW_FILE" \
    -P commonworkload \
    -s >> "${OUT_DIR}/${EXP_LABEL}_warmup.log" 2>&1
echo "=== Warmup phase done ==="

# ============================================================
# Run phase — sweep over thread counts
# ============================================================
for i in "${!WORKLOAD_LABELS[@]}"; do
    workload="${WORKLOAD_LABELS[$i]}"
    READ_PCT="${READ_PROPORTIONS[$i]}"

    for THREADS in "${SWEEP_THREADS[@]}"; do
        echo ""
        echo "======================================================"
        echo " SWEEP: workload=${workload}  threads=${THREADS}"
        echo "======================================================"

        for rep in $(seq 1 $REPEAT); do
            echo "--- Run ${rep} of ${REPEAT}: workload=${workload}, threads=${THREADS} ---"

            RAW_FILE="${OUT_DIR}/${EXP_LABEL}_threads${THREADS}_iter${rep}_${workload}.scr"
            LOG_FILE="${OUT_DIR}/${EXP_LABEL}_threads${THREADS}_${workload}.log"

            echo "------ Run phase: $MEASURE_OPS ops, threads=${THREADS}, workload=${READ_PCT}"
            $YCSB_DIR run $DB -threads $THREADS \
                -p operationcount=$MEASURE_OPS \
                -p ${READ_PCT} \
                -p recordcount=${RECORD_COUNT} \
                -p measurement.raw.output_file="$RAW_FILE" \
                -p cassandra.writeconsistencylevel=QUORUM \
                -p cassandra.readconsistencylevel=QUORUM \
                -P commonworkload \
                -s >> "$LOG_FILE" 2>&1
            echo "------ Run phase done: threads=${THREADS}"
        done

        echo "=== Sleeping ${QUEUE_DRAIN_SLEEP}s between thread counts to drain queues ==="
        sleep $QUEUE_DRAIN_SLEEP
    done
done

echo ""
echo "======================================================"
echo " Saturation sweep complete."
echo " Logs: ${OUT_DIR}/${EXP_LABEL}_threads<N>_<workload>.log"
echo " Raw:  ${OUT_DIR}/${EXP_LABEL}_threads<N>_iter<R>_<workload>.scr"
echo "======================================================"
