#!/bin/bash

# === Config ===
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql
MEASURE_OPS=5000000
WARMUP_OPS=5000000
REPEAT=5
FIELD_LENGTH=1000
RECORD_COUNT=100000000

# Workloads definition
WORKLOAD_LABELS=("read100" "read95" "read50")
READ_PROPORTIONS=("readproportion=1 -p insertproportion=0" \
                  "readproportion=0.95 -p insertproportion=0.05" \
                  "readproportion=0.5 -p insertproportion=0.5")

# OS cache sizes (in GB)
CACHE_SIZES=("16GB" "28GB" "40GB" "52GB" "64GB")

# === Experiment setup ===
echo "Is this EC or REP?"
read EXP_LABEL

echo "How many write threads?"
read WTHREADS

echo "How many read threads?"
read THREADS

BASE_OUT_DIR="result_OS_CacheRep3way"
mkdir -p "$BASE_OUT_DIR"

# === Load phase (once) ===
LOAD_FILE="${BASE_OUT_DIR}/${EXP_LABEL}_Load${FIELD_LENGTH}Bytes_run.scr"
echo "Load phase: Loading $RECORD_COUNT records of size ${FIELD_LENGTH} bytes..."
$YCSB_DIR load $DB -threads $WTHREADS \
  -p recordcount=${RECORD_COUNT} \
  -p fieldlength=${FIELD_LENGTH} \
  -p measurement.raw.output_file="$LOAD_FILE" \
  -P commonworkload \
  -s >> "${BASE_OUT_DIR}/${EXP_LABEL}_run${FIELD_LENGTH}Bytes.log" 2>&1
echo "Load phase done: ${LOAD_FILE}"

# === Main experiment ===
for cache_size in "${CACHE_SIZES[@]}"; do
  echo
  echo ">>> Prepare Cassandra with ${cache_size} OS cache"
  read -p "Start Cassandra with ${cache_size} and press Enter to continue..."

  # --- Warm-up phase (once per cache size) ---
  WARMUP_FILE="${BASE_OUT_DIR}_${cache_size}/${EXP_LABEL}_${cache_size}_Warmup${FIELD_LENGTH}Bytes_run.scr"
  mkdir -p "$(dirname "$WARMUP_FILE")"
  echo "--- Warm-up phase (${cache_size}) ---"
  $YCSB_DIR run $DB -threads $THREADS \
    -p operationcount=$WARMUP_OPS \
    -p ${READ_PROPORTIONS[2]} \
    -p recordcount=${RECORD_COUNT} \
    -p measurement.raw.output_file="$WARMUP_FILE" \
    -p cassandra.writeconsistencylevel=QUORUM \
    -p cassandra.readconsistencylevel=QUORUM \
    -P commonworkload \
    -s >> "${BASE_OUT_DIR}/${EXP_LABEL}_run${FIELD_LENGTH}Bytes.log" 2>&1

  # --- Measurement runs for all workloads ---
  for i in "${!WORKLOAD_LABELS[@]}"; do
    workload="${WORKLOAD_LABELS[$i]}"
    READ_PCT="${READ_PROPORTIONS[$i]}"
    read_ratio=$(echo "$workload" | grep -o '[0-9]*')

    echo "========================================"
    echo "Starting workload: ${workload} (cache ${cache_size})"
    echo "========================================"

    for iter in $(seq 1 $REPEAT); do
      MEASURE_FILE="${BASE_OUT_DIR}_${cache_size}/${EXP_LABEL}_${cache_size}_iter_${iter}Run${FIELD_LENGTH}Bytes_read${read_ratio}run.scr"
      echo "--- Measurement run ${iter} (${cache_size}, ${workload}) ---"
      $YCSB_DIR run $DB -threads $THREADS \
        -p operationcount=$MEASURE_OPS \
        -p ${READ_PCT} \
        -p recordcount=${RECORD_COUNT} \
        -p measurement.raw.output_file="$MEASURE_FILE" \
        -p cassandra.writeconsistencylevel=QUORUM \
        -p cassandra.readconsistencylevel=QUORUM \
        -P commonworkload \
        -s >> "${BASE_OUT_DIR}/${EXP_LABEL}_run${FIELD_LENGTH}Bytes.log" 2>&1
    done
  done
done

echo "All experiments completed successfully."
