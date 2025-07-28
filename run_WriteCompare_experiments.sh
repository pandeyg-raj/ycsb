#!/bin/bash

YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql
WARMUP_OPS=5000000
MEASURE_OPS=5000000
REPEAT=5
FIELD_LENGTH=10000
RECORD_COUNT=10000000

declare -A WORKLOADS

WORKLOAD_LABELS=("read0")
READ_PROPORTIONS=("readproportion=0.0 -p insertproportion=1.0")

echo "Is this ec or rep"
read EXP_LABEL

echo "How Many Write threads"
read WTHREADS

echo "How Many Read threads"
read THREADS

mkdir -p result_WriteCompare_Zif
OUT_DIR=result_WriteCompare_Zif

RAW_FILE="${OUT_DIR}/${EXP_LABEL}_Load${FIELD_LENGTH}Bytes_run.scr"

# Load phase once
echo "Load phase: Loading $RECORD_COUNT records of size ${FIELD_LENGTH} bytes"
$YCSB_DIR load $DB -threads $WTHREADS \
-p recordcount=${RECORD_COUNT} \
-p fieldlength=${FIELD_LENGTH} \
-p measurement.raw.output_file="$RAW_FILE" \
-P commonworkload \
-s >> "${OUT_DIR}/${EXP_LABEL}_run${FIELD_LENGTH}Bytes.log" 2>&1

echo "Load phase: Done $RECORD_COUNT records of size ${FIELD_LENGTH} bytes"
sleep 60

for i in "${!WORKLOAD_LABELS[@]}"; do
  workload="${WORKLOAD_LABELS[$i]}"
  READ_PCT="${READ_PROPORTIONS[$i]}"
  echo "Workload: $workload "

  # Warmup phase once mix workload
  echo "------ Warmup phase: $WARMUP_OPS ops of size ${FIELD_LENGTH} bytes"
  RAW_FILE="${OUT_DIR}/${EXP_LABEL}_Warmup${FIELD_LENGTH}Bytes_run.scr"

  $YCSB_DIR run $DB -threads $THREADS \
  -p operationcount=$WARMUP_OPS \
  -p ${READ_PCT} \
  -p recordcount=${RECORD_COUNT} \
  -p measurement.raw.output_file="$RAW_FILE" \
  -P commonworkload \
  -s >> "${OUT_DIR}/${EXP_LABEL}_run${FIELD_LENGTH}Bytes.log" 2>&1
  echo "------ Warmup phase done: $WARMUP_OPS ops of size ${FIELD_LENGTH} bytes"
  sleep 30

  for i in $(seq 1 $REPEAT); do
    echo "--- Run $i of $workload ---" 

    RAW_FILE="${OUT_DIR}/${EXP_LABEL}_iter_${i}Run${FIELD_LENGTH}Bytes_${workload}run.scr"
    # Measurement phase with workload mix
    echo "------ Run phase: $MEASURE_OPS ops of ${READ_PCT}"
    $YCSB_DIR run $DB -threads $THREADS \
    -p operationcount=$MEASURE_OPS \
    -p ${READ_PCT} \
    -p recordcount=${RECORD_COUNT} \
    -p measurement.raw.output_file="$RAW_FILE" \
    -P commonworkload \
    -s >> "${OUT_DIR}/${EXP_LABEL}_run${FIELD_LENGTH}Bytes.log" 2>&1
    echo "------ Run phase done: $MEASURE_OPS ops of ${READ_PCT}"
  done

done
echo "16 threads"