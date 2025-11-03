#!/bin/bash

YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql

FIELD_LENGTH=10000
RECORD_COUNT=4800000
WARMUP_OPS=5000000
declare -A WORKLOADS

echo "Is this ec or rep"
read EXP_LABEL

echo "How Many Write threads"
read WTHREADS

echo "How Many Read threads"
read THREADS

OUT_DIR=mix_Insert_Only
mkdir -p "$OUT_DIR"

RAW_FILE="${OUT_DIR}/${EXP_LABEL}_Load${FIELD_LENGTH}Bytes_run.scr"

# Load phase once
echo "Load phase: Loading $RECORD_COUNT records of size ${FIELD_LENGTH} bytes"
$YCSB_DIR load $DB -threads $WTHREADS \
-p recordcount=${RECORD_COUNT} \
-p fieldlength=${FIELD_LENGTH} \
-p measurement.raw.output_file="$RAW_FILE" \
-P mixcommonworkload \
-s >> "${OUT_DIR}/${EXP_LABEL}_run${FIELD_LENGTH}Bytes.log" 2>&1

echo "Load phase: Done $RECORD_COUNT records of size ${FIELD_LENGTH} bytes"

# Warmup phase once mix workload
echo "------ Warmup phase: $WARMUP_OPS ops of size ${FIELD_LENGTH} bytes"
RAW_FILE="${OUT_DIR}/${EXP_LABEL}_Warmup${FIELD_LENGTH}Bytes_run.scr"

$YCSB_DIR run $DB -threads $THREADS \
-p operationcount=$WARMUP_OPS \
-p readproportion=0.5 -p insertproportion=0.5 \
-p recordcount=${RECORD_COUNT} \
-p measurement.raw.output_file="$RAW_FILE" \
-P mixcommonworkload \
-s >> "${OUT_DIR}/${EXP_LABEL}_run${FIELD_LENGTH}Bytes.log" 2>&1
echo "------ Warmup phase done: $WARMUP_OPS ops of size ${FIELD_LENGTH} bytes"

echo "Warmup done Run EC read and REP read scripts " 
