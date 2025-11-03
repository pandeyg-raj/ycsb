#!/bin/bash

YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql
WARMUP_OPS=50000
MEASURE_OPS=10000
REPEAT=5

FIELD_LENGTH=10000
RECORD_COUNT=10000000

declare -A WORKLOADS

WORKLOAD_LABELS=("scanRange" )
READ_PROPORTIONS=("scanproportion=0.95 -p insertproportion=0.05")

echo "Is this ec or rep"
read EXP_LABEL

echo "How Many Write threads"
read WTHREADS

echo "How Many Read threads"
read THREADS

mkdir -p scanRange

OUT_DIR=scanRange
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



done
