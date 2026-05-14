#!/bin/bash
# Single independent experiment.
# Called by run_master.sh — no interactive prompts.
# Can also be run standalone for a single experiment.
#
# Usage:
#   ./run_single.sh <dataset> <cache_size> <workload> <label>
#
# Examples:
#   ./run_single.sh jpeg 28GB read50 EC_ComprOn
#   ./run_single.sh wiki 40GB read90 REP_ComprOff

DATASET=$1
CACHE_SIZE=$2
WORKLOAD=$3
LABEL=$4

if [ -z "$DATASET" ] || [ -z "$CACHE_SIZE" ] || [ -z "$WORKLOAD" ] || [ -z "$LABEL" ]; then
  echo "Usage: $0 <dataset> <cache_size> <workload> <label>"
  echo "  dataset    : jpeg | wiki | hdfs"
  echo "  cache_size : 16GB | 28GB | 40GB | 52GB | 64GB"
  echo "  workload   : read90 | read50"
  echo "  label      : e.g. EC_ComprOn | REP_ComprOff"
  exit 1
fi

# =====================================================================
# Config — all hardcoded
# =====================================================================
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql
THREADS=16
FIELD_LENGTH=10000
RECORD_COUNT=8000000
WARMUP_OPS=5000000
MEASURE_OPS=20000000       # 4 x 5M — one run instead of 4 iterations

POOL_DIR=/mydata/compressData

declare -A POOL_MAP=(
  ["jpeg"]="values_pool_jpeg.txt"
  ["wiki"]="values_pool_wiki.txt"
  ["hdfs"]="values_pool_hdfs.txt"
)
declare -A WORKLOAD_MAP=(
  ["read90"]="readproportion=0.9 -p updateproportion=0.1 -p insertproportion=0"
  ["read50"]="readproportion=0.5 -p updateproportion=0.5 -p insertproportion=0"
)

POOL_FILE="${POOL_DIR}/${POOL_MAP[$DATASET]}"
READ_PCT="${WORKLOAD_MAP[$WORKLOAD]}"

if [ -z "${POOL_MAP[$DATASET]+x}" ]; then
  echo "ERROR: Unknown dataset '$DATASET'. Use: jpeg | wiki | hdfs"; exit 1
fi
if [ -z "${WORKLOAD_MAP[$WORKLOAD]+x}" ]; then
  echo "ERROR: Unknown workload '$WORKLOAD'. Use: read90 | read50"; exit 1
fi
if [ ! -f "$POOL_FILE" ]; then
  echo "ERROR: Pool file not found: $POOL_FILE"; exit 1
fi

# =====================================================================
# Output setup
# =====================================================================
OUT_DIR="result_${LABEL}_${DATASET}_${CACHE_SIZE}_${WORKLOAD}"
mkdir -p "$OUT_DIR"
LOG="${OUT_DIR}/ycsb.log"

echo "============================================================"
echo " Label      : $LABEL"
echo " Dataset    : $DATASET  ->  $POOL_FILE"
echo " Cache      : $CACHE_SIZE"
echo " Workload   : $WORKLOAD"
echo " Threads    : $THREADS (read + write)"
echo " Measure ops: $MEASURE_OPS"
echo " Output dir : $OUT_DIR"
echo "============================================================"

# =====================================================================
# [1/3] Load
# =====================================================================
echo ">>> [1/3] Load: $RECORD_COUNT records x ${FIELD_LENGTH}B"
$YCSB_DIR load $DB -threads $THREADS \
  -p recordcount=${RECORD_COUNT} \
  -p fieldlength=${FIELD_LENGTH} \
  -p fieldcount=1 \
  -p valuepool.file=${POOL_FILE} \
  -p measurement.raw.output_file="${OUT_DIR}/${LABEL}_${DATASET}_Load.scr" \
  -P commonworkload \
  -s >> "$LOG" 2>&1
echo ">>> [1/3] Load done"

# =====================================================================
# [2/3] Warmup
# =====================================================================
echo ">>> [2/3] Warmup: $WARMUP_OPS ops (50/50 update)"
$YCSB_DIR run $DB -threads $THREADS \
  -p operationcount=${WARMUP_OPS} \
  -p readproportion=0.5 -p updateproportion=0.5 -p insertproportion=0 \
  -p recordcount=${RECORD_COUNT} \
  -p fieldlength=${FIELD_LENGTH} \
  -p fieldcount=1 \
  -p valuepool.file=${POOL_FILE} \
  -p cassandra.writeconsistencylevel=QUORUM \
  -p cassandra.readconsistencylevel=QUORUM \
  -p measurement.raw.output_file="${OUT_DIR}/${LABEL}_${DATASET}_${CACHE_SIZE}_Warmup.scr" \
  -P commonworkload \
  -s >> "$LOG" 2>&1
echo ">>> [2/3] Warmup done"

# =====================================================================
# [3/3] Measurement — single run, same total ops as 4x5M
# =====================================================================
echo ">>> [3/3] Measurement: $MEASURE_OPS ops of $WORKLOAD"
$YCSB_DIR run $DB -threads $THREADS \
  -p operationcount=${MEASURE_OPS} \
  -p ${READ_PCT} \
  -p recordcount=${RECORD_COUNT} \
  -p fieldlength=${FIELD_LENGTH} \
  -p fieldcount=1 \
  -p valuepool.file=${POOL_FILE} \
  -p cassandra.writeconsistencylevel=QUORUM \
  -p cassandra.readconsistencylevel=QUORUM \
  -p measurement.raw.output_file="${OUT_DIR}/${LABEL}_${DATASET}_${CACHE_SIZE}_${WORKLOAD}.scr" \
  -P commonworkload \
  -s >> "$LOG" 2>&1
echo ">>> [3/3] Measurement done"

echo "============================================================"
echo " DONE: $LABEL | $DATASET | $CACHE_SIZE | $WORKLOAD"
echo " Results : $OUT_DIR"
echo " YCSB log: $LOG"
echo "============================================================"
