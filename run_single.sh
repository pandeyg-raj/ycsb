#!/bin/bash
# Single independent experiment.
#
# Standalone usage (interactive):
#   ./run_single.sh jpeg 28GB read50 EC_ComprOn
#
# Called from master script (non-interactive):
#   ./run_single.sh jpeg 28GB read50 EC_ComprOn <wthreads> <rthreads>
#   ./run_single.sh jpeg 28GB read50 EC_ComprOn 8 16

DATASET=$1
CACHE_SIZE=$2
WORKLOAD=$3
LABEL=$4
WTHREADS=$5    # optional — prompted if missing
RTHREADS=$6    # optional — prompted if missing

# =====================================================================
# Validate required args
# =====================================================================
if [ -z "$DATASET" ] || [ -z "$CACHE_SIZE" ] || [ -z "$WORKLOAD" ] || [ -z "$LABEL" ]; then
  echo "Usage: $0 <dataset> <cache_size> <workload> <label> [wthreads] [rthreads]"
  echo "  dataset    : jpeg | wiki | hdfs"
  echo "  cache_size : 16GB | 28GB | 40GB | 52GB | 64GB"
  echo "  workload   : read90 | read50"
  echo "  label      : e.g. EC_ComprOn | REP_ComprOff"
  echo "  wthreads   : write threads (prompted if omitted)"
  echo "  rthreads   : read threads  (prompted if omitted)"
  exit 1
fi

# Prompt for threads only if not passed in
if [ -z "$WTHREADS" ]; then
  echo "How many write threads?"; read WTHREADS
fi
if [ -z "$RTHREADS" ]; then
  echo "How many read threads?"; read RTHREADS
fi

# =====================================================================
# Config
# =====================================================================
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql
FIELD_LENGTH=10000
RECORD_COUNT=8000000
WARMUP_OPS=5000000
MEASURE_OPS=5000000
REPEAT=4

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
echo " Workload   : $WORKLOAD  ->  $READ_PCT"
echo " Threads    : write=$WTHREADS  read=$RTHREADS"
echo " Output dir : $OUT_DIR"
echo "============================================================"

# =====================================================================
# Load
# =====================================================================
echo ">>> [1/3] Load: $RECORD_COUNT records x ${FIELD_LENGTH}B"
$YCSB_DIR load $DB -threads $WTHREADS \
  -p recordcount=${RECORD_COUNT} \
  -p fieldlength=${FIELD_LENGTH} \
  -p fieldcount=1 \
  -p valuepool.file=${POOL_FILE} \
  -p measurement.raw.output_file="${OUT_DIR}/${LABEL}_${DATASET}_Load.scr" \
  -P commonworkload \
  -s >> "$LOG" 2>&1
echo ">>> [1/3] Load done"

# =====================================================================
# Warmup
# =====================================================================
echo ">>> [2/3] Warmup: $WARMUP_OPS ops (50/50 update)"
$YCSB_DIR run $DB -threads $RTHREADS \
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
# Measurement
# =====================================================================
echo ">>> [3/3] Measurement: $REPEAT x $WORKLOAD"
for iter in $(seq 1 $REPEAT); do
  echo "---  Run ${iter}/${REPEAT}"
  $YCSB_DIR run $DB -threads $RTHREADS \
    -p operationcount=${MEASURE_OPS} \
    -p ${READ_PCT} \
    -p recordcount=${RECORD_COUNT} \
    -p fieldlength=${FIELD_LENGTH} \
    -p fieldcount=1 \
    -p valuepool.file=${POOL_FILE} \
    -p cassandra.writeconsistencylevel=QUORUM \
    -p cassandra.readconsistencylevel=QUORUM \
    -p measurement.raw.output_file="${OUT_DIR}/${LABEL}_${DATASET}_${CACHE_SIZE}_${WORKLOAD}_iter${iter}.scr" \
    -P commonworkload \
    -s >> "$LOG" 2>&1
  echo "---  Run ${iter}/${REPEAT} done"
done

echo "============================================================"
echo " DONE: $LABEL | $DATASET | $CACHE_SIZE | $WORKLOAD"
echo " Results : $OUT_DIR"
echo " YCSB log: $LOG"
echo "============================================================"
