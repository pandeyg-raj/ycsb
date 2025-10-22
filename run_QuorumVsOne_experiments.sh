#!/bin/bash

YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql
WARMUP_OPS=5000000
MEASURE_OPS=5000000
REPEAT=5

FIELD_LENGTH=10000
RECORD_COUNT=10000000

declare -A WORKLOADS

WORKLOAD_LABELS=("read100" "read95" "read50")
READ_PROPORTIONS=("readproportion=1 -p insertproportion=0" \
                "readproportion=0.95 -p insertproportion=0.05" \
                "readproportion=0.5 -p insertproportion=0.5")

echo "Is this ec or rep"
read EXP_LABEL

echo "How Many Write threads"
read WTHREADS

echo "How Many Read threads"
read THREADS

mkdir -p QuorumVsOne_10KBRep5way5node
OUT_DIR=QuorumVsOne_10KBRep5way5node
RAW_FILE="${OUT_DIR}/${EXP_LABEL}_Load${FIELD_LENGTH}Bytes_run.scr"

# -------------------- LOAD PHASE --------------------
echo "Load phase: Loading $RECORD_COUNT records of size ${FIELD_LENGTH} bytes"
$YCSB_DIR load $DB -threads $WTHREADS \
  -p recordcount=${RECORD_COUNT} \
  -p fieldlength=${FIELD_LENGTH} \
  -p measurement.raw.output_file="$RAW_FILE" \
  -P commonworkload \
  -s >> "${OUT_DIR}/${EXP_LABEL}_run${FIELD_LENGTH}Bytes.log" 2>&1

echo "Load phase: Done $RECORD_COUNT records of size ${FIELD_LENGTH} bytes"

breakdownresult="QuorumVsOne_10KBRep5way5node_${EXP_LABEL}_summary.txt"
touch "$breakdownresult"

echo "Insert done collecting data" >> "$breakdownresult"

for node in {2..6}; do
  ssh rzp5412@10.10.1.$node "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
done

# -------------------- WARMUP PHASE --------------------
echo "------ Warmup phase: $WARMUP_OPS ops of size ${FIELD_LENGTH} bytes"
RAW_FILE="${OUT_DIR}/${EXP_LABEL}_Warmup${FIELD_LENGTH}Bytes_run.scr"

$YCSB_DIR run $DB -threads $THREADS \
  -p operationcount=$WARMUP_OPS \
  -p readproportion=0.5 -p insertproportion=0.5 \
  -p recordcount=${RECORD_COUNT} \
  -p measurement.raw.output_file="$RAW_FILE" \
  -P commonworkload \
  -s >> "${OUT_DIR}/${EXP_LABEL}_run${FIELD_LENGTH}Bytes.log" 2>&1

echo "------ Warmup phase done: $WARMUP_OPS ops of size ${FIELD_LENGTH} bytes"
echo "Warmup done collecting data" >> "$breakdownresult"

for node in {2..6}; do
  ssh rzp5412@10.10.1.$node "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
done

# -------------------- MEASUREMENT PHASE --------------------
for i in "${!WORKLOAD_LABELS[@]}"; do
  workload="${WORKLOAD_LABELS[$i]}"
  READ_PCT="${READ_PROPORTIONS[$i]}"
  echo "Workload: $workload"

  # Default consistency levels
  WRITE_CL="QUORUM"
  READ_CL="QUORUM"

  # If EXP_LABEL is not ec/EC and this is the 3rd workload (read50)
  if [[ ! "$EXP_LABEL" =~ ^([eE][cC])$ ]] && [[ "$i" -eq 2 ]]; then
    WRITE_CL="ONE"
    READ_CL="ONE"
    echo "??  Detected non-EC experiment ($EXP_LABEL). Using ONE for read/write consistency in 0.5 read proportion workload."
  fi

  for iter in $(seq 1 $REPEAT); do
    echo "--- Run $iter of $workload ---"

    RAW_FILE="${OUT_DIR}/${EXP_LABEL}_iter_${iter}Run${FIELD_LENGTH}Bytes_${workload}run.scr"
    echo "------ Run phase: $MEASURE_OPS ops of ${READ_PCT} with writeCL=${WRITE_CL} readCL=${READ_CL}"

    $YCSB_DIR run $DB -threads $THREADS \
      -p operationcount=$MEASURE_OPS \
      -p ${READ_PCT} \
      -p recordcount=${RECORD_COUNT} \
      -p measurement.raw.output_file="$RAW_FILE" \
      -p cassandra.writeconsistencylevel=${WRITE_CL} \
      -p cassandra.readconsistencylevel=${READ_CL} \
      -P commonworkload \
      -s >> "${OUT_DIR}/${EXP_LABEL}_run${FIELD_LENGTH}Bytes.log" 2>&1

    echo "------ Run phase done: $MEASURE_OPS ops of ${READ_PCT}"
  done

  echo "run for $READ_PCT done collecting data" >> "$breakdownresult"

  for node in {2..6}; do
    ssh rzp5412@10.10.1.$node "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
  done
done

echo "All workloads complete."
