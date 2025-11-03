#!/bin/bash

YCSB_DIR=bin/ycsb.sh

DB=cassandra-cql
MEASURE_OPS=2500000
REPEAT=5

FIELD_LENGTH=10000
RECORD_COUNT=4800000

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

OUT_DIR=mix_EC_column_read
mkdir -p "$OUT_DIR"

RAW_FILE="${OUT_DIR}/${EXP_LABEL}_Load${FIELD_LENGTH}Bytes_run.scr"


# collect data from all replicas and store in a file

breakdownresult="mixEC_column_read${EXP_LABEL}_summary.txt"
touch "$breakdownresult"

for i in "${!WORKLOAD_LABELS[@]}"; do
  workload="${WORKLOAD_LABELS[$i]}"
  READ_PCT="${READ_PROPORTIONS[$i]}"
  echo "Workload: $workload "
  read -p "Start READING  ${READ_PCT}  press Enter WHEN BOTH SYTEM READY"
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
    -p cassandra.writeconsistencylevel=QUORUM \
    -p cassandra.readconsistencylevel=QUORUM \
    -P mixcommonworkloadECRead \
    -s >> "${OUT_DIR}/${EXP_LABEL}_run${FIELD_LENGTH}Bytes.log" 2>&1
    echo "------ Run phase done: $MEASURE_OPS ops of ${READ_PCT}"
  done
  echo "run for $READ_PCT done collecting data" >> "$breakdownresult"

ssh rzp5412@10.10.1.2 "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
ssh rzp5412@10.10.1.3 "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
ssh rzp5412@10.10.1.4 "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
ssh rzp5412@10.10.1.5 "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
ssh rzp5412@10.10.1.6 "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"

done

