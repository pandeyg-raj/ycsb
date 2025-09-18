#!/bin/bash

YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql
WARMUP_OPS=5000000
MEASURE_OPS=5000000
REPEAT=5
FIELD_LENGTH=1000
RECORD_COUNT=100000000

declare -A WORKLOADS

OS_CACHE_SIZES=(16 28 40 52 64)
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

mkdir -p New_OS_Cache

OUT_DIR=New_OS_Cache
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

# collect data from all replicas and store in a file

breakdownresult="New_OS_Cache_${EXP_LABEL}_summary.txt"

touch "$breakdownresult"

for cacheSize in "${OS_CACHE_SIZES[@]}"
do          

          read -p "Run cassandra in $cacheSize GB and Press Enter to continue..."
          echo "------ Warmup phase: $WARMUP_OPS ops of size ${FIELD_LENGTH} bytes"
          RAW_FILE="${OUT_DIR}/${EXP_LABEL}_${cacheSize}_Warmup${FIELD_LENGTH}Bytes_run.scr"
          
          $YCSB_DIR run $DB -threads $THREADS \
          -p operationcount=$WARMUP_OPS \
          -p readproportion=0.5 -p insertproportion=0.5 \
          -p recordcount=${RECORD_COUNT} \
          -p measurement.raw.output_file="$RAW_FILE" \
          -P commonworkload \
          -s >> "${OUT_DIR}/${EXP_LABEL}_${cacheSize}_run${FIELD_LENGTH}Bytes.log" 2>&1
          echo "------ Warmup phase done: $WARMUP_OPS ops of size ${FIELD_LENGTH} bytes"
          
          # clean data
          
          ssh rzp5412@10.10.1.2 "/mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
          ssh rzp5412@10.10.1.3 "/mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
          ssh rzp5412@10.10.1.4 "/mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
          ssh rzp5412@10.10.1.5 "/mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
          ssh rzp5412@10.10.1.6 "/mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
          
          for i in "${!WORKLOAD_LABELS[@]}"; do
            workload="${WORKLOAD_LABELS[$i]}"
            READ_PCT="${READ_PROPORTIONS[$i]}"
            echo "Workload: $workload "
          
            for i in $(seq 1 $REPEAT); do
              echo "--- Run $i of $workload ---" 
          
              RAW_FILE="${OUT_DIR}/${EXP_LABEL}_${cacheSize}_iter_${i}Run${FIELD_LENGTH}Bytes_${workload}run.scr"
              # Measurement phase with workload mix
              echo "------ Run phase: $MEASURE_OPS ops of ${READ_PCT}"
              $YCSB_DIR run $DB -threads $THREADS \
              -p operationcount=$MEASURE_OPS \
              -p ${READ_PCT} \
              -p recordcount=${RECORD_COUNT} \
              -p measurement.raw.output_file="$RAW_FILE" \
              -p cassandra.writeconsistencylevel=QUORUM \
              -p cassandra.readconsistencylevel=QUORUM \
              -P commonworkload \
              -s >> "${OUT_DIR}/${EXP_LABEL}_${cacheSize}_run${FIELD_LENGTH}Bytes.log" 2>&1
              echo "------ Run phase done: $MEASURE_OPS ops of ${READ_PCT}"
            done
            echo "run for $EXP_LABEL $cacheSize GB OS $READ_PCT and size $FIELD_LENGTH done collecting data" >> "$breakdownresult"
          
          ssh rzp5412@10.10.1.2 "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
          ssh rzp5412@10.10.1.3 "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
          ssh rzp5412@10.10.1.4 "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
          ssh rzp5412@10.10.1.5 "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
          ssh rzp5412@10.10.1.6 "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
          
          done
done
