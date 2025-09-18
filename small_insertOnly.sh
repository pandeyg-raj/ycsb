#!/bin/bash

# Define paths and variables
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql
FIELD_LENGTH=10
RECORD_COUNT=10000000000  # You can change this to your total record count

declare -A WORKLOADS
echo "Is this EC or Rep?"
read EXP_LABEL
echo "How many Write threads?"
read WTHREADS

echo "How Many Read threads"
read THREADS


mkdir -p 10Bytes_object
OUT_DIR="10Bytes_object"
RAW_FILE="${OUT_DIR}/${EXP_LABEL}_Load${FIELD_LENGTH}Bytes_run.scr"

breakdownresult="10Bytes_${EXP_LABEL}_summary.txt"
touch "$breakdownresult"

# Define batch size 
BATCH_SIZE=10000000

# Calculate number of batches and remainder
NUM_BATCHES=$((RECORD_COUNT / BATCH_SIZE))
REMAINDER=$((RECORD_COUNT % BATCH_SIZE))


echo "Total Records: $RECORD_COUNT"
echo "Batch Size: $BATCH_SIZE"
echo "Number of Batches: $NUM_BATCHES"
echo "Remainder: $REMAINDER"


# Load phase in chunks
for ((batch=0; batch<=NUM_BATCHES; batch++))
do
    # Calculate start and end keys for each batch
    START=$((batch * BATCH_SIZE))
    
    # If it's the last batch and there are fewer records left, use the remainder
    if [ $batch -eq $NUM_BATCHES ] && [ $REMAINDER -ne 0 ]; then
        END=$((START + REMAINDER - 1))
        RECORD_COUNT=$REMAINDER
    else
        END=$((START + BATCH_SIZE - 1))
        RECORD_COUNT=$BATCH_SIZE
    fi
    
    # Print the batch details
    echo "Running YCSB Load: Batch $((batch + 1))"
    echo "Start key: $START, End key: $END, Records: $RECORD_COUNT"

    # Run YCSB load for the current batch
    $YCSB_DIR load $DB -threads $WTHREADS \
    -p recordcount=$RECORD_COUNT \
    -p fieldlength=$FIELD_LENGTH \
    -p start=$START \
    -p end=$END \
    -p measurement.raw.output_file="$RAW_FILE" \
    -P commonworkload \
    -s >> "${OUT_DIR}/${EXP_LABEL}_run${FIELD_LENGTH}Bytes_batch$((batch + 1)).log" 2>&1

    # Optional: Sleep between batches if needed to avoid overwhelming the system
     sleep 10
done

echo "Load phase: Done inserting $RECORD_COUNT records of size ${FIELD_LENGTH} bytes"
echo "Insert done collecting data" >> "$breakdownresult"

ssh rzp5412@10.10.1.2 "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
ssh rzp5412@10.10.1.3 "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
ssh rzp5412@10.10.1.4 "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
ssh rzp5412@10.10.1.5 "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
ssh rzp5412@10.10.1.6 "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"

sleep 10


WARMUP_OPS=5000000
MEASURE_OPS=5000000
REPEAT=5
FIELD_LENGTH=10
RECORD_COUNT=10000000000

WORKLOAD_LABELS=("read100" "read95" "read50")
READ_PROPORTIONS=("readproportion=1 -p insertproportion=0" \
                "readproportion=0.95 -p insertproportion=0.05" \
                "readproportion=0.5 -p insertproportion=0.5")



# Warmup phase once mix workload
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

ssh rzp5412@10.10.1.2 "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
ssh rzp5412@10.10.1.3 "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
ssh rzp5412@10.10.1.4 "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
ssh rzp5412@10.10.1.5 "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"
ssh rzp5412@10.10.1.6 "/mydata/cassandra/bin/nodetool breakdown | grep -E 'keyspace|ycsb' && /mydata/cassandra/bin/nodetool breakdown --reset" >> "$breakdownresult"


for i in "${!WORKLOAD_LABELS[@]}"; do
  workload="${WORKLOAD_LABELS[$i]}"
  READ_PCT="${READ_PROPORTIONS[$i]}"
  echo "Workload: $workload "

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
    -P commonworkload \
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
echo "16 threads"
