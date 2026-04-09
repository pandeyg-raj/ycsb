#!/bin/bash

YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql

FIELD_LENGTH=10000
RECORD_COUNT=5000000

declare -A WORKLOADS

echo "Is this ec or rep"
read EXP_LABEL

echo "How Many Write threads"
read WTHREADS

echo "How Many Read threads"
read THREADS

OUT_DIR=case1Vscase2
mkdir -p "$OUT_DIR"

breakdownresult="Case1vsCase2_${EXP_LABEL}_summary.txt"
touch "$breakdownresult"

RAW_FILE="${OUT_DIR}/${EXP_LABEL}_Load${FIELD_LENGTH}Bytes_run.scr"

# reset case1 vs case2 statistic before load 
ssh rzp5412@10.10.1.2 "/mydata/cassandra/bin/nodetool ecwritestats --reset" 
ssh rzp5412@10.10.1.3 "/mydata/cassandra/bin/nodetool ecwritestats --reset" 
ssh rzp5412@10.10.1.4 "/mydata/cassandra/bin/nodetool ecwritestats --reset" 
ssh rzp5412@10.10.1.5 "/mydata/cassandra/bin/nodetool ecwritestats --reset" 
ssh rzp5412@10.10.1.6 "/mydata/cassandra/bin/nodetool ecwritestats --reset" 

# Load phase once
echo "Load phase: Loading $RECORD_COUNT records of size ${FIELD_LENGTH} bytes"
$YCSB_DIR load $DB -threads $WTHREADS \
-p recordcount=${RECORD_COUNT} \
-p fieldlength=${FIELD_LENGTH} \
-p measurement.raw.output_file="$RAW_FILE" \
-P commonworkload \
-s >> "${OUT_DIR}/${EXP_LABEL}_run${FIELD_LENGTH}Bytes.log" 2>&1

echo "Load phase: Done $RECORD_COUNT records of size ${FIELD_LENGTH} bytes"

#collect case1 vs case 2 statistic from all nodes and sava in a file 
#Load done collect data case1 vs case2
ssh rzp5412@10.10.1.2 "/mydata/cassandra/bin/nodetool ecwritestats" >> "$breakdownresult"
ssh rzp5412@10.10.1.3 "/mydata/cassandra/bin/nodetool ecwritestats" >> "$breakdownresult"
ssh rzp5412@10.10.1.4 "/mydata/cassandra/bin/nodetool ecwritestats" >> "$breakdownresult"
ssh rzp5412@10.10.1.5 "/mydata/cassandra/bin/nodetool ecwritestats" >> "$breakdownresult"
ssh rzp5412@10.10.1.6 "/mydata/cassandra/bin/nodetool ecwritestats" >> "$breakdownresult"

