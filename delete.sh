#!/bin/bash

# === USER CONFIG ===
YCSB=bin/ycsb.sh
DB=cassandra-cql
FIELD_LENGTH=10000
RECORD_COUNT=10000000
OP_COUNT=5000000
WARMUP_OPS=5000000

SERVER_USER=rzp5412
SERVER_IP=10.10.1.2
SERVER_LOG_DIR=/tmp/util_logs

OUT_DIR=lazinessLoad50Ragain
mkdir -p $OUT_DIR

echo "------ warm up phase Start: $threads threads ($WARMUP_OPS ops) "
$YCSB run $DB \
  -target 50000 -threads 500 \
  -p operationcount=$WARMUP_OPS \
  -p fieldlength=$FIELD_LENGTH \
  -p recordcount=$RECORD_COUNT \
  -p cassandra.writeconsistencylevel=QUORUM \
  -p readproportion=0.9 -p insertproportion=0.1 \
  -p cassandra.readconsistencylevel=QUORUM \
  -P commonworkload \
  -s > "$OUT_DIR/warmup_.log" 2>&1

echo "------ warm up phase Done: $threads"
