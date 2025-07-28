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

THREADS_LIST=(10 30 80 150 200)
OUT_DIR=lazinessLoad50REP
mkdir -p $OUT_DIR

CSV_OUT="$OUT_DIR/summary_util_latency.csv"
echo "Threads,AvgCPUUtilMpState,AvgCPUUtilPidState,AvgRead,AvggWrite,P95LatencyRead_us,P99LatencyRead_us,P95LatencyWrite_us,P99LatencyWrite_us,Throughput_ops" > $CSV_OUT

# Load phase once
echo "Load phase: Loading $RECORD_COUNT records of size ${FIELD_LENGTH} bytes"
$YCSB load $DB -threads 20 \
-p recordcount=${RECORD_COUNT} \
-p fieldlength=${FIELD_LENGTH} \
-P commonworkload \
-s >> "${OUT_DIR}/load_${FIELD_LENGTH}Bytes.log" 2>&1

echo "Load phase: Done $RECORD_COUNT records of size ${FIELD_LENGTH} bytes"


for threads in "${THREADS_LIST[@]}"; do
  sleep 30
  echo "------ warm up phase Start: $threads threads ($WARMUP_OPS ops) "
  $YCSB run $DB \
    -threads $threads \
    -p operationcount=$WARMUP_OPS \
    -p fieldlength=$FIELD_LENGTH \
    -p recordcount=$RECORD_COUNT \
    -p cassandra.writeconsistencylevel=QUORUM \
    -p readproportion=0.5 -p insertproportion=0.5 \
    -p cassandra.readconsistencylevel=QUORUM \
    -P commonworkload \
    -s > "$OUT_DIR/warmup_${threads}.log" 2>&1
  echo "------ warm up phase Done: $threads"


  # === Clean & Start pidstat on server ===
ssh $SERVER_USER@$SERVER_IP "mkdir -p $SERVER_LOG_DIR; rm -f $SERVER_LOG_DIR/pidstat.log; \
pgrep -f 'java.*cassandra' | head -n 1 > $SERVER_LOG_DIR/pidstat_target.pid; \
nohup pidstat -h -u -p \$(cat $SERVER_LOG_DIR/pidstat_target.pid) 1 > $SERVER_LOG_DIR/pidstat.log 2>&1 & echo \$! > $SERVER_LOG_DIR/pidstat.pid" < /dev/null

ssh $SERVER_USER@$SERVER_IP "mkdir -p $SERVER_LOG_DIR; rm -f $SERVER_LOG_DIR/mpstat.log;
  nohup mpstat 1 > $SERVER_LOG_DIR/mpstat.log 2>&1 & echo \$! > $SERVER_LOG_DIR/mpstat.pid
" < /dev/null

  # === Run YCSB on client ===
  echo "------ Run phase Start: $threads"
  YCSB_LOG="$OUT_DIR/ycsb_${threads}.log"
  row_LOG="$OUT_DIR/ycsb_${threads}_row.log"

  $YCSB run $DB \
    -threads $threads \
    -p operationcount=$OP_COUNT \
    -p fieldlength=$FIELD_LENGTH \
    -p recordcount=$RECORD_COUNT \
    -p measurement.raw.output_file="$row_LOG" \
    -p cassandra.writeconsistencylevel=QUORUM \
    -p readproportion=0.5 -p insertproportion=0.5 \
    -p cassandra.readconsistencylevel=QUORUM \
    -P commonworkload \
    -s > "$YCSB_LOG" 2>&1

  echo "------ Run phase Done: $threads"
  # === Stop pidstat on server ===
  ssh $SERVER_USER@$SERVER_IP "if [ -f $SERVER_LOG_DIR/pidstat.pid ]; then kill \$(cat $SERVER_LOG_DIR/pidstat.pid); fi"
  ssh $SERVER_USER@$SERVER_IP "if [ -f $SERVER_LOG_DIR/mpstat.pid ]; then kill \$(cat $SERVER_LOG_DIR/mpstat.pid); fi"

  # === Get average CPU utilization directly from server ===
  avg_util_pidstat=$(ssh $SERVER_USER@$SERVER_IP "
  awk ' /^[0-9]{2}:[0-9]{2}:[0-9]{2}/ { sum += \$9; count++ } 
  END { if (count > 0) printf \"%.2f\", sum / count / $(nproc) }' $SERVER_LOG_DIR/pidstat.log
  ")


  avg_util_mpstate=$(ssh $SERVER_USER@$SERVER_IP "
awk '/all/ { sum += 100 - \$NF; count++ } END { if (count > 0) printf \"%.2f\", sum / count }' $SERVER_LOG_DIR/mpstat.log
")


  # === Extract latency and throughput from YCSB logs ===
  P95Read=$(grep "\[READ\]" "$YCSB_LOG" | grep "p95" | awk -F, '{print $3}' | head -n 1)
  P99Read=$(grep "\[READ\]" "$YCSB_LOG" | grep "p99" | awk -F, '{print $3}' | head -n 1)
  AVGRead=$(grep "\[READ\]" "$YCSB_LOG" | grep "Average" | awk -F, '{print $3}' | head -n 1)

  P95Write=$(grep "\[INSERT\]" "$YCSB_LOG" | grep "p95" | awk -F, '{print $3}' | head -n 1)
  P99Write=$(grep "\[INSERT\]" "$YCSB_LOG" | grep "p99" | awk -F, '{print $3}' | head -n 1)
  AVGWrite=$(grep "\[INSERT\]" "$YCSB_LOG" | grep "Average" | awk -F, '{print $3}' | head -n 1)

  THROUGHPUT=$(grep "Throughput(ops/sec)" "$YCSB_LOG" | awk '{print $3}' | head -n 1)

  echo "$threads,$avg_util_mpstate,$avg_util_pidstat,$AVGRead,$AVGWrite,$P95Read,$P99Read,$P95Write,$P99Write,$THROUGHPUT" >> $CSV_OUT
  echo "→ $threads threads | CPUPidState: $avg_util_pidstat% | CPUMpState: $avg_util_mpstate% | TPS: $THROUGHPUT"
  echo "→ AVGR: $AVGRead | AVGRW: $AVGWrite"
  echo "→ P95R: $P95Readµs | P95W: $P95Writeµs"
  echo "→ P99R: $P99Readµs | P99W: $P99Writeµs"
done
