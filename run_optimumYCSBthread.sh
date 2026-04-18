#!/bin/bash
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql
FIELD_LENGTH=10000
RECORD_COUNT=5000000

echo "Is this ec or rep"
read EXP_LABEL

OUT_DIR=case1Vscase2
mkdir -p "$OUT_DIR"

# ── Throughput saturation sweep (load/insert only) ────────────────────────────
THRESHOLD=0.05        # Stop if throughput gain drops below 5%
THREAD_STEPS=(8 16 24 32 64 128)

PREV_THROUGHPUT=0
OPTIMAL_THREADS=0
OPTIMAL_THROUGHPUT=0

SWEEP_LOG="${OUT_DIR}/${EXP_LABEL}_sweep_${FIELD_LENGTH}Bytes.log"
echo "threads,throughput_ops_sec,p99_insert_latency_us" > "${OUT_DIR}/${EXP_LABEL}_sweep_summary.csv"

echo ""
echo "=== Starting thread sweep to find saturation point ==="

for WTHREADS in "${THREAD_STEPS[@]}"; do
  echo ""
  echo "--- Load phase: $RECORD_COUNT records, $FIELD_LENGTH bytes, $WTHREADS threads ---"

  RUN_LOG="${OUT_DIR}/${EXP_LABEL}_t${WTHREADS}_${FIELD_LENGTH}Bytes.log"
  RAW_FILE="${OUT_DIR}/${EXP_LABEL}_t${WTHREADS}_${FIELD_LENGTH}Bytes_raw.scr"

  $YCSB_DIR load $DB -threads $WTHREADS \
    -p recordcount=${RECORD_COUNT} \
    -p fieldlength=${FIELD_LENGTH} \
    -p measurement.raw.output_file="$RAW_FILE" \
    -P commonworkload \
    -s >> "$RUN_LOG" 2>&1

  # Parse results — INSERT metrics, not READ
  THROUGHPUT=$(grep "\[OVERALL\], Throughput" "$RUN_LOG" | awk -F',' '{print $3}' | tr -d ' ')
  P99=$(grep "\[INSERT\], 99thPercentileLatency" "$RUN_LOG" | awk -F',' '{print $3}' | tr -d ' ')

  if [ -z "$P99" ]; then P99=0; fi

  echo "  Throughput    : ${THROUGHPUT} ops/sec"
  echo "  p99 INSERT lat: ${P99} us"

  # Log to CSV
  echo "${WTHREADS},${THROUGHPUT},${P99}" >> "${OUT_DIR}/${EXP_LABEL}_sweep_summary.csv"
  echo "threads=$WTHREADS throughput=$THROUGHPUT p99=$P99" >> "$SWEEP_LOG"


  # ── Stop condition 2: throughput gain < threshold or declining ──
  if (( $(echo "$PREV_THROUGHPUT > 0" | bc -l) )); then
    GAIN=$(echo "scale=4; ($THROUGHPUT - $PREV_THROUGHPUT) / $PREV_THROUGHPUT" | bc -l)
    echo "  Gain vs prev  : $GAIN"

    if (( $(echo "$GAIN < 0" | bc -l) )); then
      echo ""
      echo ">>> Throughput declined at $WTHREADS threads (gain=$GAIN)."
      echo ">>> Optimal = $OPTIMAL_THREADS threads @ ${OPTIMAL_THROUGHPUT} ops/sec"
      break
    fi

    if (( $(echo "$GAIN < $THRESHOLD" | bc -l) )); then
      echo ""
      echo ">>> Saturation reached at $WTHREADS threads (gain=$GAIN < threshold=$THRESHOLD)."
      echo ">>> Optimal = $OPTIMAL_THREADS threads @ ${OPTIMAL_THROUGHPUT} ops/sec"
      break
    fi
  fi

  # Update best known point
  OPTIMAL_THREADS=$WTHREADS
  OPTIMAL_THROUGHPUT=$THROUGHPUT
  PREV_THROUGHPUT=$THROUGHPUT

done

echo ""
echo "=== Sweep complete for [$EXP_LABEL] ==="
echo "    Optimal thread count : $OPTIMAL_THREADS"
echo "    Max throughput       : $OPTIMAL_THROUGHPUT ops/sec"
echo "    Summary CSV          : ${OUT_DIR}/${EXP_LABEL}_sweep_summary.csv"
