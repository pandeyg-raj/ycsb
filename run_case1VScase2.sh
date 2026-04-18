#!/bin/bash
set -euo pipefail

# ── Configurable parameters ──────────────────────────────────────────
YCSB_DIR="bin/ycsb.sh"
DB="cassandra-cql"

MEMTABLE_MB=128                        # Match this to memtable_heap_space_in_mb in cassandra.yaml
MEMTABLE_BYTES=$((MEMTABLE_MB * 1024 * 1024))
TARGET_FLUSHES=1000                   # Number of memtable flushes to trigger per run

THREAD_COUNTS=(8 16 24 32 40 48 56 64)
FIELD_SIZES=(10000 100000 1000000)    # 10KB, 100KB, 1000KB (decimal bytes)
FIELD_LABELS=("10KB" "100KB" "1000KB")

# ── Cluster config ───────────────────────────────────────────────────
NODES=(10.10.1.2 10.10.1.3 10.10.1.4 10.10.1.5 10.10.1.6)
SSH_USER="rzp5412"
NODETOOL="/mydata/cassandra/bin/nodetool"
DROP_KS="/mydata/ycsb/CassClient/dropkeyspace"
CREATE_TABLE="/mydata/ycsb/CassClient/ycsbTableCreate"

# ── Output dir ───────────────────────────────────────────────────────
OUT_DIR="case1Vscase2"
mkdir -p "$OUT_DIR"

echo "Is this ec or rep?"
read -r EXP_LABEL

# ── Main sweep ───────────────────────────────────────────────────────
for size_idx in "${!FIELD_SIZES[@]}"; do
    FIELD_LENGTH="${FIELD_SIZES[$size_idx]}"
    SIZE_LABEL="${FIELD_LABELS[$size_idx]}"
    RECORD_COUNT=$((MEMTABLE_BYTES * TARGET_FLUSHES / FIELD_LENGTH))

    for WTHREADS in "${THREAD_COUNTS[@]}"; do

        # Unique tag encoding all variable dimensions: label, memtable, object size, threads
        RUN_TAG="${EXP_LABEL}_mem${MEMTABLE_MB}MB_${SIZE_LABEL}_t${WTHREADS}"

        LOG_FILE="${OUT_DIR}/${RUN_TAG}_load.log"
        RAW_FILE="${OUT_DIR}/${RUN_TAG}_load.scr"
        BREAKDOWN_FILE="${OUT_DIR}/${RUN_TAG}_ecwritestats.txt"

        echo ""
        echo "======================================================"
        echo " EXP=${EXP_LABEL} | Memtable=${MEMTABLE_MB}MB | Size=${SIZE_LABEL} | Threads=${WTHREADS} | Records=${RECORD_COUNT}"
        echo "======================================================"

        # 1. Drop keyspace, sleep, recreate table for clean storage
        echo "[1/4] Dropping keyspace ycsb..."
        "$DROP_KS" ycsb
        echo "[1/4] Sleeping 30s for keyspace drop to propagate..."
        sleep 30
        echo "[1/4] Recreating table..."
        "$CREATE_TABLE"
        echo "[1/4] Sleeping 30s for table creation to propagate..."
        sleep 30

        # 2. Reset case1 vs case2 metrics on all nodes before load
        echo "[2/4] Resetting ecwritestats on all nodes..."
        for node in "${NODES[@]}"; do
            ssh "${SSH_USER}@${node}" "${NODETOOL} ecwritestats --reset"
        done

        # 3. Load phase
        echo "[3/4] Load phase: ${RECORD_COUNT} records x ${SIZE_LABEL} @ ${WTHREADS} threads"
        echo "      Log  -> $LOG_FILE"
        echo "      Raw  -> $RAW_FILE"
        $YCSB_DIR load $DB -threads $WTHREADS \
            -p recordcount="${RECORD_COUNT}" \
            -p fieldlength="${FIELD_LENGTH}" \
            -p measurementtype=raw \
            -p measurement.raw.output_file="$RAW_FILE" \
            -P commonworkload \
            -s >> "$LOG_FILE" 2>&1
        echo "[3/4] Load phase done."

        # 4. Collect ecwritestats from all nodes into per-run file
        echo "[4/4] Collecting ecwritestats -> $BREAKDOWN_FILE"
        for node in "${NODES[@]}"; do
            echo "--- Node ${node} ---" >> "$BREAKDOWN_FILE"
            ssh "${SSH_USER}@${node}" "${NODETOOL} ecwritestats" >> "$BREAKDOWN_FILE"
        done

        echo "      Done: $RUN_TAG"
    done
done

echo ""
echo "All sweeps complete. Results in: ${OUT_DIR}/"
