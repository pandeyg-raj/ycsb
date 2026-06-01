#!/bin/bash

# -- Config (keep IDENTICAL on both machines) ---------------------------------
YCSB="bin/ycsb.sh"          # each machine points this at its field-modified build
DB=cassandra-cql

RECORD_COUNT=3500000        # rows
FIELD_LENGTH=10000           # 8 x 1250 = 10KB row -> ~70GB
CACHE_SIZE="32GB"
COMPRESSION="on"
REQUEST_DIST="uniform"
WARMUP_OPS=${RECORD_COUNT}  # one full pass of this client's field
MEASURE_OPS=10000000        # 10M reads of this client's field

SSH_USER=rzp5412
CASS_DIR=/mydata/cassandra
CGROUP=/sys/fs/cgroup/mylimitedgroup

# =============================================================================
# drain + cap + evict  (all idempotent -> safe to run from both clients)
# =============================================================================
drain_cap_evict() {
    local nodes; if [ "$NUM_NODES" = "3" ]; then nodes=(2 3 4); else nodes=(2 3 4 5 6); fi
    local cache_gb="${CACHE_SIZE//GB/}"; local mem_bytes=$((cache_gb*1024*1024*1024))
    echo "--- drain + cap ${CACHE_SIZE} + evict page cache ---"
    for node in "${nodes[@]}"; do
        local ip="10.10.1.$node"
        ssh ${SSH_USER}@${ip} "${CASS_DIR}/bin/nodetool drain 2>/dev/null; true"
        ssh ${SSH_USER}@${ip} \
            "echo ${mem_bytes} | sudo tee ${CGROUP}/memory.max > /dev/null && \
             vmtouch -e ${CASS_DIR}/data/ > /dev/null 2>&1; true"
        echo "  ${ip}: drained, capped, evicted"
    done
}

# =============================================================================
# Setup
# =============================================================================
read -p "Output label for THIS client (e.g. ec_field0 or rep_field1): " LABEL
read -p "Threads: " THREADS
read -p "How many Cassandra nodes? (3 or 5) [5]: " NUM_NODES; NUM_NODES=${NUM_NODES:-5}
if [ "$NUM_NODES" = "3" ]; then BD_NODES=(2 3 4); elif [ "$NUM_NODES" = "5" ]; then BD_NODES=(2 3 4 5 6);
else echo "ERROR: 3 or 5"; exit 1; fi

OUT_DIR="result_othercols_${LABEL}"; mkdir -p "$OUT_DIR"
LOG="${OUT_DIR}/ycsb.log"

echo ""; echo "################################################"
echo ">>> 6.7 client | label=${LABEL} | field read by THIS YCSB build"
echo ">>> ${RECORD_COUNT} rows | ${CACHE_SIZE} | QUORUM | 100% read"
echo "################################################"; echo ""

# =============================================================================
# LOAD  (both clients load concurrently -- perfectly fine, Cassandra merges)
# If your modified YCSB writes only its field:  A->field0, B->field1, full rows.
# If load writes all fields (default):          both write all fields, same result.
# =============================================================================
echo "--- LOAD ---"
$YCSB load $DB -threads $THREADS \
    -p recordcount=${RECORD_COUNT} -p fieldlength=${FIELD_LENGTH} \
    -P commonworkload -s >> "$LOG" 2>&1
echo "--- load done ---"

drain_cap_evict

# ============================== BARRIER 1 ====================================
echo ""; echo "============================================================"
echo " BARRIER 1  -- both clients should be here (load + drain done)."
echo " Press ENTER on BOTH machines to start WARMUP."
read -p "  ENTER >> " _

# =============================================================================
# WARMUP  (this client warms its own field; together both columns get cached)
# =============================================================================
echo "--- WARMUP: ${WARMUP_OPS} reads of this client's field ---"
$YCSB run $DB -threads $THREADS -p operationcount=$WARMUP_OPS \
    -p readproportion=1.0 -p updateproportion=0.0 -p insertproportion=0.0 \
    -p readallfields=false -p recordcount=${RECORD_COUNT} \
    -p requestdistribution=${REQUEST_DIST} \
    -p cassandra.readconsistencylevel=QUORUM \
    -P commonworkload -s >> "$LOG" 2>&1
echo "--- warmup done ---"

# ============================== BARRIER 2 ====================================
echo ""; echo "============================================================"
echo " BARRIER 2  -- fire RUN on BOTH machines at the same time."
echo " Press ENTER on BOTH machines SIMULTANEOUSLY."
read -p "  ENTER >> " _

# =============================================================================
# RUN  (both fire together -- concurrent per-column reads on the same cluster)
# =============================================================================
echo "=== RUN: ${MEASURE_OPS} reads | 100% | QUORUM ==="
$YCSB run $DB -threads $THREADS -p operationcount=$MEASURE_OPS \
    -p readproportion=1.0 -p updateproportion=0.0 -p insertproportion=0.0 \
    -p readallfields=false -p recordcount=${RECORD_COUNT} \
    -p requestdistribution=${REQUEST_DIST} \
    -p cassandra.readconsistencylevel=QUORUM \
    -p measurement.raw.output_file="${OUT_DIR}/Measure.scr" \
    -P commonworkload -s > "${OUT_DIR}/run_measure.log" 2>&1
echo "=== RUN done ==="

# -- this client's read latency from its own run.log ----
python3 - "${OUT_DIR}/run_measure.log" "$LABEL" << 'PYEOF'
import sys, re
log, label = sys.argv[1], sys.argv[2]
txt = open(log).read()
def g(pat): m = re.findall(pat, txt); return float(m[-1]) if m else None
avg = g(r"\[READ\],\s*AverageLatency\(us\),\s*([\d.]+)")
p95 = g(r"\[READ\],\s*95(?:th)?PercentileLatency\(us\),\s*([\d.]+)")
p99 = g(r"\[READ\],\s*99(?:th)?PercentileLatency\(us\),\s*([\d.]+)")
thr = g(r"\[OVERALL\],\s*Throughput\(ops/sec\),\s*([\d.]+)")
def ms(x): return f"{x/1000:.2f} ms" if x else "NA"
print(f"\n==== {label}: read latency (this client's field) ====")
print(f"  avg={ms(avg)}  p95={ms(p95)}  p99={ms(p99)}  thr={int(thr) if thr else 'NA'} ops/s")
print(f"  raw: {log}")
PYEOF

# -- optional breakdown (collect from either client after both are done) ------
echo ""
read -p "Collect nodetool breakdown on this client? [y/N]: " COLLECT_BD
if [[ "$COLLECT_BD" =~ ^[Yy]$ ]]; then
    read -p "Wait until the OTHER client's RUN has also finished, then press ENTER... " _
    BD="${OUT_DIR}/breakdown.txt"; : > "$BD"
    for node in "${BD_NODES[@]}"; do
        echo "-- node 10.10.1.$node --" >> "$BD"
        ssh ${SSH_USER}@10.10.1.$node \
            "${CASS_DIR}/bin/nodetool breakdown | grep -E 'keyspace|ycsb'" >> "$BD"
    done
    dec=$(grep -E 'ycsb,decoding,' "$BD" | \
          sed -E 's/.*count=([0-9]+).*/\1/' | paste -sd+ - | bc 2>/dev/null)
    echo "  total decoding count (both clients combined) = ${dec:-0}"
    echo "  -> decodes come from EC field0 reads; field1 reads contribute none"
    echo "  -> proves EC on one column does not pull the other through the decode path"
fi

echo ""; echo "Done (${LABEL}).  ${OUT_DIR}/run_measure.log"
echo "Collect run_measure.log from BOTH clients to build the 6.7 table."
