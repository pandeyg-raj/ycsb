#!/bin/bash

# -- Config -------------------------------------------------------------------
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql

MEASURE_OPS=10000000
FIELD_LENGTH=10000
RECORD_COUNT=7000000          # ~70GB logical (cache-pressure regime at 32GB)
CACHE_SIZE="32GB"
COMPRESSION="on"

REQUEST_DIST="uniform"
READ_PROP=0.5
UPDATE_PROP=0.5

# --- per-system consistency levels (the variable under test) ---
REP_READ_CL="ONE"
REP_WRITE_CL="ONE"            # flip to QUORUM if you want only reads at ONE
EC_READ_CL="QUORUM"
EC_WRITE_CL="QUORUM"

NUM_NODES=5
BD_NODES=(2 3 4 5 6)

SSH_USER=rzp5412
CASS_DIR=/mydata/cassandra
CGROUP=/sys/fs/cgroup/mylimitedgroup

# =============================================================================
restart_cluster() {
    local cache_size=$1
    local cache_gb="${cache_size//GB/}"
    local mem_bytes=$((cache_gb * 1024 * 1024 * 1024))
    echo ""; echo "=== Soft restart: nodes ${BD_NODES[*]}, cache=${cache_size} ==="
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node"
        echo "  --- ${ip} ---"
        ssh ${SSH_USER}@${ip} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill 2>/dev/null; true"
        local ka=0
        while ssh ${SSH_USER}@${ip} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' > /dev/null 2>&1"; do
            sleep 10; ka=$((ka + 1))
            if [ "$ka" -ge 6 ]; then
                ssh ${SSH_USER}@${ip} \
                    "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill -9 2>/dev/null; true"
                sleep 5; break
            fi
        done
        ssh ${SSH_USER}@${ip} \
            "cd ${CASS_DIR} && \
             echo ${mem_bytes} | sudo tee ${CGROUP}/memory.max && \
             echo \$\$ | sudo tee ${CGROUP}/cgroup.procs && \
             vmtouch -e data/ > /dev/null 2>&1 && \
             bin/cassandra > /dev/null 2>&1"
        local sa=0
        until ssh ${SSH_USER}@${ip} \
            "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep '${ip}' | grep -q 'UN'"; do
            sleep 10; sa=$((sa + 1)); echo "  Waiting for UN... (${sa}/30)"
            if [ "$sa" -ge 30 ]; then echo "  ERROR: ${ip} not UN after 5 min."; exit 1; fi
        done
        echo "  ${ip} UN"
    done
    echo "=== Soft restart complete (${cache_size}). ==="
}

hard_restart_cluster() {
    echo ""; echo "=== HARD restart: nodes ${BD_NODES[*]} ==="
    echo "  [1/3] Killing all in parallel..."
    for node in "${BD_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill 2>/dev/null; true" &
    done
    wait
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node"; local a=0
        while ssh ${SSH_USER}@${ip} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' > /dev/null 2>&1"; do
            sleep 10; a=$((a + 1))
            if [ "$a" -ge 6 ]; then
                ssh ${SSH_USER}@${ip} \
                    "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill -9 2>/dev/null; true"
                sleep 5; break
            fi
        done
        echo "  ${ip} stopped"
    done
    echo "  [2/3] Wiping data in parallel..."
    for node in "${BD_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} "rm -rf ${CASS_DIR}/data/" &
    done
    wait
    echo "  [3/3] Starting sequentially (seeds first)..."
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node"
        ssh ${SSH_USER}@${ip} "cd ${CASS_DIR} && bin/cassandra > /dev/null 2>&1"
        local a=0
        until ssh ${SSH_USER}@${ip} \
            "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep '${ip}' | grep -q 'UN'"; do
            sleep 10; a=$((a + 1)); echo "  Waiting ${ip} UN... (${a}/30)"
            if [ "$a" -ge 30 ]; then echo "  ERROR: ${ip} not UN."; exit 1; fi
        done
        echo "  ${ip} UN"
    done
    echo "  Creating table via /mydata/${CREATE_TABLE_BIN}..."
    /mydata/${CREATE_TABLE_BIN}
    echo "=== HARD restart complete. ==="
}

# =============================================================================
echo "Checking create-table binaries..."
for bin in create_table_ec_compr_on create_table_ec_compr_off \
           create_table_rep_compr_on create_table_rep_compr_off; do
    if [ ! -x "/mydata/${bin}" ]; then echo "ERROR: /mydata/${bin} missing."; exit 1; fi
done
echo "OK."; echo ""

echo "Is this EC or REP?"; read EXP_LABEL
read -p "Write (load) threads: " WTHREADS
read -p "Run threads (measure): " THREADS

if echo "$EXP_LABEL" | grep -qi "rep"; then
    CREATE_TABLE_BIN="create_table_rep_compr_${COMPRESSION}"; SYS_KIND="rep"
    READ_CL="$REP_READ_CL"; WRITE_CL="$REP_WRITE_CL"
else
    CREATE_TABLE_BIN="create_table_ec_compr_${COMPRESSION}"; SYS_KIND="ec"
    READ_CL="$EC_READ_CL"; WRITE_CL="$EC_WRITE_CL"
fi

OUT_DIR="result_consistency_${EXP_LABEL}_${COMPRESSION}_R${READ_CL}_W${WRITE_CL}"
mkdir -p "$OUT_DIR"
LOG="${OUT_DIR}/run.log"
BREAKDOWN_FILE="${OUT_DIR}/breakdown.txt"; touch "$BREAKDOWN_FILE"

echo ""
echo "################################################################"
echo ">>> CONSISTENCY | ${EXP_LABEL^^} | 5 nodes | 10KB | 50/50 ${REQUEST_DIST} | cache=${CACHE_SIZE} | compr=${COMPRESSION} | compaction ON"
echo ">>> read CL=${READ_CL}  write CL=${WRITE_CL}  | measure ops=${MEASURE_OPS}"
echo "################################################################"

# 1) Fresh cluster + load
hard_restart_cluster
echo "--- Loading ${RECORD_COUNT} x ${FIELD_LENGTH}B ---"
$YCSB_DIR load $DB -threads $WTHREADS \
    -p recordcount=${RECORD_COUNT} -p fieldlength=${FIELD_LENGTH} \
    -p measurement.raw.output_file="${OUT_DIR}/Load.scr" \
    -P commonworkload -s >> "$LOG" 2>&1
echo "--- Load done ---"

echo "--- Waiting for compaction to settle ---"
for node in "${BD_NODES[@]}"; do
    ip="10.10.1.$node"
    while ssh ${SSH_USER}@${ip} \
        "${CASS_DIR}/bin/nodetool compactionstats 2>/dev/null | grep -q 'pending tasks: [^0]'"; do
        sleep 30; echo "  compaction running on ${ip}..."
    done
    echo "  ${ip} settled"
done

echo "--- Draining ---"
for node in "${BD_NODES[@]}"; do ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool drain" & done
wait

# 2) Apply cache cap + evict, warm (at this system's READ CL -> steady-state cache)
restart_cluster "$CACHE_SIZE"

cache_gb="${CACHE_SIZE//GB/}"
available_bytes=$(( (cache_gb - 8) * 1024 * 1024 * 1024 ))
if [ "$SYS_KIND" = "ec" ]; then shard_size=$(( FIELD_LENGTH / 3 )); else shard_size=$FIELD_LENGTH; fi
objects_that_fit=$(( available_bytes / shard_size ))
WARMUP_OPS=$(( objects_that_fit < RECORD_COUNT ? objects_that_fit : RECORD_COUNT ))
if [ "$WARMUP_OPS" -lt 1000000 ]; then WARMUP_OPS=1000000; fi
echo ">>> Warmup ops: ${WARMUP_OPS} (read CL=${READ_CL})"

$YCSB_DIR run $DB -threads $THREADS \
    -p operationcount=$WARMUP_OPS \
    -p readproportion=1.0 -p updateproportion=0.0 -p insertproportion=0.0 \
    -p recordcount=${RECORD_COUNT} -p requestdistribution=${REQUEST_DIST} \
    -p measurement.raw.output_file="${OUT_DIR}/Warmup.scr" \
    -p cassandra.writeconsistencylevel=${WRITE_CL} \
    -p cassandra.readconsistencylevel=${READ_CL} \
    -P commonworkload -s >> "$LOG" 2>&1
echo "--- Warmup done ---"

# 3) MEASURE window (compaction stays ON; headline metrics in run.log/Measure.scr)
echo "--- Reset breakdown ---"
for node in "${BD_NODES[@]}"; do
    ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool breakdown --reset"
done

echo "=== MEASURE: 50/50 ${REQUEST_DIST} | read CL=${READ_CL} write CL=${WRITE_CL} | ${MEASURE_OPS} ops ==="
$YCSB_DIR run $DB -threads $THREADS \
    -p operationcount=$MEASURE_OPS \
    -p readproportion=${READ_PROP} -p updateproportion=${UPDATE_PROP} \
    -p insertproportion=0.0 -p scanproportion=0.0 \
    -p requestdistribution=${REQUEST_DIST} \
    -p recordcount=${RECORD_COUNT} -p fieldlength=${FIELD_LENGTH} \
    -p measurement.raw.output_file="${OUT_DIR}/Measure.scr" \
    -p cassandra.writeconsistencylevel=${WRITE_CL} \
    -p cassandra.readconsistencylevel=${READ_CL} \
    -P commonworkload -s >> "$LOG" 2>&1
echo "=== MEASURE done ==="

echo "--- Collecting breakdown (ycsb/keyspace lines) ---"
echo "run ${EXP_LABEL} consistency R${READ_CL} W${WRITE_CL} ${CACHE_SIZE} 50-50 ${REQUEST_DIST} compr=${COMPRESSION}" >> "$BREAKDOWN_FILE"
for node in "${BD_NODES[@]}"; do
    echo "-- node 10.10.1.$node --" >> "$BREAKDOWN_FILE"
    ssh ${SSH_USER}@10.10.1.$node \
        "${CASS_DIR}/bin/nodetool breakdown | grep -E 'keyspace|ycsb'" >> "$BREAKDOWN_FILE"
done

echo ""
echo "############################################################"
echo "Done. ${OUT_DIR}/   (read CL=${READ_CL}, write CL=${WRITE_CL})"
echo "  run.log / Measure.scr   latency + THROUGHPUT (headline)"
echo "  breakdown.txt           component split"
echo "############################################################"
