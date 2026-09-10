#!/bin/bash
# =============================================================================
# run_failure_3node.sh
#
# Minimal node-failure experiment on a 3-node Cassandra cluster (RF=3, QUORUM).
#
# Flow (single load, no reload, no snapshot):
#   1. hard restart all 3 nodes (wipe + 32GB cgroup cap) and create the table
#   2. load 7,000,000 x 10,000B  = ~70GB logical data
#   3. wait for compaction to settle
#   4. WARMUP  (100% read, all 3 nodes up)
#   5. KILL node3 (10.10.1.4)  -> 2 nodes remain
#   6. run workloadC            -> results in .../2node/
#   7. RESTART node3            -> back to 3 nodes
#   8. WARMUP again
#   9. run workloadC            -> results in .../3node/
#
# Node map: node0 = client (runs this script, no Cassandra).
#           node1 = 10.10.1.2   node2 = 10.10.1.3   node3 = 10.10.1.4 (killed)
#
# With RF=3 on 3 nodes, QUORUM = 2, so reads still succeed with one node down.
# YCSB *-FAILED counts in the log should be ~0; if they are not, note it.
# =============================================================================

set -u

# -- Config -------------------------------------------------------------------
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql

RECORD_COUNT=7000000         # 7M x 10KB = ~70GB
FIELD_LENGTH=10000
MEASURE_OPS=2000000          # workloadC operation count
CACHE_SIZE="32GB"            # cgroup memory cap per node
HEAP_GB=8                    # JVM heap, subtracted when sizing the warmup

SSH_USER=rzp5412
CASS_DIR=/mydata/cassandra
CGROUP=/sys/fs/cgroup/mylimitedgroup

ALL_NODES=(2 3 4)            # IP last octets of the 3 storage nodes
KILL_NODE=4                  # node3 = 10.10.1.4, the one we fail

# When node3 is restarted in phase 3, should its OS page cache be dropped first?
# "no"  = node3 comes back with whatever the OS still holds (closest to a real
#         process crash / restart).
# "yes" = node3 comes back fully cold while node1/node2 stay warm.
DROP_CACHE_ON_REJOIN="no"

SETTLE_SECS=120              # idle settle window after every cluster state change

# =============================================================================
# Helpers
# =============================================================================
log_banner() {
    local log=$1 phase=$2 what=$3 outfile=$4
    {
        echo ""
        echo "################################################################"
        echo "# SYSTEM   : ${EXP_LABEL}"
        echo "# PHASE    : ${phase}"
        echo "# CACHE    : ${CACHE_SIZE}"
        echo "# WHAT     : ${what}"
        echo "# OUTFILE  : ${outfile}"
        echo "# TIME     : $(date '+%F %T')"
        echo "################################################################"
    } >> "$log"
}

# kill_node <octet>  -- SIGTERM then SIGKILL, wait until the process is gone
kill_node() {
    local ip="10.10.1.$1" a=0
    ssh ${SSH_USER}@${ip} \
        "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill 2>/dev/null; true"
    while ssh ${SSH_USER}@${ip} "ps -ef | grep '[j]ava' | grep -i 'cassandra' > /dev/null 2>&1"; do
        sleep 10; a=$((a+1)); echo "  waiting for ${ip} to stop... (${a}/6)"
        if [ "$a" -ge 6 ]; then
            ssh ${SSH_USER}@${ip} \
                "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill -9 2>/dev/null; true"
            sleep 5; break
        fi
    done
    echo "  ${ip} stopped"
}

# start_node <octet> <drop_cache:yes|no>  -- apply cgroup cap, optionally evict
# page cache, start Cassandra, wait for UN.
start_node() {
    local node=$1 drop=$2
    local ip="10.10.1.$node"
    local cache_gb="${CACHE_SIZE//GB/}"
    local mem_bytes=$((cache_gb * 1024 * 1024 * 1024))

    local drop_cmd="true"
    if [ "$drop" = "yes" ]; then
        drop_cmd="vmtouch -e data/ > /dev/null 2>&1 ; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null"
    fi

    ssh ${SSH_USER}@${ip} \
        "cd ${CASS_DIR} && \
         echo ${mem_bytes} | sudo tee ${CGROUP}/memory.max > /dev/null && \
         echo \$\$ | sudo tee ${CGROUP}/cgroup.procs > /dev/null ; \
         ${drop_cmd} ; \
         bin/cassandra > /dev/null 2>&1"

    local a=0
    until ssh ${SSH_USER}@${ip} "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep '${ip}' | grep -q 'UN'"; do
        sleep 10; a=$((a+1)); echo "  waiting for ${ip} UN... (${a}/30)"
        if [ "$a" -ge 30 ]; then echo "  ERROR: ${ip} not UN after 5 min."; exit 1; fi
    done
    echo "  ${ip} UN"
    ssh ${SSH_USER}@${ip} "grep -E '^(MemFree|Cached):' /proc/meminfo" | sed "s/^/  ${ip} /"
}

# wait_seen_down <octet> <probe_octet>  -- block until the probe node gossips the
# target as not-UN, so we do not measure the failure-detection window.
wait_seen_down() {
    local target=$1 probe_ip="10.10.1.$2" a=0
    while ssh ${SSH_USER}@${probe_ip} \
        "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep -E '^[[:space:]]*UN[[:space:]]+10\.10\.1\.${target}\b'" > /dev/null 2>&1; do
        sleep 10; a=$((a+1)); echo "  10.10.1.${target} still gossiped as UN... (${a}/18)"
        if [ "$a" -ge 18 ]; then echo "  ERROR: failure never propagated."; exit 1; fi
    done
    echo "  10.10.1.${target} now seen as DOWN by 10.10.1.$2"
}

# wait_seen_up <octet> <probe_octet>
wait_seen_up() {
    local target=$1 probe_ip="10.10.1.$2" a=0
    until ssh ${SSH_USER}@${probe_ip} \
        "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep -E '^[[:space:]]*UN[[:space:]]+10\.10\.1\.${target}\b'" > /dev/null 2>&1; do
        sleep 10; a=$((a+1)); echo "  10.10.1.${target} not yet gossiped as UN... (${a}/18)"
        if [ "$a" -ge 18 ]; then echo "  ERROR: rejoin never propagated."; exit 1; fi
    done
    echo "  10.10.1.${target} now seen as UN by 10.10.1.$2"
}

wait_compaction() {
    local nodes=("$@")
    echo "--- waiting for compaction to settle ---"
    for node in "${nodes[@]}"; do
        local ip="10.10.1.$node"
        while ssh ${SSH_USER}@${ip} \
            "${CASS_DIR}/bin/nodetool compactionstats 2>/dev/null | grep -q 'pending tasks: [^0]'"; do
            sleep 30; echo "  compaction still running on ${ip}..."
        done
        echo "  ${ip} settled"
    done
}

# =============================================================================
# hard_restart_cluster -- wipe data on all 3, start seeds-first, create table
# =============================================================================
hard_restart_cluster() {
    echo ""
    echo "=== HARD restart: nodes ${ALL_NODES[*]} ==="
    for node in "${ALL_NODES[@]}"; do kill_node "$node"; done

    echo "  wiping data on all nodes..."
    for node in "${ALL_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} "rm -rf ${CASS_DIR}/data/" &
    done
    wait

    echo "  starting sequentially (seeds first)..."
    for node in "${ALL_NODES[@]}"; do start_node "$node" "yes"; done

    echo "  creating YCSB table via /mydata/${CREATE_TABLE_BIN}..."
    /mydata/${CREATE_TABLE_BIN}
    echo "=== HARD restart complete ==="
}

# =============================================================================
# warmup <phase_tag> <out_dir> <log>
#   100% read, sized to fill the per-node page cache once.
# =============================================================================
warmup() {
    local phase=$1 out_dir=$2 log=$3

    local cache_gb="${CACHE_SIZE//GB/}"
    local available_bytes=$(( (cache_gb - HEAP_GB) * 1024 * 1024 * 1024 ))
    local shard_size=$FIELD_LENGTH
    if echo "$EXP_LABEL" | grep -qi "ec"; then shard_size=$(( FIELD_LENGTH / 3 )); fi
    local objects_that_fit=$(( available_bytes / shard_size ))
    local WARMUP_OPS=$(( objects_that_fit < RECORD_COUNT ? objects_that_fit : RECORD_COUNT ))
    [ "$WARMUP_OPS" -lt 1000000 ] && WARMUP_OPS=1000000

    local WARMUP_FILE="${out_dir}/${EXP_LABEL}_${phase}_${CACHE_SIZE}_Warmup.scr"
    log_banner "$log" "$phase" "WARMUP" "$WARMUP_FILE"
    echo "--- [${phase}] warmup (100% read, ${WARMUP_OPS} ops, ${THREADS} threads) ---"

    $YCSB_DIR run $DB -threads $THREADS \
        -p operationcount=$WARMUP_OPS \
        -p readproportion=1.0 -p updateproportion=0.0 -p insertproportion=0.0 \
        -p recordcount=${RECORD_COUNT} \
        -p fieldlength=${FIELD_LENGTH} \
        -p measurement.raw.output_file="$WARMUP_FILE" \
        -p cassandra.writeconsistencylevel=QUORUM \
        -p cassandra.readconsistencylevel=QUORUM \
        -P commonworkload \
        -s >> "$log" 2>&1

    echo "--- [${phase}] warmup done ---"
}

# =============================================================================
# run_workloadc <phase_tag> <out_dir> <log> <breakdown_file> <live_node...>
# =============================================================================
run_workloadc() {
    local phase=$1 out_dir=$2 log=$3 bdfile=$4; shift 4
    local nodes=("$@")

    local MEASURE_FILE="${out_dir}/${EXP_LABEL}_${phase}_${CACHE_SIZE}_workloadCRun${FIELD_LENGTH}Bytes.scr"
    log_banner "$log" "$phase" "workloadC" "$MEASURE_FILE"
    echo "=== [${phase}] workloadC (read-only, ${MEASURE_OPS} ops, ${THREADS} threads) ==="

    for node in "${nodes[@]}"; do
        ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool breakdown --reset" 2>/dev/null
    done

    $YCSB_DIR run $DB -threads $THREADS \
        -p operationcount=$MEASURE_OPS \
        -p readproportion=1.0 -p updateproportion=0.0 -p insertproportion=0.0 \
        -p recordcount=${RECORD_COUNT} \
        -p fieldlength=${FIELD_LENGTH} \
        -p measurement.raw.output_file="$MEASURE_FILE" \
        -p cassandra.writeconsistencylevel=QUORUM \
        -p cassandra.readconsistencylevel=QUORUM \
        -P commonworkload \
        -s >> "$log" 2>&1

    echo "run for ${EXP_LABEL} ${phase} ${CACHE_SIZE} workloadC" >> "$bdfile"
    for node in "${nodes[@]}"; do
        echo "-- node 10.10.1.$node --" >> "$bdfile"
        ssh ${SSH_USER}@10.10.1.$node \
            "${CASS_DIR}/bin/nodetool breakdown | grep -E 'keyspace|ycsb'" >> "$bdfile" 2>/dev/null
    done
    echo "=== [${phase}] workloadC done ==="
}

# =============================================================================
# Pre-flight + prompts
# =============================================================================
for bin in create_table_ec_compr_on create_table_rep_compr_on; do
    if [ ! -x "/mydata/${bin}" ]; then echo "ERROR: /mydata/${bin} missing."; exit 1; fi
done

echo "Is this EC or REP?"; read EXP_LABEL
echo "How many write threads (for load)?"; read WTHREADS
echo "How many read/run threads?"; read THREADS

COMPRESSION="on"
if echo "$EXP_LABEL" | grep -qi "rep"; then
    CREATE_TABLE_BIN="create_table_rep_compr_${COMPRESSION}"
else
    CREATE_TABLE_BIN="create_table_ec_compr_${COMPRESSION}"
fi

BASE="result_failure3node_${EXP_LABEL}_${COMPRESSION}"
OUT2="${BASE}/2node"; OUT3="${BASE}/3node"
mkdir -p "$OUT2" "$OUT3"
LOG2="${OUT2}/${EXP_LABEL}_2node_run.log"; BD2="${OUT2}/${EXP_LABEL}_2node_breakdown.txt"; touch "$BD2"
LOG3="${OUT3}/${EXP_LABEL}_3node_run.log"; BD3="${OUT3}/${EXP_LABEL}_3node_breakdown.txt"; touch "$BD3"

echo ""
echo "################################################################"
echo ">>> 3-NODE FAILURE EXPERIMENT | ${EXP_LABEL^^}"
echo ">>> cache=${CACHE_SIZE}  records=${RECORD_COUNT}x${FIELD_LENGTH}B  ops=${MEASURE_OPS}"
echo ">>> kill target: 10.10.1.${KILL_NODE}"
echo "################################################################"

# ── Step 1-2: hard restart + load ────────────────────────────────────────────
hard_restart_cluster

LOAD_FILE="${OUT3}/${EXP_LABEL}_Load${FIELD_LENGTH}Bytes_run.scr"
log_banner "$LOG3" "LOAD" "LOAD" "$LOAD_FILE"
echo "--- loading ${RECORD_COUNT} records x ${FIELD_LENGTH}B (~70GB) ---"
$YCSB_DIR load $DB -threads $WTHREADS \
    -p recordcount=${RECORD_COUNT} \
    -p fieldlength=${FIELD_LENGTH} \
    -p measurement.raw.output_file="$LOAD_FILE" \
    -P commonworkload \
    -s >> "$LOG3" 2>&1
echo "--- load done ---"

# ── Step 3: compaction settle ────────────────────────────────────────────────
wait_compaction "${ALL_NODES[@]}"

# ── Step 4: warmup with all 3 nodes up ───────────────────────────────────────
echo ""
echo "############### WARMUP (3 nodes up) ###############"
warmup "prekill" "$OUT2" "$LOG2"

# ── Step 5: kill node3 ───────────────────────────────────────────────────────
echo ""
echo "############### KILLING 10.10.1.${KILL_NODE} ###############"
kill_node "$KILL_NODE"

SURVIVORS=()
for node in "${ALL_NODES[@]}"; do [ "$node" != "$KILL_NODE" ] && SURVIVORS+=("$node"); done
echo ">>> survivors: ${SURVIVORS[*]}"
echo "down_node=10.10.1.${KILL_NODE}  survivors=${SURVIVORS[*]}" > "${OUT2}/failure_info.txt"

wait_seen_down "$KILL_NODE" "${SURVIVORS[0]}"
echo "--- settling ${SETTLE_SECS}s before measuring ---"
sleep "$SETTLE_SECS"

# ── Step 6: workloadC on 2 nodes ─────────────────────────────────────────────
echo ""
echo "############### PHASE: 2-NODE (DEGRADED) ###############"
run_workloadc "2node" "$OUT2" "$LOG2" "$BD2" "${SURVIVORS[@]}"

# ── Step 7: bring node3 back ─────────────────────────────────────────────────
echo ""
echo "############### RESTARTING 10.10.1.${KILL_NODE} ###############"
start_node "$KILL_NODE" "$DROP_CACHE_ON_REJOIN"
wait_seen_up "$KILL_NODE" "${SURVIVORS[0]}"
wait_compaction "${ALL_NODES[@]}"
echo "--- settling ${SETTLE_SECS}s before warmup ---"
sleep "$SETTLE_SECS"

# ── Step 8-9: warmup + workloadC on 3 nodes ──────────────────────────────────
echo ""
echo "############### PHASE: 3-NODE (HEALTHY) ###############"
warmup "3node" "$OUT3" "$LOG3"
run_workloadc "3node" "$OUT3" "$LOG3" "$BD3" "${ALL_NODES[@]}"

echo ""
echo "############################################################"
echo "Done."
echo "  2-node (degraded) results: ${OUT2}/"
echo "  3-node (healthy)  results: ${OUT3}/"
echo "  Check YCSB *-FAILED counts in both logs -- with RF=3 and QUORUM=2"
echo "  they should be ~0 in the degraded phase."
echo "############################################################"
