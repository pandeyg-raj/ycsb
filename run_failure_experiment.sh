#!/bin/bash
# =============================================================================
# run_failure_experiment.sh
#
# Node-failure experiment: all YCSB core workloads at 32GB cache, run once with
# the full 5-node cluster, then AGAIN with one node down (degraded), with NO
# reload in between.
#
# Flow:
#   hard_restart (wipe + load 5 nodes) -> wait compaction -> drain -> snapshot
#   PHASE 5node:
#       stop -> restore snapshot -> restart(5 nodes, 32GB, clear cache)
#       warmup (fills 32GB cache) -> all workloads (C B D A F E C_postwrite)
#   PHASE 4node (degraded):
#       stop -> restore snapshot -> restart(5 nodes, 32GB, clear cache)
#       PAUSE: operator kills node5 (IP .6), presses Enter
#       auto-detect the down node
#       warmup on survivors -> all workloads on survivors
#
# Consistency QUORUM throughout. With 1 node down some ops may fail (QUORUM not
# met for keys whose replica set included the dead node) -- YCSB continues and
# logs *-FAILED counts; that failure signal is the point of the experiment.
#
# Node map: node0 = client (no Cassandra). Storage nodes node1..node5 = IPs
# 10.10.1.2 .. 10.10.1.6. "node5" (the one you kill) = IP 10.10.1.6.
# =============================================================================

# -- Config -------------------------------------------------------------------
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql
MEASURE_OPS=2000000          # lowered from 10M -- percentiles are steady-state
FIELD_LENGTH=10000           # object/write size (unchanged)
RECORD_COUNT=7000000         # dataset size (unchanged)
CACHE_SIZE="32GB"            # single cache size (no sweep)

SSH_USER=rzp5412
CASS_DIR=/mydata/cassandra
ALL_NODES=(2 3 4 5 6)        # IP last-octets of the 5 storage nodes

# Workloads: C read-only, B read-mostly, D read-latest, A 50/50, F rmw, E scan,
# then C_postwrite (read-only after the write-heavy runs). Stock commonworkload
# values (no compression valuepool).
WORKLOAD_LABELS=("workloadC" "workloadB" "workloadD" "workloadA" "workloadF" "workloadE" "workloadC_postwrite")
WORKLOAD_PARAMS=(
    "readproportion=1.0  -p updateproportion=0.0  -p insertproportion=0.0"                                                                                          # C read only
    "readproportion=0.95 -p updateproportion=0.05 -p insertproportion=0.0"                                                                                          # B read mostly
    "readproportion=0.95 -p updateproportion=0.0  -p insertproportion=0.05 -p requestdistribution=latest"                                                            # D read latest
    "readproportion=0.5  -p updateproportion=0.5  -p insertproportion=0.0"                                                                                          # A 50/50
    "readproportion=0.5  -p updateproportion=0.0  -p insertproportion=0.0 -p readmodifywriteproportion=0.5"                                                          # F read-modify-write
    "readproportion=0.0  -p scanproportion=0.95   -p updateproportion=0.0  -p insertproportion=0.05 -p maxscanlength=50 -p requestdistribution=uniform -p scanlengthdistribution=uniform"  # E scan
    "readproportion=1.0  -p updateproportion=0.0  -p insertproportion=0.0"                                                                                          # C_postwrite read only
)

# =============================================================================
# log_banner
# =============================================================================
log_banner() {
    local log=$1 label=$2 phase=$3 cache=$4 workload=$5 outfile=$6
    {
        echo ""
        echo "################################################################"
        echo "# SYSTEM   : ${label}"
        echo "# PHASE    : ${phase}"
        echo "# CACHE    : ${cache}"
        echo "# WORKLOAD : ${workload}"
        echo "# OUTFILE  : ${outfile}"
        echo "# TIME     : $(date '+%F %T')"
        echo "################################################################"
    } >> "$log"
}

# =============================================================================
# stop_cluster <node...>   -- stop Cassandra on the given nodes
# =============================================================================
stop_cluster() {
    local nodes=("$@")
    for node in "${nodes[@]}"; do
        local ip="10.10.1.$node"
        ssh ${SSH_USER}@${ip} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill 2>/dev/null; true"
    done
    for node in "${nodes[@]}"; do
        local ip="10.10.1.$node" a=0
        while ssh ${SSH_USER}@${ip} "ps -ef | grep '[j]ava' | grep -i 'cassandra' > /dev/null 2>&1"; do
            sleep 10; a=$((a+1))
            if [ "$a" -ge 6 ]; then
                ssh ${SSH_USER}@${ip} "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill -9 2>/dev/null; true"
                sleep 5; break
            fi
        done
        echo "  ${ip} stopped"
    done
}

# =============================================================================
# take_snapshot / restore_from_snapshot / delete_snapshot  (hard-link based)
# operate on the given node list.
# =============================================================================
take_snapshot() {
    local nodes=("$@")
    echo "--- Taking snapshot (hard-link data/ -> data_snapshot/) ---"
    for node in "${nodes[@]}"; do
        local ip="10.10.1.$node"
        ssh ${SSH_USER}@${ip} \
            "cd ${CASS_DIR} && rm -rf data_snapshot && cp -al data data_snapshot" &
    done
    wait
    echo "--- Snapshot taken ---"
}
restore_from_snapshot() {
    local nodes=("$@")
    echo "--- Restoring data/ from snapshot ---"
    for node in "${nodes[@]}"; do
        local ip="10.10.1.$node"
        ssh ${SSH_USER}@${ip} \
            "cd ${CASS_DIR} && rm -rf data && cp -al data_snapshot data" &
    done
    wait
    echo "--- Restored ---"
}
delete_snapshot() {
    local nodes=("$@")
    for node in "${nodes[@]}"; do
        ssh ${SSH_USER}@10.10.1.$node "rm -rf ${CASS_DIR}/data_snapshot" &
    done
    wait
}

# =============================================================================
# restart_cluster <cache_size> <node...>
#   Soft restart on the GIVEN nodes only: apply cgroup mem cap, evict page cache
#   (vmtouch -e data/ + non-fatal), start each, wait UN. Restarting only the
#   survivor list is what lets the degraded phase come up without the dead node.
# =============================================================================
restart_cluster() {
    local cache_size=$1; shift
    local nodes=("$@")
    local cache_gb="${cache_size//GB/}"
    local mem_bytes=$((cache_gb * 1024 * 1024 * 1024))
    echo ""
    echo "=== Soft restart: nodes ${nodes[*]}, cache=${cache_size} (${mem_bytes} bytes) ==="
    for node in "${nodes[@]}"; do
        local ip="10.10.1.$node"
        echo "  --- ${ip} ---"
        ssh ${SSH_USER}@${ip} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill 2>/dev/null; true"
        local ka=0
        while ssh ${SSH_USER}@${ip} "ps -ef | grep '[j]ava' | grep -i 'cassandra' > /dev/null 2>&1"; do
            sleep 10; ka=$((ka+1)); echo "  Waiting for stop... (${ka}/6)"
            if [ "$ka" -ge 6 ]; then
                ssh ${SSH_USER}@${ip} "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill -9 2>/dev/null; true"
                sleep 5; break
            fi
        done
        # cgroup mem cap + evict page cache (vmtouch non-fatal) + start
        ssh ${SSH_USER}@${ip} \
            "cd ${CASS_DIR} && \
             echo ${mem_bytes} | sudo tee /sys/fs/cgroup/mylimitedgroup/memory.max > /dev/null && \
             echo \$\$ | sudo tee /sys/fs/cgroup/mylimitedgroup/cgroup.procs > /dev/null ; \
             vmtouch -e data/ > /dev/null 2>&1 ; \
             sync; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null ; \
             bin/cassandra > /dev/null 2>&1"
        local sa=0
        until ssh ${SSH_USER}@${ip} "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep '${ip}' | grep -q 'UN'"; do
            sleep 10; sa=$((sa+1)); echo "  Waiting for UN... (${sa}/30)"
            if [ "$sa" -ge 30 ]; then echo "  ERROR: ${ip} not UN after 5 min."; exit 1; fi
        done
        echo "  ${ip} UN"
    done
    echo "=== Soft restart complete (${cache_size}). ==="
}

# =============================================================================
# hard_restart_cluster  -- wipe + start all 5, create table (for the load)
# =============================================================================
hard_restart_cluster() {
    local nodes=("${ALL_NODES[@]}")
    echo ""
    echo "=== HARD restart: nodes ${nodes[*]} ==="
    echo "  [1/3] Killing all in parallel..."
    for node in "${nodes[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill 2>/dev/null; true" &
    done
    wait
    for node in "${nodes[@]}"; do
        local ip="10.10.1.$node" a=0
        while ssh ${SSH_USER}@${ip} "ps -ef | grep '[j]ava' | grep -i 'cassandra' > /dev/null 2>&1"; do
            sleep 10; a=$((a+1))
            if [ "$a" -ge 6 ]; then
                ssh ${SSH_USER}@${ip} "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill -9 2>/dev/null; true"
                sleep 5; break
            fi
        done
        echo "  ${ip} stopped"
    done
    echo "  [2/3] Wiping data on all nodes..."
    for node in "${nodes[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} "rm -rf ${CASS_DIR}/data/ ${CASS_DIR}/data_snapshot/" &
    done
    wait
    echo "  [3/3] Starting sequentially (seeds first)..."
    for node in "${nodes[@]}"; do
        local ip="10.10.1.$node"
        ssh ${SSH_USER}@${ip} "cd ${CASS_DIR} && bin/cassandra > /dev/null 2>&1"
        local a=0
        until ssh ${SSH_USER}@${ip} "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep '${ip}' | grep -q 'UN'"; do
            sleep 10; a=$((a+1)); echo "  Waiting for ${ip} UN... (${a}/30)"
            if [ "$a" -ge 30 ]; then echo "  ERROR: ${ip} not UN after 5 min."; exit 1; fi
        done
        echo "  ${ip} UN"
    done
    echo "  Creating YCSB table via /mydata/${CREATE_TABLE_BIN}..."
    /mydata/${CREATE_TABLE_BIN}
    echo "=== HARD restart complete. ==="
}

# =============================================================================
# detect_down_node <candidate_node...>
#   After the operator kills a node, find which one is NOT UN. Prints the dead
#   node's octet (or empty if all still up). Queries a survivor for status.
# =============================================================================
detect_down_node() {
    local probe_ip="10.10.1.2"   # ask node1 (should always survive)
    local st; st="$(ssh ${SSH_USER}@${probe_ip} "${CASS_DIR}/bin/nodetool status 2>/dev/null")"
    for node in "${ALL_NODES[@]}"; do
        # a node is "down" if its line is NOT UN (DN, or absent)
        if ! echo "$st" | grep -E "^[[:space:]]*UN[[:space:]]+10\.10\.1\.${node}\b" >/dev/null; then
            echo "$node"; return
        fi
    done
    echo ""   # none down
}

# =============================================================================
# run_all_workloads <phase_tag> <out_dir> <log> <breakdown_file> <node...>
#   warmup (fills 32GB cache) then every workload back-to-back, QUORUM, breakdown
#   reset+collect per workload, on the given (survivor) node list.
# =============================================================================
run_all_workloads() {
    local phase=$1 out_dir=$2 log=$3 bdfile=$4; shift 4
    local nodes=("$@")

    # warmup ops fill the 32GB cache once (available = cap - 8GB JVM heap)
    local cache_gb="${CACHE_SIZE//GB/}"
    local available_bytes=$(( (cache_gb - 8) * 1024 * 1024 * 1024 ))
    local shard_size=$FIELD_LENGTH
    if echo "$EXP_LABEL" | grep -qi "ec"; then shard_size=$(( FIELD_LENGTH / 3 )); fi
    local objects_that_fit=$(( available_bytes / shard_size ))
    local WARMUP_OPS=$(( objects_that_fit < RECORD_COUNT ? objects_that_fit : RECORD_COUNT ))
    if [ "$WARMUP_OPS" -lt 1000000 ]; then WARMUP_OPS=1000000; fi

    local WARMUP_FILE="${out_dir}/${EXP_LABEL}_${phase}_${CACHE_SIZE}_Warmup.scr"
    log_banner "$log" "$EXP_LABEL" "$phase" "$CACHE_SIZE" "WARMUP" "$WARMUP_FILE"
    echo "--- [${phase}] Warmup (100% read, ${WARMUP_OPS} ops) ---"
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
    echo "--- [${phase}] Warmup done ---"

    for i in "${!WORKLOAD_LABELS[@]}"; do
        local workload="${WORKLOAD_LABELS[$i]}"
        local params="${WORKLOAD_PARAMS[$i]}"
        local MEASURE_FILE="${out_dir}/${EXP_LABEL}_${phase}_${CACHE_SIZE}_${workload}Run${FIELD_LENGTH}Bytes.scr"
        log_banner "$log" "$EXP_LABEL" "$phase" "$CACHE_SIZE" "$workload" "$MEASURE_FILE"
        echo "=== [${phase}] ${workload} ==="

        # reset breakdown on survivors
        for node in "${nodes[@]}"; do
            ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool breakdown --reset" 2>/dev/null
        done

        $YCSB_DIR run $DB -threads $THREADS \
            -p operationcount=$MEASURE_OPS \
            -p ${params} \
            -p recordcount=${RECORD_COUNT} \
            -p fieldlength=${FIELD_LENGTH} \
            -p measurement.raw.output_file="$MEASURE_FILE" \
            -p cassandra.writeconsistencylevel=QUORUM \
            -p cassandra.readconsistencylevel=QUORUM \
            -P commonworkload \
            -s >> "$log" 2>&1
        echo "=== [${phase}] Done: ${workload} ==="

        # collect breakdown from survivors
        echo "run for ${EXP_LABEL} ${phase} ${CACHE_SIZE} ${workload}" >> "$bdfile"
        for node in "${nodes[@]}"; do
            echo "-- node 10.10.1.$node --" >> "$bdfile"
            ssh ${SSH_USER}@10.10.1.$node \
                "${CASS_DIR}/bin/nodetool breakdown | grep -E 'keyspace|ycsb'" >> "$bdfile" 2>/dev/null
        done
    done
    echo ">>> [${phase}] All workloads done."
}

# =============================================================================
# Pre-flight
# =============================================================================
for bin in create_table_ec_compr_on create_table_ec_compr_off \
           create_table_rep_compr_on create_table_rep_compr_off; do
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

BASE="result_failure_${EXP_LABEL}_${COMPRESSION}"
OUT5="${BASE}_5node"; OUT4="${BASE}_4node"
mkdir -p "$OUT5" "$OUT4"
LOG5="${OUT5}/${EXP_LABEL}_5node_run.log"; BD5="${OUT5}/${EXP_LABEL}_5node_breakdown.txt"; touch "$BD5"
LOG4="${OUT4}/${EXP_LABEL}_4node_run.log"; BD4="${OUT4}/${EXP_LABEL}_4node_breakdown.txt"; touch "$BD4"

echo ""
echo "################################################################"
echo ">>> FAILURE EXPERIMENT | ${EXP_LABEL^^} | cache=${CACHE_SIZE} | ops=${MEASURE_OPS}"
echo ">>> workloads: ${WORKLOAD_LABELS[*]}"
echo "################################################################"

# ── Load (all 5 nodes) ────────────────────────────────────────────────────────
hard_restart_cluster
LOAD_FILE="${OUT5}/${EXP_LABEL}_Load${FIELD_LENGTH}Bytes_run.scr"
log_banner "$LOG5" "$EXP_LABEL" "LOAD" "FULL_MEM" "LOAD" "$LOAD_FILE"
echo "--- Loading ${RECORD_COUNT} records x ${FIELD_LENGTH}B (stock commonworkload) ---"
$YCSB_DIR load $DB -threads $WTHREADS \
    -p recordcount=${RECORD_COUNT} \
    -p fieldlength=${FIELD_LENGTH} \
    -p measurement.raw.output_file="$LOAD_FILE" \
    -P commonworkload \
    -s >> "$LOG5" 2>&1
echo "--- Load done ---"

# wait compaction settle
echo "--- Waiting for compaction to settle ---"
for node in "${ALL_NODES[@]}"; do
    ip="10.10.1.$node"
    while ssh ${SSH_USER}@${ip} "${CASS_DIR}/bin/nodetool compactionstats 2>/dev/null | grep -q 'pending tasks: [^0]'"; do
        sleep 30; echo "  compaction still running on ${ip}..."
    done
    echo "  ${ip} settled"
done
# drain -> stop -> snapshot (clean loaded state, all 5)
echo "--- Draining all nodes ---"
for node in "${ALL_NODES[@]}"; do ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool drain" & done
wait
stop_cluster "${ALL_NODES[@]}"
take_snapshot "${ALL_NODES[@]}"

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 1 — HEALTHY (5 nodes)
# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "############### PHASE 1: 5-NODE (HEALTHY) ###############"
stop_cluster "${ALL_NODES[@]}"
restore_from_snapshot "${ALL_NODES[@]}"
restart_cluster "$CACHE_SIZE" "${ALL_NODES[@]}"
run_all_workloads "5node" "$OUT5" "$LOG5" "$BD5" "${ALL_NODES[@]}"

# ══════════════════════════════════════════════════════════════════════════════
# PHASE 2 — DEGRADED (4 nodes). Restore + restart ALL 5, then operator kills one.
# ══════════════════════════════════════════════════════════════════════════════
echo ""
echo "############### PHASE 2: 4-NODE (DEGRADED) ###############"
stop_cluster "${ALL_NODES[@]}"
restore_from_snapshot "${ALL_NODES[@]}"
restart_cluster "$CACHE_SIZE" "${ALL_NODES[@]}"

echo ""
echo "################################################################"
echo ">>> ACTION REQUIRED:"
echo ">>>   All 5 nodes are UP. Now KILL node5 (IP 10.10.1.6) by hand"
echo ">>>   (e.g. on that node: sudo pkill -9 -f cassandra), then press Enter."
echo ">>>   The script will auto-detect which node went down and run the"
echo ">>>   degraded workloads on the survivors (QUORUM; failed ops are logged)."
echo "################################################################"
read -p ">>> Press Enter AFTER the node is down... " _

# auto-detect the dead node
DEAD=""
for attempt in 1 2 3; do
    DEAD="$(detect_down_node)"
    [ -n "$DEAD" ] && break
    echo "  no down node detected yet (gossip may lag) -- waiting 15s (attempt ${attempt}/3)..."
    sleep 15
done
if [ -z "$DEAD" ]; then
    echo "  WARNING: still see all nodes UN. Did the kill take effect?"
    read -p "  Type the octet of the node you killed (e.g. 6), or Enter to abort: " DEAD
    [ -z "$DEAD" ] && { echo "  aborting degraded phase."; delete_snapshot "${ALL_NODES[@]}"; exit 1; }
fi
echo ">>> Detected DOWN node: 10.10.1.${DEAD}"

# survivor list = all nodes except the dead one
SURVIVORS=()
for node in "${ALL_NODES[@]}"; do [ "$node" != "$DEAD" ] && SURVIVORS+=("$node"); done
echo ">>> Survivors: ${SURVIVORS[*]}"
echo "down_node=10.10.1.${DEAD}  survivors=${SURVIVORS[*]}" > "${OUT4}/failure_info.txt"

run_all_workloads "4node" "$OUT4" "$LOG4" "$BD4" "${SURVIVORS[@]}"

# cleanup snapshot on all nodes we can reach (survivors); dead node's is harmless
delete_snapshot "${SURVIVORS[@]}"

echo ""
echo "############################################################"
echo "Failure experiment complete."
echo "  5-node results: ${OUT5}/"
echo "  4-node results: ${OUT4}/   (down node 10.10.1.${DEAD})"
echo "  YCSB *-FAILED counts in the run logs show degraded-mode op failures."
echo "############################################################"
