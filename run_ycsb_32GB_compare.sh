#!/bin/bash
# =============================================================================
# run_ycsb_32GB_compare.sh
#
# Based on run_OS_Cache_experiments_Compress.sh. One memory budget (32GB cgroup),
# default YCSB random values (no value pool), for ELECT / ELECT-EC-off / LEAST / Cassandra.
#
# ASSUMES MANUAL SETUP: cluster running and table created BEFORE starting.
#
# Flow:
#   confirm setup -> record configs -> load (CL from commonworkload = ALL)
#   -> wait for compaction to settle -> drain -> restart all nodes in 32GB cgroup
#      (page cache evicted) -> cache-filling warmup -> workloads in order:
#      C_warmup, B, D, A, C, E, F   (compaction stays ON throughout)
#   -> per workload: disk-read + memcg counters (all systems), breakdown (LEAST/Cassandra)
#   -> summary of mean / p50 / p99 per workload and operation
# =============================================================================

# -- Config -------------------------------------------------------------------
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql
FIELD_LENGTH=10000
RECORD_COUNT=7000000
CACHE_SIZE="32GB"
HEAP_GB=8                       # default heap on these nodes (-Xmx8192M); used for warmup sizing only

NODES=(2 3 4 5 6)               # cluster nodes 10.10.1.2 - 10.10.1.6
SSH_USER=rzp5412
CGROUP=/sys/fs/cgroup/mylimitedgroup
COLD_TIER_IP=10.10.1.7
CQLSH=/mydata/cassandra/bin/cqlsh     # cqlsh on this client node (used only for DESCRIBE)
MAX_SETTLE_MIN=90               # give up waiting for compaction after this many minutes

# Workloads (definitions kept from run_OS_Cache_experiments_Compress(_Scan).sh)
WORKLOAD_LABELS=("workloadC_warmup" "workloadB" "workloadD" "workloadA" "workloadC" "workloadE" "workloadF")
WORKLOAD_OPS=(2000000 2000000 2000000 2000000 2000000 200000 2000000)
WORKLOAD_PARAMS=(
    "readproportion=1.0  -p updateproportion=0.0 -p insertproportion=0"                              # C_warmup: read only
    "readproportion=0.95 -p updateproportion=0.05 -p insertproportion=0"                             # B: read mostly
    "readproportion=0.95 -p updateproportion=0.0 -p insertproportion=0.05 -p requestdistrib=latest"  # D: kept as in original script (runs uniform)
    "readproportion=0.5  -p updateproportion=0.5 -p insertproportion=0"                              # A: 50/50
    "readproportion=1.0  -p updateproportion=0.0 -p insertproportion=0"                              # C: read only (after writes)
    "readproportion=0.0 -p scanproportion=0.95 -p updateproportion=0.0 -p insertproportion=0.05 -p maxscanlength=50 -p requestdistribution=uniform -p scanlengthdistribution=uniform"  # E: scans
    "readproportion=0.5 -p updateproportion=0.0 -p insertproportion=0 -p readmodifywriteproportion=0.5"  # F: read-modify-write
)

# =============================================================================
# Interactive setup
# =============================================================================
read -p "Test mode (tiny counts, to check the script end to end)? (yes/no) [no]: " TEST_MODE
TEST_MODE=${TEST_MODE:-no}
if [ "$TEST_MODE" = "yes" ]; then
    RECORD_COUNT=10000
    WORKLOAD_OPS=(2000 2000 2000 2000 2000 200 2000)
    TEST_WARMUP_OPS=2000
    RESULT_PREFIX="result_TEST_32GB_compare"
    echo ">>> TEST MODE: ${RECORD_COUNT} records, ${TEST_WARMUP_OPS} warmup reads, 2000 ops per workload (E: 200)"
else
    RESULT_PREFIX="result_32GB_compare"
fi

echo "Which system?  elect | elect_noec | ec (LEAST) | rep (Cassandra)"
read EXP_LABEL
case "$EXP_LABEL" in
    elect)      CASS_DIR=/mydata/elect/src/elect; NODE_CONF=conf/elect.yaml;     KEYSPACE=ycsb;    TABLE=usertable0; IS_ELECT=1; BREAKDOWN=0; SHARD=$FIELD_LENGTH ;;
    elect_noec) CASS_DIR=/mydata/elect/src/elect; NODE_CONF=conf/elect.yaml;     KEYSPACE=ycsbraw; TABLE=usertable0; IS_ELECT=1; BREAKDOWN=0; SHARD=$FIELD_LENGTH ;;
    ec)         CASS_DIR=/mydata/cassandra;       NODE_CONF=conf/cassandra.yaml; KEYSPACE=ycsb;    TABLE=usertable;  IS_ELECT=0; BREAKDOWN=1; SHARD=$(( FIELD_LENGTH / 3 )) ;;
    rep)        CASS_DIR=/mydata/cassandra;       NODE_CONF=conf/cassandra.yaml; KEYSPACE=ycsb;    TABLE=usertable;  IS_ELECT=0; BREAKDOWN=1; SHARD=$FIELD_LENGTH ;;
    *) echo "ERROR: unknown system '$EXP_LABEL'"; exit 1 ;;
esac

read -p "Keyspace [${KEYSPACE}]: " ans; KEYSPACE=${ans:-$KEYSPACE}
read -p "Table    [${TABLE}]: " ans;    TABLE=${ans:-$TABLE}
read -p "Write (load) threads [8]: " WTHREADS; WTHREADS=${WTHREADS:-8}
read -p "Run threads [8]: " THREADS;          THREADS=${THREADS:-8}
read -p "Data already loaded? Skip the load (yes/no) [no]: " SKIP_LOAD;          SKIP_LOAD=${SKIP_LOAD:-no}
read -p "Wait for compaction to settle after load (yes/no) [no]: " WAIT_SETTLE; WAIT_SETTLE=${WAIT_SETTLE:-no}

TS=$(date +%Y%m%d_%H%M)
OUT_DIR="${RESULT_PREFIX}_${EXP_LABEL}_${TS}"
mkdir -p "$OUT_DIR"
LOG="${OUT_DIR}/run.log"
BREAKDOWN_FILE="${OUT_DIR}/breakdown.txt"
YCSB_TABLE_PARAMS="-p cassandra.keyspace=${KEYSPACE} -p table=${TABLE}"

echo ""
echo "################################################################"
echo ">>> System   : ${EXP_LABEL}   (${CASS_DIR})"
echo ">>> Table    : ${KEYSPACE}.${TABLE}"
echo ">>> Cache    : ${CACHE_SIZE} cgroup  | records ${RECORD_COUNT} x ${FIELD_LENGTH}B random values"
echo ">>> Threads  : load ${WTHREADS}, run ${THREADS}"
echo ">>> Skip load: ${SKIP_LOAD}   | wait for compaction settle: ${WAIT_SETTLE}"
echo ">>> Output   : ${OUT_DIR}"
echo "################################################################"
if [ "$EXP_LABEL" = "elect_noec" ]; then
    echo "REMINDER: elect_noec needs enable_erasure_coding: false AND enable_migration: false"
    echo "          in conf/elect.yaml on all nodes (restarted), and keyspace ycsbraw created."
fi
echo ""
echo "This script assumes MANUAL setup is done:"
echo "  - cluster is running on all nodes (nodetool status: all UN)"
if [ "$SKIP_LOAD" = "yes" ]; then
    echo "  - table ${KEYSPACE}.${TABLE} is ALREADY LOADED with ${RECORD_COUNT} records"
else
    echo "  - table ${KEYSPACE}.${TABLE} exists and is EMPTY, with its final options"
fi
[ "$IS_ELECT" = "1" ] && echo "  - cold tier (OSSServer) running on ${COLD_TIER_IP}:8000"
read -p "Is setup done? (yes/no): " ok
[ "$ok" = "yes" ] || { echo "Aborting."; exit 1; }

# =============================================================================
# Helpers
# =============================================================================
ip_of() { echo "10.10.1.$1"; }

log_banner() {
    { echo ""; echo "########################################################"
      echo "# SYSTEM   : ${EXP_LABEL}"; echo "# WORKLOAD : $1"; echo "# OUTPUT   : $2"
      echo "# TIME     : $(date)"; echo "########################################################"; } >> "$LOG"
}

check_prereqs() {
    echo "--- Checking cluster, cgroup and vmtouch ---"
    local bad=0
    for n in "${NODES[@]}"; do
        local ip; ip=$(ip_of $n)
        ssh ${SSH_USER}@${ip} "test -d ${CGROUP}" || { echo "  ${ip}: cgroup ${CGROUP} MISSING"; bad=1; }
        ssh ${SSH_USER}@${ip} "command -v vmtouch >/dev/null" || { echo "  ${ip}: vmtouch MISSING"; bad=1; }
        ssh ${SSH_USER}@${ip} "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep '${ip}' | grep -q '^UN'" \
            || { echo "  ${ip}: not UN"; bad=1; }
    done
    if [ "$IS_ELECT" = "1" ]; then
        ssh ${SSH_USER}@$(ip_of ${NODES[0]}) "timeout 3 bash -c '</dev/tcp/${COLD_TIER_IP}/8000'" \
            || { echo "  cold tier ${COLD_TIER_IP}:8000 NOT reachable"; bad=1; }
    fi
    [ "$bad" = "0" ] || { echo "ERROR: prerequisites failed. Fix and rerun."; exit 1; }
    echo "  all OK"
}

record_configs() {
    local tag=$1
    local dir="${OUT_DIR}/config_${tag}"
    mkdir -p "$dir"
    for n in "${NODES[@]}"; do
        local ip; ip=$(ip_of $n)
        ssh ${SSH_USER}@${ip} "cat ${CASS_DIR}/${NODE_CONF}" > "${dir}/node${n}_$(basename ${NODE_CONF})" 2>&1
        ssh ${SSH_USER}@${ip} "ps -eo args | grep '[C]assandraDaemon' | tr ' ' '\n' | grep -E '^-X(ms|mx|mn)|GC$|^-XX:\+Use.*GC'; java -version 2>&1 | head -1" \
            > "${dir}/node${n}_jvm.txt" 2>&1
        ssh ${SSH_USER}@${ip} "cat ${CGROUP}/memory.max 2>/dev/null" > "${dir}/node${n}_cgroup_memory_max.txt" 2>&1
        ssh ${SSH_USER}@${ip} "${CASS_DIR}/bin/nodetool info 2>&1" > "${dir}/node${n}_nodetool_info.txt"
        ssh ${SSH_USER}@${ip} "cd ${CASS_DIR} && git log -1 --format='%H %ad %s' 2>/dev/null || echo 'not a git repo'" \
            > "${dir}/node${n}_git.txt" 2>&1
    done
    ssh ${SSH_USER}@$(ip_of ${NODES[0]}) "${CASS_DIR}/bin/nodetool ring 2>&1" > "${dir}/nodetool_ring.txt"
    ${CQLSH} $(ip_of ${NODES[0]}) -e "DESCRIBE KEYSPACE ${KEYSPACE};" > "${dir}/describe_keyspace.txt" 2>&1
    ${CQLSH} $(ip_of ${NODES[0]}) -e "SELECT table_name, speculative_retry, read_repair, compression, caching, compaction FROM system_schema.tables WHERE keyspace_name='${KEYSPACE}';" \
        > "${dir}/table_options.txt" 2>&1
    echo "  configs saved to ${dir}"
}

collect_tablestats() {
    local outfile=$1; : > "$outfile"
    for n in "${NODES[@]}"; do
        echo "===== node 10.10.1.$n =====" >> "$outfile"
        ssh ${SSH_USER}@$(ip_of $n) "${CASS_DIR}/bin/nodetool tablestats ${KEYSPACE}" >> "$outfile" 2>&1
    done
}

# ELECT only: evidence that erasure coding / migration actually ran
collect_elect_state() {
    local outfile=$1; : > "$outfile"
    for n in "${NODES[@]}"; do
        ssh ${SSH_USER}@$(ip_of $n) "cd ${CASS_DIR} && \
            echo \"node$n ECMetadata_files=\$(ls data/ECMetadata 2>/dev/null | wc -l) \
localParityHashes=\$(ls data/localParityHashes 2>/dev/null | wc -l) \
receivedParityHashes=\$(ls data/receivedParityHashes 2>/dev/null | wc -l) \
UnsatisfiedLinkError=\$(grep -c UnsatisfiedLinkError logs/system.log logs/debug.log 2>/dev/null | awk -F: '{s+=\$2} END{print s+0}')\"" >> "$outfile"
    done
    ssh ${SSH_USER}@${COLD_TIER_IP} "echo \"coldtier files=\$(ls /mydata/elect/src/coldTier/data 2>/dev/null | wc -l) size=\$(du -sh /mydata/elect/src/coldTier/data 2>/dev/null | cut -f1)\"" >> "$outfile"
    cat "$outfile"
}

wait_for_compaction_settle() {
    echo "--- Waiting for compaction to settle (max ${MAX_SETTLE_MIN} min per node) ---"
    for n in "${NODES[@]}"; do
        local ip; ip=$(ip_of $n); local waited=0
        while ssh ${SSH_USER}@${ip} "${CASS_DIR}/bin/nodetool compactionstats 2>/dev/null | grep -q 'pending tasks: [^0]'"; do
            sleep 30; waited=$((waited + 30))
            echo "  compaction still running on ${ip} ($((waited / 60)) min)"
            if [ "$waited" -ge $((MAX_SETTLE_MIN * 60)) ]; then
                echo "  WARNING: ${ip} still compacting after ${MAX_SETTLE_MIN} min; continuing" | tee -a "$LOG"; break
            fi
        done
        echo "  ${ip} settled"
    done
}

stop_node() {
    local ip=$1
    ssh ${SSH_USER}@${ip} "ps -eo pid,args | grep '[C]assandraDaemon' | awk '{print \$1}' | xargs -r kill"
    local a=0
    while ssh ${SSH_USER}@${ip} "ps -eo args | grep -q '[C]assandraDaemon'"; do
        sleep 10; a=$((a + 1))
        if [ "$a" -ge 6 ]; then
            ssh ${SSH_USER}@${ip} "ps -eo pid,args | grep '[C]assandraDaemon' | awk '{print \$1}' | xargs -r kill -9"
            sleep 5; break
        fi
    done
}

# Restart every node inside the cgroup with page cache evicted. Data is kept.
restart_in_cgroup() {
    local cache_gb="${CACHE_SIZE//GB/}"
    local mem_bytes=$((cache_gb * 1024 * 1024 * 1024))
    local start_cmd
    if [ "$IS_ELECT" = "1" ]; then
        start_cmd="(nohup bin/elect > logs/stdout.log 2>&1 < /dev/null &)"
    else
        start_cmd="bin/cassandra > /dev/null 2>&1"
    fi
    echo "=== Restart in ${CACHE_SIZE} cgroup (${mem_bytes} bytes), page cache evicted ==="
    for n in "${NODES[@]}"; do
        local ip; ip=$(ip_of $n)
        echo "  --- ${ip} ---"
        stop_node "$ip"
        ssh ${SSH_USER}@${ip} "cd ${CASS_DIR} && \
            echo ${mem_bytes} | sudo tee ${CGROUP}/memory.max > /dev/null && \
            echo \$\$ | sudo tee ${CGROUP}/cgroup.procs > /dev/null && \
            vmtouch -e data/ > /dev/null 2>&1; ${start_cmd}"
        local a=0
        until ssh ${SSH_USER}@${ip} "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep '${ip}' | grep -q '^UN'"; do
            sleep 10; a=$((a + 1)); echo "  waiting for UN... (${a}/30)"
            [ "$a" -ge 30 ] && { echo "  ERROR: ${ip} not UN after 5 min. Check ${CASS_DIR}/logs/system.log"; exit 1; }
        done
        echo "  ${ip} UN"
    done
}

# Disk-read and page-cache counters (same for every system)
snapshot_counters() {
    local outfile=$1; : > "$outfile"
    for n in "${NODES[@]}"; do
        ssh ${SSH_USER}@$(ip_of $n) "dev=\$(basename \$(readlink -f \$(df --output=source ${CASS_DIR} | tail -1))); \
            echo \"### node$n dev=\$dev\"; grep -w \"\$dev\" /proc/diskstats | head -1; \
            sudo cat ${CGROUP}/memory.stat 2>/dev/null | grep -E '^(file|workingset_refault_file|pgmajfault) '" >> "$outfile"
    done
}

breakdown_reset() {
    [ "$BREAKDOWN" = "1" ] || return 0
    for n in "${NODES[@]}"; do ssh ${SSH_USER}@$(ip_of $n) "${CASS_DIR}/bin/nodetool breakdown --reset"; done
}

breakdown_collect() {
    [ "$BREAKDOWN" = "1" ] || return 0
    echo "run ${EXP_LABEL} ${CACHE_SIZE} $1" >> "$BREAKDOWN_FILE"
    for n in "${NODES[@]}"; do
        echo "-- node 10.10.1.$n --" >> "$BREAKDOWN_FILE"
        ssh ${SSH_USER}@$(ip_of $n) "${CASS_DIR}/bin/nodetool breakdown | grep -E 'keyspace|ycsb'" >> "$BREAKDOWN_FILE"
    done
}

# =============================================================================
# 1) Checks + configs before load
# =============================================================================
check_prereqs
record_configs "before_load"

# =============================================================================
# 2) Load (consistency from commonworkload = ALL; load latency not reported)
# =============================================================================
if [ "$SKIP_LOAD" = "yes" ]; then
    echo "--- Skipping load (data already loaded) ---" | tee -a "$LOG"
else
LOAD_FILE="${OUT_DIR}/${EXP_LABEL}_Load.scr"
log_banner "LOAD" "$LOAD_FILE"
echo "--- Loading ${RECORD_COUNT} x ${FIELD_LENGTH}B random values into ${KEYSPACE}.${TABLE} ---"
LOAD_START=$(date +%s)
$YCSB_DIR load $DB -threads $WTHREADS \
    -p recordcount=${RECORD_COUNT} -p fieldlength=${FIELD_LENGTH} \
    ${YCSB_TABLE_PARAMS} \
    -p measurement.raw.output_file="$LOAD_FILE" \
    -P commonworkload -s >> "$LOG" 2>&1
LOAD_RC=$?
LOAD_OPS=$(grep -c '^INSERT,' "$LOAD_FILE" 2>/dev/null)
LOAD_OPS=${LOAD_OPS:-0}
echo "--- Load done in $(( ($(date +%s) - LOAD_START) / 60 )) min: exit=${LOAD_RC}, inserts recorded=${LOAD_OPS} ---"
if [ "$LOAD_RC" != "0" ] || [ "$LOAD_OPS" -lt $(( RECORD_COUNT * 99 / 100 )) ]; then
    echo "ERROR: load failed or incomplete. Last lines of ${LOG}:"
    tail -20 "$LOG"
    exit 1
fi
fi

if [ "$WAIT_SETTLE" = "yes" ]; then
    wait_for_compaction_settle
else
    echo "--- Not waiting for compaction (same for all systems) ---" | tee -a "$LOG"
fi
for n in "${NODES[@]}"; do
    echo "node$n pending=$(ssh ${SSH_USER}@$(ip_of $n) "${CASS_DIR}/bin/nodetool compactionstats 2>/dev/null | awk '/pending tasks/{print \$NF}'")"
done | tee "${OUT_DIR}/pending_compactions_before_measure.txt"
collect_tablestats "${OUT_DIR}/tablestats_after_load.txt"
[ "$IS_ELECT" = "1" ] && collect_elect_state "${OUT_DIR}/elect_state_after_load.txt"

echo "--- Draining all nodes ---"
for n in "${NODES[@]}"; do ssh ${SSH_USER}@$(ip_of $n) "${CASS_DIR}/bin/nodetool drain" & done
wait

# =============================================================================
# 3) Restart in 32GB cgroup + cache-filling warmup
# =============================================================================
restart_in_cgroup
record_configs "after_cgroup_restart"

cache_gb="${CACHE_SIZE//GB/}"
available_bytes=$(( (cache_gb - HEAP_GB) * 1024 * 1024 * 1024 ))
objects_that_fit=$(( available_bytes / SHARD ))
WARMUP_OPS=$(( objects_that_fit < RECORD_COUNT ? objects_that_fit : RECORD_COUNT ))
[ "$WARMUP_OPS" -lt 1000000 ] && WARMUP_OPS=1000000
[ "$TEST_MODE" = "yes" ] && WARMUP_OPS=$TEST_WARMUP_OPS

WARMUP_FILE="${OUT_DIR}/${EXP_LABEL}_Warmup.scr"
log_banner "WARMUP (${WARMUP_OPS} ops)" "$WARMUP_FILE"
echo "--- Cache-filling warmup: ${WARMUP_OPS} reads ---"
WARMUP_START=$(date +%s)
$YCSB_DIR run $DB -threads $THREADS \
    -p operationcount=$WARMUP_OPS \
    -p readproportion=1.0 -p updateproportion=0.0 -p insertproportion=0.0 \
    -p recordcount=${RECORD_COUNT} -p fieldlength=${FIELD_LENGTH} \
    ${YCSB_TABLE_PARAMS} \
    -p measurement.raw.output_file="$WARMUP_FILE" \
    -p cassandra.writeconsistencylevel=QUORUM -p cassandra.readconsistencylevel=QUORUM \
    -P commonworkload -s >> "$LOG" 2>&1
echo "--- Warmup done ---"

# Show warmup speed and let the user halve every workload if the run would take too long.
WARMUP_SECS=$(( $(date +%s) - WARMUP_START )); [ "$WARMUP_SECS" -lt 1 ] && WARMUP_SECS=1
WARMUP_RATE=$(( WARMUP_OPS / WARMUP_SECS ))
TOTAL_OPS=0; for o in "${WORKLOAD_OPS[@]}"; do TOTAL_OPS=$(( TOTAL_OPS + o )); done
[ "$WARMUP_RATE" -lt 1 ] && WARMUP_RATE=1
EST_MIN=$(( TOTAL_OPS / WARMUP_RATE / 60 ))
echo ""
echo ">>> Warmup: ${WARMUP_OPS} reads in $(( WARMUP_SECS / 60 )) min = ${WARMUP_RATE} ops/s"
echo ">>> Rough time for all workloads: ~${EST_MIN} min at current counts, ~$(( EST_MIN / 2 )) min if halved"
echo ">>>   (scans in E are slower per op, so add some margin)"
echo ">>> Use the SAME choice for every system."
HALVE=no
if read -t 900 -p "Halve all workload operation counts? (yes/no) [no, auto after 15 min]: " ans; then
    HALVE=${ans:-no}
else
    echo ""; echo "  no answer in 15 min, keeping full counts"
fi
if [ "$HALVE" = "yes" ]; then
    for i in "${!WORKLOAD_OPS[@]}"; do WORKLOAD_OPS[$i]=$(( WORKLOAD_OPS[$i] / 2 )); done
fi
echo "WORKLOAD_OPS: halved=${HALVE} counts=${WORKLOAD_OPS[*]} (warmup ${WARMUP_RATE} ops/s)" | tee -a "$LOG" "${OUT_DIR}/workload_ops.txt"

# =============================================================================
# 4) Workloads (compaction ON)
# =============================================================================
for i in "${!WORKLOAD_LABELS[@]}"; do
    workload="${WORKLOAD_LABELS[$i]}"
    ops="${WORKLOAD_OPS[$i]}"
    params="${WORKLOAD_PARAMS[$i]}"
    MEASURE_FILE="${OUT_DIR}/${EXP_LABEL}_${workload}.scr"
    log_banner "$workload (${ops} ops)" "$MEASURE_FILE"
    echo "=== ${workload} | ${ops} ops | $(date +%H:%M) ==="

    breakdown_reset
    snapshot_counters "${OUT_DIR}/counters_${workload}_before.txt"
    W_START=$(date +%s)
    $YCSB_DIR run $DB -threads $THREADS \
        -p operationcount=${ops} \
        -p ${params} \
        -p recordcount=${RECORD_COUNT} -p fieldlength=${FIELD_LENGTH} \
        ${YCSB_TABLE_PARAMS} \
        -p measurement.raw.output_file="$MEASURE_FILE" \
        -p cassandra.writeconsistencylevel=QUORUM -p cassandra.readconsistencylevel=QUORUM \
        -P commonworkload -s >> "$LOG" 2>&1
    echo "=== Done: ${workload} in $(( ($(date +%s) - W_START) / 60 )) min ==="
    snapshot_counters "${OUT_DIR}/counters_${workload}_after.txt"
    breakdown_collect "$workload"
done

# =============================================================================
# 5) Final state + summary
# =============================================================================
collect_tablestats "${OUT_DIR}/tablestats_end.txt"
[ "$IS_ELECT" = "1" ] && collect_elect_state "${OUT_DIR}/elect_state_end.txt"

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
python3 "${SCRIPT_DIR}/summarize_32GB.py" "$OUT_DIR" "$EXP_LABEL"

echo ""
echo "All done: ${OUT_DIR}/  (summary.txt, run.log, *.scr, configs, counters, tablestats)"
if [ "$TEST_MODE" = "yes" ]; then
    echo ""
    echo "TEST MODE finished. Before the real run, start clean:"
    echo "  1. stop the cluster, delete data/ and logs/ on every node (ELECT: recreate the data/ subdirectories)"
    echo "  2. echo max | sudo tee ${CGROUP}/memory.max on every node"
    echo "  3. start the cluster without the memory limit and recreate the keyspace and tables"
fi
