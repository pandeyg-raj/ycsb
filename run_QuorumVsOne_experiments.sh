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
DATA_DEV=dm-0

DISABLE_COMPACTION_DURING_MEASURE=0

# =============================================================================
stop_cluster() {
    echo ""; echo "=== Stopping Cassandra on all nodes ==="
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node"
        ssh ${SSH_USER}@${ip} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill 2>/dev/null; true"
        local attempts=0
        while ssh ${SSH_USER}@${ip} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' > /dev/null 2>&1"; do
            sleep 10; attempts=$((attempts + 1))
            if [ "$attempts" -ge 6 ]; then
                ssh ${SSH_USER}@${ip} \
                    "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill -9 2>/dev/null; true"
                sleep 5; break
            fi
        done
        echo "  ${ip} stopped"
    done
    echo "=== All nodes stopped ==="
}

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

snapshot_memstat() {
    local tag=$1 outfile=$2
    : > "$outfile"
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node"
        echo "### node${node} ${tag}" >> "$outfile"
        ssh ${SSH_USER}@${ip} \
            "sudo cat ${CGROUP}/memory.stat 2>/dev/null | grep -E '^(file|pgfault|pgmajfault|workingset_refault_file) '" >> "$outfile"
    done
}
snapshot_diskstats() {
    local outfile=$1
    : > "$outfile"
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node"
        local line; line="$(ssh ${SSH_USER}@${ip} "grep -w ${DATA_DEV} /proc/diskstats | head -1")"
        echo "node${node} ${line}" >> "$outfile"
    done
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
echo ">>> CONSISTENCY | ${EXP_LABEL^^} | 5 nodes | 10KB | 50/50 ${REQUEST_DIST} | cache=${CACHE_SIZE} | compr=${COMPRESSION}"
echo ">>> read CL=${READ_CL}  write CL=${WRITE_CL}"
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

# 3) MEASURE window (the metrics that matter for 6.4 are in run.log/Measure.scr)
if [ "$DISABLE_COMPACTION_DURING_MEASURE" = "1" ]; then
    for node in "${BD_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool disableautocompaction"
    done
fi

echo "--- Reset breakdown + snapshot counters (BEFORE) ---"
for node in "${BD_NODES[@]}"; do
    ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool breakdown --reset"
done
snapshot_memstat   "before" "${OUT_DIR}/memstat_before.txt"
snapshot_diskstats          "${OUT_DIR}/diskstats_before.txt"

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

snapshot_memstat   "after" "${OUT_DIR}/memstat_after.txt"
snapshot_diskstats         "${OUT_DIR}/diskstats_after.txt"

echo "--- Collecting breakdown (ycsb/keyspace lines) ---"
echo "run ${EXP_LABEL} consistency R${READ_CL} W${WRITE_CL} ${CACHE_SIZE} 50-50 ${REQUEST_DIST} compr=${COMPRESSION}" >> "$BREAKDOWN_FILE"
for node in "${BD_NODES[@]}"; do
    echo "-- node 10.10.1.$node --" >> "$BREAKDOWN_FILE"
    ssh ${SSH_USER}@10.10.1.$node \
        "${CASS_DIR}/bin/nodetool breakdown | grep -E 'keyspace|ycsb'" >> "$BREAKDOWN_FILE"
done

if [ "$DISABLE_COMPACTION_DURING_MEASURE" = "1" ]; then
    for node in "${BD_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool enableautocompaction"
    done
fi

# 4) Corroborating cache-miss summary (headline latency/throughput is in run.log)
READ_OPS=$(python3 -c "print(int(${MEASURE_OPS} * ${READ_PROP}))")
SUMMARY="${OUT_DIR}/cachemiss_summary.txt"
python3 - "$OUT_DIR" "$SUMMARY" "$READ_OPS" "$READ_CL" "$WRITE_CL" << 'PYEOF'
import sys, re, os
outdir, summary, read_ops, rcl, wcl = sys.argv[1:6]
read_ops=int(read_ops)
def diskstats(p):
    d={}
    for line in open(p):
        x=line.split()
        if len(x)<7: continue
        try: d[x[0]]=(int(x[4]),int(x[6]))
        except: pass
    return d
def memstat(p):
    d={}; n=None
    for line in open(p):
        line=line.rstrip(); m=re.match(r'### node(\d+)',line)
        if m: n='node'+m.group(1); d[n]={}; continue
        if n is None: continue
        t=line.split()
        if len(t)==2 and t[1].isdigit(): d[n][t[0]]=int(t[1])
    return d
db=diskstats(os.path.join(outdir,"diskstats_before.txt")); da=diskstats(os.path.join(outdir,"diskstats_after.txt"))
mb=memstat(os.path.join(outdir,"memstat_before.txt")); ma=memstat(os.path.join(outdir,"memstat_after.txt"))
lines=[]; tot=0; tot_rf=0
for n in sorted(set(db)&set(da)):
    miss=(da[n][1]-db[n][1])*512; ro=da[n][0]-db[n][0]; tot+=miss
    rf=ma.get(n,{}).get('workingset_refault_file',0)-mb.get(n,{}).get('workingset_refault_file',0); tot_rf+=rf
    res=ma.get(n,{}).get('file',None); rs=f"{res/1024**3:.2f}GiB" if res is not None else "n/a"
    lines.append(f"{n}: disk_read(miss)={miss/1024**2:9.1f}MB  disk_read_ops={ro:>9}  refault(+)={rf:>10}  resident={rs}")
with open(summary,"w") as out:
    out.write(f"=== Cache-miss corroboration (read CL={rcl}, write CL={wcl}) ===\n")
    out.write(f"read_ops={read_ops}\n\n")
    out.write("\n".join(lines)+"\n\n")
    out.write(f"TOTAL disk-read (miss) bytes : {tot/1024**2:.1f} MB ({tot/1024**3:.2f} GiB)\n")
    out.write(f"Disk bytes per read-op       : {tot/read_ops:.1f} B/op\n")
    out.write(f"TOTAL cache-pressure refaults: {tot_rf:,}\n\n")
    out.write("Headline 6.4 metrics (mean/p99 read latency, throughput) are in run.log\n"
              "and Measure.scr; this file corroborates the cache-hit explanation.\n")
print(open(summary).read())
PYEOF

echo ""
echo "############################################################"
echo "Done. ${OUT_DIR}/   (read CL=${READ_CL}, write CL=${WRITE_CL})"
echo "  run.log / Measure.scr   latency + THROUGHPUT (headline)"
echo "  cachemiss_summary.txt   cache-hit corroboration"
echo "  breakdown.txt           component split"
echo "  diskstats/memstat       before/after"
echo "############################################################"
