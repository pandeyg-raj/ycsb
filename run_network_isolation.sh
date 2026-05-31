#!/bin/bash
# =============================================================================
# run_network_isolation.sh

# -- Config -------------------------------------------------------------------
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql

FIELD_LENGTH=10000
RECORD_COUNT=1000000          # 1M x 10KB = ~10GB logical (fits in memory)
WARMUP_OPS=$(( 2 * RECORD_COUNT ))   # read all keys twice
MEASURE_OPS=1000000          # read-only run 
CACHE_SIZE="32GB"             # matches paper ("10GB fits within 32GB memory")
COMPRESSION="on"

REQUEST_DIST="sequential"     # sequential wraps modulo recordcount, so warmup
                              # (2*N ops) reads every key EXACTLY twice and the
                              # measure (10M ops) makes 10 even passes over all
                              # 1M keys. vmtouch -t is then belt-and-suspenders.

# Both EC and REP on the same 5-node cluster (.2-.6).
NUM_NODES=5
BD_NODES=(2 3 4 5 6)

SSH_USER=rzp5412
CASS_DIR=/mydata/cassandra
CGROUP=/sys/fs/cgroup/mylimitedgroup
DATA_DEV=dm-0                 # /mydata LV (confirmed 253:0)

# Read-only measure has no writes -> no compaction; we still disable it so the
# residual-disk-read check is airtight.
DISABLE_COMPACTION_DURING_MEASURE=1

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

# restart + PRELOAD: vmtouch -t loads all data pages into the cgroup page cache
restart_cluster_preload() {
    local cache_size=$1
    local cache_gb="${cache_size//GB/}"
    local mem_bytes=$((cache_gb * 1024 * 1024 * 1024))
    echo ""; echo "=== Restart + PRELOAD: nodes ${BD_NODES[*]}, cache=${cache_size} ==="
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
        # Enter cgroup, PRELOAD data pages (vmtouch -t) so they are charged to
        # the daemon's cgroup and resident before Cassandra starts, then start.
        ssh ${SSH_USER}@${ip} \
            "cd ${CASS_DIR} && \
             echo ${mem_bytes} | sudo tee ${CGROUP}/memory.max && \
             echo \$\$ | sudo tee ${CGROUP}/cgroup.procs && \
             vmtouch -t data/ > /dev/null 2>&1 && \
             bin/cassandra > /dev/null 2>&1"
        local sa=0
        until ssh ${SSH_USER}@${ip} \
            "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep '${ip}' | grep -q 'UN'"; do
            sleep 10; sa=$((sa + 1)); echo "  Waiting for UN... (${sa}/30)"
            if [ "$sa" -ge 30 ]; then echo "  ERROR: ${ip} not UN after 5 min."; exit 1; fi
        done
        # confirm residency
        local res; res="$(ssh ${SSH_USER}@${ip} "vmtouch data/ 2>/dev/null | grep -i 'resident pages' || true")"
        echo "  ${ip} UN | ${res}"
    done
    echo "=== Restart + preload complete (${cache_size}). ==="
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
# Setup
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
else
    CREATE_TABLE_BIN="create_table_ec_compr_${COMPRESSION}"; SYS_KIND="ec"
fi

OUT_DIR="result_netiso_${EXP_LABEL}_${COMPRESSION}_${CACHE_SIZE}"
mkdir -p "$OUT_DIR"
LOG="${OUT_DIR}/run.log"
BREAKDOWN_FILE="${OUT_DIR}/breakdown.txt"; touch "$BREAKDOWN_FILE"

echo ""
echo "################################################################"
echo ">>> NETWORK ISOLATION | ${EXP_LABEL^^} | 5 nodes | ~10GB | read-only | 32GB cache | QUORUM"
echo "################################################################"

# 1) Fresh cluster + load 10GB
hard_restart_cluster
echo "--- Loading ${RECORD_COUNT} x ${FIELD_LENGTH}B (~10GB) ---"
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

# 2) Restart + PRELOAD (vmtouch -t) so all data is resident; then warm internals
restart_cluster_preload "$CACHE_SIZE"

echo "--- Warmup: read all keys twice (${WARMUP_OPS} ops, ${REQUEST_DIST}) ---"
$YCSB_DIR run $DB -threads $THREADS \
    -p operationcount=$WARMUP_OPS \
    -p readproportion=1.0 -p updateproportion=0.0 -p insertproportion=0.0 -p scanproportion=0.0 \
    -p recordcount=${RECORD_COUNT} -p requestdistribution=${REQUEST_DIST} \
    -p measurement.raw.output_file="${OUT_DIR}/Warmup.scr" \
    -p cassandra.writeconsistencylevel=QUORUM -p cassandra.readconsistencylevel=QUORUM \
    -P commonworkload -s >> "$LOG" 2>&1
echo "--- Warmup done ---"

# 3) READ-ONLY measure window
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

echo "=== MEASURE: READ-ONLY ${REQUEST_DIST} | ${MEASURE_OPS} ops ==="
$YCSB_DIR run $DB -threads $THREADS \
    -p operationcount=$MEASURE_OPS \
    -p readproportion=1.0 -p updateproportion=0.0 -p insertproportion=0.0 -p scanproportion=0.0 \
    -p requestdistribution=${REQUEST_DIST} \
    -p recordcount=${RECORD_COUNT} \
    -p measurement.raw.output_file="${OUT_DIR}/Measure.scr" \
    -p cassandra.writeconsistencylevel=QUORUM -p cassandra.readconsistencylevel=QUORUM \
    -P commonworkload -s >> "$LOG" 2>&1
echo "=== MEASURE done ==="

snapshot_memstat   "after" "${OUT_DIR}/memstat_after.txt"
snapshot_diskstats         "${OUT_DIR}/diskstats_after.txt"

echo "--- Collecting breakdown (ycsb/keyspace lines) ---"
echo "run ${EXP_LABEL} netiso ${CACHE_SIZE} read-only ${REQUEST_DIST} compr=${COMPRESSION}" >> "$BREAKDOWN_FILE"
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

# 4) Parser: CONFIRM disk reads during measure ~= 0 (isolation achieved)
READ_OPS=$MEASURE_OPS
SUMMARY="${OUT_DIR}/isolation_check.txt"
python3 - "$OUT_DIR" "$SUMMARY" "$READ_OPS" "$FIELD_LENGTH" << 'PYEOF'
import sys, re, os
outdir, summary, read_ops, field_len = sys.argv[1:5]
read_ops=int(read_ops); field_len=int(field_len)

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
        line=line.rstrip()
        m=re.match(r'### node(\d+)',line)
        if m: n='node'+m.group(1); d[n]={}; continue
        if n is None: continue
        t=line.split()
        if len(t)==2 and t[1].isdigit(): d[n][t[0]]=int(t[1])
    return d

db=diskstats(os.path.join(outdir,"diskstats_before.txt"))
da=diskstats(os.path.join(outdir,"diskstats_after.txt"))
mb=memstat(os.path.join(outdir,"memstat_before.txt"))
ma=memstat(os.path.join(outdir,"memstat_after.txt"))

lines=[]; tot=0; tot_rf=0
for n in sorted(set(db)&set(da)):
    miss=(da[n][1]-db[n][1])*512; ro=da[n][0]-db[n][0]; tot+=miss
    rf=ma.get(n,{}).get('workingset_refault_file',0)-mb.get(n,{}).get('workingset_refault_file',0); tot_rf+=rf
    res=ma.get(n,{}).get('file',None); rs=f"{res/1024**3:.2f}GiB" if res is not None else "n/a"
    lines.append(f"{n}: disk_read(measure)={miss/1024**2:8.2f}MB  disk_read_ops={ro:>7}  refault(+)={rf:>8}  resident={rs}")

per_op=tot/read_ops if read_ops else 0
ideal=read_ops*field_len  # logical bytes a read-only pass would touch
frac=tot/ideal*100 if ideal else 0
verdict = ("ISOLATION ACHIEVED: disk reads during measure are negligible "
           "(<1% of logical read bytes) -> no disk I/O; any LEAST-vs-Cassandra "
           "latency gap is from network transfer.") if frac < 1.0 else \
          ("WARNING: non-trivial disk reads during measure -> working set did "
           "NOT fully fit / preload incomplete. Investigate before attributing "
           "to network.")

with open(summary,"w") as out:
    out.write("=== NETWORK ISOLATION CHECK (read-only, ~10GB, should be ~0 disk I/O) ===\n")
    out.write(f"read_ops={read_ops}  object={field_len}B\n\n")
    out.write("\n".join(lines)+"\n\n")
    out.write(f"TOTAL disk read during measure : {tot/1024**2:.2f} MB ({tot/1024**3:.3f} GiB)\n")
    out.write(f"Disk bytes per read-op         : {per_op:.1f} B/op\n")
    out.write(f"As fraction of logical reads   : {frac:.3f}%  (logical = read_ops x object)\n")
    out.write(f"TOTAL refaults during measure  : {tot_rf:,}\n\n")
    out.write(verdict+"\n")
print(open(summary).read())
PYEOF

echo ""
echo "############################################################"
echo "Done. ${OUT_DIR}/"
echo "  isolation_check.txt   confirms disk reads ~= 0 during measure"
echo "  Measure.scr           READ latency raw (the headline result)"
echo "  breakdown.txt         server-side component split"
echo "  diskstats/memstat     before/after"
echo "############################################################"
