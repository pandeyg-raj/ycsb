#!/bin/bash
# =============================================================================
# run_compaction_writeheavy.sh   -- Paper Section 6.6 (Impact on Compaction)
#
# Goal: under a write-heavy workload, quantify the compaction / write-
# amplification cost of LEAST RS(5,3) vs Cassandra RF=3, and show whether
# EC's update-after-flush path (Case 2) inflates it. Compaction stays ON.
#
# Metrics (strongest first):
#   1. PHYSICAL WRITE VOLUME  -- dm-0 /proc/diskstats sectors_written delta
#        * cluster-summed bytes written to the data LV over the window
#        * / logical bytes ingested = total write amplification (fair, kernel)
#   2. COMPACTION VOLUME      -- system.compaction_history (cqlsh COPY -> CSV)
#        * id-set diff before/after, sum bytes_in/bytes_out for the ycsb ks
#        * isolates the compaction slice of (1); bytes_out/bytes_in = merge amp
#   3. MECHANISM (EC only)    -- nodetool ecwritestats  (RESET before measure)
#        * case1(memtable_replace) vs case2(sstable_insert); case2% is the
#          knob that, if it climbs under heavy writes, drives write-amp up
#   4. STEADY-STATE STORAGE   -- du + nodetool tablestats "Space used", at
#        quiescence (after compaction drains)
#   5. BACKLOG / EFFORT       -- nodetool compactionstats pending sampled during
#        the run; nodetool breakdown "keyspace,Compaction" timer (count+avg)
#
# Fixed config: 70 GB (7M x 10KB), 5 nodes BOTH systems, 90% update / 10% read,
# uniform, compression ON, 32 GB cap, QUORUM r/w, MEASURE_OPS=10M.
#
# Methodological key: the measure window is bracketed by quiescent states
# (pending->0 before AND after), so every byte of compaction in the diff was
# caused by the measure-phase writes -- including compaction that finishes
# AFTER YCSB exits (that is why we wait-for-quiescence before the AFTER snap).
# =============================================================================

# -- Config -------------------------------------------------------------------
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql

MEASURE_OPS=10000000        # 10M ops
FIELD_LENGTH=10000          # 10 KB objects (updates stay 10KB)
RECORD_COUNT=7000000        # ~70 GB logical
CACHE_SIZE="32GB"
COMPRESSION="on"

REQUEST_DIST="uniform"
READ_PROP=0.1               # write-heavy: 90% update / 10% read
UPDATE_PROP=0.9

SSH_USER=rzp5412
CASS_DIR=/mydata/cassandra
CGROUP=/sys/fs/cgroup/mylimitedgroup
DATA_DEV=dm-0               # /mydata LV (253:0); used for write-volume delta
KEYSPACE=ycsb               # YCSB cassandra-cql keyspace
TABLE=usertable

# =============================================================================
# stop_cluster
# =============================================================================
stop_cluster() {
    local nodes
    if [ "$NUM_NODES" = "3" ]; then nodes=(2 3 4); else nodes=(2 3 4 5 6); fi
    echo ""; echo "=== Stopping Cassandra on all nodes ==="
    for node in "${nodes[@]}"; do
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

# =============================================================================
# restart_cluster <cache_size>  (soft: keep data, set cgroup, evict page cache)
# =============================================================================
restart_cluster() {
    local cache_size=$1
    local nodes
    if [ "$NUM_NODES" = "3" ]; then nodes=(2 3 4); else nodes=(2 3 4 5 6); fi
    local cache_gb="${cache_size//GB/}"
    local mem_bytes=$((cache_gb * 1024 * 1024 * 1024))
    echo ""; echo "=== Soft restart: nodes ${nodes[*]}, cache=${cache_size} ==="
    for node in "${nodes[@]}"; do
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

# =============================================================================
# hard_restart_cluster
# =============================================================================
hard_restart_cluster() {
    local nodes
    if [ "$NUM_NODES" = "3" ]; then nodes=(2 3 4); else nodes=(2 3 4 5 6); fi
    echo ""; echo "=== HARD restart: nodes ${nodes[*]} ==="
    echo "  [1/3] Killing all in parallel..."
    for node in "${nodes[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill 2>/dev/null; true" &
    done
    wait
    for node in "${nodes[@]}"; do
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
    for node in "${nodes[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} "rm -rf ${CASS_DIR}/data/" &
    done
    wait
    echo "  [3/3] Starting sequentially (seeds first)..."
    for node in "${nodes[@]}"; do
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
# wait_for_quiescence  -- block until every storage node reports 0 pending
# =============================================================================
wait_for_quiescence() {
    echo "--- Waiting for compaction to fully drain (pending->0 everywhere) ---"
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node"
        while ssh ${SSH_USER}@${ip} \
            "${CASS_DIR}/bin/nodetool compactionstats 2>/dev/null | grep -q 'pending tasks: [^0]'"; do
            sleep 30; echo "  compaction still running on ${ip}..."
        done
        echo "  ${ip} quiescent"
    done
}

# =============================================================================
# Snapshots
# =============================================================================
# dm-0 diskstats: writes_completed = field 8 (p[8]), sectors_written = field 10
# (p[10]) with the leading nodeN token. One file per tag, all nodes.
snapshot_diskstats() {
    local outfile=$1
    : > "$outfile"
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node"
        local line; line="$(ssh ${SSH_USER}@${ip} "grep -w ${DATA_DEV} /proc/diskstats | head -1")"
        echo "node${node} ${line}" >> "$outfile"
    done
}

# system.compaction_history -> CSV via cqlsh COPY (robust, typed columns).
# Each node's compaction_history is node-local (LocalStrategy), so query each.
# Also dumps raw `nodetool compactionhistory` text alongside as a record.
snapshot_compaction_history() {
    local tag=$1 outdir=$2
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node"
        # CSV (primary parse source)
        ssh ${SSH_USER}@${ip} \
            "${CASS_DIR}/bin/cqlsh ${ip} -e \"COPY system.compaction_history (id, keyspace_name, columnfamily_name, bytes_in, bytes_out) TO '/tmp/ch_${tag}_${node}.csv' WITH HEADER=false\" > /dev/null 2>&1; true"
        scp ${SSH_USER}@${ip}:/tmp/ch_${tag}_${node}.csv "${outdir}/ch_${tag}_node${node}.csv" 2>/dev/null
        # raw text (record / fallback)
        ssh ${SSH_USER}@${ip} \
            "${CASS_DIR}/bin/nodetool compactionhistory 2>/dev/null" \
            > "${outdir}/compactionhistory_${tag}_node${node}.txt" 2>/dev/null
    done
}

# Background sampler: pending compaction tasks per node, every 30s, timestamped.
PENDING_PID=""
start_pending_sampler() {
    local outfile=$1
    : > "$outfile"
    (
      while true; do
        ts="$(date +%s)"
        for node in "${BD_NODES[@]}"; do
            p="$(ssh ${SSH_USER}@10.10.1.$node \
                "${CASS_DIR}/bin/nodetool compactionstats 2>/dev/null | grep 'pending tasks' | head -1 | awk '{print \$NF}'")"
            echo "${ts} node${node} pending=${p:-NA}" >> "$outfile"
        done
        sleep 30
      done
    ) &
    PENDING_PID=$!
}
stop_pending_sampler() {
    [ -n "$PENDING_PID" ] && kill "$PENDING_PID" 2>/dev/null; PENDING_PID=""
}

# =============================================================================
# Interactive setup
# =============================================================================
echo "Checking create-table binaries..."
for bin in create_table_ec_compr_on create_table_ec_compr_off \
           create_table_rep_compr_on create_table_rep_compr_off; do
    if [ ! -x "/mydata/${bin}" ]; then echo "ERROR: /mydata/${bin} missing."; exit 1; fi
done
echo "OK."; echo ""

echo "Is this EC or REP?"; read EXP_LABEL
read -p "How many Cassandra nodes? (3 or 5) [5]: " NUM_NODES
NUM_NODES=${NUM_NODES:-5}
if [ "$NUM_NODES" = "3" ]; then BD_NODES=(2 3 4)
elif [ "$NUM_NODES" = "5" ]; then BD_NODES=(2 3 4 5 6)
else echo "ERROR: must be 3 or 5"; exit 1; fi
echo "Storage nodes: ${BD_NODES[*]}"
read -p "Write (load) threads: " WTHREADS
read -p "Run threads (measure): " THREADS

if echo "$EXP_LABEL" | grep -qi "rep"; then
    CREATE_TABLE_BIN="create_table_rep_compr_${COMPRESSION}"; SYS_KIND="rep"
else
    CREATE_TABLE_BIN="create_table_ec_compr_${COMPRESSION}"; SYS_KIND="ec"
fi

# cqlsh preflight (non-fatal): compaction-volume CSV depends on it.
if ! ssh ${SSH_USER}@10.10.1.${BD_NODES[0]} "${CASS_DIR}/bin/cqlsh 10.10.1.${BD_NODES[0]} -e 'SELECT release_version FROM system.local' > /dev/null 2>&1"; then
    echo "WARN: cqlsh check failed on node ${BD_NODES[0]}. compaction_history CSV may be empty;"
    echo "      dm-0 write volume + ecwritestats + storage are unaffected. Continuing."
fi

OUT_DIR="result_compaction_${EXP_LABEL}_${COMPRESSION}_writeheavy"
mkdir -p "$OUT_DIR"
LOG="${OUT_DIR}/run.log"
BREAKDOWN_FILE="${OUT_DIR}/breakdown.txt"; touch "$BREAKDOWN_FILE"

echo ""
echo "################################################################"
echo ">>> ${EXP_LABEL^^} | compr=${COMPRESSION} | 90/10 write-heavy | 10KB | QUORUM | compaction ON"
echo ">>> 70GB (7M x 10KB), ${CACHE_SIZE} cap, ${MEASURE_OPS} ops"
echo "################################################################"

# =============================================================================
# 1) Fresh cluster + load
# =============================================================================
hard_restart_cluster

echo "--- Loading ${RECORD_COUNT} x ${FIELD_LENGTH}B ---"
$YCSB_DIR load $DB -threads $WTHREADS \
    -p recordcount=${RECORD_COUNT} -p fieldlength=${FIELD_LENGTH} \
    -p measurement.raw.output_file="${OUT_DIR}/Load.scr" \
    -P commonworkload -s >> "$LOG" 2>&1
echo "--- Load done ---"

wait_for_quiescence

echo "--- Draining ---"
for node in "${BD_NODES[@]}"; do ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool drain" & done
wait

# =============================================================================
# 2) Apply cap + evict cache, warm read cache (comparable read start state)
# =============================================================================
restart_cluster "$CACHE_SIZE"

cache_gb="${CACHE_SIZE//GB/}"
available_bytes=$(( (cache_gb - 8) * 1024 * 1024 * 1024 ))
if [ "$SYS_KIND" = "ec" ]; then shard_size=$(( FIELD_LENGTH / 3 )); else shard_size=$FIELD_LENGTH; fi
objects_that_fit=$(( available_bytes / shard_size ))
WARMUP_OPS=$(( objects_that_fit < RECORD_COUNT ? objects_that_fit : RECORD_COUNT ))
if [ "$WARMUP_OPS" -lt 1000000 ]; then WARMUP_OPS=1000000; fi
echo ">>> Warmup ops: ${WARMUP_OPS}"

echo "--- Warmup (100% read) ---"
$YCSB_DIR run $DB -threads $THREADS \
    -p operationcount=$WARMUP_OPS \
    -p readproportion=1.0 -p updateproportion=0.0 -p insertproportion=0.0 \
    -p recordcount=${RECORD_COUNT} -p requestdistribution=${REQUEST_DIST} \
    -p measurement.raw.output_file="${OUT_DIR}/Warmup.scr" \
    -p cassandra.writeconsistencylevel=QUORUM -p cassandra.readconsistencylevel=QUORUM \
    -P commonworkload -s >> "$LOG" 2>&1
echo "--- Warmup done ---"

# =============================================================================
# 3) Instrument, MEASURE (write-heavy), then drain compaction fully
# =============================================================================
echo "--- Resetting breakdown + ecwritestats (all nodes) ---"
for node in "${BD_NODES[@]}"; do
    ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool breakdown --reset"
    ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool ecwritestats --reset 2>/dev/null; true"
done

snapshot_diskstats          "${OUT_DIR}/diskstats_before.txt"
snapshot_compaction_history "before" "$OUT_DIR"
start_pending_sampler       "${OUT_DIR}/pending_timeline.txt"

echo "=== MEASURE: 90/10 write-heavy ${REQUEST_DIST} | ${MEASURE_OPS} ops ==="
$YCSB_DIR run $DB -threads $THREADS \
    -p operationcount=$MEASURE_OPS \
    -p readproportion=${READ_PROP} -p updateproportion=${UPDATE_PROP} \
    -p insertproportion=0.0 -p scanproportion=0.0 \
    -p requestdistribution=${REQUEST_DIST} \
    -p recordcount=${RECORD_COUNT} -p fieldlength=${FIELD_LENGTH} \
    -p measurement.raw.output_file="${OUT_DIR}/Measure.scr" \
    -p cassandra.writeconsistencylevel=QUORUM -p cassandra.readconsistencylevel=QUORUM \
    -P commonworkload -s >> "$LOG" 2>&1
echo "=== MEASURE done ==="

stop_pending_sampler
wait_for_quiescence            # capture post-run compaction (critical)

snapshot_diskstats          "${OUT_DIR}/diskstats_after.txt"
snapshot_compaction_history "after" "$OUT_DIR"

echo "--- Collecting ecwritestats (EC mechanism) ---"
ECW="${OUT_DIR}/ecwritestats.txt"; : > "$ECW"
for node in "${BD_NODES[@]}"; do
    echo "-- node 10.10.1.$node --" >> "$ECW"
    ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool ecwritestats 2>/dev/null" >> "$ECW"
done

echo "--- Collecting breakdown (Compaction timer + components) ---"
echo "run ${EXP_LABEL} writeheavy 90-10 ${REQUEST_DIST} compr=${COMPRESSION}" >> "$BREAKDOWN_FILE"
for node in "${BD_NODES[@]}"; do
    echo "-- node 10.10.1.$node --" >> "$BREAKDOWN_FILE"
    ssh ${SSH_USER}@10.10.1.$node \
        "${CASS_DIR}/bin/nodetool breakdown | grep -E 'keyspace|ycsb'" >> "$BREAKDOWN_FILE"
done

echo "--- Storage at quiescence (du + tablestats) ---"
STORAGE="${OUT_DIR}/storage.txt"; : > "$STORAGE"
for node in "${BD_NODES[@]}"; do
    ip="10.10.1.$node"
    du_b="$(ssh ${SSH_USER}@${ip} "du -sb ${CASS_DIR}/data/${KEYSPACE} 2>/dev/null | awk '{print \$1}'")"
    sp="$(ssh ${SSH_USER}@${ip} "${CASS_DIR}/bin/nodetool tablestats ${KEYSPACE}.${TABLE} 2>/dev/null | grep -m1 'Space used (total)' | awk '{print \$NF}'")"
    echo "node${node} du_bytes=${du_b:-0} tablestats_space_used=${sp:-NA}" >> "$STORAGE"
done

# =============================================================================
# 4) Parse -> compaction_summary.txt
# =============================================================================
WRITE_OPS=$(python3 -c "print(int(${MEASURE_OPS} * ${UPDATE_PROP}))")
SUMMARY="${OUT_DIR}/compaction_summary.txt"

python3 - "$OUT_DIR" "$SUMMARY" "$WRITE_OPS" "$FIELD_LENGTH" "$SYS_KIND" "$KEYSPACE" << 'PYEOF'
import sys, os, re, glob, csv
outdir, summary, write_ops, field_len, sys_kind, keyspace = sys.argv[1:7]
write_ops = int(write_ops); field_len = int(field_len)
logical = write_ops * field_len      # cluster-wide user bytes ingested (updates)

# ---- (1) dm-0 write volume ----
# nodeN <maj> <min> <name> <reads_c> <reads_m> <sec_read> <ms_r> <writes_c>
#       <writes_m> <sec_written> ...  -> writes_c=p[8], sectors_written=p[10]
def parse_diskstats(path):
    d = {}
    with open(path) as f:
        for line in f:
            p = line.split()
            if len(p) < 11: continue
            try: wc = int(p[8]); sw = int(p[10])
            except ValueError: continue
            d[p[0]] = (wc, sw)
    return d

db = parse_diskstats(os.path.join(outdir, "diskstats_before.txt"))
da = parse_diskstats(os.path.join(outdir, "diskstats_after.txt"))

dlines, tot_wbytes, tot_wops = [], 0, 0
for node in sorted(set(db) & set(da)):
    wops = da[node][0] - db[node][0]
    wbytes = (da[node][1] - db[node][1]) * 512
    tot_wbytes += wbytes; tot_wops += wops
    dlines.append(f"  {node}: disk_write={wbytes/(1024**3):7.2f} GiB  write_ops={wops:>10}")

# ---- (2) compaction_history CSV id-diff (ycsb only) ----
def load_ch(tag, node):
    path = os.path.join(outdir, f"ch_{tag}_node{node}.csv")
    rows = {}
    if not os.path.exists(path): return rows
    with open(path) as f:
        for r in csv.reader(f):
            if len(r) < 5: continue
            cid, ks, cf, bi, bo = r[0], r[1], r[2], r[3], r[4]
            try: rows[cid] = (ks, int(bi), int(bo))
            except ValueError: continue
    return rows

clines, tot_in, tot_out, ch_ok = [], 0, 0, False
nodes = sorted({re.search(r'node(\d+)', p).group(1)
                for p in glob.glob(os.path.join(outdir, "ch_after_node*.csv"))})
for node in nodes:
    ch_ok = True
    before = load_ch("before", node); after = load_ch("after", node)
    new_ids = set(after) - set(before)
    bi = sum(after[i][1] for i in new_ids if after[i][0] == keyspace)
    bo = sum(after[i][2] for i in new_ids if after[i][0] == keyspace)
    tot_in += bi; tot_out += bo
    clines.append(f"  node{node}: compactions={len(new_ids):>5}  "
                  f"bytes_in={bi/(1024**3):6.2f} GiB  bytes_out={bo/(1024**3):6.2f} GiB")

# ---- (3) ecwritestats (EC mechanism) ----
ecw_path = os.path.join(outdir, "ecwritestats.txt")
elines, c1_tot, c2_tot = [], 0, 0
if sys_kind == "ec" and os.path.exists(ecw_path):
    cur = None
    with open(ecw_path) as f:
        for line in f:
            m = re.search(r'node (10\.10\.1\.\d+)', line)
            if m: cur = m.group(1); continue
            m1 = re.search(r'case1\(memtable_replace\)=(\d+)', line)
            m2 = re.search(r'case2\(sstable_insert\)=(\d+)', line)
            if m1 and m2:
                c1 = int(m1.group(1)); c2 = int(m2.group(1))
                c1_tot += c1; c2_tot += c2
                tot = c1 + c2
                pct = (c2 / tot * 100.0) if tot else 0.0
                elines.append(f"  {cur}: case1={c1:>9} case2={c2:>8} (case2={pct:.2f}%)")

# ---- (4) storage ----
stor_path = os.path.join(outdir, "storage.txt")
slines, du_tot = [], 0
if os.path.exists(stor_path):
    with open(stor_path) as f:
        for line in f:
            m = re.search(r'(node\d+) du_bytes=(\d+) tablestats_space_used=(\S+)', line)
            if m:
                du = int(m.group(2)); du_tot += du
                slines.append(f"  {m.group(1)}: du={du/(1024**3):6.2f} GiB  "
                              f"tablestats_space_used={m.group(3)}")

# ---- (5) compaction timer from breakdown ----
bd_path = os.path.join(outdir, "breakdown.txt")
comp_count, comp_time_us = 0, 0.0
if os.path.exists(bd_path):
    for line in open(bd_path):
        m = re.search(r'keyspace,Compaction,avg=([\d.]+),.*count=(\d+)', line)
        if m:
            avg = float(m.group(1)); cnt = int(m.group(2))
            comp_count += cnt; comp_time_us += avg * cnt

# ---- write ----
def amp(x): return (x / logical) if logical else float('nan')
with open(summary, "w") as o:
    o.write("================ Section 6.6  Impact on Compaction ================\n")
    o.write(f"system={sys_kind}  write_ops={write_ops}  object={field_len}B\n")
    o.write(f"logical user bytes ingested (cluster): {logical/(1024**3):.2f} GiB\n\n")

    o.write("[1] PHYSICAL WRITE VOLUME (dm-0 sectors_written; flush+compaction+commitlog)\n")
    o.write("\n".join(dlines) + "\n")
    o.write(f"  TOTAL disk write : {tot_wbytes/(1024**3):.2f} GiB   write_ops={tot_wops}\n")
    o.write(f"  TOTAL write amplification (phys/logical): {amp(tot_wbytes):.2f}x\n\n")

    o.write("[2] COMPACTION VOLUME (system.compaction_history, ycsb keyspace)\n")
    if ch_ok:
        o.write("\n".join(clines) + "\n")
        o.write(f"  TOTAL compaction bytes_in : {tot_in/(1024**3):.2f} GiB\n")
        o.write(f"  TOTAL compaction bytes_out: {tot_out/(1024**3):.2f} GiB\n")
        ratio = (tot_out / tot_in) if tot_in else float('nan')
        o.write(f"  merge ratio (out/in): {ratio:.3f}   "
                f"compaction write amp (out/logical): {amp(tot_out):.2f}x\n\n")
    else:
        o.write("  (no compaction_history CSV captured -- check cqlsh; "
                "dm-0 total above still valid)\n\n")

    o.write("[3] EC WRITE PATH (mechanism, EC only)\n")
    if elines:
        o.write("\n".join(elines) + "\n")
        tot = c1_tot + c2_tot
        pct = (c2_tot / tot * 100.0) if tot else 0.0
        o.write(f"  CLUSTER: case1={c1_tot} case2={c2_tot} total={tot}  case2={pct:.2f}%\n")
        o.write("  (low case2% => updates merge in memtable; EC adds little compaction)\n\n")
    else:
        o.write("  n/a (REP)\n\n")

    o.write("[4] STEADY-STATE STORAGE (at quiescence)\n")
    if slines:
        o.write("\n".join(slines) + "\n")
        o.write(f"  TOTAL du: {du_tot/(1024**3):.2f} GiB\n\n")
    else:
        o.write("  (no storage captured)\n\n")

    o.write("[5] COMPACTION EFFORT (breakdown timer)\n")
    o.write(f"  compactions={comp_count}  total_compaction_time={comp_time_us/1e6:.1f} s "
            f"(sum avg*count across nodes)\n")
    o.write("  backlog over time: see pending_timeline.txt\n")

print(open(summary).read())
PYEOF

echo ""
echo "############################################################"
echo "Done. ${OUT_DIR}/"
echo "  compaction_summary.txt    headline: write-amp, compaction vol, case1/2, storage"
echo "  ecwritestats.txt          raw case1/case2 per node (EC)"
echo "  diskstats_before/after    dm-0 raw (write volume)"
echo "  ch_{before,after}_node*.csv  compaction_history CSV (volume source)"
echo "  compactionhistory_*_node*.txt raw nodetool (record/fallback)"
echo "  pending_timeline.txt      compaction backlog during the run"
echo "  storage.txt               du + tablestats at quiescence"
echo "  breakdown.txt             Compaction timer + read components"
echo "  Measure.scr               YCSB latency raw"
echo "############################################################"
