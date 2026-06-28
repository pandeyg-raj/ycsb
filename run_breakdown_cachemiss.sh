#!/bin/bash
# =============================================================================
# run_breakdown_cachemiss.sh  (FINAL)
#
# Section 6.3. Single fixed config, 5-node cluster for BOTH EC and REP:
#   - Component read breakdown (ycsb/keyspace lines) via `nodetool breakdown`
#     -> LEAST-side SSTable read-latency analysis (kept separate from cache)
#   - Cassandra read-path MISS via kernel-level counters identical on EC/REP:
#        PRIMARY:        dm-0 (/mydata LV) diskstats sectors_read delta * 512
#        CORROBORATION:  memcg workingset_refault_file (+), pgmajfault (+ idx)
#
#   Verified config notes:
#     - cgroup io controller NOT enabled (no io.stat) -> use dm-0 diskstats
#     - /mydata = LVM LV dm-0 (253:0) over nvme0n1p4; dm-0 stats scoped to /mydata
#     - disk_access_mode=mmap_index_only -> DATA reads buffered (hit dm-0),
#       only INDEX mmap'd -> pgmajfault is index-only (minor signal)
#
#   Workload: default YCSB values, 10KB (fieldcount=1), 50% read / 50% update,
#   uniform, 32GB cgroup cap, QUORUM r/w, compression ON.
# =============================================================================

# -- Config -------------------------------------------------------------------
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql

MEASURE_OPS=5000000
FIELD_LENGTH=10000
RECORD_COUNT=7000000
CACHE_SIZE="32GB"
COMPRESSION="on"

REQUEST_DIST="uniform"
READ_PROP=0.5
UPDATE_PROP=0.5

# Both EC and REP run on the same 5-node cluster (.2-.6).
NUM_NODES=5
BD_NODES=(2 3 4 5 6)

SSH_USER=rzp5412
CASS_DIR=/mydata/cassandra
CGROUP=/sys/fs/cgroup/mylimitedgroup
DATA_DEV=dm-0              # /mydata LV (confirmed 253:0)

# Disable autocompaction during measure so dm-0 reads = read-path misses only.
DISABLE_COMPACTION_DURING_MEASURE=1

# After the MEASURE window, optionally flush + wait for compaction to FULLY
# settle, then collect compaction/tablestats. Note: disableautocompaction stops
# COMPACTION but not FLUSHING, so the 50/50 UPDATEs still pile up un-merged
# SSTables during the window; re-enabling autocompaction then compacts that
# backlog. This flag waits for that DEFERRED compaction, which is the only way
# to get a meaningful compaction delta. With it =0, the backlog hasn't run yet,
# so compaction numbers would be partial/~0 -- so compaction is collected ONLY
# when this is 1. The cache-miss diskstats/memstat "after" snapshot is taken
# BEFORE this wait, so the settle's compaction I/O never pollutes the miss bytes.
WAIT_COMPACTION_AFTER_MEASURE=1

# =============================================================================
# Wait for REAL compaction settlement on every node:
#   pending tasks == 0  AND  ycsb compaction count unchanged,
#   sustained for STABLE_NEEDED consecutive polls (defeats momentary lulls).
wait_for_compaction_settle() {
    local poll=30 stable_needed=3
    echo "--- Waiting for real compaction settlement ---"
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node" stable=0 prev=-1
        while [ "$stable" -lt "$stable_needed" ]; do
            local pending hist
            pending=$(ssh ${SSH_USER}@${ip} "${CASS_DIR}/bin/nodetool compactionstats 2>/dev/null | awk '/pending tasks/{print \$NF}'")
            hist=$(ssh ${SSH_USER}@${ip} "${CASS_DIR}/bin/nodetool compactionhistory 2>/dev/null | awk '\$2==\"ycsb\"' | wc -l")
            pending=${pending:-1}
            if [ "$pending" = "0" ] && [ "$hist" = "$prev" ]; then stable=$((stable+1)); else stable=0; fi
            prev="$hist"
            echo "  ${ip}: pending=${pending} ycsb_compactions=${hist} stable=${stable}/${stable_needed}"
            [ "$stable" -lt "$stable_needed" ] && sleep "$poll"
        done
        echo "  ${ip} settled (ycsb compactions=${prev})"
    done
}

# Per-node ycsb compaction-history summary (bytes_in/out/count). Same one-liner
# as the load script; used for BEFORE/AFTER baselining of the 50/50 window.
collect_compaction() {
    local outfile=$1
    : > "$outfile"
    for node in "${BD_NODES[@]}"; do
        echo "-- node 10.10.1.$node --" >> "$outfile"
        ssh ${SSH_USER}@10.10.1.$node \
            "${CASS_DIR}/bin/nodetool compactionhistory | awk '\$2==\"ycsb\"{i+=\$5; o+=\$6; k++} END{printf \"ycsb,CompactionHistory,bytes_in=%d,bytes_out=%d,out_gb=%.3f,count=%d\\n\", i+0, o+0, (o+0)/1e9, k+0}'" >> "$outfile"
    done
}

# Per-node nodetool tablestats for ycsb.usertable.
collect_tablestats() {
    local outfile=$1
    : > "$outfile"
    for node in "${BD_NODES[@]}"; do
        echo "===== node 10.10.1.$node =====" >> "$outfile"
        ssh ${SSH_USER}@10.10.1.$node \
            "${CASS_DIR}/bin/nodetool tablestats ycsb.usertable" >> "$outfile" 2>&1
        echo "" >> "$outfile"
    done
}

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

# =============================================================================
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
# Interactive setup (EC/REP + threads; node count fixed at 5)
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

OUT_DIR="result_breakdown_cachemiss_${EXP_LABEL}_${COMPRESSION}_${CACHE_SIZE}"
mkdir -p "$OUT_DIR"
LOG="${OUT_DIR}/run.log"
BREAKDOWN_FILE="${OUT_DIR}/breakdown.txt"; touch "$BREAKDOWN_FILE"

echo ""
echo "################################################################"
echo ">>> ${EXP_LABEL^^} | 5 nodes | compr=${COMPRESSION} | cache=${CACHE_SIZE} | 50/50 ${REQUEST_DIST} | 10KB | QUORUM"
echo ">>> miss metric: ${DATA_DEV} diskstats (primary) + workingset_refault_file (corroboration)"
echo "################################################################"

# 1) Fresh cluster + load
hard_restart_cluster

echo "--- Loading ${RECORD_COUNT} x ${FIELD_LENGTH}B (default YCSB values) ---"
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

# 2) Apply 32GB cap + evict cache, warm the cache
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

# 3) MEASURE window
if [ "$DISABLE_COMPACTION_DURING_MEASURE" = "1" ]; then
    echo "--- Disabling autocompaction (clean dm-0 reads) ---"
    for node in "${BD_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool disableautocompaction"
    done
fi

echo "--- Reset breakdown + baseline compaction/tablestats (BEFORE 50/50) ---"
for node in "${BD_NODES[@]}"; do
    ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool breakdown --reset"
done
# Baseline the new metrics BEFORE the window. Done first so any minor metadata
# reads they trigger are not counted in the cache-miss diskstats snapshot, which
# is taken last, immediately before MEASURE.
collect_tablestats "${OUT_DIR}/tablestats_before.txt"
if [ "$WAIT_COMPACTION_AFTER_MEASURE" = "1" ]; then
    collect_compaction "${OUT_DIR}/compaction_before.txt"
fi
snapshot_memstat   "before" "${OUT_DIR}/memstat_before.txt"
snapshot_diskstats          "${OUT_DIR}/diskstats_before.txt"

echo "=== MEASURE: 50/50 ${REQUEST_DIST} | ${MEASURE_OPS} ops ==="
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

snapshot_memstat   "after" "${OUT_DIR}/memstat_after.txt"
snapshot_diskstats         "${OUT_DIR}/diskstats_after.txt"

echo "--- Collecting breakdown (ycsb/keyspace lines only) ---"
echo "run ${EXP_LABEL} ${CACHE_SIZE} 50-50 ${REQUEST_DIST} compr=${COMPRESSION}" >> "$BREAKDOWN_FILE"
for node in "${BD_NODES[@]}"; do
    echo "-- node 10.10.1.$node --" >> "$BREAKDOWN_FILE"
    ssh ${SSH_USER}@10.10.1.$node \
        "${CASS_DIR}/bin/nodetool breakdown | grep -E 'keyspace|ycsb'" >> "$BREAKDOWN_FILE"
done

if [ "$DISABLE_COMPACTION_DURING_MEASURE" = "1" ]; then
    echo "--- Re-enabling autocompaction ---"
    for node in "${BD_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool enableautocompaction"
    done
fi

# Optional: flush + let the deferred 50/50 compaction fully settle, THEN collect
# the AFTER metrics. The cache-miss after-snapshot above already closed the miss
# window, so this compaction I/O does not affect the miss measurement.
if [ "$WAIT_COMPACTION_AFTER_MEASURE" = "1" ]; then
    echo "--- Flushing, then settling compaction (post-MEASURE) ---"
    for node in "${BD_NODES[@]}"; do ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool flush" & done
    wait
    wait_for_compaction_settle
fi

echo "--- Collecting tablestats (AFTER 50/50) ---"
collect_tablestats "${OUT_DIR}/tablestats_after.txt"
if [ "$WAIT_COMPACTION_AFTER_MEASURE" = "1" ]; then
    echo "--- Collecting compaction history (AFTER settle) ---"
    collect_compaction "${OUT_DIR}/compaction_after.txt"
fi

# 4) Parse: dm-0 miss bytes (primary) + memcg refault/majfault (corroboration)
READ_OPS=$(python3 -c "print(int(${MEASURE_OPS} * ${READ_PROP}))")
SUMMARY="${OUT_DIR}/cachemiss_summary.txt"

python3 - "$OUT_DIR" "$SUMMARY" "$READ_OPS" "$SYS_KIND" "$FIELD_LENGTH" << 'PYEOF'
import sys, re, os
outdir, summary, read_ops, sys_kind, field_len = sys.argv[1:6]
read_ops = int(read_ops); field_len = int(field_len)

def parse_diskstats(path):
    d = {}
    with open(path) as f:
        for line in f:
            p = line.split()
            if len(p) < 7:
                continue
            try:
                d[p[0]] = (int(p[4]), int(p[6]))  # reads_done, sectors_read
            except ValueError:
                continue
    return d

def parse_memstat(path):
    data, node = {}, None
    with open(path) as f:
        for line in f:
            line = line.rstrip("\n")
            m = re.match(r'### node(\d+)', line)
            if m:
                node = "node" + m.group(1); data[node] = {}; continue
            if node is None:
                continue
            p = line.split()
            if len(p) == 2 and p[1].isdigit():
                data[node][p[0]] = int(p[1])
    return data

db = parse_diskstats(os.path.join(outdir, "diskstats_before.txt"))
da = parse_diskstats(os.path.join(outdir, "diskstats_after.txt"))
mb = parse_memstat(os.path.join(outdir, "memstat_before.txt"))
ma = parse_memstat(os.path.join(outdir, "memstat_after.txt"))

if sys_kind == 'ec':
    served = read_ops * 4 * (field_len / 3.0); model = "EC(5,3): 4 nodes x (field/3)"
else:
    served = read_ops * 2 * field_len;          model = "REP RF=3: 2 nodes x field"

lines, tot_miss = [], 0
for node in sorted(set(db) & set(da)):
    miss_bytes   = (da[node][1] - db[node][1]) * 512
    read_ops_dev =  da[node][0] - db[node][0]
    tot_miss += miss_bytes
    refault = ma.get(node, {}).get('workingset_refault_file', 0) - mb.get(node, {}).get('workingset_refault_file', 0)
    majf    = ma.get(node, {}).get('pgmajfault', 0)              - mb.get(node, {}).get('pgmajfault', 0)
    resident = ma.get(node, {}).get('file', None)
    rstr = f"{resident/(1024**3):.2f}GiB" if resident is not None else "n/a"
    lines.append(
        f"{node}: disk_read(miss)={miss_bytes/(1024**2):9.1f}MB  "
        f"disk_read_ops={read_ops_dev:>9}  "
        f"refault_file(+)={refault:>10}  pgmajfault_idx(+)={majf:>8}  "
        f"resident_pagecache={rstr}")

per_op   = tot_miss / read_ops if read_ops else float('nan')
hit_rate = (1.0 - tot_miss / served) * 100.0 if served else float('nan')

# --- compaction generated by the 50/50 window (after - before) ---------------
def parse_compaction(path):
    d, node = {}, None
    if not os.path.exists(path):
        return d
    with open(path) as f:
        for line in f:
            m = re.search(r'-- node 10\.10\.1\.(\d+) --', line)
            if m:
                node = "node" + m.group(1); continue
            if node and 'CompactionHistory' in line:
                bi = re.search(r'bytes_in=(\d+)', line)
                bo = re.search(r'bytes_out=(\d+)', line)
                c  = re.search(r'count=(\d+)', line)
                d[node] = (int(bi.group(1)) if bi else 0,
                           int(bo.group(1)) if bo else 0,
                           int(c.group(1))  if c  else 0)
    return d

cb = parse_compaction(os.path.join(outdir, "compaction_before.txt"))
ca = parse_compaction(os.path.join(outdir, "compaction_after.txt"))
comp_lines, tot_ci, tot_co, tot_cnt = [], 0, 0, 0
for node in sorted(set(cb) | set(ca)):
    bi0, bo0, c0 = cb.get(node, (0, 0, 0))
    bi1, bo1, c1 = ca.get(node, (0, 0, 0))
    dci, dco, dcnt = bi1 - bi0, bo1 - bo0, c1 - c0
    tot_ci += dci; tot_co += dco; tot_cnt += dcnt
    comp_lines.append(
        f"{node}: ycsb_compactions(+)={dcnt:>5}  "
        f"bytes_in(+)={dci/(1024**2):9.1f}MB  bytes_out(+)={dco/(1024**2):9.1f}MB")

with open(summary, "w") as out:
    out.write("=== Cassandra read-path MISS (dm-0 diskstats primary) ===\n")
    out.write(f"system={sys_kind}  nodes=5  read_ops={read_ops}  object={field_len}B\n\n")
    out.write("\n".join(lines) + "\n\n")
    out.write(f"TOTAL disk-read (miss) bytes : {tot_miss/(1024**2):.1f} MB ({tot_miss/(1024**3):.2f} GiB)\n")
    out.write(f"Disk bytes read per read-op  : {per_op:.1f} B/op  (lower = better cache effectiveness)\n\n")
    out.write("Corroboration (memcg, per-cgroup):\n")
    out.write("  workingset_refault_file(+) = cache-pressure misses (data, buffered reads)\n")
    out.write("  pgmajfault(+)              = INDEX misses only (mmap_index_only); minor signal\n\n")
    out.write(f"Derived effective hit rate   : {hit_rate:.2f}%   model: {model}\n")
    out.write("  NOTE: derived % is perturbed by readahead. Lead with disk-read MB / B/op\n"
              "  and refault(+), which are model-free. For a sharp %, set readahead low:\n"
              "    sudo blockdev --setra 16 /dev/mapper/emulab-node1--bs\n")

    if comp_lines:
        out.write("\n=== Compaction from the 50/50 window (delta: after - before) ===\n")
        out.write("\n".join(comp_lines) + "\n")
        out.write(f"TOTAL ycsb compactions(+) : {tot_cnt}\n")
        out.write(f"TOTAL bytes_in(+)         : {tot_ci/(1024**2):.1f} MB ({tot_ci/(1024**3):.2f} GiB)\n")
        out.write(f"TOTAL bytes_out(+)        : {tot_co/(1024**2):.1f} MB ({tot_co/(1024**3):.2f} GiB)\n")
        out.write("  Compaction is disabled DURING measure; this delta is the deferred\n")
        out.write("  compaction from the 50/50 UPDATEs, realized during the post-measure\n")
        out.write("  settle (WAIT_COMPACTION_AFTER_MEASURE=1). With it =0, expect ~0 here.\n")

print(open(summary).read())
PYEOF

echo ""
echo "############################################################"
echo "Done. ${OUT_DIR}/"
echo "  breakdown.txt              ycsb/keyspace component lines, per node"
echo "  cachemiss_summary.txt      miss MB + B/op + refault + derived % + compaction delta"
echo "  tablestats_before/after    nodetool tablestats ycsb.usertable (50/50 window)"
echo "  compaction_before/after    ycsb compactionhistory summary (50/50 window)"
echo "  diskstats_before/after     dm-0 raw (primary)"
echo "  memstat_before/after       memcg raw (corroboration)"
echo "  Measure.scr                YCSB latency raw"
echo "############################################################"
