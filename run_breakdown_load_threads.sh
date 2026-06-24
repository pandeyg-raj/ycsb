#!/bin/bash
# =============================================================================
# run_breakdown_load_threads.sh
#
# THREAD-SWEEP version of run_breakdown_load.sh.
# Runs the YCSB insert/load phase once for each thread count in THREAD_LIST,
# HARD-resetting (kill + wipe all nodes + restart under the cap + RECREATE the
# table) before EVERY thread count. Compaction stays ON throughout.
#
# Per thread count it captures the same instrumentation as before:
#   - nodetool breakdown (ycsb/keyspace lines)
#   - nodetool tablestats ycsb.usertable
#   - dm-0 diskstats delta (disk READ + WRITE bytes)
#   - memcg memory.stat delta (pgfault/pgmajfault/refault/file)
#   - load_io_summary.txt (write/read B/op + write-amp)
#
# Interactive input is asked ONCE at the start (EC/REP + memory cap); the thread
# count is no longer prompted -- it comes from THREAD_LIST below.
#
# Labeling: every thread run gets its own dir and every file/header carries the
# thread count, so results never collide:
#   result_breakdown_load_<EC|REP>_<compr>_<cap>_t<threads>/
#       run.log  breakdown.txt  tablestats.txt  load_io_summary.txt
#       diskstats_before/after  memstat_before/after  Load.scr
#
#   Verified config notes (same cluster as run script):
#     - cgroup io controller NOT enabled -> use dm-0 diskstats
#     - /mydata = LVM LV dm-0 (253:0) over nvme0n1p4
#     - disk_access_mode=mmap_index_only -> pgmajfault is index-only (minor signal)
# =============================================================================

# -- Config -------------------------------------------------------------------
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql

THREAD_LIST=(4 8 12 16)   # one load run per thread count, hard reset between each

FIELD_LENGTH=10000
RECORD_COUNT=700000
COMPRESSION="on"

NUM_NODES=5
BD_NODES=(2 3 4 5 6)

SSH_USER=rzp5412
CASS_DIR=/mydata/cassandra
CGROUP=/sys/fs/cgroup/mylimitedgroup
DATA_DEV=dm-0              # /mydata LV (confirmed 253:0)

# If 1: flush + let compaction settle BEFORE the AFTER snapshot, so residual
# memtables and in-flight compaction land inside the measured window (complete
# write-amp). If 0 (default): snapshot the instant `ycsb load` returns.
MEASURE_INCLUDES_FLUSH_SETTLE=0

# =============================================================================
# HARD restart: wipe data, then start each node UNDER the X GB cgroup cap.
# Compaction is left ON (we never disableautocompaction in the load script).
# Uses globals: CREATE_TABLE_BIN, CACHE_SIZE.
# =============================================================================
hard_restart_cluster() {
    local cache_size=$1
    local cache_gb="${cache_size//GB/}"
    local mem_bytes=$((cache_gb * 1024 * 1024 * 1024))
    echo ""; echo "=== HARD restart (capped ${cache_size}): nodes ${BD_NODES[*]} ==="

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

    echo "  [3/3] Starting sequentially under ${cache_size} cgroup cap (seeds first)..."
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node"
        # Set memory.max, move this shell into the cgroup, then exec cassandra so
        # the daemon inherits the cgroup membership (same pattern as restart_cluster).
        ssh ${SSH_USER}@${ip} \
            "cd ${CASS_DIR} && \
             echo ${mem_bytes} | sudo tee ${CGROUP}/memory.max > /dev/null && \
             echo \$\$ | sudo tee ${CGROUP}/cgroup.procs > /dev/null && \
             bin/cassandra > /dev/null 2>&1"
        local a=0
        until ssh ${SSH_USER}@${ip} \
            "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep '${ip}' | grep -q 'UN'"; do
            sleep 10; a=$((a + 1)); echo "  Waiting ${ip} UN... (${a}/30)"
            if [ "$a" -ge 30 ]; then echo "  ERROR: ${ip} not UN after 5 min."; exit 1; fi
        done
        echo "  ${ip} UN"
    done

    echo "  Creating table via /mydata/${CREATE_TABLE_BIN}..."
    /mydata/${CREATE_TABLE_BIN}
    echo "=== HARD restart complete (capped ${cache_size}). ==="
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
# One full load run for a single thread count. Uses globals:
#   EXP_LABEL, SYS_KIND, CREATE_TABLE_BIN, CACHE_SIZE
# =============================================================================
run_one_load() {
    local threads=$1

    OUT_DIR="result_breakdown_load_${EXP_LABEL}_${COMPRESSION}_${CACHE_SIZE}_t${threads}"
    mkdir -p "$OUT_DIR"
    LOG="${OUT_DIR}/run.log"
    BREAKDOWN_FILE="${OUT_DIR}/breakdown.txt"; touch "$BREAKDOWN_FILE"
    TABLESTATS_FILE="${OUT_DIR}/tablestats.txt"

    # --- YCSB log header at the very beginning of the data ---
    {
        echo "# =============================================================="
        echo "# YCSB LOAD LOG"
        echo "# SYSTEM=${EXP_LABEL^^}  THREADS=${threads}  layout=${SYS_KIND}"
        echo "# create_table_binary=${CREATE_TABLE_BIN}  compr=${COMPRESSION}  cache_cap=${CACHE_SIZE}"
        echo "# nodes=${NUM_NODES} (${BD_NODES[*]})  record_count=${RECORD_COUNT}  field_length=${FIELD_LENGTH}B"
        echo "# =============================================================="
    } > "$LOG"

    echo ""
    echo "################################################################"
    echo ">>> ${EXP_LABEL^^} | THREADS=${threads} | ${NUM_NODES} nodes | compr=${COMPRESSION} | cap=${CACHE_SIZE} | LOAD-only | 10KB"
    echo ">>> metrics: ${DATA_DEV} diskstats (read+write) + memcg refault/majfault + breakdown lines"
    echo "################################################################"

    # 1) Fresh cluster, started UNDER the cap, compaction ON, table recreated
    hard_restart_cluster "$CACHE_SIZE"

    # 2) BEFORE: reset breakdown + snapshot counters
    echo "--- Reset breakdown + snapshot counters (BEFORE load) ---"
    for node in "${BD_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool breakdown --reset"
    done
    snapshot_memstat   "before" "${OUT_DIR}/memstat_before.txt"
    snapshot_diskstats          "${OUT_DIR}/diskstats_before.txt"

    # 3) LOAD window (insert only) -- compaction stays ON
    echo "=== LOAD: ${RECORD_COUNT} x ${FIELD_LENGTH}B | threads=${threads} ==="
    $YCSB_DIR load $DB -threads $threads \
        -p recordcount=${RECORD_COUNT} -p fieldlength=${FIELD_LENGTH} \
        -p measurement.raw.output_file="${OUT_DIR}/Load.scr" \
        -P commonworkload -s >> "$LOG" 2>&1
    echo "=== LOAD done (threads=${threads}) ==="

    # Optional: pull residual memtables + in-flight compaction into the window.
    if [ "$MEASURE_INCLUDES_FLUSH_SETTLE" = "1" ]; then
        echo "--- Flushing + settling compaction (included in window) ---"
        for node in "${BD_NODES[@]}"; do ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool flush" & done
        wait
        for node in "${BD_NODES[@]}"; do
            local ip="10.10.1.$node"
            while ssh ${SSH_USER}@${ip} \
                "${CASS_DIR}/bin/nodetool compactionstats 2>/dev/null | grep -q 'pending tasks: [^0]'"; do
                sleep 30; echo "  compaction running on ${ip}..."
            done
            echo "  ${ip} settled"
        done
    fi

    # 4) AFTER: snapshot counters + collect breakdown + tablestats
    snapshot_memstat   "after" "${OUT_DIR}/memstat_after.txt"
    snapshot_diskstats         "${OUT_DIR}/diskstats_after.txt"

    echo "--- Collecting breakdown (ycsb/keyspace lines only) ---"
    echo "load ${EXP_LABEL} threads=${threads} ${CACHE_SIZE} compr=${COMPRESSION}" >> "$BREAKDOWN_FILE"
    for node in "${BD_NODES[@]}"; do
        echo "-- node 10.10.1.$node --" >> "$BREAKDOWN_FILE"
        ssh ${SSH_USER}@10.10.1.$node \
            "${CASS_DIR}/bin/nodetool breakdown | grep -E 'keyspace|ycsb'" >> "$BREAKDOWN_FILE"
    done

    echo "--- Collecting nodetool tablestats ycsb.usertable ---"
    echo "tablestats ${EXP_LABEL} threads=${threads} ${CACHE_SIZE} compr=${COMPRESSION}" > "$TABLESTATS_FILE"
    for node in "${BD_NODES[@]}"; do
        echo "" >> "$TABLESTATS_FILE"
        echo "===== node 10.10.1.$node =====" >> "$TABLESTATS_FILE"
        ssh ${SSH_USER}@10.10.1.$node \
            "${CASS_DIR}/bin/nodetool tablestats ycsb.usertable" >> "$TABLESTATS_FILE" 2>&1
    done

    # 5) Parse: dm-0 write+read bytes (primary) + memcg (corroboration) + write-amp
    local ins_ops=$RECORD_COUNT
    local summary="${OUT_DIR}/load_io_summary.txt"

    python3 - "$OUT_DIR" "$summary" "$ins_ops" "$SYS_KIND" "$FIELD_LENGTH" \
                "$CACHE_SIZE" "$COMPRESSION" "$threads" << 'PYEOF'
import sys, re, os
outdir, summary, ins_ops, sys_kind, field_len, cache_size, compression, threads = sys.argv[1:9]
ins_ops = int(ins_ops); field_len = int(field_len)

def parse_diskstats(path):
    # stored line: node<N> <maj> <min> <name> rd_done rd_merged sec_read t_read wr_done wr_merged sec_write ...
    d = {}
    with open(path) as f:
        for line in f:
            p = line.split()
            if len(p) < 11:
                continue
            try:
                d[p[0]] = (int(p[4]), int(p[6]), int(p[8]), int(p[10]))  # rd_done, sec_read, wr_done, sec_write
            except (ValueError, IndexError):
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

logical = ins_ops * field_len
if sys_kind == 'ec':
    expected_payload = logical * 5.0 / 3.0   # RS(5,3): 3 data + 2 parity shards, each ~field/3
    model = "EC RS(5,3): 5 shards x ~(field/3) = field*5/3 across cluster"
else:
    expected_payload = logical * 3.0          # RF=3
    model = "REP RF=3: 3 replicas x field across cluster"

lines, tot_write, tot_read = [], 0, 0
for node in sorted(set(db) & set(da)):
    rd_done_b, sec_read_b, wr_done_b, sec_write_b = db[node]
    rd_done_a, sec_read_a, wr_done_a, sec_write_a = da[node]
    write_bytes = (sec_write_a - sec_write_b) * 512
    read_bytes  = (sec_read_a  - sec_read_b)  * 512
    wr_ops      =  wr_done_a - wr_done_b
    rd_ops      =  rd_done_a - rd_done_b
    tot_write += write_bytes
    tot_read  += read_bytes

    pgf     = ma.get(node, {}).get('pgfault', 0)               - mb.get(node, {}).get('pgfault', 0)
    majf    = ma.get(node, {}).get('pgmajfault', 0)            - mb.get(node, {}).get('pgmajfault', 0)
    refault = ma.get(node, {}).get('workingset_refault_file', 0) - mb.get(node, {}).get('workingset_refault_file', 0)
    resident = ma.get(node, {}).get('file', None)
    rstr = f"{resident/(1024**3):.2f}GiB" if resident is not None else "n/a"
    lines.append(
        f"{node}: disk_write={write_bytes/(1024**2):9.1f}MB  disk_read={read_bytes/(1024**2):8.1f}MB  "
        f"wr_ops={wr_ops:>9}  rd_ops={rd_ops:>9}  "
        f"pgfault(+)={pgf:>11}  pgmajfault_idx(+)={majf:>8}  refault_file(+)={refault:>10}  "
        f"resident_pagecache={rstr}")

w_per_op = tot_write / ins_ops if ins_ops else float('nan')
r_per_op = tot_read  / ins_ops if ins_ops else float('nan')
write_amp = tot_write / logical if logical else float('nan')
payload_ratio = tot_write / expected_payload if expected_payload else float('nan')

with open(summary, "w") as out:
    out.write("=== Cassandra LOAD-phase I/O (dm-0 diskstats, compaction ON) ===\n")
    out.write(f"system={sys_kind}  THREADS={threads}  nodes=5  insert_ops={ins_ops}  object={field_len}B  "
              f"cache_cap={cache_size}  compr={compression}\n")
    out.write(f"logical inserted bytes (cluster) : {logical/(1024**3):.2f} GiB\n\n")
    out.write("\n".join(lines) + "\n\n")
    out.write(f"TOTAL disk-write bytes : {tot_write/(1024**2):.1f} MB ({tot_write/(1024**3):.2f} GiB)\n")
    out.write(f"TOTAL disk-read  bytes : {tot_read/(1024**2):.1f} MB ({tot_read/(1024**3):.2f} GiB)  (compaction merge reads)\n\n")
    out.write(f"Disk WRITE bytes per insert-op : {w_per_op:.1f} B/op\n")
    out.write(f"Disk READ  bytes per insert-op : {r_per_op:.1f} B/op\n\n")
    out.write(f"Write amplification (disk_write / logical)        : {write_amp:.2f}x\n")
    out.write(f"Disk_write / expected encoded payload             : {payload_ratio:.2f}x\n")
    out.write(f"  expected payload model: {model}\n")
    out.write(f"  expected payload bytes: {expected_payload/(1024**3):.2f} GiB\n\n")
    out.write("Notes:\n")
    out.write("  - disk_write captures memtable flushes + compaction output + commitlog (all on /mydata).\n")
    out.write("  - With compression ON, payload_ratio < 1.0 is expected when data compresses well;\n")
    out.write("    write_amp folds in compaction rewrites and commitlog on top of that.\n")
    out.write("  - pgmajfault(+) is INDEX-only (mmap_index_only); minor signal during load.\n")
    out.write("  - If MEASURE_INCLUDES_FLUSH_SETTLE=0, residual memtables / in-flight compaction\n")
    out.write("    at load-end are NOT in the window. Set it to 1 for a complete write-amp number.\n")

print(open(summary).read())
PYEOF
}

# =============================================================================
# Interactive setup -- asked ONCE (EC/REP + memory cap). Threads come from
# THREAD_LIST, not a prompt.
# =============================================================================
echo "Checking create-table binaries..."
for bin in create_table_ec_compr_on create_table_ec_compr_off \
           create_table_rep_compr_on create_table_rep_compr_off; do
    if [ ! -x "/mydata/${bin}" ]; then echo "ERROR: /mydata/${bin} missing."; exit 1; fi
done
echo "OK."; echo ""

echo "Is this EC or REP?"; read EXP_LABEL
read -p "Cassandra memory cap in GB (e.g. 32): " CACHE_GB

CACHE_SIZE="${CACHE_GB}GB"

if echo "$EXP_LABEL" | grep -qi "rep"; then
    CREATE_TABLE_BIN="create_table_rep_compr_${COMPRESSION}"; SYS_KIND="rep"
else
    CREATE_TABLE_BIN="create_table_ec_compr_${COMPRESSION}"; SYS_KIND="ec"
fi

echo ""
echo "################################################################"
echo ">>> THREAD SWEEP : ${EXP_LABEL^^} | threads = ${THREAD_LIST[*]} | one load each, hard reset between"
echo ">>> cap=${CACHE_SIZE}  compr=${COMPRESSION}  records=${RECORD_COUNT}x${FIELD_LENGTH}B  nodes=${NUM_NODES}"
echo "################################################################"

for threads in "${THREAD_LIST[@]}"; do
    run_one_load "$threads"
done

echo ""
echo "############################################################"
echo "ALL THREAD RUNS DONE (${EXP_LABEL^^}, threads ${THREAD_LIST[*]})."
echo "Per-thread dir: result_breakdown_load_${EXP_LABEL}_${COMPRESSION}_${CACHE_SIZE}_t<threads>/"
echo "  run.log                 YCSB log (header: SYSTEM/THREADS/layout)"
echo "  breakdown.txt           ycsb/keyspace component lines, per node"
echo "  tablestats.txt          nodetool tablestats ycsb.usertable, per node"
echo "  load_io_summary.txt     disk write/read MB + B/op + write-amp (THREADS tagged)"
echo "  diskstats_before/after  dm-0 raw (primary)"
echo "  memstat_before/after    memcg raw (corroboration)"
echo "  Load.scr                YCSB load latency raw"
echo "############################################################"
