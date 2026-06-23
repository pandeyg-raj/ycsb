#!/bin/bash
# =============================================================================
# run_breakdown_load_sweep.sh
#
# Multi-config LOAD sweep built on run_breakdown_load.sh.
# Runs the YCSB insert/load phase across a fixed config matrix, HARD-resetting
# (kill + wipe + fresh start) before EVERY run, capturing per run:
#   - nodetool breakdown (ycsb/keyspace lines)
#   - nodetool tablestats ycsb.usertable
#   - dm-0 diskstats delta (disk READ + WRITE bytes)
#   - memcg memory.stat delta (pgfault/pgmajfault/refault/file)
#   - load_io_summary.txt (write/read B/op + write-amp)
#
# Config matrix (each run 2x; hard reset between every run):
#   LEAST     cs=5  rf=5  cl=TWO     -> ec binary
#   LEAST     cs=5  rf=5  cl=THREE   -> ec binary
#   --- one manual pause: swap server build LEAST -> CASSANDRA on all nodes ---
#   CASSANDRA cs=3  rf=3  cl=TWO     -> rep binary
#   CASSANDRA cs=5  rf=5  cl=THREE   -> ec binary (per user: rf=5 always ec)
#   CASSANDRA cs=5  rf=3  cl=TWO     -> rep binary
#
# Rules:
#   - rf is set SOLELY by the create-table binary: ec -> rf=5, rep -> rf=3.
#   - compression always ON.
#   - consistency level (cl) is passed to YCSB at load time (write+read CL).
#   - cluster size (cs) = number of node daemons started (.2 .. .2+cs-1).
#     All five nodes are still killed+wiped each reset so a cs=5 run never
#     inherits stale data from a previous cs=3 run.
#   - server build (LEAST vs vanilla CASSANDRA) is swapped MANUALLY exactly once,
#     between the LEAST block and the CASSANDRA block (only interactive prompt).
#
#   Verified config notes (same cluster as run script):
#     - cgroup io controller NOT enabled -> use dm-0 diskstats
#     - /mydata = LVM LV dm-0 (253:0) over nvme0n1p4
#     - disk_access_mode=mmap_index_only -> pgmajfault is index-only (minor signal)
# =============================================================================

# -- Editable constants (NO prompt; only the binary swap is interactive) -------
CACHE_GB=32               # cgroup memory cap applied at daemon start
LOAD_THREADS=15           # YCSB load (insert) threads
ITERS=2                   # repeats per config (hard reset between each)

FIELD_LENGTH=10000
RECORD_COUNT=400000
COMPRESSION="on"

# If 1: flush + settle compaction BEFORE the AFTER snapshot (complete write-amp).
# If 0 (default): snapshot the instant `ycsb load` returns.
MEASURE_INCLUDES_FLUSH_SETTLE=0

# -- Cluster / paths ----------------------------------------------------------
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql

ALL_NODES=(2 3 4 5 6)     # every potential node (killed+wiped each reset)
SSH_USER=rzp5412
CASS_DIR=/mydata/cassandra
CGROUP=/sys/fs/cgroup/mylimitedgroup
DATA_DEV=dm-0             # /mydata LV (confirmed 253:0)

# Globals set per-run by run_one_load():
ACTIVE_NODES=()           # nodes whose daemon is started this run
CREATE_TABLE_BIN=""
CACHE_SIZE=""
OUT_DIR=""; LOG=""; BREAKDOWN_FILE=""; TABLESTATS_FILE=""

# =============================================================================
# HARD restart: kill+wipe ALL nodes, start only ACTIVE_NODES under the cap.
# Compaction left ON. Uses globals: CREATE_TABLE_BIN, CACHE_SIZE, ACTIVE_NODES.
# =============================================================================
hard_restart_cluster() {
    local cache_gb="${CACHE_SIZE//GB/}"
    local mem_bytes=$((cache_gb * 1024 * 1024 * 1024))
    echo ""; echo "=== HARD restart (capped ${CACHE_SIZE}): active ${ACTIVE_NODES[*]} ==="

    echo "  [1/3] Killing on ALL nodes in parallel..."
    for node in "${ALL_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill 2>/dev/null; true" &
    done
    wait
    for node in "${ALL_NODES[@]}"; do
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

    echo "  [2/3] Wiping data on ALL nodes in parallel..."
    for node in "${ALL_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} "rm -rf ${CASS_DIR}/data/" &
    done
    wait

    echo "  [3/3] Starting ACTIVE nodes sequentially under ${CACHE_SIZE} cap (seed first)..."
    for node in "${ACTIVE_NODES[@]}"; do
        local ip="10.10.1.$node"
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
    echo "=== HARD restart complete (capped ${CACHE_SIZE}). ==="
}

# =============================================================================
snapshot_memstat() {   # uses ACTIVE_NODES
    local tag=$1 outfile=$2
    : > "$outfile"
    for node in "${ACTIVE_NODES[@]}"; do
        local ip="10.10.1.$node"
        echo "### node${node} ${tag}" >> "$outfile"
        ssh ${SSH_USER}@${ip} \
            "sudo cat ${CGROUP}/memory.stat 2>/dev/null | grep -E '^(file|pgfault|pgmajfault|workingset_refault_file) '" >> "$outfile"
    done
}

snapshot_diskstats() { # uses ACTIVE_NODES
    local outfile=$1
    : > "$outfile"
    for node in "${ACTIVE_NODES[@]}"; do
        local ip="10.10.1.$node"
        local line; line="$(ssh ${SSH_USER}@${ip} "grep -w ${DATA_DEV} /proc/diskstats | head -1")"
        echo "node${node} ${line}" >> "$outfile"
    done
}

# =============================================================================
# One full load run for a single (system, cs, rf, cl, iter).
# =============================================================================
run_one_load() {
    local system=$1 cs=$2 rf=$3 cl=$4 iter=$5
    local sys_kind
    if [ "$rf" = "5" ]; then sys_kind=ec; else sys_kind=rep; fi
    CREATE_TABLE_BIN="create_table_${sys_kind}_compr_${COMPRESSION}"
    ACTIVE_NODES=("${ALL_NODES[@]:0:cs}")
    CACHE_SIZE="${CACHE_GB}GB"

    OUT_DIR="result_load_${system}_cs${cs}_rf${rf}_cl${cl}_iter${iter}"
    mkdir -p "$OUT_DIR"
    LOG="${OUT_DIR}/run.log"
    BREAKDOWN_FILE="${OUT_DIR}/breakdown.txt"; touch "$BREAKDOWN_FILE"
    TABLESTATS_FILE="${OUT_DIR}/tablestats.txt"

    # --- YCSB log header at the very beginning of the data ---
    {
        echo "# =============================================================="
        echo "# YCSB LOAD LOG"
        echo "# SYSTEM=${system^^}  RF=${rf}  CL=${cl}  CLUSTER_SIZE=${cs}  ITER=${iter}"
        echo "# create_table_binary=${CREATE_TABLE_BIN}  layout=${sys_kind}  compr=${COMPRESSION}"
        echo "# cache_cap=${CACHE_SIZE}  active_nodes=${ACTIVE_NODES[*]}"
        echo "# record_count=${RECORD_COUNT}  field_length=${FIELD_LENGTH}B  threads=${LOAD_THREADS}"
        echo "# =============================================================="
    } > "$LOG"

    echo ""
    echo "############################################################"
    echo ">>> ${system^^} | cs=${cs} | rf=${rf} | cl=${cl} | iter=${iter} | binary=${sys_kind} | compr=${COMPRESSION}"
    echo "############################################################"

    # 1) Fresh cluster, capped, compaction ON
    hard_restart_cluster

    # 2) BEFORE: reset breakdown + snapshot counters
    echo "--- Reset breakdown + snapshot counters (BEFORE load) ---"
    for node in "${ACTIVE_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool breakdown --reset"
    done
    snapshot_memstat   "before" "${OUT_DIR}/memstat_before.txt"
    snapshot_diskstats          "${OUT_DIR}/diskstats_before.txt"

    # 3) LOAD window (insert only), CL passed to YCSB
    echo "=== LOAD: ${RECORD_COUNT} x ${FIELD_LENGTH}B | cl=${cl} ==="
    $YCSB_DIR load $DB -threads $LOAD_THREADS \
        -p recordcount=${RECORD_COUNT} -p fieldlength=${FIELD_LENGTH} \
        -p cassandra.writeconsistencylevel=${cl} -p cassandra.readconsistencylevel=${cl} \
        -p measurement.raw.output_file="${OUT_DIR}/Load.scr" \
        -P commonworkload -s >> "$LOG" 2>&1
    echo "=== LOAD done ==="

    # Optional: pull residual memtables + in-flight compaction into the window.
    if [ "$MEASURE_INCLUDES_FLUSH_SETTLE" = "1" ]; then
        echo "--- Flushing + settling compaction (included in window) ---"
        for node in "${ACTIVE_NODES[@]}"; do ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool flush" & done
        wait
        for node in "${ACTIVE_NODES[@]}"; do
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
    echo "load ${system} cs${cs} rf${rf} cl${cl} iter${iter} compr=${COMPRESSION}" >> "$BREAKDOWN_FILE"
    for node in "${ACTIVE_NODES[@]}"; do
        echo "-- node 10.10.1.$node --" >> "$BREAKDOWN_FILE"
        ssh ${SSH_USER}@10.10.1.$node \
            "${CASS_DIR}/bin/nodetool breakdown | grep -E 'keyspace|ycsb'" >> "$BREAKDOWN_FILE"
    done

    echo "--- Collecting nodetool tablestats ycsb.usertable ---"
    echo "tablestats ${system} cs${cs} rf${rf} cl${cl} iter${iter} compr=${COMPRESSION}" > "$TABLESTATS_FILE"
    for node in "${ACTIVE_NODES[@]}"; do
        echo "" >> "$TABLESTATS_FILE"
        echo "===== node 10.10.1.$node =====" >> "$TABLESTATS_FILE"
        ssh ${SSH_USER}@10.10.1.$node \
            "${CASS_DIR}/bin/nodetool tablestats ycsb.usertable" >> "$TABLESTATS_FILE" 2>&1
    done

    # 5) Parse: dm-0 write+read bytes + memcg + write-amp
    local ins_ops=$RECORD_COUNT
    local summary="${OUT_DIR}/load_io_summary.txt"

    python3 - "$OUT_DIR" "$summary" "$ins_ops" "$sys_kind" "$FIELD_LENGTH" \
                "$CACHE_SIZE" "$COMPRESSION" "$system" "$rf" "$cl" "$cs" "$iter" << 'PYEOF'
import sys, re, os
(outdir, summary, ins_ops, sys_kind, field_len, cache_size, compression,
 system, rf, cl, cs, iter_) = sys.argv[1:13]
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
    expected_payload = logical * 5.0 / 3.0   # RS(5,3): 3 data + 2 parity, each ~field/3
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

    pgf     = ma.get(node, {}).get('pgfault', 0)                 - mb.get(node, {}).get('pgfault', 0)
    majf    = ma.get(node, {}).get('pgmajfault', 0)              - mb.get(node, {}).get('pgmajfault', 0)
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
    out.write(f"SYSTEM={system.upper()}  RF={rf}  CL={cl}  CLUSTER_SIZE={cs}  ITER={iter_}\n")
    out.write(f"layout={sys_kind}  insert_ops={ins_ops}  object={field_len}B  "
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
# Pre-flight: verify create-table binaries exist
# =============================================================================
echo "Checking create-table binaries..."
for bin in create_table_ec_compr_on create_table_ec_compr_off \
           create_table_rep_compr_on create_table_rep_compr_off; do
    if [ ! -x "/mydata/${bin}" ]; then echo "ERROR: /mydata/${bin} missing."; exit 1; fi
done
echo "OK."

# =============================================================================
# Config matrix.  "system cs rf cl"  (each run ITERS times)
# LEAST block first, then CASSANDRA block (one manual swap between them).
# =============================================================================
CONFIGS=(
  "least     5 5 TWO"
  "least     5 5 THREE"
  "cassandra 3 3 TWO"
  "cassandra 5 5 THREE"
  "cassandra 5 3 TWO"
)

echo ""
echo "################################################################"
echo ">>> LOAD SWEEP : ${#CONFIGS[@]} configs x ${ITERS} iters, hard reset between every run"
echo ">>> cache_cap=${CACHE_GB}GB  load_threads=${LOAD_THREADS}  records=${RECORD_COUNT}x${FIELD_LENGTH}B"
echo "################################################################"

swapped=0
for cfg in "${CONFIGS[@]}"; do
    read -r system cs rf cl <<< "$cfg"

    # One-time manual binary swap when entering the CASSANDRA block.
    if [ "$system" = "cassandra" ] && [ "$swapped" = "0" ]; then
        echo ""
        echo "##############################################################"
        echo ">>> LEAST runs complete."
        echo ">>> Swap the SERVER build from LEAST to vanilla CASSANDRA on ALL nodes"
        echo ">>> (${ALL_NODES[*]}) in the background now."
        read -p ">>> Press Enter once the CASSANDRA build is in place to continue... " _
        swapped=1
    fi

    for ((i=1; i<=ITERS; i++)); do
        run_one_load "$system" "$cs" "$rf" "$cl" "$i"
    done
done

echo ""
echo "############################################################"
echo "ALL LOADS DONE."
echo "Per-run dir: result_load_<system>_cs<cs>_rf<rf>_cl<cl>_iter<i>/"
echo "  run.log                 YCSB log (header: SYSTEM/RF/CL/CLUSTER_SIZE/ITER)"
echo "  breakdown.txt           ycsb/keyspace component lines, per node"
echo "  tablestats.txt          nodetool tablestats ycsb.usertable, per node"
echo "  load_io_summary.txt     disk write/read MB + B/op + write-amp"
echo "  diskstats_before/after  dm-0 raw (primary)"
echo "  memstat_before/after    memcg raw (corroboration)"
echo "  Load.scr                YCSB load latency raw"
echo "############################################################"
