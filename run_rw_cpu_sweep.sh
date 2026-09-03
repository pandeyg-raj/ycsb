#!/usr/bin/env bash
# =============================================================================
# run_read_sweep.sh -- 100%-read CPU-cap sweep against an ALREADY-LOADED dataset.
#
# Idea: the load is identical across caps, so load ONCE (uncapped) with
# run_breakdown_load.sh PHASE_MODE=1, then run this to read that same on-disk
# data under each CPU cap. Never wipes data -- it restarts the cluster on the
# existing SSTables under a new cpu.max, drops the page cache COLD (vmtouch -e +
# drop_caches), runs the 100% read phase with the full instrumentation (CPU,
# throttling, /proc/stat cross-check, memory bandwidth, latency), and collects a
# cap-tagged read dir.
#
# Data is system-specific: EC data can only be read by EC, REP by REP. So the
# system you pass MUST match the data currently on disk.
#
# FLOW:
#   1) load once (uncapped), e.g.:
#        EXP_LABEL=ec CACHE_GB=32 WTHREADS=64 PHASE_MODE=1 CPU_MAX=max \
#           bash run_breakdown_load.sh
#   2) read-sweep that data across caps:
#        bash run_read_sweep.sh ec
#   3) repeat 1-2 for rep.
#
# Usage:
#   bash run_read_sweep.sh <ec|rep>
# Env overrides (else prompted): CACHE_GB, RTHREADS, CAPS, MEASURE_OPS,
#   READ_DIST, MEASURE_NETWORK, MEASURE_MEMBW.
# =============================================================================
set -u

# ---------- config (mirror run_breakdown_load.sh) ----------
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql
FIELD_LENGTH=10000
RECORD_COUNT=5000000
COMPRESSION="on"
NUM_NODES=5
BD_NODES=(2 3 4 5 6)
SSH_USER="${SSH_USER:-rzp5412}"
CASS_DIR=/mydata/cassandra
CGROUP=/sys/fs/cgroup/mylimitedgroup
DATA_DEV=dm-0
READ_DIST="${READ_DIST:-uniform}"
SAMPLE_SEC=5
PERIOD=100000

# instrumentation toggles (same semantics as the harness)
MEASURE_DISK_TIMESERIES=1
MEASURE_MEMBW="${MEASURE_MEMBW:-1}"
MEMBW_MAXDUR=14400
PERF_BIN=perf
MEASURE_NETWORK="${MEASURE_NETWORK:-0}"
MEASURE_NET_PORTS=0

# ---------- system is determined from the EXP_LABEL prompt below ----------

# ---------- prompts (env-overridable) ----------
echo "=== read-only CPU-cap sweep (reads an ALREADY-LOADED dataset) ==="
if [ -z "${EXP_LABEL:-}" ]; then read -p "Is the loaded data EC or REP? " EXP_LABEL; fi
if echo "$EXP_LABEL" | grep -qi rep; then SYS_KIND=rep; else SYS_KIND=ec; fi
if [ -z "${CACHE_GB:-}" ]; then read -p "Cassandra memory cap in GB (e.g. 32): " CACHE_GB; fi
if [ -z "${RTHREADS:-}" ]; then read -p "Run threads (50/50 read+update): " RTHREADS; fi
if [ -z "${MEASURE_OPS:-}" ]; then read -p "Read operationcount [default 2000000]: " MEASURE_OPS; MEASURE_OPS="${MEASURE_OPS:-2000000}"; fi
if [ -z "${CAPS:-}" ]; then read -p "CPU caps to sweep, cores/node [default: 16..1 full]: " CAPS; CAPS="${CAPS:-16 15 14 13 12 11 10 9 8 7 6 5 4 3 2 1}"; fi
if [ -z "${ITERATIONS:-}" ]; then read -p "How many iterations (repeat the whole sweep N times) [default 1]: " ITERATIONS; ITERATIONS="${ITERATIONS:-1}"; fi
case "$ITERATIONS" in ''|*[!0-9]*) ITERATIONS=1;; esac
[ "$ITERATIONS" -lt 1 ] 2>/dev/null && ITERATIONS=1
if [ -z "${MEASURE_NETWORK:-}" ]; then MEASURE_NETWORK=0; fi
CACHE_SIZE="${CACHE_GB}GB"
mem_bytes=$(( ${CACHE_GB} * 1024 * 1024 * 1024 ))

# ---------- always clear cpu cap on exit ----------
clear_cpu_cap() {
    echo ">>> clearing CPU cap on all nodes..."
    for n in "${BD_NODES[@]}"; do
        ssh -n ${SSH_USER}@10.10.1.$n "echo max | sudo tee ${CGROUP}/cpu.max > /dev/null" 2>/dev/null
    done
}
trap clear_cpu_cap EXIT INT TERM

# =============================================================================
# restart_preserving_data: stop cluster, set cpu.max to the cap, restart ON THE
# EXISTING data (NO wipe), vmtouch-evict the data dir at startup. This is the
# hard-restart minus the rm -rf and minus the load.
# =============================================================================
# =============================================================================
# Snapshot / restore (hard-link) -- so every run reads the SAME starting dataset
# despite the 50/50 writes mutating it. take_snapshot ONCE after load; restore
# before each capped run resets data/ to that clean loaded state.
# =============================================================================
take_snapshot() {
    echo "  [snapshot] hard-linking data/ -> data_snapshot/ on all nodes..."
    for node in "${BD_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.$node "cd ${CASS_DIR} && rm -rf data_snapshot && cp -al data data_snapshot" &
    done
    wait
    echo "  [snapshot] done"
}
restore_from_snapshot() {
    echo "  [restore] resetting data/ from snapshot on all nodes..."
    for node in "${BD_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.$node "cd ${CASS_DIR} && rm -rf data && cp -al data_snapshot data" &
    done
    wait
    echo "  [restore] done"
}

stop_cluster_all() {
    echo "  [stop] stopping cluster on all nodes..."
    for node in "${BD_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill 2>/dev/null; true" &
    done
    wait
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node" a=0
        while ssh ${SSH_USER}@${ip} "ps -ef | grep '[j]ava' | grep -i 'cassandra' > /dev/null 2>&1"; do
            sleep 10; a=$((a+1))
            if [ "$a" -ge 6 ]; then
                ssh ${SSH_USER}@${ip} "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill -9 2>/dev/null; true"
                sleep 5; break
            fi
        done
    done
}

start_under_cap() {
    local cpumax=$1
    local cache_gb="${CACHE_SIZE//GB/}"
    local mem_bytes=$((cache_gb * 1024 * 1024 * 1024))
    echo "  [start] starting under cpu.max='${cpumax}' on existing data (seeds first)..."
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node"
        ssh ${SSH_USER}@${ip} \
            "cd ${CASS_DIR} && \
             echo '+cpu' | sudo tee /sys/fs/cgroup/cgroup.subtree_control > /dev/null 2>&1 ; \
             echo ${mem_bytes} | sudo tee ${CGROUP}/memory.max > /dev/null && \
             echo '${cpumax}' | sudo tee ${CGROUP}/cpu.max > /dev/null && \
             echo \$\$ | sudo tee ${CGROUP}/cgroup.procs > /dev/null ; \
             vmtouch -e data/ > /dev/null 2>&1 ; \
             bin/cassandra > /dev/null 2>&1"
        local a=0
        until ssh ${SSH_USER}@${ip} "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep '${ip}' | grep -q 'UN'"; do
            sleep 10; a=$((a+1)); echo "    waiting ${ip} UN... (${a}/30)"
            if [ "$a" -ge 30 ]; then echo "    ERROR: ${ip} not UN after 5 min."; exit 1; fi
        done
        echo "    ${ip} UN"
    done
    for node in "${BD_NODES[@]}"; do
        local got; got=$(ssh ${SSH_USER}@10.10.1.$node "cat ${CGROUP}/cpu.max 2>/dev/null")
        echo "    node${node}: cpu.max=${got}"
    done
}

restart_preserving_data() {
    local cpumax=$1
    echo "  [restart] stopping cluster (data preserved)..."
    for node in "${BD_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.${node} \
            "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill 2>/dev/null; true" &
    done
    wait
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node" a=0
        while ssh ${SSH_USER}@${ip} "ps -ef | grep '[j]ava' | grep -i 'cassandra' > /dev/null 2>&1"; do
            sleep 10; a=$((a+1))
            if [ "$a" -ge 6 ]; then
                ssh ${SSH_USER}@${ip} "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill -9 2>/dev/null; true"
                sleep 5; break
            fi
        done
    done
    echo "  [restart] starting under cpu.max='${cpumax}' on existing data (seeds first)..."
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node"
        # NO rm -rf. Set caps, join cgroup, vmtouch-evict data/ (non-fatal),
        # then start. vmtouch NOT chained with && to cassandra.
        ssh ${SSH_USER}@${ip} \
            "cd ${CASS_DIR} && \
             echo '+cpu' | sudo tee /sys/fs/cgroup/cgroup.subtree_control > /dev/null 2>&1 ; \
             echo ${mem_bytes} | sudo tee ${CGROUP}/memory.max > /dev/null && \
             echo '${cpumax}' | sudo tee ${CGROUP}/cpu.max > /dev/null && \
             echo \$\$ | sudo tee ${CGROUP}/cgroup.procs > /dev/null ; \
             vmtouch -e data/ > /dev/null 2>&1 ; \
             bin/cassandra > /dev/null 2>&1"
        local a=0
        until ssh ${SSH_USER}@${ip} "${CASS_DIR}/bin/nodetool status 2>/dev/null | grep '${ip}' | grep -q 'UN'"; do
            sleep 10; a=$((a+1)); echo "    waiting ${ip} UN... (${a}/30)"
            if [ "$a" -ge 30 ]; then echo "    ERROR: ${ip} not UN after 5 min."; exit 1; fi
        done
        echo "    ${ip} UN"
    done
    # record the actual cpu.max landed
    for node in "${BD_NODES[@]}"; do
        local got; got=$(ssh ${SSH_USER}@10.10.1.$node "cat ${CGROUP}/cpu.max 2>/dev/null")
        echo "    node${node}: cpu.max=${got}"
    done
}

# COLD cache: vmtouch -e (targeted) + drop_caches (total), on all nodes, AFTER
# the cluster is UP but BEFORE the measured read. Belt-and-suspenders.
go_cold() {
    echo "  [cold] evicting page cache on all nodes (vmtouch -e + drop_caches)..."
    for node in "${BD_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.$node \
            "cd ${CASS_DIR}; vmtouch -e data/ > /dev/null 2>&1; sync; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null" &
    done
    wait
}

# ---------- snapshot helpers (identical to the harness) ----------
snapshot_memstat()  { local tag=$1 out=$2; : > "$out"; for n in "${BD_NODES[@]}"; do echo "### node${n} ${tag}" >> "$out"; ssh ${SSH_USER}@10.10.1.$n "sudo cat ${CGROUP}/memory.stat 2>/dev/null | grep -E '^(file|pgfault|pgmajfault|workingset_refault_file) '" >> "$out"; done; }
snapshot_diskstats(){ local out=$1; : > "$out"; for n in "${BD_NODES[@]}"; do local l; l="$(ssh ${SSH_USER}@10.10.1.$n "grep -w ${DATA_DEV} /proc/diskstats | head -1")"; echo "node${n} ${l}" >> "$out"; done; }
snapshot_cpustat()  { local tag=$1 out=$2; : > "$out"; for n in "${BD_NODES[@]}"; do echo "### node${n} ${tag}" >> "$out"; ssh ${SSH_USER}@10.10.1.$n "sudo cat ${CGROUP}/cpu.stat 2>/dev/null | grep -E '^(usage_usec|user_usec|system_usec|nr_throttled|throttled_usec) '; echo nproc \$(nproc)" >> "$out"; done; }
snapshot_procstat() { local tag=$1 out=$2; : > "$out"; for n in "${BD_NODES[@]}"; do echo "### node${n} ${tag}" >> "$out"; ssh ${SSH_USER}@10.10.1.$n "grep '^cpu ' /proc/stat; echo clk_tck \$(getconf CLK_TCK); echo nproc \$(nproc)" >> "$out"; done; }
snapshot_vmstat()   { local tag=$1 out=$2; : > "$out"; for n in "${BD_NODES[@]}"; do echo "### node${n} ${tag}" >> "$out"; ssh ${SSH_USER}@10.10.1.$n "grep -E '^(pgpgin|pgpgout|pgfault|pgmajfault) ' /proc/vmstat" >> "$out"; done; }
snapshot_netstat()  { local tag=$1 out=$2; : > "$out"; for n in "${BD_NODES[@]}"; do ssh ${SSH_USER}@10.10.1.$n "dev=\$(ip -o -4 addr show | awk '/inet 10\\.10\\.1\\./{print \$2; exit}'); line=\$(sed 's/:/ /' /proc/net/dev | awk -v d=\"\$dev\" '\$1==d{print \$2,\$3,\$10,\$11}'); echo \"node${n} \$dev \$line\"" >> "$out"; done; }

# ---------- disk %util sampler (background) ----------
start_samplers() {
    local pdir=$1; SAMPLER_STOP="${pdir}/.stop_sampler"; rm -f "$SAMPLER_STOP"; SAMPLER_PIDS=()
    [ "$MEASURE_DISK_TIMESERIES" = "1" ] || return 0
    : > "${pdir}/diskutil_timeseries.txt"
    ( while [ ! -f "$SAMPLER_STOP" ]; do ts=$(date +%s); for n in "${BD_NODES[@]}"; do l=$(ssh ${SSH_USER}@10.10.1.$n "grep -w ${DATA_DEV} /proc/diskstats | head -1" 2>/dev/null); echo "${ts} node${n} ${l}"; done >> "${pdir}/diskutil_timeseries.txt"; sleep "${SAMPLE_SEC}"; done ) &
    SAMPLER_PIDS+=($!)
}
stop_samplers() { [ -n "${SAMPLER_STOP:-}" ] && touch "$SAMPLER_STOP"; for p in "${SAMPLER_PIDS[@]:-}"; do wait "$p" 2>/dev/null; done; SAMPLER_PIDS=(); }

# ---------- memory bandwidth (perf uncore_imc), ssh -n stdin-safe ----------
start_membw() {
    local pdir=$1; [ "$MEASURE_MEMBW" = "1" ] || return 0
    MEMBW_PIDFILE="${pdir}/.membw_pids"; : > "$MEMBW_PIDFILE"; local ms=$((SAMPLE_SEC*1000))
    for node in "${BD_NODES[@]}"; do
        ssh -n ${SSH_USER}@10.10.1.$node "sudo pkill -KILL -x perf 2>/dev/null; sudo pkill -KILL -f 'perf stat -a' 2>/dev/null; sudo rm -f /mydata/membw_${node}.out 2>/dev/null" 2>/dev/null
    done
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node" ok pid
        ok=$(ssh -n ${SSH_USER}@${ip} "ls /sys/bus/event_source/devices/ 2>/dev/null | grep -qi imc && command -v ${PERF_BIN} >/dev/null 2>&1 && echo yes" 2>/dev/null)
        [ "$ok" = "yes" ] || { echo "  perf/IMC not on node ${node}, skipping membw"; continue; }
        pid=$(ssh -n ${SSH_USER}@${ip} "sudo nohup timeout ${MEMBW_MAXDUR} ${PERF_BIN} stat -a -x, -I ${ms} -e uncore_imc/cas_count_read/,uncore_imc/cas_count_write/ > /mydata/membw_${node}.out 2>&1 & echo \$!")
        echo "node${node} ${pid}" >> "$MEMBW_PIDFILE"
    done
}
stop_membw() {
    local pdir=$1; [ "$MEASURE_MEMBW" = "1" ] || return 0; [ -f "${MEMBW_PIDFILE:-}" ] || return 0
    while read -r node pid; do
        local n="${node#node}" ip="10.10.1.${node#node}"
        ssh -n ${SSH_USER}@${ip} "sudo kill ${pid} 2>/dev/null; sudo pkill -TERM -f 'perf stat -a' 2>/dev/null; sleep 1; sudo pkill -KILL -f 'perf stat -a' 2>/dev/null" 2>/dev/null
        ssh -n ${SSH_USER}@${ip} "cat /mydata/membw_${n}.out 2>/dev/null" > "${pdir}/membw_${node}.txt" 2>/dev/null
        if [ -s "${pdir}/membw_${node}.txt" ]; then ssh -n ${SSH_USER}@${ip} "sudo rm -f /mydata/membw_${n}.out" 2>/dev/null
        else echo "  WARN: membw empty for ${node} (left on node)"; fi
    done < "$MEMBW_PIDFILE"
}

# =============================================================================
# read_once: run one capped read against the loaded data, full instrumentation.
# =============================================================================
read_once() {
    local cores=$1
    local iter=$2
    local quota=$((cores*PERIOD)) cpumax cap_tag
    if [ "$cores" -ge 16 ] 2>/dev/null; then cpumax="max"; cap_tag="cpu16"; else cpumax="${quota} ${PERIOD}"; cap_tag="cpu${cores}"; fi

    local OUT_DIR="result_rwsweep_${SYS_KIND}_${COMPRESSION}_${CACHE_SIZE}_${cap_tag}_iter${iter}"
    local pdir="${OUT_DIR}/read"

    # skip-if-done: if this run already completed (summary exists & non-empty), skip.
    if [ -s "${pdir}/resource_summary.txt" ]; then
        echo ">>> SKIP (already done): ${OUT_DIR}"
        return 0
    fi
    mkdir -p "$pdir"
    echo "CPU_MAX=${cpumax}  (tag=${cap_tag})  iteration=${iter}" > "${OUT_DIR}/cpu_cap.txt"
    echo "mem_cap=${CACHE_SIZE}  system=${SYS_KIND}  workload=50/50_read_update  QUORUM  restore_per_run" >> "${OUT_DIR}/cpu_cap.txt"

    echo ""
    echo "==================================================================="
    echo ">>> RW(50/50)  iter=${iter}  cap=${cores} core(s)  cpu.max='${cpumax}'  -> ${OUT_DIR}"
    echo "==================================================================="

    # reset the dataset to the clean loaded state: stop, restore data/ while the
    # cluster is DOWN (can't swap data/ while Cassandra holds it open), then start
    # under the cap. So every cap run reads the same starting data despite writes.
    stop_cluster_all
    restore_from_snapshot
    start_under_cap "$cpumax"
    go_cold

    echo "  [read] BEFORE snapshots..."
    snapshot_memstat  "before" "${pdir}/memstat_before.txt"
    snapshot_diskstats         "${pdir}/diskstats_before.txt"
    snapshot_cpustat  "before" "${pdir}/cpustat_before.txt"
    snapshot_procstat "before" "${pdir}/procstat_before.txt"
    [ "$MEASURE_NETWORK" = "1" ] && snapshot_netstat "before" "${pdir}/netstat_before.txt"
    snapshot_vmstat   "before" "${pdir}/vmstat_before.txt"

    start_samplers "$pdir"; start_membw "$pdir"
    local full_start full_end load_start load_end
    full_start=$(date +%s); load_start=$full_start
    echo "  [rw] running 50/50 read/update (${MEASURE_OPS} ops, QUORUM)..."
    $YCSB_DIR run $DB -threads $RTHREADS \
        -p recordcount=${RECORD_COUNT} -p operationcount=${MEASURE_OPS} \
        -p fieldlength=${FIELD_LENGTH} \
        -p readproportion=0.5 -p updateproportion=0.5 -p scanproportion=0 -p insertproportion=0 \
        -p requestdistribution=${READ_DIST} \
        -p cassandra.writeconsistencylevel=QUORUM \
        -p cassandra.readconsistencylevel=QUORUM \
        -p measurement.raw.output_file="${pdir}/Read.scr" \
        -P commonworkload -s >> "${pdir}/run.log" 2>&1
    load_end=$(date +%s)
    stop_samplers; stop_membw "$pdir"
    [ "$MEASURE_NETWORK" = "1" ] && snapshot_netstat "after" "${pdir}/netstat_after.txt"
    snapshot_vmstat   "after" "${pdir}/vmstat_after.txt"

    echo "  [read] AFTER snapshots..."
    snapshot_memstat  "after" "${pdir}/memstat_after.txt"
    snapshot_diskstats         "${pdir}/diskstats_after.txt"
    snapshot_cpustat  "after" "${pdir}/cpustat_after.txt"
    snapshot_procstat "after" "${pdir}/procstat_after.txt"
    full_end=$(date +%s)
    { echo "FULL_START ${full_start}"; echo "FULL_END ${full_end}"; echo "LOAD_START ${load_start}"; echo "LOAD_END ${load_end}"; } > "${pdir}/timings.txt"

    # footprint
    echo "space read ${CACHE_SIZE}" > "${pdir}/space.txt"
    for node in "${BD_NODES[@]}"; do b=$(ssh ${SSH_USER}@10.10.1.$node "du -sb ${CASS_DIR}/data 2>/dev/null | awk '{print \$1}'"); echo "node${node} ${b:-0}" >> "${pdir}/space.txt"; done

    # parse via the shared python (same as harness). Pass cpumax as 11th arg for %-of-cap.
    python3 - "$pdir" "${pdir}/resource_summary.txt" "$MEASURE_OPS" "$SYS_KIND" "$FIELD_LENGTH" "$CACHE_SIZE" "$COMPRESSION" "read" "read" "$RECORD_COUNT" "$cpumax" << 'PYEOF'
import sys, re, os
outdir, summary, ops, sys_kind, field_len, cache_size, compression, phase, op_label, record_count = sys.argv[1:11]
cpu_max_str = sys.argv[11] if len(sys.argv) > 11 else "max"
ops=int(ops); field_len=int(field_len); record_count=int(record_count)
dataset_logical = record_count*field_len
def _cap_cores(s):
    s=(s or "max").strip()
    if s.lower() in ("max",""): return None
    p=s.split()
    if len(p)==2:
        try:
            q,pp=float(p[0]),float(p[1])
            if pp>0: return q/pp
        except ValueError: pass
    return None
cap_cores=_cap_cores(cpu_max_str)
def parse_diskstats(path):
    d={}
    if not os.path.exists(path): return d
    for line in open(path):
        p=line.split()
        if len(p)<11: continue
        try: d[p[0]]=(int(p[4]),int(p[6]),int(p[8]),int(p[10]))
        except: continue
    return d
def parse_kv(path):
    data,node={},None
    if not os.path.exists(path): return data
    for line in open(path):
        line=line.rstrip("\n"); m=re.match(r'### node(\d+)',line)
        if m: node="node"+m.group(1); data[node]={}; continue
        if node is None: continue
        p=line.split()
        if len(p)==2 and p[1].lstrip('-').isdigit(): data[node][p[0]]=int(p[1])
    return data
def parse_procstat(path):
    data,node={},None
    if not os.path.exists(path): return data
    for line in open(path):
        m=re.match(r'### node(\d+)',line)
        if m: node="node"+m.group(1); data[node]={}; continue
        if node is None: continue
        p=line.split()
        if p and p[0]=='cpu': data[node]['fields']=[int(x) for x in p[1:] if x.isdigit()]
        elif len(p)==2 and p[0] in ('clk_tck','nproc'): data[node][p[0]]=int(p[1])
    return data
def parse_space(path):
    tot,per=0,{}
    if not os.path.exists(path): return tot,per
    for line in open(path):
        p=line.split()
        if len(p)==2 and p[0].startswith("node") and p[1].isdigit(): per[p[0]]=int(p[1]); tot+=int(p[1])
    return tot,per
def parse_tim(path):
    t={}
    if not os.path.exists(path): return t
    for line in open(path):
        p=line.split()
        if len(p)==2 and p[1].lstrip("-").isdigit(): t[p[0]]=int(p[1])
    return t
db=parse_diskstats(os.path.join(outdir,"diskstats_before.txt")); da=parse_diskstats(os.path.join(outdir,"diskstats_after.txt"))
mb=parse_kv(os.path.join(outdir,"memstat_before.txt")); ma=parse_kv(os.path.join(outdir,"memstat_after.txt"))
cpu_b=parse_kv(os.path.join(outdir,"cpustat_before.txt")); cpu_a=parse_kv(os.path.join(outdir,"cpustat_after.txt"))
ps_b=parse_procstat(os.path.join(outdir,"procstat_before.txt")); ps_a=parse_procstat(os.path.join(outdir,"procstat_after.txt"))
space_tot,space_per=parse_space(os.path.join(outdir,"space.txt"))
tim=parse_tim(os.path.join(outdir,"timings.txt"))
full_wall=max(tim.get("FULL_END",0)-tim.get("FULL_START",0),0)
disk_lines,tot_write,tot_read=[],0,0
for node in sorted(set(db)&set(da)):
    rb,srb,wb,swb=db[node]; ra,sra,wa,swa=da[node]
    write_bytes=(swa-swb)*512; read_bytes=(sra-srb)*512
    tot_write+=write_bytes; tot_read+=read_bytes
    majf=ma.get(node,{}).get('pgmajfault',0)-mb.get(node,{}).get('pgmajfault',0)
    refault=ma.get(node,{}).get('workingset_refault_file',0)-mb.get(node,{}).get('workingset_refault_file',0)
    resident=ma.get(node,{}).get('file',None); rstr=f"{resident/(1024**3):.2f}GiB" if resident is not None else "n/a"
    disk_lines.append(f"{node}: disk_read={read_bytes/(1024**2):9.1f}MB  disk_write={write_bytes/(1024**2):9.1f}MB  pgmajfault(+)={majf:>8}  refault_file(+)={refault:>10}  resident_pagecache={rstr}")
cpu_lines,tot_cpu_us=[],0
thr_lines,tot_nr,tot_tus=[],0,0
for node in sorted(set(cpu_b)&set(cpu_a)):
    du=cpu_a[node].get("usage_usec",0)-cpu_b[node].get("usage_usec",0); cores=cpu_a[node].get("nproc",0) or 1
    util=(du/1e6)/full_wall/cores*100 if full_wall else float("nan"); tot_cpu_us+=du
    cap_str=""
    if cap_cores and full_wall: cap_str=f"  util_of_cap({cap_cores:g})={(du/1e6)/full_wall/cap_cores*100:5.1f}%"
    cpu_lines.append(f"{node}: cpu={du/1e6:8.2f}s  cores={cores:>3}  util={util:5.1f}%{cap_str}")
    dthr=cpu_a[node].get("nr_throttled",0)-cpu_b[node].get("nr_throttled",0); dtus=cpu_a[node].get("throttled_usec",0)-cpu_b[node].get("throttled_usec",0)
    tot_nr+=dthr; tot_tus+=dtus
    thr_lines.append(f"{node}: nr_throttled={dthr:>8}  throttled={dtus/1e6:8.2f}s")
proc_lines=[]; pbsum=0.0; pn=0
for node in sorted(set(ps_b)&set(ps_a)):
    fb=ps_b[node].get('fields'); fa=ps_a[node].get('fields')
    if not fb or not fa or len(fb)<5 or len(fa)<5: continue
    d=[a-b for a,b in zip(fa,fb)]; total=sum(d)
    if total<=0: continue
    busy=(1-(d[3]+d[4])/total)*100; pbsum+=busy; pn+=1
    cg=None
    if node in cpu_b and node in cpu_a and full_wall:
        du=cpu_a[node].get("usage_usec",0)-cpu_b[node].get("usage_usec",0); cores=cpu_a[node].get("nproc",0) or 1
        cg=(du/1e6)/full_wall/cores*100
    proc_lines.append(f"{node}: node_busy={busy:5.1f}%  (cgroup={cg:5.1f}%)  [diff={busy-cg:+5.1f}pp]" if cg is not None else f"{node}: node_busy={busy:5.1f}%")
pbavg=pbsum/pn if pn else float('nan')
logical_gib=dataset_logical/(1024**3)
ncores = (cpu_a[list(cpu_a)[0]].get('nproc',16) or 16) if cpu_a else 16
nnodes = len(cpu_lines) or 5
with open(summary,"w") as out:
    out.write(f"=== READ-phase resource summary (read-only sweep, preloaded data) ===\n")
    out.write(f"system={sys_kind}  nodes=5  read_ops={ops}  object={field_len}B  cache_cap={cache_size}  compr={compression}\n")
    out.write(f"dataset logical bytes (cluster) : {logical_gib:.2f} GiB (record_count={record_count})\n\n")
    out.write("--- DISK + PAGE-CACHE (dm-0 diskstats + memcg) ---\n")
    out.write("\n".join(disk_lines)+"\n")
    out.write(f"TOTAL disk-read  : {tot_read/(1024**3):.2f} GiB\n")
    out.write(f"TOTAL disk-write : {tot_write/(1024**3):.2f} GiB (should be ~0 for reads)\n")
    out.write(f"Disk READ per read-op : {tot_read/ops:.1f} B/op\n\n")
    out.write("--- CPU (cgroup cpu.stat) ---\n")
    out.write(f"window wall time : {full_wall}s\n")
    if cap_cores:
        out.write(f"CPU cap active : {cpu_max_str}  = {cap_cores:g} core(s)/node ({cap_cores/ncores*100:.1f}% of full node). util%=of full node; util_of_cap%=of the {cap_cores:g}-core budget.\n")
    if cpu_lines:
        out.write("\n".join(cpu_lines)+"\n")
        out.write(f"TOTAL CPU-time : {tot_cpu_us/1e6:.2f}s\n")
        out.write(f"CPU per read-op : {tot_cpu_us/ops:.1f} us/op\n")
        if full_wall:
            out.write(f"Aggregate busy cores (cluster) : {tot_cpu_us/1e6/full_wall:.2f} cores-equivalent\n")
            out.write(f"Cluster util (of full {ncores}x{nnodes}) : {tot_cpu_us/1e6/full_wall/ncores/nnodes*100:.1f}%\n")
            if cap_cores:
                out.write(f"Cluster util (of {cap_cores:g}-core cap x{nnodes}) : {tot_cpu_us/1e6/full_wall/cap_cores/nnodes*100:.1f}%  <- how fully the cap was used\n")
    out.write("\n--- CPU cross-check (/proc/stat) ---\n")
    if proc_lines:
        out.write("\n".join(proc_lines)+"\n"); out.write(f"cluster mean node_busy : {pbavg:.1f}%\n")
    out.write("\n--- CPU THROTTLING (cgroup cpu.stat) ---\n")
    if thr_lines:
        out.write("\n".join(thr_lines)+"\n")
        out.write(f"CLUSTER nr_throttled : {tot_nr}   total throttled : {tot_tus/1e6:.2f}s\n")
        out.write("  LARGE throttled = cap is bottlenecking this system; NEAR-ZERO = not biting.\n")
    else:
        out.write("no throttling data (uncapped?).\n")
    out.write("\n--- STORAGE FOOTPRINT (du) ---\n")
    if space_per:
        for node in sorted(space_per): out.write(f"{node}: on_disk={space_per[node]/(1024**3):.2f} GiB\n")
        out.write(f"TOTAL on-disk : {space_tot/(1024**3):.2f} GiB\n")
print(open(summary).read())
PYEOF
    echo "  -> ${pdir}/resource_summary.txt"
}

# ---------- drive the sweep: OUTER = iteration, INNER = caps ----------
echo ""
echo "############################################################"
echo "# 50/50 read/update CPU-cap sweep | system=${SYS_KIND} | caps=${CAPS}"
echo "# iterations=${ITERATIONS} (full sweep repeated ${ITERATIONS}x)"
echo "# ops=${MEASURE_OPS} | mem cap=${CACHE_SIZE} | threads=${RTHREADS} | dist=${READ_DIST}"
echo "# QUORUM r+w. COLD cache every run. Data RESTORED from snapshot before each"
echo "# run so writes don't accumulate (every cap starts from the same dataset)."
echo "############################################################"

# Take the clean-state snapshot ONCE, up front. Requires the cluster to be stopped
# so data/ is quiescent. If a snapshot already exists (resumed run), keep it.
echo ""
echo ">>> Preparing clean-state snapshot (stop cluster, snapshot data/)..."
snap_exists=$(ssh ${SSH_USER}@10.10.1.${BD_NODES[0]} "[ -d ${CASS_DIR}/data_snapshot ] && echo yes || echo no")
if [ "$snap_exists" = "yes" ]; then
    echo ">>> data_snapshot already present on node ${BD_NODES[0]} -- reusing it (resumed run)."
else
    stop_cluster_all
    take_snapshot
    echo ">>> snapshot created from current loaded data."
fi

total=0; ncaps=0; for c in $CAPS; do ncaps=$((ncaps+1)); done
planned=$((ncaps * ITERATIONS))
for iter in $(seq 1 "$ITERATIONS"); do
    echo ""
    echo "########## ITERATION ${iter} / ${ITERATIONS} ##########"
    for cores in $CAPS; do
        total=$((total+1))
        echo "[run ${total}/${planned}]"
        read_once "$cores" "$iter"
    done
done

echo ""
echo "############################################################"
echo "# 50/50 rw sweep complete (system=${SYS_KIND}, ${ITERATIONS} iteration(s)). Result dirs:"
ls -d result_rwsweep_${SYS_KIND}_*_cpu*_iter* 2>/dev/null | sed 's/^/#   /'
echo "############################################################"
# trap clears the cap
