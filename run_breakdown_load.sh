#!/bin/bash
# =============================================================================
# run_breakdown_load.sh
#
# LOAD-ONLY companion to run_breakdown_cachemiss.sh (Section 6.3).
# Captures the SAME instrumentation as the run-phase breakdown script, but only
# around the YCSB insert/load phase:
#   - nodetool breakdown (ycsb/keyspace lines)   -> LEAST SSTable component lines
#   - dm-0 diskstats delta                        -> disk READ + WRITE bytes
#   - memcg memory.stat delta                     -> pgfault/pgmajfault/refault/file
#
# Flow:
#   1) HARD restart cluster (wipe data), bring Cassandra up UNDER an X GB cgroup
#      cap (X from user input). Compaction stays ON the whole time.
#   2) Reset nodetool breakdown + snapshot diskstats/memstat (BEFORE).
#   3) YCSB load (insert only).
#   4) Snapshot diskstats/memstat (AFTER) + collect breakdown.
#   5) Parse -> load-phase I/O summary (write/read bytes, B/op, write-amp).
#
# Config matches the run script: 10KB objects (fieldcount=1), compression ON,
# RS(5,3) EC vs RF=3 REP on the same 5-node cluster (.2-.6), QUORUM not relevant
# for load (consistency irrelevant to insert path here; default used).
#
#   Verified config notes (same cluster as run script):
#     - cgroup io controller NOT enabled -> use dm-0 diskstats
#     - /mydata = LVM LV dm-0 (253:0) over nvme0n1p4
#     - disk_access_mode=mmap_index_only -> pgmajfault is index-only (minor signal)
# =============================================================================

# -- Config -------------------------------------------------------------------
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql

FIELD_LENGTH=10000
RECORD_COUNT=5000000
MEASURE_OPS=10000000       # read-phase operationcount (100% read workload)
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
MEASURE_INCLUDES_FLUSH_SETTLE=1

# -- Extra resource instrumentation (CPU / network / space) -------------------
# CPU: read cgroup cpu.stat (usage_usec) delta for the SAME window as disk/mem.
#      Requires the cpu controller delegated to mylimitedgroup (script enables it).
# NET: /proc/net/dev rx/tx byte deltas on the experiment NIC, captured tightly
#      around the LOAD window only (before the flush/settle SSH storm, which would
#      otherwise contaminate the counters). Leave NET_IFACE empty to auto-detect
#      the interface holding each node's 10.10.1.x address.
NET_IFACE=""                       # e.g. "eno1"/"eth1"; empty => auto-detect per node
# Optional: isolate INTER-NODE traffic (gossip/streaming/mutation) from client CQL
# ingest using nft byte counters on the storage ports. This is the clean number for
# an EC-vs-REP network comparison, but it installs a firewall table on every node.
# Default OFF: verify `sudo nft list table inet least_meter` on one node before
# trusting it. /proc/net/dev totals are always captured regardless.
MEASURE_NETWORK=0                  # master switch: 0 => skip ALL network measurement
                                   # (set by the startup prompt). When 0, neither the
                                   # /proc/net/dev snapshots nor the nft meter run.
MEASURE_NET_PORTS=0                 # 1 => also install+read nft counters (only if MEASURE_NETWORK=1)
STORAGE_PORTS="7000 7001"          # Cassandra inter-node (storage_port + SSL)

# -- Read phase ---------------------------------------------------------------
READ_DIST="uniform"                # request distribution for the 100% read phase
DROP_CACHES_BEFORE_READ=0          # 1 => drop page cache before read (cold, disk-bound)

# -- During-workload sampling (evidence for WHY one system is faster) ----------
# Device %util over time is the smoking gun: if REP saturates dm-0 (~100%) while
# EC has headroom, that IS why EC's write throughput is higher despite more work.
MEASURE_DISK_TIMESERIES=1          # sample /proc/diskstats during each phase (cheap, no JVM)
SAMPLE_SEC=5                       # sampling interval (seconds)
# Cassandra-side backpressure (pending compactions / blocked flush writers) is the
# mechanism translating disk saturation into throttled client writes. Uses nodetool
# (JVM cost per sample) -> run it in a SEPARATE confirmatory trial, not the one you
# quote CPU numbers from, since the JVM sampling perturbs CPU.
MEASURE_CASSANDRA_BACKPRESSURE=0   # 1 => also sample nodetool tpstats/compactionstats

# -- Exact page-cache hit rate (read phase) -----------------------------------
# The refault/dm-0 numbers give a miss PROXY. For the exact per-access hit ratio,
# run BCC 'cachestat' on each node during the workload (kernel-level HITS/MISSES).
# Needs bcc-tools + root on the nodes; auto-skips any node where it's absent.
# NOTE: cachestat is a BPF tracer running ON the measured node, so it adds a small
# CPU overhead there -- for a CPU-precision run set this to 0; for a cache-focused
# run (e.g. the resized dataset), leave it on.
MEASURE_CACHESTAT=0                 # cachestat-bpfcc gates output on a TTY on these nodes
                                   # (verified: writes nothing to a file even foreground+stdbuf),
                                   # so exact hit rate comes from /proc/vmstat instead (always on).

# -- Memory bandwidth per phase (node-level, IMC counters) --------------------
# perf reads the integrated memory controller (uncore_imc) CAS read/write counts
# -- socket-wide DRAM traffic on the node. Node-level (all of Cassandra + a small
# idle floor), which is the approved approximation since Cassandra is the only load.
# perf's IMC PMU auto-scales to bytes (each CAS = 64B baked into .scale), so the
# output is already MiB. Needs perf + root; auto-skips a node if the IMC PMU is absent.
MEASURE_MEMBW=1                    # 1 => sample uncore_imc read/write bandwidth per phase
MEMBW_MAXDUR=14400                 # hard timeout (s) so perf can never orphan
PERF_BIN=perf                      # override if the wrapper fails, e.g. /usr/lib/linux-tools-5.15.0-181/perf
CACHESTAT_MAXDUR=14400             # hard timeout (s) so a tracer can never orphan

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
# =============================================================================
# HARD restart: wipe data, then start each node UNDER the X GB cgroup cap.
# Compaction is left ON (we never disableautocompaction in the load script).
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
             echo '+cpu' | sudo tee /sys/fs/cgroup/cgroup.subtree_control > /dev/null 2>&1 ; \
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

# CPU: cumulative core-microseconds consumed by the Cassandra cgroup. Same
# ### node<N> <tag> / key value layout as memstat, so the parser reuses it.
# nproc is appended so the parser can compute per-machine utilization %.
snapshot_cpustat() {
    local tag=$1 outfile=$2
    : > "$outfile"
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node"
        echo "### node${node} ${tag}" >> "$outfile"
        ssh ${SSH_USER}@${ip} \
            "sudo cat ${CGROUP}/cpu.stat 2>/dev/null | grep -E '^(usage_usec|user_usec|system_usec|nr_throttled|throttled_usec) '; echo nproc \$(nproc)" >> "$outfile"
    done
}

# Node-level CPU cross-check via /proc/stat (independent of the cgroup). The
# aggregate 'cpu' line gives cumulative jiffies: user nice system idle iowait
# irq softirq steal. Busy% over the phase = 1 - (idle+iowait)delta / totaldelta.
# On a node where Cassandra is the only load, this should track the cgroup number
# -- a divergence flags background noise or a cgroup-accounting problem.
snapshot_procstat() {
    local tag=$1 outfile=$2
    : > "$outfile"
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node"
        echo "### node${node} ${tag}" >> "$outfile"
        ssh ${SSH_USER}@${ip} \
            "grep '^cpu ' /proc/stat; echo clk_tck \$(getconf CLK_TCK); echo nproc \$(nproc)" >> "$outfile"
    done
}

# NET: rx/tx byte+packet counters on the experiment NIC (auto-detected from the
# node's 10.10.1.x address unless NET_IFACE is set). If MEASURE_NET_PORTS=1, also
# read the nft inter-node byte counters. Line formats the parser expects:
#   node<N> <dev> <rx_bytes> <rx_packets> <tx_bytes> <tx_packets>
#   node<N> nft rx <bytes> tx <bytes>
snapshot_netstat() {
    local tag=$1 outfile=$2
    : > "$outfile"
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node"
        ssh ${SSH_USER}@${ip} "
            dev='${NET_IFACE}'
            [ -z \"\$dev\" ] && dev=\$(ip -o -4 addr show | awk '/inet 10\\.10\\.1\\./{print \$2; exit}')
            line=\$(sed 's/:/ /' /proc/net/dev | awk -v d=\"\$dev\" '\$1==d{print \$2, \$3, \$10, \$11}')
            echo \"node${node} \$dev \$line\"
        " >> "$outfile"
        if [ "$MEASURE_NET_PORTS" = "1" ]; then
            local rxb txb
            rxb=$(ssh ${SSH_USER}@${ip} "sudo nft list chain inet least_meter rx 2>/dev/null | awk '{for(i=1;i<NF;i++) if(\$i==\"bytes\") s+=\$(i+1)} END{print s+0}'")
            txb=$(ssh ${SSH_USER}@${ip} "sudo nft list chain inet least_meter tx 2>/dev/null | awk '{for(i=1;i<NF;i++) if(\$i==\"bytes\") s+=\$(i+1)} END{print s+0}'")
            echo "node${node} nft rx ${rxb:-0} tx ${txb:-0}" >> "$outfile"
        fi
    done
}

# Install a self-contained nft table that only COUNTS bytes on the storage ports
# (policy accept, never drops). Its own table => does not touch existing firewall
# rules. The table/delete/table idiom resets counters idempotently on each run.
install_net_meter() {
    { [ "$MEASURE_NETWORK" != "1" ] || [ "$MEASURE_NET_PORTS" != "1" ]; } && return 0
    local ports="${STORAGE_PORTS// /, }"
    echo "--- Installing nft inter-node byte meter (ports ${ports}) ---"
    for node in "${BD_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.$node "sudo nft -f - <<'NFT' 2>/dev/null || echo '  WARN: nft meter install failed on 10.10.1.$node (nft present? sudo ok?)'
table inet least_meter
delete table inet least_meter
table inet least_meter {
    chain rx { type filter hook input priority -450; policy accept;
        tcp sport { ${ports} } counter
        tcp dport { ${ports} } counter
    }
    chain tx { type filter hook output priority -450; policy accept;
        tcp sport { ${ports} } counter
        tcp dport { ${ports} } counter
    }
}
NFT"
    done
}

# =============================================================================
# Interactive setup (EC/REP + memory cap + load threads; node count fixed at 5)
# =============================================================================
echo "Checking create-table binaries..."
for bin in create_table_ec_compr_on create_table_ec_compr_off \
           create_table_rep_compr_on create_table_rep_compr_off; do
    if [ ! -x "/mydata/${bin}" ]; then echo "ERROR: /mydata/${bin} missing."; exit 1; fi
done
echo "OK."; echo ""

echo "Is this EC or REP?"; read EXP_LABEL
read -p "Cassandra memory cap in GB (e.g. 32): " CACHE_GB
read -p "Load (insert) threads: " WTHREADS
read -p "Phase mode -- 1 = load only, 2 = load + read: " PHASE_MODE
PHASE_MODE="${PHASE_MODE:-2}"
if [ "$PHASE_MODE" = "1" ]; then
    RTHREADS=0
    echo "  -> LOAD-ONLY run (read phase skipped)."
else
    PHASE_MODE=2
    read -p "Read (run) threads: " RTHREADS
fi
read -p "Measure network? 1 = yes, 0 = no (skip all network measurement): " MEASURE_NETWORK
MEASURE_NETWORK="${MEASURE_NETWORK:-0}"
[ "$MEASURE_NETWORK" = "1" ] || { MEASURE_NETWORK=0; MEASURE_NET_PORTS=0; echo "  -> network measurement OFF."; }

CACHE_SIZE="${CACHE_GB}GB"

if echo "$EXP_LABEL" | grep -qi "rep"; then
    CREATE_TABLE_BIN="create_table_rep_compr_${COMPRESSION}"; SYS_KIND="rep"
else
    CREATE_TABLE_BIN="create_table_ec_compr_${COMPRESSION}"; SYS_KIND="ec"
fi

OUT_DIR="result_breakdown_${EXP_LABEL}_${COMPRESSION}_${CACHE_SIZE}"
mkdir -p "$OUT_DIR"
LOG="${OUT_DIR}/run.log"
BREAKDOWN_FILE="${OUT_DIR}/breakdown.txt"; touch "$BREAKDOWN_FILE"

echo ""
echo "################################################################"
echo ">>> ${EXP_LABEL^^} | 5 nodes | compr=${COMPRESSION} | cap=${CACHE_SIZE} | LOAD+READ | 10KB | compaction ON"
echo ">>> metrics: ${DATA_DEV} diskstats + memcg + cgroup cpu.stat + /proc/net/dev + du footprint"
echo "################################################################"

# =============================================================================
# Per-phase compaction snapshot (before/after -> delta), so compaction bytes are
# PER PHASE. compactionhistory is cumulative and non-resettable, so the only way
# to isolate a phase's compaction is to diff the summed bytes across the window.
# =============================================================================
snapshot_compaction() {
    local outfile=$1
    : > "$outfile"
    for node in "${BD_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool compactionhistory 2>/dev/null | awk '\$2==\"ycsb\"{i+=\$5; o+=\$6} END{printf \"node%s bytes_in=%d bytes_out=%d\\n\", \"${node}\", i+0, o+0}'" >> "$outfile"
    done
}

# =============================================================================
# Time-series samplers (background). Device %util answers "why is X faster";
# backpressure (opt-in) shows the flush/compaction queue building under load.
# =============================================================================
start_samplers() {
    local pdir=$1
    SAMPLER_STOP="${pdir}/.stop_sampler"; rm -f "$SAMPLER_STOP"
    SAMPLER_PIDS=()
    if [ "$MEASURE_DISK_TIMESERIES" = "1" ]; then
        : > "${pdir}/diskutil_timeseries.txt"
        ( while [ ! -f "$SAMPLER_STOP" ]; do
            ts=$(date +%s)
            for node in "${BD_NODES[@]}"; do
                line=$(ssh ${SSH_USER}@10.10.1.$node "grep -w ${DATA_DEV} /proc/diskstats | head -1" 2>/dev/null)
                echo "${ts} node${node} ${line}"
            done >> "${pdir}/diskutil_timeseries.txt"
            sleep "${SAMPLE_SEC}"
          done ) &
        SAMPLER_PIDS+=($!)
    fi
    if [ "$MEASURE_CASSANDRA_BACKPRESSURE" = "1" ]; then
        : > "${pdir}/backpressure_timeseries.txt"
        ( while [ ! -f "$SAMPLER_STOP" ]; do
            ts=$(date +%s)
            for node in "${BD_NODES[@]}"; do
                pend=$(ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool compactionstats 2>/dev/null | awk '/pending tasks/{print \$NF}'")
                tp=$(ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool tpstats 2>/dev/null | awk '/CompactionExecutor|MemtableFlushWriter|MutationStage/{print \$1\"=pend\"\$3\"/blk\"\$5}' | tr '\n' ' '")
                echo "${ts} node${node} compaction_pending=${pend:-NA} ${tp}"
            done >> "${pdir}/backpressure_timeseries.txt"
            sleep "$((SAMPLE_SEC*2))"
          done ) &
        SAMPLER_PIDS+=($!)
    fi
}
stop_samplers() {
    [ -n "$SAMPLER_STOP" ] && touch "$SAMPLER_STOP"
    for p in "${SAMPLER_PIDS[@]}"; do wait "$p" 2>/dev/null; done
    SAMPLER_PIDS=()
}

# =============================================================================
# /proc/vmstat snapshot (page-cache miss counters). pgpgin = KB paged in from
# block devices into the page cache = read-phase cache MISSES. Delta before/after
# a phase -> miss bytes; combined with requested bytes -> exact-ish hit ratio.
# No BPF / TTY / root needed (cachestat-bpfcc gates output on a TTY on these nodes).
# =============================================================================
snapshot_vmstat() {
    local tag=$1 outfile=$2
    : > "$outfile"
    for node in "${BD_NODES[@]}"; do
        echo "### node${node} ${tag}" >> "$outfile"
        ssh ${SSH_USER}@10.10.1.$node \
            "grep -E '^(pgpgin|pgpgout|pgfault|pgmajfault) ' /proc/vmstat" >> "$outfile"
    done
}

# =============================================================================
# Exact page-cache hit rate via BCC cachestat, launched ON each node for the
# workload window. timeout() bounds it so a tracer can never orphan; any node
# without a cachestat binary is skipped (the run continues on the dm-0/refault
# proxy). Output per node -> cachestat_node<N>.txt for cache_hit.py.
# =============================================================================
start_cachestat() {
    local pdir=$1
    [ "$MEASURE_CACHESTAT" != "1" ] && return 0
    CACHESTAT_PIDFILE="${pdir}/.cachestat_pids"; : > "$CACHESTAT_PIDFILE"
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node" bin pid
        bin=$(ssh ${SSH_USER}@${ip} "command -v cachestat || command -v cachestat-bpfcc || ls /usr/share/bcc/tools/cachestat 2>/dev/null" 2>/dev/null | head -1)
        if [ -z "$bin" ]; then
            echo "  cachestat not found on node ${node} -- skipping (apt install bcc-tools for exact hit rate)"
            continue
        fi
        pid=$(ssh ${SSH_USER}@${ip} "sudo nohup timeout ${CACHESTAT_MAXDUR} ${bin} ${SAMPLE_SEC} > /tmp/cachestat_${node}.out 2>/dev/null & echo \$!")
        echo "node${node} ${pid}" >> "$CACHESTAT_PIDFILE"
    done
}
stop_cachestat() {
    local pdir=$1
    [ "$MEASURE_CACHESTAT" != "1" ] && return 0
    [ -f "$CACHESTAT_PIDFILE" ] || return 0
    while read -r node pid; do
        local n="${node#node}" ip
        ip="10.10.1.${n}"
        # kill the timeout wrapper + the cachestat child, then pull the log back
        ssh ${SSH_USER}@${ip} "sudo kill ${pid} 2>/dev/null; sudo pkill -f cachestat 2>/dev/null" 2>/dev/null
        ssh ${SSH_USER}@${ip} "cat /tmp/cachestat_${n}.out 2>/dev/null" > "${pdir}/cachestat_${node}.txt" 2>/dev/null
    done < "$CACHESTAT_PIDFILE"
}

# =============================================================================
# Memory bandwidth per phase via perf uncore_imc counters, launched ON each node
# for the workload window. perf -I interval mode writes a timestamped read/write
# byte line every SAMPLE_SEC; timeout bounds it so it can never orphan. Any node
# without perf or the IMC PMU is skipped. Output -> membw_node<N>.txt for parsing.
# =============================================================================
start_membw() {
    local pdir=$1
    [ "$MEASURE_MEMBW" != "1" ] && return 0
    MEMBW_PIDFILE="${pdir}/.membw_pids"; : > "$MEMBW_PIDFILE"
    local ms=$((SAMPLE_SEC * 1000))
    for node in "${BD_NODES[@]}"; do
        local ip="10.10.1.$node" ok pid
        # verify perf + IMC PMU on this node before launching
        ok=$(ssh ${SSH_USER}@${ip} "ls /sys/bus/event_source/devices/ 2>/dev/null | grep -qi imc && command -v ${PERF_BIN} >/dev/null 2>&1 && echo yes" 2>/dev/null)
        if [ "$ok" != "yes" ]; then
            echo "  perf/IMC not available on node ${node} -- skipping memory bandwidth there"
            continue
        fi
        # -I interval (ms), -x, field-separated, aggregate read+write IMC events.
        # perf's IMC scale prints bytes; -x, gives machine-parseable lines.
        pid=$(ssh ${SSH_USER}@${ip} \
            "sudo nohup timeout ${MEMBW_MAXDUR} ${PERF_BIN} stat -a -x, -I ${ms} \
                -e uncore_imc/cas_count_read/,uncore_imc/cas_count_write/ \
                > /mydata/membw_${node}.out 2>&1 & echo \$!")
        echo "node${node} ${pid}" >> "$MEMBW_PIDFILE"
    done
}
stop_membw() {
    local pdir=$1
    [ "$MEASURE_MEMBW" != "1" ] && return 0
    [ -f "$MEMBW_PIDFILE" ] || return 0
    while read -r node pid; do
        local n="${node#node}" ip
        ip="10.10.1.${n}"
        # NOTE: ssh -n (stdin from /dev/null) is REQUIRED here -- without it, ssh
        # consumes the rest of the pidfile from the loop's stdin and the while-read
        # loop exits after the first node. This is why only node2 was ever processed.
        # Kill the timeout wrapper AND its perf child, flush, then collect.
        ssh -n ${SSH_USER}@${ip} "sudo kill ${pid} 2>/dev/null; sudo pkill -TERM -f 'perf stat -a' 2>/dev/null; sleep 1; sudo pkill -KILL -f 'perf stat -a' 2>/dev/null" 2>/dev/null
        # collect to a temp, verify non-empty, THEN remove remote file
        ssh -n ${SSH_USER}@${ip} "cat /mydata/membw_${n}.out 2>/dev/null" > "${pdir}/membw_${node}.txt" 2>/dev/null
        if [ -s "${pdir}/membw_${node}.txt" ]; then
            ssh -n ${SSH_USER}@${ip} "sudo rm -f /mydata/membw_${n}.out" 2>/dev/null
        else
            echo "  WARN: membw collection empty for ${node} -- leaving /mydata/membw_${n}.out on the node for manual recovery"
        fi
    done < "$MEMBW_PIDFILE"
}

# =============================================================================
# PHASE RUNNER: BEFORE snapshots -> workload -> (load only: flush+settle) ->
# AFTER snapshots -> collect -> parse into a per-phase resource_summary.txt.
# Counters are NEVER reset: each phase takes its own before/after pair, so the
# read window is cleanly isolated from load without any fragile reset step.
# Args: <phase: load|read> <ops> <op_label> <full ycsb command ...>
# =============================================================================
run_phase() {
    local phase=$1 ops=$2 op_label=$3; shift 3
    local pdir="${OUT_DIR}/${phase}"; mkdir -p "$pdir"
    echo ""
    echo "################################################################"
    echo ">>> PHASE ${phase^^} | ${ops} ${op_label}-ops | ${EXP_LABEL^^} | cap=${CACHE_SIZE} | compr=${COMPRESSION}"
    echo "################################################################"

    echo "--- ${phase}: reset breakdown + BEFORE snapshots ---"
    for node in "${BD_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool breakdown --reset" >/dev/null 2>&1
    done
    snapshot_memstat   "before" "${pdir}/memstat_before.txt"
    snapshot_diskstats          "${pdir}/diskstats_before.txt"
    snapshot_cpustat   "before" "${pdir}/cpustat_before.txt"
    snapshot_procstat  "before" "${pdir}/procstat_before.txt"
    # NET snapshotted tightly around the workload (before any flush/settle SSH
    # storm), so /proc/net/dev is not polluted by our own control-plane traffic.
    [ "$MEASURE_NETWORK" = "1" ] && snapshot_netstat "before" "${pdir}/netstat_before.txt"
    snapshot_compaction        "${pdir}/compaction_before.txt"
    snapshot_vmstat    "before" "${pdir}/vmstat_before.txt"

    start_samplers "$pdir"
    start_cachestat "$pdir"
    start_membw "$pdir"
    local full_start load_start load_end full_end
    full_start=$(date +%s); load_start=$full_start
    echo "=== ${phase}: running workload ==="
    "$@" >> "${pdir}/run.log" 2>&1
    load_end=$(date +%s)
    stop_samplers
    stop_cachestat "$pdir"
    stop_membw "$pdir"
    [ "$MEASURE_NETWORK" = "1" ] && snapshot_netstat "after" "${pdir}/netstat_after.txt"
    snapshot_vmstat    "after" "${pdir}/vmstat_after.txt"
    echo "=== ${phase}: workload done ==="

    # Only the write phase produces durable state that needs to settle.
    if [ "$phase" = "load" ] && [ "$MEASURE_INCLUDES_FLUSH_SETTLE" = "1" ]; then
        echo "--- Flushing, then settling compaction ---"
        for node in "${BD_NODES[@]}"; do ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool flush" & done
        wait
        wait_for_compaction_settle
        echo "--- load compaction settled at epoch $(date +%s) ($(date '+%F %T')) ---"
    fi

    snapshot_memstat   "after" "${pdir}/memstat_after.txt"
    snapshot_diskstats         "${pdir}/diskstats_after.txt"
    snapshot_cpustat   "after" "${pdir}/cpustat_after.txt"
    snapshot_procstat  "after" "${pdir}/procstat_after.txt"
    full_end=$(date +%s)
    { echo "FULL_START ${full_start}"; echo "FULL_END ${full_end}"; \
      echo "LOAD_START ${load_start}"; echo "LOAD_END ${load_end}"; } > "${pdir}/timings.txt"

    # ---- write-path artifacts (meaningful for load; ~inert for read) ----
    echo "--- ${phase}: collecting breakdown / tablestats / compaction / space ---"
    echo "${phase} ${EXP_LABEL} ${CACHE_SIZE} compr=${COMPRESSION}" > "${pdir}/breakdown.txt"
    for node in "${BD_NODES[@]}"; do
        echo "-- node 10.10.1.$node --" >> "${pdir}/breakdown.txt"
        ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool breakdown | grep -E 'keyspace|ycsb'" >> "${pdir}/breakdown.txt"
    done
    echo "tablestats ${phase} ${CACHE_SIZE} compr=${COMPRESSION}" > "${pdir}/tablestats.txt"
    for node in "${BD_NODES[@]}"; do
        echo "" >> "${pdir}/tablestats.txt"
        echo "===== node 10.10.1.$node =====" >> "${pdir}/tablestats.txt"
        ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool tablestats ycsb.usertable" >> "${pdir}/tablestats.txt" 2>&1
    done
    snapshot_compaction "${pdir}/compaction_after.txt"
    cp "${pdir}/compaction_after.txt" "${pdir}/compaction_history.txt" 2>/dev/null
    echo "space ${phase} ${CACHE_SIZE} compr=${COMPRESSION}" > "${pdir}/space.txt"
    for node in "${BD_NODES[@]}"; do
        b=$(ssh ${SSH_USER}@10.10.1.$node "du -sb ${CASS_DIR}/data 2>/dev/null | awk '{print \$1}'")
        echo "node${node} ${b:-0}" >> "${pdir}/space.txt"
    done

    # ---- parse -> per-phase resource summary ----
    python3 - "$pdir" "${pdir}/resource_summary.txt" "$ops" "$SYS_KIND" "$FIELD_LENGTH" "$CACHE_SIZE" "$COMPRESSION" "$phase" "$op_label" "$RECORD_COUNT" << 'PYEOF'
import sys, re, os
outdir, summary, ops, sys_kind, field_len, cache_size, compression, phase, op_label, record_count = sys.argv[1:11]
ops = int(ops); field_len = int(field_len); record_count = int(record_count)
dataset_logical = record_count * field_len   # bytes of loaded dataset (phase-independent)

def parse_diskstats(path):
    d = {}
    if not os.path.exists(path): return d
    with open(path) as f:
        for line in f:
            p = line.split()
            if len(p) < 11: continue
            try:
                d[p[0]] = (int(p[4]), int(p[6]), int(p[8]), int(p[10]))  # rd_done, sec_read, wr_done, sec_write
            except (ValueError, IndexError):
                continue
    return d

def parse_kv(path):
    data, node = {}, None
    if not os.path.exists(path): return data
    with open(path) as f:
        for line in f:
            line = line.rstrip("\n")
            m = re.match(r'### node(\d+)', line)
            if m:
                node = "node" + m.group(1); data[node] = {}; continue
            if node is None: continue
            p = line.split()
            if len(p) == 2 and p[1].lstrip('-').isdigit():
                data[node][p[0]] = int(p[1])
    return data

def parse_netstat(path):
    dev, nft = {}, {}
    if not os.path.exists(path): return dev, nft
    with open(path) as f:
        for line in f:
            p = line.split()
            if len(p) >= 6 and p[0].startswith("node") and p[1] == "nft":
                try: nft[p[0]] = (int(p[3]), int(p[5]))
                except (ValueError, IndexError): pass
            elif len(p) >= 6 and p[0].startswith("node") and p[2].isdigit():
                try: dev[p[0]] = (int(p[2]), int(p[4]))
                except (ValueError, IndexError): pass
    return dev, nft

def parse_space(path):
    tot, per = 0, {}
    if not os.path.exists(path): return tot, per
    with open(path) as f:
        for line in f:
            p = line.split()
            if len(p) == 2 and p[0].startswith("node") and p[1].isdigit():
                per[p[0]] = int(p[1]); tot += int(p[1])
    return tot, per

def parse_timings(path):
    t = {}
    if not os.path.exists(path): return t
    with open(path) as f:
        for line in f:
            p = line.split()
            if len(p) == 2 and p[1].lstrip("-").isdigit():
                t[p[0]] = int(p[1])
    return t

db = parse_diskstats(os.path.join(outdir, "diskstats_before.txt"))
da = parse_diskstats(os.path.join(outdir, "diskstats_after.txt"))
mb = parse_kv(os.path.join(outdir, "memstat_before.txt"))
ma = parse_kv(os.path.join(outdir, "memstat_after.txt"))
cpu_b = parse_kv(os.path.join(outdir, "cpustat_before.txt"))
cpu_a = parse_kv(os.path.join(outdir, "cpustat_after.txt"))

# ---- node-level CPU from /proc/stat (cross-check on the cgroup number) ----
def parse_procstat(path):
    data, node = {}, None
    if not os.path.exists(path):
        return data
    for line in open(path):
        m = re.match(r'### node(\d+)', line)
        if m:
            node = "node" + m.group(1); data[node] = {}; continue
        if node is None:
            continue
        p = line.split()
        if p and p[0] == 'cpu':
            # user nice system idle iowait irq softirq steal guest guest_nice
            vals = [int(x) for x in p[1:] if x.isdigit()]
            data[node]['fields'] = vals
        elif len(p) == 2 and p[0] in ('clk_tck', 'nproc'):
            data[node][p[0]] = int(p[1])
    return data
ps_b = parse_procstat(os.path.join(outdir, "procstat_before.txt"))
ps_a = parse_procstat(os.path.join(outdir, "procstat_after.txt"))
net_b_dev, net_b_nft = parse_netstat(os.path.join(outdir, "netstat_before.txt"))
net_a_dev, net_a_nft = parse_netstat(os.path.join(outdir, "netstat_after.txt"))
space_tot, space_per = parse_space(os.path.join(outdir, "space.txt"))
tim = parse_timings(os.path.join(outdir, "timings.txt"))
full_wall = max(tim.get("FULL_END", 0) - tim.get("FULL_START", 0), 0)
load_wall = max(tim.get("LOAD_END", 0) - tim.get("LOAD_START", 0), 0)

# ---- disk + page-cache ----
disk_lines, tot_write, tot_read = [], 0, 0
for node in sorted(set(db) & set(da)):
    rd_done_b, sec_read_b, wr_done_b, sec_write_b = db[node]
    rd_done_a, sec_read_a, wr_done_a, sec_write_a = da[node]
    write_bytes = (sec_write_a - sec_write_b) * 512
    read_bytes  = (sec_read_a  - sec_read_b)  * 512
    wr_ops = wr_done_a - wr_done_b
    rd_ops = rd_done_a - rd_done_b
    tot_write += write_bytes; tot_read += read_bytes
    pgf     = ma.get(node, {}).get('pgfault', 0)                 - mb.get(node, {}).get('pgfault', 0)
    majf    = ma.get(node, {}).get('pgmajfault', 0)              - mb.get(node, {}).get('pgmajfault', 0)
    refault = ma.get(node, {}).get('workingset_refault_file', 0) - mb.get(node, {}).get('workingset_refault_file', 0)
    resident = ma.get(node, {}).get('file', None)
    rstr = f"{resident/(1024**3):.2f}GiB" if resident is not None else "n/a"
    disk_lines.append(
        f"{node}: disk_write={write_bytes/(1024**2):9.1f}MB  disk_read={read_bytes/(1024**2):9.1f}MB  "
        f"wr_ops={wr_ops:>9}  rd_ops={rd_ops:>9}  "
        f"pgfault(+)={pgf:>11}  pgmajfault_idx(+)={majf:>8}  refault_file(+)={refault:>10}  "
        f"resident_pagecache={rstr}")
w_per_op = tot_write / ops if ops else float('nan')
r_per_op = tot_read  / ops if ops else float('nan')

# ---- cpu ----
cpu_lines, tot_cpu_us, tot_user_us, tot_sys_us = [], 0, 0, 0
for node in sorted(set(cpu_b) & set(cpu_a)):
    du  = cpu_a[node].get("usage_usec", 0)  - cpu_b[node].get("usage_usec", 0)
    duu = cpu_a[node].get("user_usec", 0)   - cpu_b[node].get("user_usec", 0)
    dus = cpu_a[node].get("system_usec", 0) - cpu_b[node].get("system_usec", 0)
    cores = cpu_a[node].get("nproc", 0) or 1
    util = (du/1e6)/full_wall/cores*100 if full_wall else float("nan")
    tot_cpu_us += du; tot_user_us += duu; tot_sys_us += dus
    cpu_lines.append(f"{node}: cpu={du/1e6:8.2f}s  (user={duu/1e6:7.2f}s sys={dus/1e6:7.2f}s)  cores={cores:>3}  util={util:5.1f}%")
cpu_per_op_us = tot_cpu_us / ops if ops else float("nan")

# ---- node-level CPU busy% from /proc/stat (cross-check) ----
# busy% = 1 - (idle+iowait)delta / total_jiffies_delta, per node (all cores).
proc_lines = []
proc_busy_sum = 0.0; proc_n = 0
for node in sorted(set(ps_b) & set(ps_a)):
    fb = ps_b[node].get('fields'); fa = ps_a[node].get('fields')
    if not fb or not fa or len(fb) < 5 or len(fa) < 5:
        continue
    d = [a - b for a, b in zip(fa, fb)]
    total = sum(d)
    if total <= 0:
        continue
    idle = d[3] + d[4]  # idle + iowait
    busy_pct = (1 - idle/total) * 100
    # also express as cgroup-comparable: cgroup util% is over the SAME cores
    proc_busy_sum += busy_pct; proc_n += 1
    # cgroup busy% for the same node, for side-by-side
    cg = None
    if node in cpu_b and node in cpu_a and full_wall:
        du = cpu_a[node].get("usage_usec",0) - cpu_b[node].get("usage_usec",0)
        cores = cpu_a[node].get("nproc",0) or 1
        cg = (du/1e6)/full_wall/cores*100
    cgs = f"{cg:5.1f}%" if cg is not None else "  n/a"
    proc_lines.append(f"{node}: node_busy={busy_pct:5.1f}%  (cgroup={cgs})  [diff={busy_pct-cg:+5.1f}pp]" if cg is not None
                      else f"{node}: node_busy={busy_pct:5.1f}%  (cgroup=  n/a)")
proc_busy_avg = proc_busy_sum/proc_n if proc_n else float('nan')

# ---- net ----
net_lines, tot_rx, tot_tx = [], 0, 0
for node in sorted(set(net_b_dev) & set(net_a_dev)):
    drx = net_a_dev[node][0] - net_b_dev[node][0]
    dtx = net_a_dev[node][1] - net_b_dev[node][1]
    tot_rx += drx; tot_tx += dtx
    net_lines.append(f"{node}: net_tx={dtx/(1024**2):9.1f}MB  net_rx={drx/(1024**2):9.1f}MB")
tx_per_op = tot_tx / ops if ops else float("nan")
rx_per_op = tot_rx / ops if ops else float("nan")

nft_lines, ntx, nrx = [], 0, 0
for node in sorted(set(net_b_nft) & set(net_a_nft)):
    drx = net_a_nft[node][0] - net_b_nft[node][0]
    dtx = net_a_nft[node][1] - net_b_nft[node][1]
    nrx += drx; ntx += dtx
    nft_lines.append(f"{node}: internode_tx={dtx/(1024**2):9.1f}MB  internode_rx={drx/(1024**2):9.1f}MB")

logical_gib = dataset_logical / (1024**3)
with open(summary, "w") as out:
    out.write(f"=== {phase.upper()}-phase resource summary (compaction ON) ===\n")
    out.write(f"system={sys_kind}  nodes=5  {op_label}_ops={ops}  object={field_len}B  "
              f"cache_cap={cache_size}  compr={compression}\n")
    out.write(f"dataset logical bytes (cluster) : {logical_gib:.2f} GiB (record_count={record_count})\n\n")

    out.write("--- DISK + PAGE-CACHE (dm-0 diskstats + memcg) ---\n")
    out.write("\n".join(disk_lines) + "\n\n")
    out.write(f"TOTAL disk-write : {tot_write/(1024**2):.1f} MB ({tot_write/(1024**3):.2f} GiB)\n")
    out.write(f"TOTAL disk-read  : {tot_read/(1024**2):.1f} MB ({tot_read/(1024**3):.2f} GiB)\n")
    out.write(f"Disk WRITE per {op_label}-op : {w_per_op:.1f} B/op\n")
    out.write(f"Disk READ  per {op_label}-op : {r_per_op:.1f} B/op\n")
    if phase == "load":
        if sys_kind == 'ec':
            expected_payload = dataset_logical * 5.0 / 3.0
            model = "EC RS(5,3): 5 shards x ~(field/3) = field*5/3 across cluster"
        else:
            expected_payload = dataset_logical * 3.0
            model = "REP RF=3: 3 replicas x field across cluster"
        write_amp = tot_write / dataset_logical if dataset_logical else float('nan')
        payload_ratio = tot_write / expected_payload if expected_payload else float('nan')
        out.write(f"Write amplification (disk_write / logical) : {write_amp:.2f}x\n")
        out.write(f"Disk_write / expected encoded payload      : {payload_ratio:.2f}x\n")
        out.write(f"  expected payload model: {model}\n")
    else:
        out.write("  (read phase: disk_write ~0 expected; disk_read/op + pgmajfault/refault\n")
        out.write("   are the headline -- how hard the read path hits disk under the cap.)\n")
    out.write("\n")

    out.write("--- CPU (cgroup cpu.stat) ---\n")
    out.write(f"window wall time : {full_wall}s\n")
    if cpu_lines:
        out.write("\n".join(cpu_lines) + "\n")
        out.write(f"TOTAL CPU-time : {tot_cpu_us/1e6:.2f}s (user={tot_user_us/1e6:.2f}s system={tot_sys_us/1e6:.2f}s)\n")
        out.write(f"CPU per {op_label}-op : {cpu_per_op_us:.1f} us/op\n")
        out.write(f"CPU per dataset GiB : {tot_cpu_us/1e6/logical_gib:.2f} s/GiB\n")
        if full_wall:
            out.write(f"Aggregate busy cores (cluster) : {tot_cpu_us/1e6/full_wall:.2f} cores-equivalent\n")
    else:
        out.write("no cpu.stat data -- is the cpu controller delegated? "
                  "check `cat /sys/fs/cgroup/mylimitedgroup/cgroup.controllers`\n")
    out.write("\n")

    out.write("--- CPU cross-check (/proc/stat, node-level, all cores) ---\n")
    if proc_lines:
        out.write("\n".join(proc_lines) + "\n")
        out.write(f"cluster mean node_busy : {proc_busy_avg:.1f}%\n")
        out.write("  node_busy = whole-node CPU busy% (1 - (idle+iowait)/total) from /proc/stat.\n")
        out.write("  cgroup = Cassandra-only util% from cpu.stat. On a dedicated node the two\n")
        out.write("  should track; a large positive diff = non-Cassandra load on the node,\n")
        out.write("  a large negative diff = a cgroup cpu-accounting problem. This is the\n")
        out.write("  independent check on the cgroup CPU number.\n")
    else:
        out.write("no /proc/stat data captured.\n")
    out.write("\n")

    out.write("--- NETWORK (/proc/net/dev, workload window only) ---\n")
    out.write(f"workload wall time : {load_wall}s\n")
    if net_lines:
        out.write("\n".join(net_lines) + "\n")
        out.write(f"TOTAL net TX : {tot_tx/(1024**2):.1f} MB ({tot_tx/(1024**3):.2f} GiB)\n")
        out.write(f"TOTAL net RX : {tot_rx/(1024**2):.1f} MB ({tot_rx/(1024**3):.2f} GiB)\n")
        out.write(f"Net TX per {op_label}-op : {tx_per_op:.1f} B/op\n")
        out.write(f"Net RX per {op_label}-op : {rx_per_op:.1f} B/op\n")
        out.write("  NOTE: whole-NIC counters include client<->coordinator CQL traffic,\n")
        out.write("        not just inter-node. Set MEASURE_NET_PORTS=1 for the isolated number.\n")
    else:
        out.write("no net data (interface auto-detect failed? set NET_IFACE explicitly)\n")
    if nft_lines:
        out.write("\n  -- inter-node only (nft, storage ports) --\n")
        out.write("\n".join(nft_lines) + "\n")
        out.write(f"  TOTAL inter-node TX : {ntx/(1024**2):.1f} MB   RX : {nrx/(1024**2):.1f} MB\n")
        out.write(f"  Inter-node TX per {op_label}-op : {ntx/ops:.1f} B/op\n")
        out.write(f"  Inter-node RX per {op_label}-op : {nrx/ops:.1f} B/op\n")
    out.write("\n")

    out.write("--- STORAGE FOOTPRINT (du, post-workload) ---\n")
    if space_per:
        for node in sorted(space_per):
            out.write(f"{node}: on_disk={space_per[node]/(1024**3):.2f} GiB\n")
        out.write(f"TOTAL on-disk : {space_tot/(1024**2):.1f} MB ({space_tot/(1024**3):.2f} GiB)\n")
        ratio = space_tot / dataset_logical if dataset_logical else float("nan")
        out.write(f"Storage overhead (on_disk / dataset_logical) : {ratio:.2f}x\n")
        out.write("  expected: EC RS(5,3) ~1.67x, REP RF=3 ~3.0x (compression pulls both down).\n")
    else:
        out.write("no space data\n")

print(open(summary).read())
PYEOF
    echo "  -> ${pdir}/resource_summary.txt"
}

# =============================================================================
# MAIN: fresh cluster, then PHASE 1 (100% load) and PHASE 2 (100% read) on the
# SAME cluster/data -- no restart between phases.
# =============================================================================

# Cleanup trap: if the script exits for ANY reason (Ctrl-C, kill, error, normal
# end), stop the background samplers/tracers so they can't orphan and spam the
# shell or steal CPU on a node. Safe to call even if nothing was started.
cleanup() {
    trap - EXIT INT TERM
    echo ""; echo "--- cleanup: stopping samplers/tracers ---"
    stop_samplers 2>/dev/null
    for d in "${OUT_DIR}"/load "${OUT_DIR}"/read; do
        [ -d "$d" ] && stop_cachestat "$d" 2>/dev/null
    done
    # belt-and-suspenders: kill any stray background probes on the nodes
    for node in "${BD_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.$node "sudo pkill -f cachestat 2>/dev/null" 2>/dev/null &
    done
    wait 2>/dev/null
}
trap cleanup EXIT INT TERM

# Pre-run health gate: refuse to run on a degraded cluster, because a node that
# is down or dropping mutations silently corrupts the byte counters and latency
# tails. Aborts if any node is not 'UN', or if any node already shows dropped
# mutations (a sign of a sick node from a prior run).
preflight_health() {
    echo "--- preflight: cluster health ---"
    local bad=0
    # node0 is the CLIENT and has no local Cassandra -- query a storage node over
    # SSH, like everywhere else in this script. Use the first BD node as the probe.
    local probe="${BD_NODES[0]}"
    local st; st="$(ssh ${SSH_USER}@10.10.1.${probe} "${CASS_DIR}/bin/nodetool status 2>/dev/null")"
    local up; up="$(echo "$st" | grep -E '^[[:space:]]*UN[[:space:]]+10\.10\.1\.' | wc -l | tr -d ' ')"
    echo "  nodes UN: ${up}/${NUM_NODES}  (via node ${probe})"
    if [ "${up:-0}" -ne "$NUM_NODES" ] 2>/dev/null; then
        echo "  !! not all nodes UN. Raw nodetool status:"
        echo "$st" | sed 's/^/     /'
        bad=1
    fi
    # Dropped check: skip header (NR>1), match 'MSG_TYPE  <count> ...' rows, flag >0.
    for node in "${BD_NODES[@]}"; do
        local dropped
        dropped="$(ssh ${SSH_USER}@10.10.1.$node "${CASS_DIR}/bin/nodetool tpstats 2>/dev/null | awk 'NR>1 && /^[A-Z_]+[[:space:]]+[0-9]+/ && \$2+0>0 {print \$1\"=\"\$2}'" 2>/dev/null)"
        if [ -n "$dropped" ]; then
            echo "  !! node ${node} has dropped messages: ${dropped}"; bad=1
        fi
    done
    if [ "$bad" = "1" ]; then
        echo ""
        echo "  ABORTING: cluster is degraded (see above). Fix the node(s) first --"
        echo "  a run on a dropping/down cluster produces corrupt counters and tails."
        echo "  (To bypass for a deliberate fault-injection run, comment out the preflight_health call.)"
        exit 1
    fi
    echo "  cluster healthy."
}

hard_restart_cluster "$CACHE_SIZE"
preflight_health
install_net_meter            # no-op unless MEASURE_NETWORK=1 and MEASURE_NET_PORTS=1

# ---- PHASE 1: 100% write ----
run_phase load "$RECORD_COUNT" insert \
    $YCSB_DIR load $DB -threads $WTHREADS \
        -p recordcount=${RECORD_COUNT} -p fieldlength=${FIELD_LENGTH} \
        -p measurement.raw.output_file="${OUT_DIR}/load/Load.scr" \
        -P commonworkload -s

# ---- PHASE 2: 100% read (uniform), same cluster/data -- only when requested ----
if [ "$PHASE_MODE" = "2" ]; then

# Optional cold-cache read: drop page cache on every node so the read phase is
# disk-bound instead of steady-state-under-cap. Default OFF (steady state).
if [ "$DROP_CACHES_BEFORE_READ" = "1" ]; then
    echo "--- Dropping page cache on all nodes (cold read) ---"
    for node in "${BD_NODES[@]}"; do
        ssh ${SSH_USER}@10.10.1.$node "sync; echo 3 | sudo tee /proc/sys/vm/drop_caches > /dev/null"
    done
fi

run_phase read "$MEASURE_OPS" read \
    $YCSB_DIR run $DB -threads $RTHREADS \
        -p recordcount=${RECORD_COUNT} -p operationcount=${MEASURE_OPS} \
        -p fieldlength=${FIELD_LENGTH} \
        -p readproportion=1 -p updateproportion=0 -p scanproportion=0 -p insertproportion=0 \
        -p requestdistribution=${READ_DIST} \
        -p measurement.raw.output_file="${OUT_DIR}/read/Read.scr" \
        -P commonworkload -s

else
    echo ""
    echo "--- PHASE_MODE=1: read phase skipped (load-only run) ---"
fi

echo ""
echo "############################################################"
echo "Done (PHASE_MODE=${PHASE_MODE}: $([ "$PHASE_MODE" = "1" ] && echo 'load only' || echo 'load + read')). ${OUT_DIR}/"
echo "  load/resource_summary.txt   WRITE phase: disk/cpu/net/footprint + write-amp"
if [ "$PHASE_MODE" = "2" ]; then
echo "  read/resource_summary.txt   READ  phase: disk/cpu/net + read B/op + faults"
fi
echo "  <phase>/Load.scr | Read.scr YCSB latency raw"
echo "  <phase>/{diskstats,memstat,cpustat,netstat,vmstat}_before/after  raw counters"
echo "  <phase>/{breakdown,tablestats,compaction_history,space,timings}.txt"
echo "  <phase>/run.log             YCSB stdout/stderr"
echo ""
if [ "$PHASE_MODE" = "1" ]; then
echo "  Load-only: verify the write-decomposition model against load/breakdown.txt"
echo "  and reconcile_compaction.py (commitlog/flush/compaction vs dm-0)."
else
echo "  Compare write-vs-read: diff load/resource_summary.txt read/resource_summary.txt"
fi
echo "############################################################"
