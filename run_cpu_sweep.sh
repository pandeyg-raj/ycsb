#!/usr/bin/env bash
# =============================================================================
# run_cpu_sweep.sh -- drive run_breakdown_load.sh across the CPU-cap sweep.
#
# Runs the EC-vs-REP comparison at three cgroup CPU caps (3, 2, 1 logical cores
# per node) to expose EC's decode/encode cost. Each run does a full hard reset
# (wipe data+logs, restart clean, recreate schema) so every collection is clean,
# applies the cap for BOTH load and read, and lands in a cap-tagged result dir.
#
# 6 runs total: {cpu3, cpu2, cpu1} x {ec, rep}. After the sweep, the CPU cap is
# cleared on every node so future runs get full CPU.
#
# Usage:
#   bash run_cpu_sweep.sh
# Override the defaults via env, e.g.:
#   CACHE_GB=32 WTHREADS=64 RTHREADS=64 CAPS="3 2 1" SYSTEMS="ec rep" bash run_cpu_sweep.sh
# =============================================================================
set -u

SSH_USER="${SSH_USER:-rzp5412}"
BD_NODES=(2 3 4 5 6)
CGROUP="${CGROUP:-/sys/fs/cgroup/mylimitedgroup}"
HARNESS="${HARNESS:-./run_breakdown_load.sh}"

# --- sweep parameters (all overridable via env) ---
CACHE_GB="${CACHE_GB:-32}"          # memory cap (GB), held constant across the sweep
WTHREADS="${WTHREADS:-64}"          # load threads
RTHREADS="${RTHREADS:-64}"          # read threads
PHASE_MODE="${PHASE_MODE:-2}"       # 2 = load+read (the point of this experiment)
MEASURE_NETWORK="${MEASURE_NETWORK:-0}"
CAPS="${CAPS:-3 2 1}"               # cores/node to sweep
SYSTEMS="${SYSTEMS:-ec rep}"        # systems to compare
PERIOD=100000                       # cpu.max period (us); quota = cores*period

echo "############################################################"
echo "# CPU-cap sweep"
echo "#   caps (cores/node): ${CAPS}"
echo "#   systems          : ${SYSTEMS}"
echo "#   mem cap           : ${CACHE_GB}GB   load thr=${WTHREADS} read thr=${RTHREADS}"
echo "#   16 logical cores/node (8 physical x2 HT) -- 'N cores' = N logical"
echo "############################################################"

clear_cpu_cap() {
    echo ">>> clearing CPU cap on all nodes (restoring full CPU)..."
    for n in "${BD_NODES[@]}"; do
        ssh -n ${SSH_USER}@10.10.1.$n "echo max | sudo tee ${CGROUP}/cpu.max > /dev/null" 2>/dev/null
        echo -n "  node$n cpu.max now: "; ssh -n ${SSH_USER}@10.10.1.$n "cat ${CGROUP}/cpu.max 2>/dev/null"
    done
}
# always clear the cap on exit, even on error/ctrl-c, so we never leave the
# cluster throttled for the next experiment.
trap clear_cpu_cap EXIT

run_one() {
    local cores=$1 sys=$2 quota cpumax
    quota=$((cores * PERIOD))
    cpumax="${quota} ${PERIOD}"
    echo ""
    echo "==================================================================="
    echo ">>> RUN: system=${sys}  cap=${cores} core(s)/node  cpu.max='${cpumax}'"
    echo "==================================================================="
    # Drive the harness fully non-interactively via env overrides.
    EXP_LABEL="$sys" \
    CACHE_GB="$CACHE_GB" \
    WTHREADS="$WTHREADS" \
    RTHREADS="$RTHREADS" \
    PHASE_MODE="$PHASE_MODE" \
    MEASURE_NETWORK="$MEASURE_NETWORK" \
    CPU_MAX="$cpumax" \
    bash "$HARNESS"
    echo ">>> done: system=${sys} cap=${cores}"
}

for cores in $CAPS; do
    for sys in $SYSTEMS; do
        run_one "$cores" "$sys"
    done
done

echo ""
echo "############################################################"
echo "# sweep complete. Result dirs (cap-tagged):"
ls -d result_breakdown_*_cpu* 2>/dev/null | sed 's/^/#   /'
echo "#"
echo "# compare throttling + latency across caps, e.g.:"
echo "#   for d in result_breakdown_*_cpu*; do echo \"== \$d ==\"; grep -A2 'CPU THROTTLING' \$d/*/resource_summary.txt; done"
echo "############################################################"
# trap will clear the cap now
