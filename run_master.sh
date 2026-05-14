#!/bin/bash
# Master script — runs 15 experiments for one (label × workload) combination.
# Place in the SAME directory as run_single.sh.
#
# Usage:
#   ./run_master.sh <label> <workload>
#
# Examples:
#   ./run_master.sh EC_ComprOn  read90
#   ./run_master.sh EC_ComprOn  read50
#   ./run_master.sh EC_ComprOff read90
#   ./run_master.sh EC_ComprOff read50
#   ./run_master.sh REP_ComprOn read90
#   ./run_master.sh REP_ComprOn read50
#   (etc.)
#
# Each run = 3 datasets × 5 cache sizes = 15 experiments
#
# Echo output:  shown on terminal AND saved to master_<label>_<workload>.log
# YCSB results: saved in result_<label>_<dataset>_<cache>_<workload>/ per experiment

LABEL=$1
WORKLOAD=$2

if [ -z "$LABEL" ] || [ -z "$WORKLOAD" ]; then
  echo "Usage: $0 <label> <workload>"
  echo "  label   : EC_ComprOn | EC_ComprOff | REP_ComprOn | REP_ComprOff"
  echo "  workload: read90 | read50"
  exit 1
fi

if [ "$WORKLOAD" != "read90" ] && [ "$WORKLOAD" != "read50" ]; then
  echo "ERROR: Unknown workload '$WORKLOAD'. Use: read90 | read50"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SINGLE="${SCRIPT_DIR}/run_single.sh"
MASTER_LOG="${SCRIPT_DIR}/master_${LABEL}_${WORKLOAD}.log"

if [ ! -x "$SINGLE" ]; then
  echo "ERROR: run_single.sh not found or not executable at $SINGLE"
  exit 1
fi

# =====================================================================
# Experiment dimensions (workload fixed, not looped)
# =====================================================================
DATASETS=("jpeg" "wiki" "hdfs")
CACHE_SIZES=("16GB" "28GB" "40GB" "52GB" "64GB")

TOTAL=$(( ${#DATASETS[@]} * ${#CACHE_SIZES[@]} ))   # 3 x 5 = 15
COUNT=0

echo "============================================================" | tee -a "$MASTER_LOG"
echo " Master script started"                                       | tee -a "$MASTER_LOG"
echo " Label      : $LABEL"                                         | tee -a "$MASTER_LOG"
echo " Workload   : $WORKLOAD"                                      | tee -a "$MASTER_LOG"
echo " Total runs : $TOTAL  (3 datasets x 5 cache sizes)"           | tee -a "$MASTER_LOG"
echo " Master log : $MASTER_LOG"                                    | tee -a "$MASTER_LOG"
echo "============================================================" | tee -a "$MASTER_LOG"

# =====================================================================
# Main loop
# =====================================================================
for DATASET in "${DATASETS[@]}"; do
  for CACHE_SIZE in "${CACHE_SIZES[@]}"; do
    COUNT=$(( COUNT + 1 ))

    echo "" | tee -a "$MASTER_LOG"
    echo "############################################################" | tee -a "$MASTER_LOG"
    echo "# Experiment $COUNT / $TOTAL"                               | tee -a "$MASTER_LOG"
    echo "#   Label    : $LABEL"                                       | tee -a "$MASTER_LOG"
    echo "#   Workload : $WORKLOAD"                                    | tee -a "$MASTER_LOG"
    echo "#   Dataset  : $DATASET"                                     | tee -a "$MASTER_LOG"
    echo "#   Cache    : $CACHE_SIZE"                                  | tee -a "$MASTER_LOG"
    echo "############################################################" | tee -a "$MASTER_LOG"

    # --- 3x confirmation before each experiment ---
    read -p "[1/3] DO NOW: (1) wipe Cassandra data  (2) set OS cache to ${CACHE_SIZE}  (3) create table based on label (4) start Cassandra. Press Enter when Cassandra is starting..."
    read -p "[2/3] nodetool status all nodes UN? Press Enter to confirm..."
    read -p "[3/3] Ready to run: $LABEL | $DATASET | ${CACHE_SIZE} | $WORKLOAD. Press Enter to START experiment $COUNT/$TOTAL..."

    echo "$(date): Starting $LABEL | $WORKLOAD | $DATASET | $CACHE_SIZE" | tee -a "$MASTER_LOG"

    # --- Call run_single.sh ---
    "$SINGLE" "$DATASET" "$CACHE_SIZE" "$WORKLOAD" "$LABEL" 2>&1 | tee -a "$MASTER_LOG"

    echo "$(date): Finished $LABEL | $WORKLOAD | $DATASET | $CACHE_SIZE" | tee -a "$MASTER_LOG"
    echo "############################################################"  | tee -a "$MASTER_LOG"

  done
done

echo "" | tee -a "$MASTER_LOG"
echo "============================================================" | tee -a "$MASTER_LOG"
echo " ALL $TOTAL EXPERIMENTS DONE"                                | tee -a "$MASTER_LOG"
echo " Label    : $LABEL"                                          | tee -a "$MASTER_LOG"
echo " Workload : $WORKLOAD"                                       | tee -a "$MASTER_LOG"
echo " Master log: $MASTER_LOG"                                    | tee -a "$MASTER_LOG"
echo "============================================================" | tee -a "$MASTER_LOG"
