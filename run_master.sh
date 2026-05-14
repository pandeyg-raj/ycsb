#!/bin/bash
# Master script — runs all experiments for one configuration label.
# Calls run_single.sh for each (dataset × cache_size × workload) combination.
# Place this file in the SAME directory as run_single.sh.
#
# Usage:
#   ./run_master.sh EC_ComprOn
#   ./run_master.sh REP_ComprOff
#
# All echo output from run_single.sh appears on your terminal AND is
# saved to master_<label>.log in the current directory.

LABEL=$1

if [ -z "$LABEL" ]; then
  echo "Usage: $0 <label>"
  echo "  label: EC_ComprOn | EC_ComprOff | REP_ComprOn | REP_ComprOff"
  exit 1
fi

# Directory of this script — run_single.sh must be here too
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SINGLE="${SCRIPT_DIR}/run_single.sh"

if [ ! -x "$SINGLE" ]; then
  echo "ERROR: run_single.sh not found or not executable at $SINGLE"
  exit 1
fi

# =====================================================================
# Ask for threads ONCE — passed to every run_single.sh call
# =====================================================================
echo "============================================================"
echo " Master script: $LABEL"
echo "============================================================"
echo "How many write threads?"
read WTHREADS
echo "How many read threads?"
read RTHREADS

# =====================================================================
# Experiment dimensions
# =====================================================================
DATASETS=("jpeg" "wiki" "hdfs")
CACHE_SIZES=("16GB" "28GB" "40GB" "52GB" "64GB")
WORKLOADS=("read90" "read50")

TOTAL=$(( ${#DATASETS[@]} * ${#CACHE_SIZES[@]} * ${#WORKLOADS[@]} ))
COUNT=0

# Master log — all echo from this script AND run_single.sh goes here too
MASTER_LOG="${SCRIPT_DIR}/master_${LABEL}.log"

echo "Total experiments: $TOTAL"
echo "Master log: $MASTER_LOG"
echo ""

# =====================================================================
# Main loop
# =====================================================================
for DATASET in "${DATASETS[@]}"; do
  for CACHE_SIZE in "${CACHE_SIZES[@]}"; do
    for WORKLOAD in "${WORKLOADS[@]}"; do
      COUNT=$(( COUNT + 1 ))

      echo ""
      echo "############################################################"
      echo "# Experiment $COUNT / $TOTAL"
      echo "#   Label     : $LABEL"
      echo "#   Dataset   : $DATASET"
      echo "#   Cache     : $CACHE_SIZE"
      echo "#   Workload  : $WORKLOAD"
      echo "############################################################"

      # Pause here — user wipes cluster and sets cache before each run
      read -p "Wipe Cassandra + restart with $CACHE_SIZE cache, then press Enter..."

      # Call run_single.sh — passes threads, no interactive prompts needed
      # tee appends output to master log while still showing on terminal
      "$SINGLE" "$DATASET" "$CACHE_SIZE" "$WORKLOAD" "$LABEL" \
        "$WTHREADS" "$RTHREADS" 2>&1 | tee -a "$MASTER_LOG"

      echo ""
      echo "# Done $COUNT/$TOTAL: $LABEL | $DATASET | $CACHE_SIZE | $WORKLOAD"
      echo "############################################################"
      echo ""

    done
  done
done

echo "============================================================"
echo " All $TOTAL experiments completed for label: $LABEL"
echo " Master log: $MASTER_LOG"
echo "============================================================"
