#!/bin/bash
# run_with_monitoring.sh
#
# Starts metric collectors on all Cassandra nodes, runs the benchmark,
# stops collectors, and downloads all CSVs to the client.
#
# Usage:
#   bash run_with_monitoring.sh <mode> <output_prefix>
#
#   mode          : "least" or "default"
#   output_prefix : e.g. "c46_least" → files: c46_least_client.csv,
#                   c46_least_node0.csv, etc.
#
# Example:
#   bash run_with_monitoring.sh least  results/c46_least
#   bash run_with_monitoring.sh default results/c46_default

set -e

MODE=${1:-least}
PREFIX=${2:-results/run}

# ── Config — edit these ───────────────────────────────────────────────────────

# All 5 node IPs and their labels
NODES=(
    "10.10.1.2 node1"
    "10.10.1.3 node2"
    "10.10.1.4 node3"
    "10.10.1.5 node4"
    "10.10.1.6 node5"
)

CASS_HOST="10.10.1.2"          # coordinator node
TRACE_FILE="cluster46.filtered"
CONSISTENCY="QUORUM"
THREADS=64
DURATION=3600
REMOTE_DIR="/mydata"
SSH_USER="rzp5412"

# LEAST flags
if [ "$MODE" = "least" ]; then
    LEAST_FLAG="--least"
else
    LEAST_FLAG=""
fi

# ── Setup ─────────────────────────────────────────────────────────────────────

mkdir -p "$(dirname "$PREFIX")"
CLIENT_CSV="${PREFIX}_client.csv"

echo "════════════════════════════════════════════════"
echo "Mode     : $MODE"
echo "Trace    : $TRACE_FILE"
echo "Host     : $CASS_HOST"
echo "Prefix   : $PREFIX"
echo "════════════════════════════════════════════════"

# ── Start collectors on all nodes ────────────────────────────────────────────

echo ""
echo "Starting metric collectors on all nodes..."
for entry in "${NODES[@]}"; do
    IP=$(echo $entry | cut -d' ' -f1)
    LABEL=$(echo $entry | cut -d' ' -f2)
    REMOTE_CSV="${REMOTE_DIR}/metrics_${LABEL}.csv"

    ssh ${SSH_USER}@${IP} \
        "nohup python3 ${REMOTE_DIR}/collect_metrics.py \
            --node ${LABEL} \
            --output ${REMOTE_CSV} \
            > ${REMOTE_DIR}/collector_${LABEL}.log 2>&1 &
         echo \$! > ${REMOTE_DIR}/collector_${LABEL}.pid
         echo 'Started collector on ${LABEL} (PID '\$(cat ${REMOTE_DIR}/collector_${LABEL}.pid)')'
        " &
done
wait   # wait for all SSH commands to complete

echo "Collectors started. Sleeping 3s to let them initialise..."
sleep 3

# ── Run benchmark ─────────────────────────────────────────────────────────────

echo ""
echo "Starting benchmark (mode=$MODE)..."
echo "Client CSV → $CLIENT_CSV"
echo ""

./trace_driver \
    --trace       "$TRACE_FILE" \
    --host        "$CASS_HOST"  \
    $LEAST_FLAG                 \
    --disable-ttl               \
    --consistency "$CONSISTENCY" \
    --speed       1.0           \
    --duration    "$DURATION"   \
    --threads     "$THREADS"    \
    --output      "$CLIENT_CSV"

echo ""
echo "Benchmark complete."

# ── Stop collectors ───────────────────────────────────────────────────────────

echo ""
echo "Stopping metric collectors..."
for entry in "${NODES[@]}"; do
    IP=$(echo $entry | cut -d' ' -f1)
    LABEL=$(echo $entry | cut -d' ' -f2)

    ssh ${SSH_USER}@${IP} \
        "PID=\$(cat ${REMOTE_DIR}/collector_${LABEL}.pid 2>/dev/null);
         if [ -n \"\$PID\" ]; then
             kill \$PID 2>/dev/null && echo 'Stopped collector on ${LABEL} (PID '\$PID')'
             rm -f ${REMOTE_DIR}/collector_${LABEL}.pid
         fi" &
done
wait
sleep 2   # give collectors time to flush and close files

# ── Download metrics ──────────────────────────────────────────────────────────

echo ""
echo "Downloading metrics CSVs..."
NODE_CSVS=""
for entry in "${NODES[@]}"; do
    IP=$(echo $entry | cut -d' ' -f1)
    LABEL=$(echo $entry | cut -d' ' -f2)
    REMOTE_CSV="${REMOTE_DIR}/metrics_${LABEL}.csv"
    LOCAL_CSV="${PREFIX}_${LABEL}.csv"

    scp ${SSH_USER}@${IP}:${REMOTE_CSV} "${LOCAL_CSV}"
    echo "  Downloaded: $LOCAL_CSV"
    NODE_CSVS="$NODE_CSVS $LOCAL_CSV"
done

# ── Generate plot ─────────────────────────────────────────────────────────────

PLOT_FILE="${PREFIX}_plot.png"
echo ""
echo "Generating plot → $PLOT_FILE"

python3 plot_benchmark.py \
    --client  "$CLIENT_CSV" \
    --nodes   $NODE_CSVS \
    --title   "LEAST Benchmark — $MODE — $TRACE_FILE" \
    --output  "$PLOT_FILE"

echo ""
echo "════════════════════════════════════════════════"
echo "Done!"
echo "  Client CSV : $CLIENT_CSV"
echo "  Node CSVs  : ${PREFIX}_node*.csv"
echo "  Plot       : $PLOT_FILE"
echo "════════════════════════════════════════════════"
