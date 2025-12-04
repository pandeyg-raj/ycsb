#!/usr/bin/env bash

ARG="$1"
[[ -z "$ARG" ]] && { echo "Usage: $0 <start<TAG>|stop>"; exit 1; }

STATE_DIR="/tmp/cache_monitor_state"
STATE_FILE="$STATE_DIR/state"
OUTPUT_FILE="$STATE_DIR/output"
BPF_PID_FILE="$STATE_DIR/bpf_pid"
LOGFILE="/var/log/cache_miss_log.csv"

mkdir -p "$STATE_DIR"

# Init log file if not exists
if [[ ! -f "$LOGFILE" ]]; then
    echo "timestamp,tag,pid,total_hits,total_misses,missrate" > "$LOGFILE"
fi

###################################
# Load state
###################################
load_state() {
    if [[ -f "$STATE_FILE" ]]; then
        source "$STATE_FILE"
    else
        MONITOR_RUNNING=0
        MONITOR_PID=0
        TARGET_PID=0
        TAG=""
    fi
}

save_state() {
    echo "MONITOR_RUNNING=$MONITOR_RUNNING" > "$STATE_FILE"
    echo "MONITOR_PID=$MONITOR_PID" >> "$STATE_FILE"
    echo "TARGET_PID=$TARGET_PID" >> "$STATE_FILE"
    echo "TAG=$TAG" >> "$STATE_FILE"
}

###################################
# Start monitor
###################################
start_monitor() {
    [[ $MONITOR_RUNNING -eq 1 ]] && { echo "Monitor already running!"; exit 1; }

    # Extract tag from argument: remove "start" prefix
    TAG="${ARG#start}"
    [[ -z "$TAG" ]] && { echo "Error: Tag cannot be empty"; exit 1; }

    # Detect Cassandra Java PID
    TARGET_PID=$(ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print $2}' | head -n1)
    [[ -z "$TARGET_PID" ]] && { echo "Error: Cassandra Java PID not found"; exit 1; }

    > "$OUTPUT_FILE"

    echo "Starting cache monitor for PID $TARGET_PID with tag $TAG"

    sudo bpftrace -e "
    kprobe:add_to_page_cache_lru /pid == $TARGET_PID/  { @misses = count(); }
    kprobe:mark_page_accessed    /pid == $TARGET_PID/  { @hits = count(); }

    interval:s:1 {
       printf(\"%d %d\n\", @hits, @misses);
    }
    " > "$OUTPUT_FILE" &
    MONITOR_PID=$!
    echo $MONITOR_PID > "$BPF_PID_FILE"

    MONITOR_RUNNING=1
    save_state

    echo "Monitor started. PID: $MONITOR_PID"
    echo "Use '$0 stop' to stop and save results."
}

###################################
# Stop monitor
###################################
stop_monitor() {
    [[ $MONITOR_RUNNING -eq 0 ]] && { echo "No monitor running!"; exit 1; }

    MONITOR_PID=$(cat "$BPF_PID_FILE")
    echo "Stopping monitor PID $MONITOR_PID ..."
    kill $MONITOR_PID 2>/dev/null
    sleep 1

    # Read final cumulative values
    last=$(tail -n 1 "$OUTPUT_FILE")
    hits=$(echo "$last" | awk '{print $1}')
    misses=$(echo "$last" | awk '{print $2}')

    if (( hits + misses == 0 )); then
        missrate="N/A"
    else
        missrate=$(echo "scale=6; $misses/($hits+$misses)" | bc)
    fi

    timestamp=$(date +%Y-%m-%dT%H:%M:%S)

    echo "$timestamp,$TAG,$TARGET_PID,$hits,$misses,$missrate" >> "$LOGFILE"

    echo "Logged:"
    echo "  timestamp: $timestamp"
    echo "  tag: $TAG"
    echo "  pid: $TARGET_PID"
    echo "  hits: $hits"
    echo "  misses: $misses"
    echo "  missrate: $missrate"

    MONITOR_RUNNING=0
    TARGET_PID=0
    MONITOR_PID=0
    TAG=""
    save_state
}

###################################
# Dispatch
###################################
load_state
case "$ARG" in
    start*) start_monitor ;;
    stop) stop_monitor ;;
    *) echo "Usage: $0 <start<TAG>|stop>" ; exit 1 ;;
esac
