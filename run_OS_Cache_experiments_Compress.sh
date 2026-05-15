#!/bin/bash
# === Config ===
YCSB_DIR=bin/ycsb.sh
DB=cassandra-cql
MEASURE_OPS=5000000
WARMUP_OPS=5000000
REPEAT=5
FIELD_LENGTH=10000            # was 1000; pool cells are 10240 bytes
RECORD_COUNT=10000000         # was 100000000; 10M * 10KB == 100GB target

# Workloads definition
WORKLOAD_LABELS=("read100" "read95" "read50")
READ_PROPORTIONS=("readproportion=0.9 -p insertproportion=0.1" \
                  "readproportion=0.5 -p insertproportion=0.5")

# OS cache sizes (in GB)
CACHE_SIZES=("16GB" "28GB" "40GB" "52GB" "64GB")

# Compression datasets (outermost sweep dimension)
POOL_DIR=/mydata/compressData
COMPRESS_LABELS=("jpeg" "wiki" "hdfs")
POOL_FILES=("values_pool_jpeg.txt" "values_pool_wiki.txt" "values_pool_hdfs.txt")

# === Experiment setup ===
echo "Is this EC or REP?"
read EXP_LABEL
echo "How many write threads?"
read WTHREADS
echo "How many read threads?"
read THREADS

# === Outermost loop: compression datasets ===
for compress_idx in "${!COMPRESS_LABELS[@]}"; do
  COMPRESS_LABEL="${COMPRESS_LABELS[$compress_idx]}"
  POOL_FILE="${POOL_DIR}/${POOL_FILES[$compress_idx]}"

  echo
  echo "============================================================"
  echo ">>> Compression dataset: ${COMPRESS_LABEL}"
  echo ">>> Pool file:          ${POOL_FILE}"
  echo "============================================================"
  read -p "Wipe Cassandra data + restart in full memory, CREATE TABLE according to LABEL, then press Enter to continue..."

  BASE_OUT_DIR="result_OS_CacheCompress_${COMPRESS_LABEL}"
  mkdir -p "$BASE_OUT_DIR"

  # === Load phase (once per compression dataset) ===
  LOAD_FILE="${BASE_OUT_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_Load${FIELD_LENGTH}Bytes_run.scr"
  echo "Load phase: Loading $RECORD_COUNT records of ${FIELD_LENGTH} bytes from ${COMPRESS_LABEL} pool..."
  $YCSB_DIR load $DB -threads $WTHREADS \
    -p recordcount=${RECORD_COUNT} \
    -p fieldlength=${FIELD_LENGTH} \
    -p valuepool.file=${POOL_FILE} \
    -p measurement.raw.output_file="$LOAD_FILE" \
    -P commonworkload \
    -s >> "${BASE_OUT_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_run${FIELD_LENGTH}Bytes.log" 2>&1
  echo "Load phase done: ${LOAD_FILE}"

  # === Main experiment ===
  for cache_size in "${CACHE_SIZES[@]}"; do
    echo
    echo ">>> Prepare Cassandra with ${cache_size} OS cache (${COMPRESS_LABEL})"

    # review start
    
    # here stop cassandra gracefully on all nodes, one by one and re start cassandra on all nodes with ${cache_size}
    # --- Restart Cassandra cluster with target memory/cache size ---
    echo "Stopping Cassandra on all nodes..."
    
    for node in {2..6}; do
      ssh rzp5412@10.10.1.$node "ps -ef | grep '[j]ava' | grep -i 'cassandra' | awk '{print \$2}' | xargs kill"
      # Wait until java process disappears
        while ssh rzp5412@10.10.1.$node "ps -a | grep java > /dev/null"; do
          sleep 5
        done
        echo "Cassandra stopped on 10.10.1.$node"
        
         # Start Cassandra with target memory limit
                 
        ssh rzp5412@10.10.1.$node "
          #strip GB from cache size
          CACHE_GB=$(echo ${cache_size} | sed 's/GB//')
          MEM_BYTES=\$((CACHE_GB * 1024 * 1024 * 1024))
        
          # set memory limit
          echo \$MEM_BYTES | sudo tee /sys/fs/cgroup/mylimitedgroup/memory.max
        
          # attach THIS shell to cgroup (important)
          echo \$\$ | sudo tee /sys/fs/cgroup/mylimitedgroup/cgroup.procs
        
          # clean OS cache
          vmtouch -e /mydata/cassandra/data/
        
          # start cassandra (inherits cgroup automatically)
          nohup /mydata/cassandra/bin/cassandra > cassandra.log 2>&1 &
        "

        # Wait until nodetool works
        echo "Waiting for Cassandra startup on 10.10.1.$node ..."
        until ssh rzp5412@10.10.1.$node "/mydata/cassandra/bin/nodetool status | grep -q 'UN'"; do
          sleep 5
        done
      
        echo "Cassandra is up on 10.10.1.$node"
    done

    # review end
  
    #read -p "Start Cassandra with ${cache_size} and press Enter to continue..."
    

    # --- Warm-up phase (once per cache size) ---
    WARMUP_FILE="${BASE_OUT_DIR}_${cache_size}/${EXP_LABEL}_${COMPRESS_LABEL}_${cache_size}_Warmup${FIELD_LENGTH}Bytes_run.scr"
    mkdir -p "$(dirname "$WARMUP_FILE")"
    echo "--- Warm-up phase (${cache_size}) ---"
    $YCSB_DIR run $DB -threads $THREADS \
      -p operationcount=$WARMUP_OPS \
      -p ${READ_PROPORTIONS[1]} \
      -p recordcount=${RECORD_COUNT} \
      -p fieldlength=${FIELD_LENGTH} \
      -p valuepool.file=${POOL_FILE} \
      -p measurement.raw.output_file="$WARMUP_FILE" \
      -p cassandra.writeconsistencylevel=QUORUM \
      -p cassandra.readconsistencylevel=QUORUM \
      -P commonworkload \
      -s >> "${BASE_OUT_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_run${FIELD_LENGTH}Bytes.log" 2>&1

    # --- Measurement runs for all workloads ---
    for i in "${!WORKLOAD_LABELS[@]}"; do
      workload="${WORKLOAD_LABELS[$i]}"
      READ_PCT="${READ_PROPORTIONS[$i]}"
      read_ratio=$(echo "$workload" | grep -o '[0-9]*')
      echo "========================================"
      echo "Starting workload: ${workload} (cache ${cache_size}, ${COMPRESS_LABEL})"
      echo "========================================"
      for iter in $(seq 1 $REPEAT); do
        MEASURE_FILE="${BASE_OUT_DIR}_${cache_size}/${EXP_LABEL}_${COMPRESS_LABEL}_${cache_size}_iter_${iter}Run${FIELD_LENGTH}Bytes_read${read_ratio}run.scr"
        echo "--- Measurement run ${iter} (${cache_size}, ${workload}) ---"
        $YCSB_DIR run $DB -threads $THREADS \
          -p operationcount=$MEASURE_OPS \
          -p ${READ_PCT} \
          -p recordcount=${RECORD_COUNT} \
          -p measurement.raw.output_file="$MEASURE_FILE" \
          -p cassandra.writeconsistencylevel=QUORUM \
          -p cassandra.readconsistencylevel=QUORUM \
          -P commonworkload \
          -s >> "${BASE_OUT_DIR}/${EXP_LABEL}_${COMPRESS_LABEL}_run${FIELD_LENGTH}Bytes.log" 2>&1
      done
    done
  done

  echo ">>> Completed all runs for ${COMPRESS_LABEL}"
done

echo "All experiments completed successfully."
