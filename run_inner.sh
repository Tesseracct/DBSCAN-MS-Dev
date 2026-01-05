#!/bin/bash
module purge
module load openjdk/21.0.2

DATASET=$1; DIM=$2; EPS=$3; MINPTS=$4; NUM_PARTITIONS=$5; EXP_DIR=$6; NUM_PIVOT=$7; SAMPLING_DENSITY=$8
# Define JOBID early so SCRATCH_DIR expansion includes it
JOBID=$SLURM_JOB_ID
SCRATCH_DIR=${SCRATCH_DIR:-/scratch_shared/$USER/scratch-$JOBID}
mkdir -p "$SCRATCH_DIR"

# Configure Spark to use scratch directory for work/local storage
export SPARK_LOCAL_DIRS="$SCRATCH_DIR/spark-local"
export SPARK_WORKER_DIR="$SCRATCH_DIR/spark-worker"
mkdir -p "$SPARK_LOCAL_DIRS" "$SPARK_WORKER_DIR"

GLOBAL_RANK=${SLURM_PROCID:-0}
NUM_TASKS=${SLURM_NTASKS:-1}
MASTER_NODE_HOSTNAME=$(scontrol show hostnames "$SLURM_JOB_NODELIST" | head -n 1)
HOST=$(hostname)

echo "[INNER-INIT] Host=$HOST Rank=$GLOBAL_RANK Tasks=$NUM_TASKS MasterNode=$MASTER_NODE_HOSTNAME JobID=$JOBID"

BASE_PORT=$((20000 + (JOBID % 20000)))
SPARK_MASTER_PORT=$BASE_PORT
SPARK_UI_PORT=$((SPARK_MASTER_PORT + 2))
RC_PORT=$((SPARK_MASTER_PORT + 50))
export SPARK_MASTER_PORT SPARK_UI_PORT SPARK_MASTER_HOST="$MASTER_NODE_HOSTNAME" RC_PORT

echo "[CONFIG] Ports: master=$SPARK_MASTER_PORT ui=$SPARK_UI_PORT rc=$RC_PORT"

# Simplified static executor/driver sizing (overridable via environment)
# Environment matches: 4 Slurm nodes, each with 16 CPUs and 128GB memory
# We'll use a static layout but reserve some resources on the master node (rank 0)
# to ensure the master and the client driver have breathing room.
# NUM_EXECUTORS: number of executor instances (default 12 -> 3 per node)
# EXECUTOR_CORES: cores per executor (default 4)
# EXECUTOR_MEMORY: memory per executor (default 20g)
# DRIVER_CORES / DRIVER_MEMORY: driver resources (default 2 cores, 12g)
# EXECUTOR_MEMORY_OVERHEAD: off-heap overhead per executor (default 6144 MB = 6GB)
# SPARK_DEFAULT_PARALLELISM: if unset, computed as min(max(4, 2 * total_executor_cores), 500)
# Memory budget per node: 3 executors × (20GB heap + 6GB overhead) = 78GB, leaving ~50GB for OS/driver/master
NUM_EXECUTORS=${NUM_EXECUTORS:-12}
EXECUTOR_CORES=${EXECUTOR_CORES:-4}
EXECUTOR_MEMORY=${EXECUTOR_MEMORY:-20g}
EXECUTOR_MEMORY_OVERHEAD=${EXECUTOR_MEMORY_OVERHEAD:-6144}
DRIVER_CORES=${DRIVER_CORES:-2}
DRIVER_MEMORY=${DRIVER_MEMORY:-12g}

# Node hardware (static, matches your Slurm allocation)
NODE_CPUS=16
NODE_MEM_GB=128

# Default worker settings (non-master nodes)
SPARK_WORKER_CORES=${SPARK_WORKER_CORES:-$NODE_CPUS}
SPARK_WORKER_MEMORY=${SPARK_WORKER_MEMORY:-${NODE_MEM_GB}g}

# If this process is the master (GLOBAL_RANK==0) reduce worker resources
# to reserve capacity for the master process and the driver that will run here.
if [ "${GLOBAL_RANK:-0}" -eq 0 ]; then
  # Reserve driver cores + 1 spare for master/OS
  RESERVED_CORES=$(( DRIVER_CORES + 1 ))
  # Ensure positive result
  SPARK_WORKER_CORES=$(( NODE_CPUS - RESERVED_CORES ))
  if [ $SPARK_WORKER_CORES -lt 1 ]; then SPARK_WORKER_CORES=1; fi

  # Parse numeric part of DRIVER_MEMORY (strip non-digits)
  DRIVER_MEM_NUM=$(echo "$DRIVER_MEMORY" | sed 's/[^0-9]*//g')
  if [ -z "$DRIVER_MEM_NUM" ]; then DRIVER_MEM_NUM=0; fi
  # Reserve driver memory + ~4GB for master/OS overhead
  RESERVED_MEM_GB=$(( DRIVER_MEM_NUM + 4 ))
  WORKER_MEM_GB=$(( NODE_MEM_GB - RESERVED_MEM_GB ))
  if [ $WORKER_MEM_GB -lt 1 ]; then WORKER_MEM_GB=1; fi
  SPARK_WORKER_MEMORY="${WORKER_MEM_GB}g"
fi

# Keep executor-facing variables (used for spark-submit)
SPARK_EXECUTOR_CORES=${SPARK_EXECUTOR_CORES:-$EXECUTOR_CORES}
SPARK_EXECUTOR_MEMORY=${SPARK_EXECUTOR_MEMORY:-$EXECUTOR_MEMORY}
SPARK_EXECUTOR_INSTANCES=${SPARK_EXECUTOR_INSTANCES:-$NUM_EXECUTORS}

# Compute a sensible default for spark.default.parallelism when not provided
if [ -z "$SPARK_DEFAULT_PARALLELISM" ]; then
  TOTAL_CORES=$(( NUM_EXECUTORS * EXECUTOR_CORES ))
  PAR=$(( TOTAL_CORES * 2 ))
  if [ $PAR -lt 4 ]; then PAR=4; fi
  if [ $PAR -gt 500 ]; then PAR=500; fi
  SPARK_DEFAULT_PARALLELISM=$PAR
fi

wait_for_master() {
  local timeout=${1:-300} start end
  start=$(date +%s)
  while true; do
    (echo > /dev/tcp/$MASTER_NODE_HOSTNAME/$SPARK_MASTER_PORT) &>/dev/null && return 0
    end=$(date +%s); if [ $((end-start)) -ge $timeout ]; then echo "[WAIT] Timeout waiting for master $MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT"; return 1; fi
    sleep 1
  done
}

# --- Master startup (rank 0) ---
if [ "$GLOBAL_RANK" -eq 0 ]; then
  echo "[MASTER] Starting master host=$MASTER_NODE_HOSTNAME port=$SPARK_MASTER_PORT ui=$SPARK_UI_PORT"
  while (echo > /dev/tcp/127.0.0.1/$SPARK_MASTER_PORT) &>/dev/null; do SPARK_MASTER_PORT=$((SPARK_MASTER_PORT+1)); echo "[MASTER] Port in use bump -> $SPARK_MASTER_PORT"; done
  export SPARK_MASTER_PORT
  $SPARK_HOME/sbin/start-master.sh --host "$MASTER_NODE_HOSTNAME" --port "$SPARK_MASTER_PORT" --webui-port "$SPARK_UI_PORT"
  wait_for_master 120 || { echo "[MASTER] Failed to detect master port open"; exit 1; }
  echo "[MASTER] Master up spark://$MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT"
fi

# --- Worker startup (all ranks) ---
if [ "$GLOBAL_RANK" -ne 0 ]; then
  echo "[WORKER-$GLOBAL_RANK] Waiting for master spark://$MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT"
  wait_for_master 600 || { echo "[WORKER-$GLOBAL_RANK] Master not reachable"; exit 1; }
fi

echo "[WORKER-$GLOBAL_RANK] Starting worker cores=$SPARK_WORKER_CORES mem=$SPARK_WORKER_MEMORY workDir=$SPARK_WORKER_DIR"
$SPARK_HOME/sbin/start-worker.sh "spark://$MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT" \
  --cores "$SPARK_WORKER_CORES" \
  --memory "$SPARK_WORKER_MEMORY" \
  --work-dir "$SPARK_WORKER_DIR" \
  || { echo "[WORKER-$GLOBAL_RANK] Failed to start worker"; exit 1; }

SUBMIT_RC=0
if [ "$GLOBAL_RANK" -eq 0 ]; then
  echo "[SUBMIT] spark-submit (executorMemory=$SPARK_EXECUTOR_MEMORY executorCores=$SPARK_EXECUTOR_CORES parallelism=$SPARK_DEFAULT_PARALLELISM instances=${SPARK_EXECUTOR_INSTANCES:-1})"
  echo "[SUBMIT] Dataset=$DATASET Dim=$DIM Eps=$EPS MinPts=$MINPTS NumPartitions=$NUM_PARTITIONS ExpDir=$EXP_DIR NumPivot=$NUM_PIVOT SamplingDensity=$SAMPLING_DENSITY"

  # Capture spark-submit output to a logfile so we can scan for OOM signs and still show live output
  LOGFILE="${SCRATCH_DIR:-/tmp}/spark-submit.${JOBID:-$$}.${GLOBAL_RANK}.log"
  mkdir -p "$(dirname "$LOGFILE")"
  echo "[SUBMIT] Logging spark-submit output to $LOGFILE"

  # Run spark-submit, tee output to logfile. Use bash PIPESTATUS to capture the real exit code of spark-submit.
  $SPARK_HOME/bin/spark-submit \
    --master "spark://$MASTER_NODE_HOSTNAME:$SPARK_MASTER_PORT" \
    --deploy-mode client \
    --class app.Main \
    --num-executors "$NUM_EXECUTORS" \
    --executor-cores "$EXECUTOR_CORES" \
    --executor-memory "$EXECUTOR_MEMORY" \
    --driver-cores "$DRIVER_CORES" \
    --driver-memory "$DRIVER_MEMORY" \
    --conf spark.executor.memoryOverhead="$EXECUTOR_MEMORY_OVERHEAD" \
    --conf spark.driver.maxResultSize=12g \
    --conf spark.default.parallelism="$SPARK_DEFAULT_PARALLELISM" \
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
    --conf spark.kryoserializer.buffer.max=512m \
    --conf spark.local.dir="$SCRATCH_DIR/spark-local" \
    --conf spark.memory.fraction=0.6 \
    --conf spark.memory.storageFraction=0.3 \
    --conf spark.shuffle.compress=true \
    --conf spark.shuffle.spill.compress=true \
    --conf spark.rdd.compress=true \
    --conf spark.executor.extraJavaOptions="-XX:+UseG1GC -XX:G1HeapRegionSize=16m -XX:+ParallelRefProcEnabled -XX:InitiatingHeapOccupancyPercent=25 -XX:MaxGCPauseMillis=500" \
    --conf spark.driver.extraJavaOptions="-XX:+UseG1GC -XX:G1HeapRegionSize=16m -XX:InitiatingHeapOccupancyPercent=25" \
    /home/siepef/code/DBSCAN-MS/target/scala-2.13/dbscan_ms.jar \
    "$DATASET" "$EPS" "$MINPTS" "$NUM_PIVOT" "$NUM_PARTITIONS" "$SAMPLING_DENSITY" 42 "$EXP_DIR" false false false \
    2>&1 | tee "$LOGFILE"
  SUBMIT_RC=${PIPESTATUS[0]}

  # Scan logfile for common Java/Spark OOM patterns. If found, create a .oom marker in the experiment directory.
  # Patterns include Java OOM, GC overhead, Metaspace/PermGen errors and some container memory kill messages.
  if grep -E -i -q 'java\.lang\.OutOfMemoryError|OutOfMemoryError|GC overhead limit exceeded|Requested array size exceeds VM limit|PermGen|Metaspace|Container killed by YARN for exceeding memory|Killed process' "$LOGFILE"; then
    echo "[OOM] OutOfMemory detected in spark-submit output"
    OOMFILE="$EXP_DIR/.oom"
    touch "$OOMFILE"
    echo "[OOM] Created OOM marker file at $OOMFILE"
  fi
fi

exit $SUBMIT_RC
