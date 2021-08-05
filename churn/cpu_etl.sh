# Source the environment file for global settings
. cpu.env.sh

CMD_PARAMS="--master $MASTER \
    --driver-memory ${DRIVER_MEMORY}G \
    --executor-cores $NUM_EXECUTOR_CORES \
    --executor-memory ${EXECUTOR_MEMORY}G \
    --conf spark.cores.max=$TOTAL_CORES \
    --conf spark.task.cpus=$NUM_EXECUTOR_CORES \
    --conf spark.sql.shuffle.partitions=512 \
    --conf spark.sql.files.maxPartitionBytes=4G \
    $S3PARAMS"

${SPARK_HOME}/bin/spark-submit \
$CMD_PARAMS \
--py-files=archive.zip \
do-analytics.py \
--input-prefix=$INPUT_PREFIX \
--output-prefix=$OUTPUT_PREFIX 2>&1 | tee -a $LOGFILE
