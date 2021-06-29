# Source the environment file for global settings
. gen.env.sh

CMD_PARAMS="--master $MASTER \
    --jars $JARS \
    --driver-memory ${DRIVER_MEMORY}G \
    --executor-cores $NUM_EXECUTOR_CORES \
    --executor-memory ${EXECUTOR_MEMORY}G \
    --conf spark.cores.max=$TOTAL_CORES \
    --conf spark.task.cpus=1 \
    --conf spark.sql.shuffle.partitions=1024 \
    --conf spark.sql.files.maxPartitionBytes=2G \
    $S3PARAMS"

${SPARK_HOME}/bin/spark-submit \
$CMD_PARAMS \
--py-files=archive.zip \
generate.py \
--input-file=${INPUT_FILE} \
--output-prefix=${OUTPUT_PREFIX} \
--dup-times=${SCALE}  2>&1 | tee -a $LOGFILE
