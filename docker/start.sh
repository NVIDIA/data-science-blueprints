#!/bin/bash

set -x

export JARPATH=$(realpath $(dirname $0))

if [ x$"LIGHTRUN" != "x" ]; then
    DUP_TIMES="--dup-times 5000"
else
    DUP_TIMES="--dup-times 100"
fi

mkdir tmp
export SPARK_TEMP=$(pwd)/tmp

pipenv run spark-submit --driver-memory 128G --master="local[*]" --conf spark.sql.analyzer.failAmbiguousSelfJoin=false --conf spark.local.dir=${SPARK_TEMP} ./generate.py ${DUP_TIMES} 
pipenv run spark-submit --driver-memory 128G --master="local[*]" --jars ${JARPATH}/cudf-*.jar,${JARPATH}/rapids-4-spark_2.12-*.jar --conf spark.plugins=com.nvidia.spark.SQLPlugin --conf spark.sql.adaptive.enabled=true --conf spark.rapids.sql.batchSizeBytes=1073741824 --conf spark.rapids.sql.concurrentGpuTasks=4 --conf spark.default.parallelism=256 --conf spark.rapids.sql.explain=NOT_ON_GPU --conf spark.rapids.sql.decimalType.enabled=true --conf spark.rapids.sql.variableFloatAgg.enabled=true --conf spark.local.dir=${SPARK_TEMP} --conf spark.eventLog.enabled=True ./do-analytics.py --log-level INFO
