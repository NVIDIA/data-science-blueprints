#!/bin/sh

export JARPATH=$(realpath $(dirname $0))

pipenv run spark-submit --driver-memory 16G --master="local[*]" ./generate.py
pipenv run spark-submit --driver-memory 16G --master="local[*]" --jars ${JARPATH}/cudf-*.jar,${JARPATH}/rapids-4-spark_2.12-*.jar --conf spark.plugins=com.nvidia.spark.SQLPlugin ./do-etl.py
