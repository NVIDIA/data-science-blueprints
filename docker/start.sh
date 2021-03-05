#!/bin/sh

pipenv run spark-submit --driver-memory 16G --master="local[*]" ./generate.py
pipenv run spark-submit --driver-memory 16G --master="local[*]" --packages com.nvidia:rapids-4-spark_2.12:0.3.0,ai.rapids:cudf:0.17 --conf spark.plugins=com.nvidia.spark.SQLPlugin ./do-etl.py