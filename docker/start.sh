#!/bin/sh

pipenv run spark-submit --driver-memory 16G --master="local[*]" ./generate.py
pipenv run spark-submit --driver-memory 16G --master="local[*]" --jars cudf-0.17-cuda11.jar,rapids-4-spark_2.12-0.3.0.jar --conf spark.plugins=com.nvidia.spark.SQLPlugin ./do-etl.py