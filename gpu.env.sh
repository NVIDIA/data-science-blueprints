SPARK_HOME=/opt/spark/spark-3.0.2-bin-hadoop3.2
PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
SPARK_RAPIDS_DIR=/opt/sparkRapidsPlugin

SPARK_CUDF_JAR=${SPARK_RAPIDS_DIR}/cudf-21.06.0-20210608.055834-26-cuda11.jar
SPARK_RAPIDS_PLUGIN_JAR=${SPARK_RAPIDS_DIR}/rapids-4-spark_2.12-21.06.0-20210608.043519-22.jar

JARS=${SPARK_RAPIDS_PLUGIN_JAR},${SPARK_CUDF_JAR}

export PYTHONPATH=/usr/bin/python3:$PYTHONPATH
export PYSPARK_PYTHON=python3.6

# Create log files for each query that is run
LOG_SECOND=`date +%s`
LOGFILE="logs/$0.txt.$LOG_SECOND"
mkdir -p logs

# This is the IP address of the master node for your spark cluster
MASTER="spark://<SPARK_MASTER_IP>:7077"
HDFS_MASTER="<HDFS_MASTER_IP>"

# Set this value to 5 times the number of GPUs you have in the cluster.
# We have found that 5 CPU cores per GPU executor performs better than
# having a higher number of cores. e.g. If you have 8 GPUs in your cluster
# set this value to 40.
TOTAL_CORES=40
#
# Set this value to the number of GPUs that you have within your cluster. If
# each server has 2 GPUs count that as 2
NUM_EXECUTORS=8   # change to fit how many GPUs you have
#
NUM_EXECUTOR_CORES=$((${TOTAL_CORES}/${NUM_EXECUTORS}))
#
# This setting needs to be a decimal equivalent to the ratio of cores to
# executors. In our example we have 40 cores and 8 executors. So, this
# would be 1/5, hench the 0.1 value. 

RESOURCE_GPU_AMT="0.2"

# Set this to the total memory across all your worker nodes. e.g. 8 server
# with 96GB of ram = 768
TOTAL_MEMORY=4000   # unit: GB 
DRIVER_MEMORY=50    # unit: GB
#
# This takes the total memory and calculates the maximum amount of memory
# per executor
EXECUTOR_MEMORY=$(($((${TOTAL_MEMORY}-$((${DRIVER_MEMORY}*1000/1024))))/${NUM_EXECUTORS}))

# If you are going to use storage that supports S3, set your credential
# here for use during the run
# NOTE: You will need to download additonal jar files and place them in
# $SPARK_HOME/jars. The following link has instructions
# https://github.com/NVIDIA/spark-xgboost-examples/blob/spark-3/getting-started-guides/csp/aws/ec2.md#step-3-download-jars-for-s3a-optional
#
#S3A_CREDS_USR=<S3_USERNAME>
#S3A_CREDS_PSW=<S3_PASS>
#S3_ENDPOINT="https://<S3_URL>
#
# These paths need to be set based on what storage mediume you are using
#
# For local disk use file:/// - Note that every node in your cluster must have 
# a copy of the local data on it. You can use shared storage as well, but the
# path must be consistent on all nodes
#
# For S3 storageuse s3a://
#
# **** NOTE TRAILING SLASH IS REQUIRED FOR ALL PREFIXES
#
# **** NOTE TRAILING SLASH IS REQUIRED FOR ALL PREFIXES
#
# Input prefix designates where the data to be processed is located
# INPUT_PREFIX="s3a://data/churn-benchmark/10k/"
INPUT_PREFIX="hdfs://$HDFS_MASTER:9000/data/churn-benchmark/10k/"
# INPUT_PREFIX="file:///data/churn-benchmark/10k/"
#
# Output prefix is where results from the queries are stored
# OUTPUT_PREFIX="s3a://data/output/churn/10k/cpu/"
OUTPUT_PREFIX="hdfs://$HDFS_MASTER:9000/data/output/churn/10k/cpu/"
# OUTPUT_PREFIX="file:///data/output/churn/10k/cpu/"
#

S3PARAMS="--conf spark.hadoop.fs.s3a.access.key=$S3A_CREDS_USR \
  --conf spark.hadoop.fs.s3a.secret.key=$S3A_CREDS_PSW \
  --conf spark.hadoop.fs.s3a.impl=org.apache.hadoop.fs.s3a.S3AFileSystem \
  --conf spark.hadoop.fs.s3a.endpoint=$S3_ENDPOINT \
  --conf spark.hadoop.fs.s3a.path.style.access=true \
  --conf spark.hadoop.fs.s3a.experimental.input.fadvise=sequential \
  --conf spark.hadoop.fs.s3a.connection.maximum=1000\
  --conf spark.hadoop.fs.s3a.threads.core=1000\
  --conf spark.hadoop.parquet.enable.summary-metadata=false \
  --conf spark.sql.parquet.mergeSchema=false \
  --conf spark.sql.parquet.filterPushdown=true \
  --conf spark.sql.hive.metastorePartitionPruning=true \
  --conf spark.hadoop.fs.s3a.connection.ssl.enabled=true"

