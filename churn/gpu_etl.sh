# Copyright (c) 2020–2021, NVIDIA Corporation.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Source the environment file for global settings
. gpu.env.sh

CMD_PARAMS="--master $MASTER \
    --driver-memory ${DRIVER_MEMORY}G \
    --executor-cores $NUM_EXECUTOR_CORES \
    --executor-memory ${EXECUTOR_MEMORY}G \
    --conf spark.cores.max=$TOTAL_CORES \
    --conf spark.task.cpus=1 \
    --conf spark.task.resource.gpu.amount=$RESOURCE_GPU_AMT \
    --conf spark.rapids.sql.enabled=True \
    --conf spark.plugins=com.nvidia.spark.SQLPlugin \
    --conf spark.rapids.memory.pinnedPool.size=2G \
    --conf spark.sql.shuffle.partitions=1024 \
    --conf spark.sql.files.maxPartitionBytes=1G \
    --conf spark.rapids.sql.concurrentGpuTasks=8 \
    --conf spark.executor.resource.gpu.amount=1 \
    --conf spark.sql.adaptive.enabled=True \
    --conf spark.rapids.sql.variableFloatAgg.enabled=True \
    --conf spark.rapids.sql.explain=NOT_ON_GPU \
    --conf spark.rapids.sql.decimalType.enabled=True \
    --conf spark.executor.extraClassPath=${SPARK_CUDF_JAR}:${SPARK_RAPIDS_PLUGIN_JAR} \
    --conf spark.driver.extraClassPath=${SPARK_CUDF_JAR}:${SPARK_RAPIDS_PLUGIN_JAR} \
    --jars $JARS \
    $S3PARAMS"

${SPARK_HOME}/bin/spark-submit \
$CMD_PARAMS \
  --py-files=archive.zip \
  do-analytics.py \
  --coalesce-output=8 \
  --input-prefix=$INPUT_PREFIX \
  --output-prefix=$OUTPUT_PREFIX 2>&1 | tee -a $LOGFILE
    

#--conf spark.executorEnv.PYSPARK_PYTHON=/home/rladmin/miniconda3/envs/churn-benchmark/bin/python \
    #--conf spark.shuffle.manager=com.nvidia.spark.rapids.spark302.RapidsShuffleManager \
    #--conf spark.executorEnv.UCX_RNDV_SCHEME=put_zcopy \
    #--conf spark.executorEnv.UCX_ERROR_SIGNALS= \
    #--conf spark.executorEnv.UCX_IB_GPU_DIRECT_RDMA=yes \
    #--conf spark.executorEnv.UCX_IB_RX_QUEUE_LEN=1024 \
    #--conf spark.rapids.shuffle.ucx.bounceBuffers.size=4MB \
    #--conf spark.rapids.shuffle.ucx.bounceBuffers.device.count=64 \
    #--conf spark.rapids.shuffle.ucx.bounceBuffers.host.count=64 \
    #--conf spark.shuffle.service.enabled=false \
    #--conf spark.rapids.shuffle.transport.enabled=true \
    #--conf  spark.rapids.shuffle.maxMetadataSize=512KB \
