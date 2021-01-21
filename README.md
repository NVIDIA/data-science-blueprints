# telco-churn-augmentation

This repository shows a realistic ETL workflow based on synthetic normalized data.  It consists of two pieces:

1.  _an augmentation script_, which synthesizes normalized (long-form) data from a wide-form input file, optionally augmenting it by duplicating records, and
2. _an ETL script_, which performs joins and aggregations in order to generate wide-form data from the synthetic long-form data.

From a performance evaluation perspective, the latter is the interesting workload; the former is just a data generator for the latter.

## Running as notebooks

The notebooks ([`augment.ipynb`](augment.ipynb) and [`etl.ipynb`](etl.ipynb)) are the best resource to understand the code and can be run interactively or with Papermill.  The published Papermill parameters are near the top of each notebook.

## Running as scripts

There are also script versions of each job: `generate.py` and `do-etl.py`.  Each of these supports some command-line arguments and has online help.

To run these in cluster mode on Spark, you'll need to package up the [modules](churn) that each script depends on, by creating a zip file:

`zip archive.zip -r churn generate.py`

Then you can pass `archive.zip` to your `--py-files` argument.

Here's an example command-line to run the data generator on Google Cloud Dataproc:

```
gcloud dataproc jobs submit pyspark generate.py \
  --py-files=archive.zip --cluster=$MYCLUSTER \
  --project=$MYPROJECT --region=$MYREGION \
  --properties spark.rapids.sql.enabled=False -- \
  --input-file=gs://$MYBUCKET/raw.csv \
  --output-prefix=gs://$MYBUCKET/generated-700m/ \
  --dup-times=100000
```

This will generate 100000 output records for every input record, or roughly 700 million records.  Note that we have disabled the RAPIDS Spark Accelerator plugin; this may be necessary for the data generator.

## Tuning and configuration

The most critical configuration parameter for good GPU performance on the ETL job is `spark.rapids.sql.variableFloatAgg.enabled` -- if it isn't set to true, all of the aggregations will run on CPU, requiring costly transfers from device to host memory.

Here are the parameters I used when I tested on Dataproc:

- `spark.rapids.memory.pinnedPool.size=2G`
- `spark.sql.shuffle.partitions=16`
- `spark.sql.files.maxPartitionBytes=4096MB`
- `spark.rapids.sql.enabled=True`
- `spark.executor.cores=2`
- `spark.task.cpus=1`
- `spark.rapids.sql.concurrentGpuTasks=2`
- `spark.task.resource.gpu.amount=.5`
- `spark.executor.instances=8`
- `spark.rapids.sql.variableFloatAgg.enabled=True`
- `spark.rapids.sql.explain=NOT_ON_GPU`
