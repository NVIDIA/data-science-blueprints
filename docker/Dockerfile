FROM nvcr.io/nvidia/cuda:11.0-base-ubuntu20.04

RUN apt-get update && apt-get install -y openjdk-8-jre-headless git python3 python3-pip pipenv

WORKDIR /workspace

RUN git clone -b v0.4-branch https://github.com/willb/telco-churn-augmentation .

RUN grep -v altair < Pipfile > Pipfile.tmp && mv Pipfile.tmp Pipfile

RUN pipenv install

COPY https://repo1.maven.org/maven2/ai/rapids/cudf/0.17/cudf-0.17-cuda11.jar /workspace
COPY https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.3.0/rapids-4-spark_2.12-0.3.0.jar /workspace

COPY start.sh /workspace

RUN chmod 755 /workspace/start.sh

CMD ["./start.sh"]

