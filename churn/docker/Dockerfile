FROM nvcr.io/nvidia/cuda:11.0-runtime-ubuntu20.04

RUN apt-get update && apt-get install -y openjdk-8-jre-headless git python3 python3-pip pipenv

WORKDIR /workspace

RUN git clone -b v0.7 https://github.com/willb/telco-churn-augmentation .

RUN grep -v altair < Pipfile > Pipfile.tmp && mv Pipfile.tmp Pipfile

RUN pipenv install

ADD https://repo1.maven.org/maven2/ai/rapids/cudf/0.18.1/cudf-0.18.1-cuda11.jar /workspace
ADD https://repo1.maven.org/maven2/com/nvidia/rapids-4-spark_2.12/0.4.0/rapids-4-spark_2.12-0.4.0.jar /workspace
COPY start.sh /workspace

RUN chmod 755 /workspace/start.sh

CMD ["./start.sh"]

