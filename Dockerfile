FROM python:3.11-slim
RUN apt update
RUN apt install curl mlocate default-jdk -y
RUN pip install --no-cache-dir pandas matplotlib confluent-kafka pyarrow pyspark==3.4.1 pytest delta-spark
RUN mkdir ~/spark_training/
WORKDIR /root/spark_training
