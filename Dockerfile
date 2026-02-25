# syntax=docker/dockerfile:1
FROM apache/airflow:2.9.3-python3.11

############################
# Install Java and system dependencies
############################
USER root
RUN apt-get update && apt-get install -y \
    openjdk-17-jdk \
    curl \
    wget \
    procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

############################
# Install Spark
############################
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz -C /opt/ && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

############################
# Set Spark environment variables
############################
ENV SPARK_HOME=/opt/spark
ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin"

############################
# Download PostgreSQL JDBC Driver for Spark
############################
RUN wget -q https://jdbc.postgresql.org/download/postgresql-42.7.1.jar -P $SPARK_HOME/jars/

############################
# IMPORTANT: PYSPARK needs these or JVM does NOT start
############################
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3
ENV PYTHONPATH="$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip"

############################
# Python packages
############################
USER airflow

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

WORKDIR /opt/airflow
