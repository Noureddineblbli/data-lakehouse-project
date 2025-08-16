# Use the official Airflow image as our base
FROM apache/airflow:2.8.0

# Switch to root user to install system packages
USER root

# NOTE: We are running everything as the 'root' user to simplify permissions
# for this local-only development environment. This is not a production practice.

# Install Java, wget, and other system packages + AWS CLI dependencies
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    wget \
    procps \
    curl \
    unzip \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Install AWS CLI v2 system-wide
RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip" \
    && unzip awscliv2.zip \
    && ./aws/install \
    && rm -rf awscliv2.zip aws/

# Define arguments for Spark and Hadoop versions to keep them consistent
ARG SPARK_VERSION=3.5.0
ARG HADOOP_VERSION=3

# Set environment variables that Spark and its scripts rely on
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Download and unpack Spark
RUN wget -O /tmp/spark.tgz https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf /tmp/spark.tgz -C /opt && \
    rm /tmp/spark.tgz && \
    mv /opt/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} $SPARK_HOME

# Switch back to airflow user for pip installation
USER airflow

# Copy and install python packages.
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt