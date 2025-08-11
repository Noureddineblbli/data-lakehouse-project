# Use the official Airflow image as our base
FROM apache/airflow:2.8.0

# Switch to root user to install packages and set permissions
USER root

# Install Java (a Spark prerequisite) and procps (for the 'ps' command).
RUN apt-get update && \
    apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    && apt-get clean && rm -rf /var/lib/apt/lists/*

# Set up environment variables for Spark
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin

# Copy the Spark binary tarball into the image
COPY spark-3.5.0-bin-hadoop3.tgz /tmp/

# Unpack Spark, move it to its final destination, and then clean up the tarball
RUN tar -xzf /tmp/spark-3.5.0-bin-hadoop3.tgz -C /opt && \
    mv /opt/spark-3.5.0-bin-hadoop3 $SPARK_HOME && \
    rm /tmp/spark-3.5.0-bin-hadoop3.tgz

# Now, switch back to the non-privileged airflow user for security.
USER airflow

# Copy and install python packages as the airflow user.
COPY requirements.txt /
RUN pip install --no-cache-dir -r /requirements.txt