FROM eclipse-temurin:11-jre

# Install Python and required tools
RUN apt-get update && \
    apt-get install -y python3 python3-pip wget procps && \
    apt-get clean && \
    rm -rf /var/lib/apt/lists/*

# Install Spark
ENV SPARK_VERSION=3.5.0
ENV HADOOP_VERSION=3
ENV SPARK_HOME=/opt/spark
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
ENV PYTHONPATH=$SPARK_HOME/python:$SPARK_HOME/python/lib/py4j-0.10.9.7-src.zip:$PYTHONPATH

RUN wget -q https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar -xzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} ${SPARK_HOME} && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

# Install Python dependencies
RUN pip3 install --no-cache-dir --break-system-packages \
    boto3 \
    pyspark==${SPARK_VERSION}

# Create working directory
WORKDIR /app

# Copy the script
COPY generate_tables.py /app/

# Create output directory
RUN mkdir -p /app/output && chmod 777 /app/output

# Set environment variables
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3

# Run the script
CMD ["python3", "generate_tables.py"]
