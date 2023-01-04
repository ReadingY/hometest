FROM gcr.io/spark-operator/spark:v2.4.0

ENV PYSPARK_MAJOR_PYTHON_VERSION=3
WORKDIR /opt/application/

COPY main.py .