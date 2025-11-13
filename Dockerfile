FROM apache/spark:3.5.3

USER root
RUN apt-get update && apt-get install -y python3-pip \
    && pip3 install --no-cache-dir pyspark psycopg2-binary pymongo kafka-python pandas

USER spark
WORKDIR /opt/spark/app
