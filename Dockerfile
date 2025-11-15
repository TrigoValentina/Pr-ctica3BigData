FROM apache/spark:3.5.3

USER root
RUN apt-get update && apt-get install -y python3-pip \
    && pip3 install --no-cache-dir pyspark psycopg2-binary pymongo kafka-python pandas

# Crear el directorio .ivy2 y darle permisos al usuario spark
RUN mkdir -p /home/spark/.ivy2 && \
    chown -R spark:spark /home/spark/.ivy2

USER spark
WORKDIR /opt/spark/app
