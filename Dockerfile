FROM apache/spark:3.5.3

USER root
RUN apt-get update && apt-get install -y python3-pip wget \
    && pip3 install --no-cache-dir pyspark pymongo kafka-python pandas mysql-connector-python \
       scikit-learn matplotlib seaborn numpy \
    && wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-sql-kafka-0-10_2.12/3.5.3/spark-sql-kafka-0-10_2.12-3.5.3.jar -O /opt/spark/jars/spark-sql-kafka-0-10_2.12-3.5.3.jar \
    && wget -q https://repo1.maven.org/maven2/org/apache/spark/spark-token-provider-kafka-0-10_2.12/3.5.3/spark-token-provider-kafka-0-10_2.12-3.5.3.jar -O /opt/spark/jars/spark-token-provider-kafka-0-10_2.12-3.5.3.jar \
    && wget -q https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.5.1/kafka-clients-3.5.1.jar -O /opt/spark/jars/kafka-clients-3.5.1.jar \
    && wget -q https://repo1.maven.org/maven2/org/apache/commons/commons-pool2/2.12.0/commons-pool2-2.12.0.jar -O /opt/spark/jars/commons-pool2-2.12.0.jar \
    && wget -q https://repo1.maven.org/maven2/com/mysql/mysql-connector-j/8.0.33/mysql-connector-j-8.0.33.jar -O /opt/spark/jars/mysql-connector-j-8.0.33.jar \
    && chown -R spark:spark /opt/spark/jars/*kafka* /opt/spark/jars/*pool* /opt/spark/jars/*mysql*

USER spark
WORKDIR /opt/spark/app
