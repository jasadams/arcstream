# Stage 1: Build the identity resolution JAR
FROM docker.io/library/maven:3.9-eclipse-temurin-17 AS builder

WORKDIR /build
COPY identity-resolution/pom.xml .
RUN mvn dependency:go-offline -B
COPY identity-resolution/src src
RUN mvn package -B -DskipTests

# Stage 2: Flink image with connector JARs + identity resolution JAR
FROM docker.io/library/flink:1.20.1-java17

ENV HADOOP_VERSION=3.4.1

RUN cd /opt/flink/lib && \
    # Flink Kafka connector (DataStream API) + raw kafka-clients for HA recovery classloading
    curl -LO https://repo1.maven.org/maven2/org/apache/flink/flink-connector-kafka/3.3.0-1.20/flink-connector-kafka-3.3.0-1.20.jar && \
    curl -LO https://repo1.maven.org/maven2/org/apache/kafka/kafka-clients/3.4.0/kafka-clients-3.4.0.jar && \
    # Flink SQL Kafka connector (for SQL/Table API)
    curl -LO https://repo1.maven.org/maven2/org/apache/flink/flink-sql-connector-kafka/3.3.0-1.20/flink-sql-connector-kafka-3.3.0-1.20.jar && \
    curl -LO https://repo1.maven.org/maven2/org/apache/flink/flink-connector-jdbc/3.2.0-1.19/flink-connector-jdbc-3.2.0-1.19.jar && \
    # Iceberg Flink runtime
    curl -LO https://repo1.maven.org/maven2/org/apache/iceberg/iceberg-flink-runtime-1.20/1.9.0/iceberg-flink-runtime-1.20-1.9.0.jar && \
    # Hadoop 3.x (required by Iceberg Hadoop catalog + S3A)
    curl -LO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-common/${HADOOP_VERSION}/hadoop-common-${HADOOP_VERSION}.jar && \
    curl -LO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-hdfs-client/${HADOOP_VERSION}/hadoop-hdfs-client-${HADOOP_VERSION}.jar && \
    curl -LO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-aws/${HADOOP_VERSION}/hadoop-aws-${HADOOP_VERSION}.jar && \
    curl -LO https://repo1.maven.org/maven2/org/apache/hadoop/hadoop-auth/${HADOOP_VERSION}/hadoop-auth-${HADOOP_VERSION}.jar && \
    curl -LO https://repo1.maven.org/maven2/org/apache/hadoop/thirdparty/hadoop-shaded-guava/1.3.0/hadoop-shaded-guava-1.3.0.jar && \
    # AWS SDK v1 (for Hadoop S3A)
    curl -LO https://repo1.maven.org/maven2/com/amazonaws/aws-java-sdk-bundle/1.12.780/aws-java-sdk-bundle-1.12.780.jar && \
    # AWS SDK v2 bundle (for Iceberg S3FileIO)
    curl -LO https://repo1.maven.org/maven2/software/amazon/awssdk/bundle/2.29.51/bundle-2.29.51.jar && \
    curl -LO https://repo1.maven.org/maven2/software/amazon/awssdk/url-connection-client/2.29.51/url-connection-client-2.29.51.jar && \
    # Woodstox XML parser (Hadoop Configuration)
    curl -LO https://repo1.maven.org/maven2/com/fasterxml/woodstox/woodstox-core/7.1.0/woodstox-core-7.1.0.jar && \
    curl -LO https://repo1.maven.org/maven2/org/codehaus/woodstox/stax2-api/4.2.2/stax2-api-4.2.2.jar && \
    # Commons needed by Hadoop
    curl -LO https://repo1.maven.org/maven2/org/apache/commons/commons-configuration2/2.11.0/commons-configuration2-2.11.0.jar && \
    curl -LO https://repo1.maven.org/maven2/commons-collections/commons-collections/3.2.2/commons-collections-3.2.2.jar && \
    curl -LO https://repo1.maven.org/maven2/commons-io/commons-io/2.16.1/commons-io-2.16.1.jar && \
    curl -LO https://repo1.maven.org/maven2/commons-logging/commons-logging/1.3.4/commons-logging-1.3.4.jar && \
    curl -LO https://repo1.maven.org/maven2/org/apache/commons/commons-lang3/3.17.0/commons-lang3-3.17.0.jar && \
    curl -LO https://repo1.maven.org/maven2/com/google/re2j/re2j/1.7/re2j-1.7.jar

# Copy identity resolution JAR
COPY --from=builder /build/target/identity-resolution-1.0.0.jar /opt/flink/jobs/identity-resolution.jar
