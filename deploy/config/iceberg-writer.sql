CREATE TABLE unified_events (
    event_id        STRING,
    event_type      STRING,
    tenant_id       STRING,
    event_time      TIMESTAMP(3),
    canonical_id    STRING,
    anonymous_id    STRING,
    user_id         STRING,
    session_id      STRING,
    page_url        STRING,
    referrer        STRING,
    element_id      STRING,
    feature_name    STRING,
    device_type     STRING,
    browser         STRING,
    os              STRING,
    country         STRING,
    properties      STRING,
    WATERMARK FOR event_time AS event_time - INTERVAL '5' SECOND
) WITH (
    'connector' = 'kafka',
    'topic' = 'unified-events',
    'properties.bootstrap.servers' = 'redpanda:9092',
    'properties.group.id' = 'flink-iceberg-writer',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

CREATE CATALOG iceberg_catalog WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hadoop',
    'warehouse' = 's3a://iceberg-warehouse',
    'io-impl' = 'org.apache.iceberg.aws.s3.S3FileIO',
    's3.endpoint' = 'http://minio:9000',
    's3.access-key-id' = 'minioadmin',
    's3.secret-access-key' = 'minioadmin',
    's3.path-style-access' = 'true'
);

CREATE DATABASE IF NOT EXISTS iceberg_catalog.analytics;

CREATE TABLE IF NOT EXISTS iceberg_catalog.analytics.events (
    event_id        STRING,
    event_type      STRING,
    tenant_id       STRING,
    event_time      TIMESTAMP(3),
    canonical_id    STRING,
    anonymous_id    STRING,
    user_id         STRING,
    session_id      STRING,
    page_url        STRING,
    referrer        STRING,
    element_id      STRING,
    feature_name    STRING,
    device_type     STRING,
    browser         STRING,
    os              STRING,
    country         STRING,
    properties      STRING
) PARTITIONED BY (tenant_id);

INSERT INTO iceberg_catalog.analytics.events
SELECT event_id, event_type, tenant_id, event_time, canonical_id, anonymous_id,
       user_id, session_id, page_url, referrer, element_id,
       feature_name, device_type, browser, os, country, properties
FROM unified_events;
