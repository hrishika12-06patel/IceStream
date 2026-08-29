-- IceStream Flink SQL Streaming Job DDL with Checkpointing

-- 1. Enable Flink Checkpointing for Iceberg Snapshot Commits
SET 'execution.checkpointing.interval' = '5s';
SET 'execution.checkpointing.mode' = 'EXACTLY_ONCE';

-- 2. Define Kafka Source Table
CREATE TABLE kafka_transactions (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    price DECIMAL(10, 2),
    tax_amount DECIMAL(10, 2),
    payment_method STRING,
    `timestamp` STRING
) WITH (
    'connector' = 'kafka',
    'topic' = 'transactions',
    'properties.bootstrap.servers' = 'kafka:9092',
    'properties.group.id' = 'icestream-flink-sql-group',
    'scan.startup.mode' = 'earliest-offset',
    'format' = 'json',
    'json.fail-on-missing-field' = 'false',
    'json.ignore-parse-errors' = 'true'
);

-- 3. Define Apache Iceberg Catalog and Sink Table
CREATE CATALOG iceberg WITH (
    'type' = 'iceberg',
    'catalog-type' = 'hadoop',
    'warehouse' = 'file:///opt/flink/warehouse'
);

CREATE TABLE IF NOT EXISTS iceberg.icestream.transactions (
    order_id STRING,
    customer_id STRING,
    product_id STRING,
    quantity INT,
    price DECIMAL(10, 2),
    tax_amount DECIMAL(10, 2),
    payment_method STRING,
    `timestamp` STRING,
    total_amount DECIMAL(12, 2)
) WITH (
    'format-version' = '2'
);

-- 4. Streaming Insert: Kafka -> Flink Validation & Calculation -> Iceberg Table
INSERT INTO iceberg.icestream.transactions
SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    price,
    tax_amount,
    payment_method,
    `timestamp`,
    CAST((price * quantity + tax_amount) AS DECIMAL(12, 2)) AS total_amount
FROM kafka_transactions
WHERE order_id IS NOT NULL 
  AND customer_id IS NOT NULL 
  AND product_id IS NOT NULL 
  AND quantity > 0 
  AND price >= 0 
  AND tax_amount >= 0;
