-- Flink SQL Quickstart — Reading from Iggy
-- 
-- Run this in the SQL Client:
-- docker exec -it flink-sql-client /opt/flink/bin/sql-client.sh

SET 'pipeline.jars' = 'file:///opt/flink/lib/flink-connector-iggy.jar';

CREATE TABLE sensor_data (
  sensor_id   STRING,
  temperature DOUBLE,
  humidity    DOUBLE,
  `timestamp` STRING,
  -- Extract event time for windowing
  ts AS TO_TIMESTAMP(REPLACE(`timestamp`, 'Z', '')),
  WATERMARK FOR ts AS ts - INTERVAL '5' SECOND
) WITH (
  'connector' = 'iggy',
  'host'      = 'iggy',
  'stream'    = 'quickstart',
  'topic'     = 'sensors',
  'format'    = 'json'
);

-- Simple streaming query
SELECT 
  sensor_id, 
  AVG(temperature) as avg_temp, 
  COUNT(*) as msg_count 
FROM sensor_data 
GROUP BY sensor_id;
