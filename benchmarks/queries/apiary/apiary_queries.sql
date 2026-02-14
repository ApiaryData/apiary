-- ============================================================
-- Apiary-Specific Benchmark Queries
--
-- These queries exercise capabilities unique to Apiary:
-- format-agnostic storage, ACID consistency, heterogeneous
-- cluster coordination, and swarm elasticity.
--
-- Table: sensor_readings (fact)
--   - device_id (INT32), timestamp_ms (INT64), value (FLOAT64)
--   - quality (INT8), facility (STRING), location (STRING)
--
-- Table: devices (dimension)
--   - device_id (INT32), device_type (STRING), facility (STRING)
--   - location (STRING), install_date (STRING)
--   - calibration_offset (FLOAT64), min_threshold (FLOAT64)
--   - max_threshold (FLOAT64)
-- ============================================================


-- ============================================================
-- FORMAT AGNOSTIC QUERIES (FA)
-- These queries must read across Parquet, CSV, and Arrow
-- partitions simultaneously. Performance should be compared
-- against the same queries on uniform-format datasets.
-- ============================================================

-- FA-1: Full scan aggregate across all formats
-- Baseline: how fast can we scan every Cell regardless of format?
SELECT
    COUNT(*) AS total_readings,
    AVG(value) AS avg_value,
    MIN(value) AS min_value,
    MAX(value) AS max_value,
    SUM(CASE WHEN quality = 0 THEN 1 ELSE 0 END) AS good_count,
    SUM(CASE WHEN quality = 2 THEN 1 ELSE 0 END) AS bad_count
FROM sensor_readings;

-- FA-2: Filtered scan with predicate pushdown test
-- Tests whether Parquet stats-based pruning works alongside
-- CSV (no stats) and Arrow (column-level stats) formats
SELECT
    facility,
    COUNT(*) AS anomaly_count,
    AVG(value) AS avg_anomaly_value
FROM sensor_readings
WHERE value > 90.0
  AND quality <> 2
GROUP BY facility
ORDER BY anomaly_count DESC;

-- FA-3: Join across formats (dimension in one format, facts in mixed)
SELECT
    d.device_type,
    d.facility,
    COUNT(*) AS reading_count,
    AVG(r.value) AS avg_value,
    AVG(r.value - d.calibration_offset) AS calibrated_avg
FROM sensor_readings r
JOIN devices d ON r.device_id = d.device_id
WHERE r.quality = 0
GROUP BY d.device_type, d.facility
ORDER BY reading_count DESC;

-- FA-4: Time-windowed aggregation across format boundaries
-- Partitions by day cross format boundaries (even=parquet, odd=arrow, 6x=csv)
SELECT
    CAST(timestamp_ms / 86400000 AS INTEGER) AS day_bucket,
    location,
    COUNT(*) AS readings,
    AVG(value) AS avg_value,
    MAX(value) - MIN(value) AS value_range
FROM sensor_readings
GROUP BY day_bucket, location
ORDER BY day_bucket, location;

-- FA-5: Top-N devices by reading frequency per format boundary
SELECT
    device_id,
    COUNT(*) AS total_readings,
    SUM(CASE WHEN value > 90 THEN 1 ELSE 0 END) AS alerts,
    CAST(SUM(CASE WHEN value > 90 THEN 1 ELSE 0 END) AS DOUBLE) /
        COUNT(*) AS alert_rate
FROM sensor_readings
GROUP BY device_id
HAVING COUNT(*) > 100
ORDER BY alert_rate DESC
LIMIT 50;


-- ============================================================
-- ACID CONSISTENCY QUERIES (AC)
-- Run these during concurrent writes. Results should be
-- consistent (snapshot isolation) regardless of write activity.
-- ============================================================

-- AC-1: Count consistency check
-- During writes, this should return a consistent snapshot count
-- that doesn't change mid-query even as batches are appended.
SELECT
    COUNT(*) AS total_rows,
    COUNT(DISTINCT device_id) AS unique_devices,
    COUNT(DISTINCT facility) AS unique_facilities
FROM sensor_readings;

-- AC-2: Per-facility aggregation (consistency across partitions)
-- Each facility partition should show internally consistent numbers
SELECT
    facility,
    COUNT(*) AS readings,
    AVG(value) AS avg_value,
    MIN(timestamp_ms) AS earliest,
    MAX(timestamp_ms) AS latest
FROM sensor_readings
GROUP BY facility
ORDER BY facility;

-- AC-3: Running total that would break under dirty reads
-- If writes are visible mid-query, the cumulative sum will be inconsistent
SELECT
    facility,
    location,
    SUM(value) AS total_value,
    COUNT(*) AS count,
    SUM(value) / COUNT(*) AS computed_avg,
    AVG(value) AS builtin_avg
FROM sensor_readings
WHERE quality = 0
GROUP BY facility, location
ORDER BY facility, location;

-- AC-4: Cross-partition join during writes
-- Tests whether dimension + fact join is consistent under concurrent appends
SELECT
    d.device_type,
    COUNT(*) AS readings,
    AVG(r.value) AS avg_value,
    SUM(CASE WHEN r.value > d.max_threshold THEN 1 ELSE 0 END) AS over_threshold,
    SUM(CASE WHEN r.value < d.min_threshold THEN 1 ELSE 0 END) AS under_threshold
FROM sensor_readings r
JOIN devices d ON r.device_id = d.device_id
GROUP BY d.device_type
ORDER BY d.device_type;


-- ============================================================
-- HETEROGENEOUS CLUSTER QUERIES (HC)
-- Designed to expose skew in partition processing.
-- On a uniform scheduler, large partitions become bottlenecks.
-- On swarm scheduling, faster nodes should claim more work.
-- ============================================================

-- HC-1: Full scan (affected by partition skew)
-- Large partitions dominate wall-clock time on uniform schedulers
SELECT
    COUNT(*) AS total,
    AVG(value) AS avg_val,
    APPROX_PERCENTILE_CONT(value, 0.5) AS median_val,
    APPROX_PERCENTILE_CONT(value, 0.95) AS p95_val,
    APPROX_PERCENTILE_CONT(value, 0.99) AS p99_val
FROM sensor_readings;

-- HC-2: Per-partition aggregation (measures scheduling fairness)
-- Report per-partition stats to measure which node processed what
SELECT
    facility,
    location,
    COUNT(*) AS readings,
    AVG(value) AS avg_value,
    STDDEV(value) AS stddev_value
FROM sensor_readings
GROUP BY facility, location
ORDER BY readings DESC;

-- HC-3: Complex analytical query (stresses slower nodes)
-- Window function requires materialising sorted partitions
SELECT device_id, timestamp_ms, value,
       AVG(value) OVER (
           PARTITION BY device_id
           ORDER BY timestamp_ms
           ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING
       ) AS moving_avg,
       value - AVG(value) OVER (
           PARTITION BY device_id
           ORDER BY timestamp_ms
           ROWS BETWEEN 10 PRECEDING AND 10 FOLLOWING
       ) AS deviation
FROM sensor_readings
WHERE quality = 0
ORDER BY device_id, timestamp_ms
LIMIT 1000;

-- HC-4: Aggregation with heavy GROUP BY (many groups)
-- Tests hash aggregation memory pressure on constrained nodes
SELECT
    device_id,
    CAST(timestamp_ms / 3600000 AS INTEGER) AS hour_bucket,
    COUNT(*) AS readings,
    AVG(value) AS avg_value,
    MIN(value) AS min_value,
    MAX(value) AS max_value
FROM sensor_readings
GROUP BY device_id, CAST(timestamp_ms / 3600000 AS INTEGER)
ORDER BY device_id, hour_bucket;


-- ============================================================
-- SWARM ELASTICITY QUERIES (SE)
-- Long-running queries over many partitions. The benchmark
-- runner will kill/add nodes mid-execution and measure the
-- impact on query completion and correctness.
-- ============================================================

-- SE-1: Long full scan (target: 30+ seconds at SF1 on Pi)
-- Must complete correctly even if a node dies mid-scan
SELECT
    facility,
    location,
    device_id,
    COUNT(*) AS total_readings,
    AVG(value) AS avg_value,
    STDDEV(value) AS stddev_value,
    MIN(timestamp_ms) AS first_reading,
    MAX(timestamp_ms) AS last_reading,
    SUM(CASE WHEN quality = 0 THEN 1 ELSE 0 END) AS good,
    SUM(CASE WHEN quality = 1 THEN 1 ELSE 0 END) AS suspect,
    SUM(CASE WHEN quality = 2 THEN 1 ELSE 0 END) AS bad
FROM sensor_readings
GROUP BY facility, location, device_id
ORDER BY total_readings DESC;

-- SE-2: Complex join with many partitions
SELECT
    d.device_type,
    d.facility AS device_facility,
    r.facility AS reading_facility,
    CAST(r.timestamp_ms / 86400000 AS INTEGER) AS day,
    COUNT(*) AS readings,
    AVG(r.value) AS avg_value,
    AVG(r.value - d.calibration_offset) AS calibrated_avg
FROM sensor_readings r
JOIN devices d ON r.device_id = d.device_id
WHERE r.quality IN (0, 1)
GROUP BY d.device_type, d.facility, r.facility,
         CAST(r.timestamp_ms / 86400000 AS INTEGER)
ORDER BY day, readings DESC;

-- SE-3: Repeated aggregation (run multiple times to observe recovery)
-- Compare results before/after node failure for consistency
SELECT
    COUNT(*) AS n,
    SUM(value) AS sum_val,
    SUM(value * value) AS sum_sq_val,
    MIN(value) AS min_val,
    MAX(value) AS max_val
FROM sensor_readings
WHERE quality = 0;
