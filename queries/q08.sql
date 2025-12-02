-- Statistical Outlier Detection for ERCOT Load Data
-- This query performs outlier detection using the ±N standard deviations method.
--
-- What it does:
-- 1. Converts wide-format ercot_load table into long format (one row per region per hour)
-- 2. Calculates mean and standard deviation for each region
-- 3. Identifies outliers: values falling outside ±N standard deviations from the mean
--    - High outlier: load > mean + (N × std_dev)
--    - Low outlier: load < mean - (N × std_dev)
-- 4. Computes z-scores: number of standard deviations each value is from the mean
-- 5. Returns only outlier data points with statistical metrics
--
-- Default threshold: 3 standard deviations (99.7% confidence interval)
-- This identifies unusual spikes or drops in electricity demand that deviate significantly from normal patterns.

WITH load_long AS (
  -- Convert wide format to long format for all regions
  SELECT hour_end, 'coast' AS region, coast AS load_mw FROM ercot_load
  UNION ALL SELECT hour_end, 'east', east FROM ercot_load
  UNION ALL SELECT hour_end, 'far_west', far_west FROM ercot_load
  UNION ALL SELECT hour_end, 'north', north FROM ercot_load
  UNION ALL SELECT hour_end, 'north_c', north_c FROM ercot_load
  UNION ALL SELECT hour_end, 'southern', southern FROM ercot_load
  UNION ALL SELECT hour_end, 'south_c', south_c FROM ercot_load
  UNION ALL SELECT hour_end, 'west', west FROM ercot_load
  UNION ALL SELECT hour_end, 'ercot', ercot FROM ercot_load
),
filtered_load AS (
  SELECT * FROM load_long
  WHERE 1=1
  -- Optional: AND hour_end >= 'start_date'
  -- Optional: AND hour_end <= 'end_date'
),
region_stats AS (
  -- Calculate mean and std dev for each region
  SELECT
    region,
    AVG(load_mw) AS mean,
    STDDEV_SAMP(load_mw) AS std_dev
  FROM filtered_load
  GROUP BY region
),
outliers AS (
  -- Identify outliers based on threshold
  SELECT
    fl.hour_end,
    fl.region,
    fl.load_mw,
    rs.mean,
    rs.std_dev,
    (fl.load_mw - rs.mean) / NULLIF(rs.std_dev, 0) AS z_score,
    CASE
      WHEN fl.load_mw > rs.mean + 3 * rs.std_dev THEN 'high'
      WHEN fl.load_mw < rs.mean - 3 * rs.std_dev THEN 'low'
      ELSE NULL
    END AS outlier_type
  FROM filtered_load fl
  JOIN region_stats rs ON fl.region = rs.region
)
SELECT
  hour_end,
  region,
  load_mw,
  mean,
  std_dev,
  z_score,
  outlier_type
FROM outliers
WHERE outlier_type IS NOT NULL
ORDER BY hour_end DESC, region
LIMIT 1000;