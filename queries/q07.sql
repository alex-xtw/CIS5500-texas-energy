-- Detects statistical outliers in daily ERCOT system load using ±3 standard deviations from monthly means.
-- Identifies unusually high or low demand days, then correlates them with weather conditions (temperature, humidity, precipitation, wind, pressure, cloud cover).
-- Returns monthly aggregated weather patterns for outlier days to understand what drives extreme load events.

WITH daily_load AS (
  SELECT
    (hour_end AT TIME ZONE 'UTC')::date AS day_utc,
    AVG(ercot) AS daily_avg_mw
  FROM ercot_load
  GROUP BY (hour_end AT TIME ZONE 'UTC')::date
),
monthly_stats AS (
  SELECT
    date_trunc('month', day_utc)::date AS month_start,
    AVG(daily_avg_mw)                  AS mu,
    STDDEV_SAMP(daily_avg_mw)          AS sigma
  FROM daily_load
  GROUP BY date_trunc('month', day_utc)::date
),
outlier_days AS (
  SELECT
    dl.day_utc,
    ms.month_start,
    CASE
      WHEN dl.daily_avg_mw > ms.mu + 3*ms.sigma THEN 'high'
      WHEN dl.daily_avg_mw < ms.mu - 3*ms.sigma THEN 'low'
      ELSE NULL
    END AS outlier_group
  FROM daily_load dl
  JOIN monthly_stats ms
    ON ms.month_start = date_trunc('month', dl.day_utc)::date
),
daily_weather AS (
  SELECT
    (wh.time AT TIME ZONE 'UTC')::date AS day_utc,
    AVG(wh.temperature_2m_c)             AS temp_c_avg,
    AVG(wh.relative_humidity_2m_percent) AS rh_pct_avg,
    SUM(wh.precipitation_mm)             AS precip_mm_sum,
    AVG(wh.wind_speed_10m_kmh)           AS wind_10m_kmh_avg,
    AVG(wh.pressure_msl_hpa)             AS pressure_hpa_avg,
    AVG(wh.cloud_cover_mid_percent)          AS cloud_cover_pct_avg
  FROM weather_hourly wh
  GROUP BY (wh.time AT TIME ZONE 'UTC')::date
)
SELECT
  od.month_start,
  od.outlier_group,              -- 'high' or 'low'
  COUNT(*)               AS num_days,
  AVG(dw.temp_c_avg)     AS avg_temp_c,
  AVG(dw.rh_pct_avg)     AS avg_rh_pct,
  AVG(dw.precip_mm_sum)  AS avg_precip_mm,
  AVG(dw.wind_10m_kmh_avg)    AS avg_wind_kmh,
  AVG(dw.pressure_hpa_avg)    AS avg_pressure_hpa,
  AVG(dw.cloud_cover_pct_avg) AS avg_cloud_cover_pct
FROM outlier_days od
JOIN daily_weather dw
  ON dw.day_utc = od.day_utc
WHERE od.outlier_group IS NOT NULL
GROUP BY od.month_start, od.outlier_group
ORDER BY od.month_start, od.outlier_group DESC;
