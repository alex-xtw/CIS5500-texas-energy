-- Analyzes electricity demand during extreme heat events by examining the hottest 1% of days in each zone.
-- Calculates the 99th percentile temperature cutoff per zone, then reports median peak load and day count for days above that threshold.
-- Reveals how grid demand responds during the most extreme temperature conditions.

WITH weather_zone_daily AS (
  SELECT
    (wh.time AT TIME ZONE 'UTC')::date AS day_utc,
    szm.zone,
    MAX((wh.temperature_2m_c * 9.0/5.0) + 32.0) AS temp_max_f
  FROM weather_hourly wh
  JOIN station_zone_map szm
    ON szm.station_id = wh.station_id
  GROUP BY (wh.time AT TIME ZONE 'UTC')::date, szm.zone
),
daily_peak_load AS (
  SELECT
    (el.hour_end AT TIME ZONE 'UTC')::date AS day_utc,
    z.zone,
    MAX(z.load_mw) AS daily_peak_mw
  FROM ercot_load el
  CROSS JOIN LATERAL (
    VALUES
      ('coast',   el.coast),
      ('east',    el.east),
      ('far_west',el.far_west),
      ('north',   el.north),
      ('north_c', el.north_c),
      ('southern',el.southern),
      ('south_c', el.south_c),
      ('west',    el.west)
  ) AS z(zone, load_mw)
  GROUP BY (el.hour_end AT TIME ZONE 'UTC')::date, z.zone
),
hot_cutoff AS (
  SELECT
    zone,
    percentile_cont(0.99) WITHIN GROUP (ORDER BY temp_max_f) AS p99_temp_f
  FROM weather_zone_daily
  GROUP BY zone
)
SELECT
  wzd.zone,
  percentile_cont(0.5) WITHIN GROUP (ORDER BY dpl.daily_peak_mw) AS median_peak_load_mw,
  COUNT(*) AS num_top1pct_days
FROM weather_zone_daily wzd
JOIN hot_cutoff hc USING (zone)
JOIN daily_peak_load dpl USING (zone, day_utc)
WHERE wzd.temp_max_f >= hc.p99_temp_f
GROUP BY wzd.zone
ORDER BY wzd.zone;
