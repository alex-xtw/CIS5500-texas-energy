-- Identifies heat waves (3+ consecutive days with max temp >= 100°F) and their impact on electricity demand.
-- Returns each heat wave's date range, duration, and average daily peak load (MW) for each ERCOT zone.
-- Useful for understanding how sustained extreme heat affects grid stress and peak demand patterns.

WITH weather_zone_daily AS (
  SELECT
    (wh.time AT TIME ZONE 'UTC')::date AS day_utc,
    szm.zone,
    MAX( (wh.temperature_2m_c * 9.0/5.0) + 32.0 ) AS temp_max_f
  FROM weather_hourly wh
  JOIN station_zone_map	 szm
    ON szm.station_id = wh.station_id
  GROUP BY (wh.time AT TIME ZONE 'UTC')::date, szm.zone
),
hot_only AS (
  -- Keep only hot days >= 100°F
  SELECT
    zone,
    day_utc,
    temp_max_f
  FROM weather_zone_daily
  WHERE temp_max_f >= 100.0
),
hot_islands AS (
  SELECT
    zone,
    day_utc,
    temp_max_f,
    CASE
      WHEN LAG(day_utc) OVER (PARTITION BY zone ORDER BY day_utc) = day_utc - INTERVAL '1 day'
      THEN 0 ELSE 1
    END AS is_new_streak
  FROM hot_only
),
streaks AS (
  SELECT
    zone,
    day_utc,
    temp_max_f,
    SUM(is_new_streak) OVER (PARTITION BY zone ORDER BY day_utc
                              ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS streak_id
  FROM hot_islands
),
streak_summary AS (
  -- Keep only streaks with length >= 3 days
  SELECT
    zone,
    streak_id,
    MIN(day_utc) AS streak_start,
    MAX(day_utc) AS streak_end,
    COUNT(*)     AS streak_days
  FROM streaks
  GROUP BY zone, streak_id
  HAVING COUNT(*) >= 3
),
load_long AS (
  -- Wide → long: hourly load by zone
  SELECT
    el.hour_end,
    (el.hour_end AT TIME ZONE 'UTC')::date AS day_utc,
    z.zone_code,
    z.load_mw
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
      -- exclude system-wide 'ercot' here; we want zone-level peak load
  ) AS z(zone_code, load_mw)
),
daily_peak_load AS (
  SELECT
    day_utc,
    zone_code,
    MAX(load_mw) AS daily_peak_mw
  FROM load_long
  GROUP BY day_utc, zone_code
)
SELECT
  s.zone,
  s.streak_start,
  s.streak_end,
  s.streak_days,
  AVG(dpl.daily_peak_mw) AS avg_peak_load_mw
FROM streak_summary s
LEFT JOIN daily_peak_load dpl
  ON dpl.zone_code = s.zone
 AND dpl.day_utc  BETWEEN s.streak_start AND s.streak_end
GROUP BY
  s.zone, s.streak_start, s.streak_end, s.streak_days
ORDER BY
  s.zone, s.streak_start;