-- Compares average electricity load between rainy days and non-rainy days for each ERCOT zone.
-- Returns average daily load (MW) and count of days for both rainy and non-rainy conditions per zone.
-- Helps analyze whether precipitation correlates with changes in electricity consumption patterns.

WITH weather_zone_daily AS (
  SELECT
    (wh.time AT TIME ZONE 'UTC')::date AS day_utc,
    szm.zone,
    SUM(wh.precipitation_mm) AS precip_mm_sum,
    (SUM(wh.precipitation_mm) > 0) AS rainy_day
  FROM weather_hourly wh
  JOIN station_zone_map szm
    ON szm.station_id = wh.station_id
  GROUP BY (wh.time AT TIME ZONE 'UTC')::date, szm.zone
),
load_long AS (
  SELECT
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
  ) AS z(zone_code, load_mw)
),
daily_avg_load AS (
  SELECT
    day_utc,
    zone_code,
    AVG(load_mw) AS daily_avg_mw
  FROM load_long
  GROUP BY day_utc, zone_code
)
SELECT
  wzd.zone,
  wzd.rainy_day,
  AVG(dal.daily_avg_mw) AS avg_load_mw,
  COUNT(*)              AS num_days
FROM weather_zone_daily wzd
JOIN daily_avg_load dal
  ON dal.zone_code = wzd.zone
 AND dal.day_utc  = wzd.day_utc
GROUP BY wzd.zone, wzd.rainy_day
ORDER BY wzd.zone, wzd.rainy_day DESC;



