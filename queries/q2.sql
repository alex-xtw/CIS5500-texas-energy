-- Sets up forecast comparison infrastructure by creating staging tables and joining forecasted loads with actual loads.
-- Creates staging.ercot_load_wide_expected (holds predicted values) and staging.ercot_load_wide_compare (joins expected vs actual side-by-side for all regions).
-- Used as the foundation for calculating forecast accuracy metrics in subsequent queries.
CREATE SCHEMA IF NOT EXISTS staging;
CREATE TABLE IF NOT EXISTS staging.ercot_load_wide_expected (
  hour_end   TIMESTAMPTZ PRIMARY KEY,
  coast      NUMERIC,
  east       NUMERIC,
  far_west   NUMERIC,
  north      NUMERIC,
  north_c    NUMERIC,
  southern   NUMERIC,
  south_c    NUMERIC,
  west       NUMERIC,
  ercot      NUMERIC
);
DROP TABLE IF EXISTS staging.ercot_load_wide_compare;
CREATE TABLE staging.ercot_load_wide_compare AS
WITH actual AS (
  SELECT
    hour_end,
    SUM(coast)    AS coast,
    SUM(east)     AS east,
    SUM(far_west) AS far_west,
    SUM(north)    AS north,
    SUM(north_c)  AS north_c,
    SUM(southern) AS southern,
    SUM(south_c)  AS south_c,
    SUM(west)     AS west,
    SUM(ercot)    AS ercot
  FROM ercot_load
  GROUP BY hour_end
)
SELECT
  e.hour_end,
  e.coast     AS coast_expected,     a.coast     AS coast_actual,
  e.east      AS east_expected,      a.east      AS east_actual,
  e.far_west  AS far_west_expected,  a.far_west  AS far_west_actual,
  e.north     AS north_expected,     a.north     AS north_actual,
  e.north_c   AS north_c_expected,   a.north_c   AS north_c_actual,
  e.southern  AS southern_expected,  a.southern  AS southern_actual,
  e.south_c   AS south_c_expected,   a.south_c   AS south_c_actual,
  e.west      AS west_expected,      a.west      AS west_actual,
  e.ercot     AS ercot_expected,     a.ercot     AS ercot_actual
FROM staging.ercot_load_wide_expected e
LEFT JOIN actual a
  ON a.hour_end = e.hour_end
ORDER BY e.hour_end;
