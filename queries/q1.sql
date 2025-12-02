-- Retrieves hourly electricity load data for all ERCOT regions in chronological order.
-- Returns raw aggregated load values (MW) for each of the 8 zones (coast, east, far_west, north, north_c, southern, south_c, west) plus total ERCOT system load.

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
ORDER BY hour_end;
