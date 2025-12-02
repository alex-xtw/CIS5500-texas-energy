-- Query 10: Temperature Range by Zone
-- Returns minimum, maximum, and average temperatures for each ERCOT zone
-- Shows temperature statistics in Celsius for all zones with weather station data

SELECT
    szm.zone,
    ROUND(MIN(wh.temperature_2m_c)::numeric, 1) as min_temp_c,
    ROUND(AVG(wh.temperature_2m_c)::numeric, 1) as avg_temp_c,
    ROUND(MAX(wh.temperature_2m_c)::numeric, 1) as max_temp_c
FROM weather_hourly wh
JOIN station_zone_map szm ON wh.station_id = szm.station_id
GROUP BY szm.zone
ORDER BY szm.zone;
