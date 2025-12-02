-- Query 9: Monthly Load Distribution
-- Returns the load distribution aggregated by month across all ERCOT zones
-- Shows each month-zone combination with average load and percentage of total ERCOT load

WITH monthly_aggregated AS (
    SELECT
        DATE_TRUNC('month', hour_end) as month,
        AVG(coast) as avg_coast,
        AVG(east) as avg_east,
        AVG(far_west) as avg_far_west,
        AVG(north) as avg_north,
        AVG(north_c) as avg_north_c,
        AVG(southern) as avg_southern,
        AVG(south_c) as avg_south_c,
        AVG(west) as avg_west,
        AVG(ercot) as avg_ercot
    FROM public.ercot_load
    WHERE ercot IS NOT NULL
    GROUP BY DATE_TRUNC('month', hour_end)
)
SELECT
    month,
    'Coast' as zone,
    ROUND(avg_coast::numeric, 2) as avg_load_mw,
    ROUND((avg_coast / NULLIF(avg_ercot, 0) * 100)::numeric, 2) as percentage
FROM monthly_aggregated
UNION ALL
SELECT month, 'East', ROUND(avg_east::numeric, 2), ROUND((avg_east / NULLIF(avg_ercot, 0) * 100)::numeric, 2) FROM monthly_aggregated
UNION ALL
SELECT month, 'Far West', ROUND(avg_far_west::numeric, 2), ROUND((avg_far_west / NULLIF(avg_ercot, 0) * 100)::numeric, 2) FROM monthly_aggregated
UNION ALL
SELECT month, 'North', ROUND(avg_north::numeric, 2), ROUND((avg_north / NULLIF(avg_ercot, 0) * 100)::numeric, 2) FROM monthly_aggregated
UNION ALL
SELECT month, 'North Central', ROUND(avg_north_c::numeric, 2), ROUND((avg_north_c / NULLIF(avg_ercot, 0) * 100)::numeric, 2) FROM monthly_aggregated
UNION ALL
SELECT month, 'Southern', ROUND(avg_southern::numeric, 2), ROUND((avg_southern / NULLIF(avg_ercot, 0) * 100)::numeric, 2) FROM monthly_aggregated
UNION ALL
SELECT month, 'South Central', ROUND(avg_south_c::numeric, 2), ROUND((avg_south_c / NULLIF(avg_ercot, 0) * 100)::numeric, 2) FROM monthly_aggregated
UNION ALL
SELECT month, 'West', ROUND(avg_west::numeric, 2), ROUND((avg_west / NULLIF(avg_ercot, 0) * 100)::numeric, 2) FROM monthly_aggregated
ORDER BY month DESC, avg_load_mw DESC;
