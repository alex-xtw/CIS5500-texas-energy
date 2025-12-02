-- Calculates forecast accuracy metrics for each ERCOT region using staging.ercot_load_wide_compare.
-- Computes MSE (Mean Squared Error), MAE (Mean Absolute Error), MAPE (Mean Absolute Percentage Error), and R² (coefficient of determination).
-- Returns one row per region showing how well the predicted load values match actual observations. 
WITH pairs AS (
  -- One row per (hour, region) with actual (y) and expected (yhat)
  SELECT 'coast' AS region, coast_actual AS y, coast_expected AS yhat FROM staging.ercot_load_wide_compare
  UNION ALL SELECT 'east',       east_actual,       east_expected       FROM staging.ercot_load_wide_compare
  UNION ALL SELECT 'far_west',   far_west_actual,   far_west_expected   FROM staging.ercot_load_wide_compare
  UNION ALL SELECT 'north',      north_actual,      north_expected      FROM staging.ercot_load_wide_compare
  UNION ALL SELECT 'north_c',    north_c_actual,    north_c_expected    FROM staging.ercot_load_wide_compare
  UNION ALL SELECT 'southern',   southern_actual,   southern_expected   FROM staging.ercot_load_wide_compare
  UNION ALL SELECT 'south_c',    south_c_actual,    south_c_expected    FROM staging.ercot_load_wide_compare
  UNION ALL SELECT 'west',       west_actual,       west_expected       FROM staging.ercot_load_wide_compare
  UNION ALL SELECT 'ercot',      ercot_actual,      ercot_expected      FROM staging.ercot_load_wide_compare
),
means AS (
  SELECT region, AVG(y) AS y_bar
  FROM pairs
  GROUP BY region
)
SELECT
  p.region,
  COUNT(*) AS n,
  AVG( (p.y - p.yhat)^2 ) AS mse,
  AVG( ABS(p.y - p.yhat) ) AS mae,
  100.0 * AVG(CASE WHEN p.y = 0 THEN NULL ELSE ABS(p.y - p.yhat) / ABS(p.y) END) AS mape_pct,
  1.0 - (SUM( (p.y - p.yhat)^2 ) / NULLIF(SUM( (p.y - m.y_bar)^2 ), 0)) AS r2
FROM pairs p
JOIN means m USING (region)
GROUP BY p.region
ORDER BY p.region;
