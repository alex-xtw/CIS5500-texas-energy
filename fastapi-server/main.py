from fastapi import FastAPI, Query, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel, Field
from typing import Optional, List, Literal
from datetime import datetime, date
import psycopg2
from psycopg2.extras import RealDictCursor
from contextlib import contextmanager
import os
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

app = FastAPI(
    title="ERCOT Regional Load Data API",
    description="API for retrieving aggregated electricity demand data across ERCOT regions",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Database configuration
DB_CONFIG = {
    "host": os.getenv("DB_HOST", "cit5500db.cpye6sya8y1z.us-east-1.rds.amazonaws.com"),
    "port": os.getenv("DB_PORT", "5432"),
    "user": os.getenv("DB_USER", "cit5500projectDB"),
    "password": os.getenv("DB_PASSWORD", "3rc0t-Data"),
    "database": os.getenv("DB_NAME", "cit5500")
}

@contextmanager
def get_db_connection():
    """Context manager for database connections"""
    conn = psycopg2.connect(**DB_CONFIG)
    try:
        yield conn
    finally:
        conn.close()

@app.get("/")
def root():
    """Health check endpoint"""
    return {"status": "ok", "message": "CIS5500 Texas Energy API"}

@app.get("/health")
def health_check():
    """Database health check"""
    try:
        with get_db_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                cursor.fetchone()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}

# Pydantic Models
class HourlyLoadData(BaseModel):
    hour_end: datetime = Field(description="Timestamp marking the end of the hourly period")
    coast: float = Field(description="Total electricity demand for Coast region (MW)")
    east: float = Field(description="Total electricity demand for East region (MW)")
    far_west: float = Field(description="Total electricity demand for Far West region (MW)")
    north: float = Field(description="Total electricity demand for North region (MW)")
    north_c: float = Field(description="Total electricity demand for North Central region (MW)")
    southern: float = Field(description="Total electricity demand for Southern region (MW)")
    south_c: float = Field(description="Total electricity demand for South Central region (MW)")
    west: float = Field(description="Total electricity demand for West region (MW)")
    ercot: float = Field(description="Total electricity demand across entire ERCOT system (MW)")

class ForecastMetrics(BaseModel):
    region: str = Field(description="ERCOT region name")
    n: int = Field(description="Number of data points used in the calculation")
    mse: float = Field(description="Mean Squared Error between actual and forecasted values")
    mae: float = Field(description="Mean Absolute Error between actual and forecasted values")
    mape_pct: float = Field(description="Mean Absolute Percentage Error (as percentage)")
    r2: float = Field(ge=-1, le=1, description="R-squared (coefficient of determination) value")

class HeatwaveStreak(BaseModel):
    zone: str = Field(description="ERCOT zone code")
    streak_start: date = Field(description="Start date of the heatwave streak")
    streak_end: date = Field(description="End date of the heatwave streak")
    streak_days: int = Field(ge=1, description="Number of consecutive days in the heatwave")
    avg_peak_load_mw: Optional[float] = Field(None, description="Average daily peak load during the heatwave period (MW)")

class PrecipitationImpact(BaseModel):
    zone: str = Field(description="ERCOT zone code")
    rainy_day: bool = Field(description="Whether this row represents rainy days (true) or dry days (false)")
    avg_load_mw: float = Field(description="Average daily electricity load for this weather condition (MW)")
    num_days: int = Field(ge=0, description="Number of days in the dataset with this weather condition")

class ExtremeHeatLoad(BaseModel):
    zone: str = Field(description="ERCOT zone code")
    median_peak_load_mw: float = Field(description="Median of daily peak electricity load during extreme heat days (MW)")
    num_extreme_heat_days: int = Field(ge=0, description="Number of days that qualified as extreme heat within the analysis period")
    threshold_percentile: float = Field(ge=0, le=100, description="The percentile threshold used to define extreme heat")
    threshold_temp_f: float = Field(description="The temperature threshold in Fahrenheit corresponding to the percentile")

class LoadOutlierWeather(BaseModel):
    month_start: date = Field(description="Start date of the month (YYYY-MM-01)")
    outlier_group: Literal["high", "low"] = Field(description="Type of outlier - high (above mean + N*σ) or low (below mean - N*σ)")
    num_days: int = Field(ge=0, description="Number of outlier days in this month and group")
    avg_temp_c: float = Field(description="Average temperature in Celsius across outlier days")
    avg_rh_pct: float = Field(description="Average relative humidity percentage across outlier days")
    avg_precip_mm: float = Field(description="Average daily precipitation in millimeters across outlier days")
    avg_wind_kmh: float = Field(description="Average wind speed in km/h across outlier days")
    avg_pressure_hpa: float = Field(description="Average atmospheric pressure in hPa across outlier days")
    avg_cloud_cover_pct: float = Field(description="Average cloud cover percentage across outlier days")

class LoadOutlierWeatherResponse(BaseModel):
    data: List[LoadOutlierWeather]
    metadata: dict = Field(description="Metadata about the analysis parameters")

class LoadOutlier(BaseModel):
    hour_end: datetime = Field(description="Timestamp marking the end of the hourly period")
    region: str = Field(description="ERCOT region name")
    load_mw: float = Field(description="Actual load value (MW)")
    mean: float = Field(description="Statistical mean for this region (MW)")
    std_dev: float = Field(description="Standard deviation for this region (MW)")
    z_score: float = Field(description="Z-score (number of standard deviations from mean)")
    outlier_type: Literal["high", "low"] = Field(description="Type of outlier - high or low")

class LoadOutlierResponse(BaseModel):
    data: List[LoadOutlier]
    metadata: dict = Field(description="Metadata about the analysis parameters including threshold")

class LoadComparison(BaseModel):
    hour_end: datetime = Field(description="Timestamp marking the end of the hourly period")
    coast_actual: Optional[float] = Field(None, description="Actual electricity demand for Coast region (MW)")
    coast_expected: Optional[float] = Field(None, description="Expected electricity demand for Coast region (MW)")
    east_actual: Optional[float] = Field(None, description="Actual electricity demand for East region (MW)")
    east_expected: Optional[float] = Field(None, description="Expected electricity demand for East region (MW)")
    far_west_actual: Optional[float] = Field(None, description="Actual electricity demand for Far West region (MW)")
    far_west_expected: Optional[float] = Field(None, description="Expected electricity demand for Far West region (MW)")
    north_actual: Optional[float] = Field(None, description="Actual electricity demand for North region (MW)")
    north_expected: Optional[float] = Field(None, description="Expected electricity demand for North region (MW)")
    north_c_actual: Optional[float] = Field(None, description="Actual electricity demand for North Central region (MW)")
    north_c_expected: Optional[float] = Field(None, description="Expected electricity demand for North Central region (MW)")
    southern_actual: Optional[float] = Field(None, description="Actual electricity demand for Southern region (MW)")
    southern_expected: Optional[float] = Field(None, description="Expected electricity demand for Southern region (MW)")
    south_c_actual: Optional[float] = Field(None, description="Actual electricity demand for South Central region (MW)")
    south_c_expected: Optional[float] = Field(None, description="Expected electricity demand for South Central region (MW)")
    west_actual: Optional[float] = Field(None, description="Actual electricity demand for West region (MW)")
    west_expected: Optional[float] = Field(None, description="Expected electricity demand for West region (MW)")
    ercot_actual: Optional[float] = Field(None, description="Actual total electricity demand across entire ERCOT system (MW)")
    ercot_expected: Optional[float] = Field(None, description="Expected total electricity demand across entire ERCOT system (MW)")

# API Endpoints
@app.get("/load/hourly", response_model=List[HourlyLoadData], tags=["Load Data"])
def get_hourly_load(
    start_date: Optional[datetime] = Query(None, description="Start date"),
    end_date: Optional[datetime] = Query(None, description="End date")
):
    """
    Retrieves hourly electricity demand data aggregated across all ERCOT regions.
    Returns time-series data showing regional load trends ordered chronologically.
    """
    query = """
        SELECT hour_end, coast, east, far_west, north, north_c, southern, south_c, west, ercot
        FROM ercot_load
        WHERE 1=1
    """
    params = []

    if start_date:
        query += " AND hour_end >= %s"
        params.append(start_date)
    if end_date:
        query += " AND hour_end <= %s"
        params.append(end_date)

    query += " ORDER BY hour_end"

    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                logger.info(f"[GET /load/hourly] Query: {query}")
                logger.info(f"[GET /load/hourly] Params: {params}")
                cursor.execute(query, params)
                results = cursor.fetchall()
                logger.info(f"[GET /load/hourly] Returned {len(results)} rows")
                return results
    except Exception as e:
        logger.error(f"[GET /load/hourly] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/load/comparison", response_model=List[LoadComparison], tags=["Load Data"])
def get_load_comparison(
    start_date: Optional[datetime] = Query(None, description="Start date"),
    end_date: Optional[datetime] = Query(None, description="End date"),
    region: Optional[str] = Query(None, description="Filter by specific region(s). Comma-separated for multiple regions. Options: coast, east, far_west, north, north_c, southern, south_c, west, ercot"),
    model: str = Query("statistical", description="Model type to use for comparison. Options: 'statistical' (default) or 'xgb'")
):
    """
    Retrieves both expected and actual electricity demand data for all ERCOT regions.
    Uses either staging.ercot_load_wide_compare (statistical model) or staging.ercot_load_wide_compare_xgb (XGBoost model).
    Returns time-series data comparing forecasted vs actual loads ordered chronologically.
    When region filter is applied, only returns data for the specified region(s).
    """
    # Validate model parameter
    valid_models = ['statistical', 'xgb']
    if model not in valid_models:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid model: {model}. Valid options are: {', '.join(valid_models)}"
        )

    # Select the appropriate comparison table based on model
    compare_table = "staging.ercot_load_wide_compare" if model == "statistical" else "staging.ercot_load_wide_compare_xgb"

    # Parse regions if provided
    selected_regions = None
    if region:
        selected_regions = [r.strip() for r in region.split(',')]
        valid_regions = ['coast', 'east', 'far_west', 'north', 'north_c', 'southern', 'south_c', 'west', 'ercot']
        invalid_regions = [r for r in selected_regions if r not in valid_regions]
        if invalid_regions:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid region(s): {', '.join(invalid_regions)}. Valid options are: {', '.join(valid_regions)}"
            )

    # Build SELECT clause based on region filter
    if selected_regions:
        select_fields = ["hour_end"]
        for reg in selected_regions:
            select_fields.append(f"{reg}_actual")
            select_fields.append(f"{reg}_expected")
        select_clause = ",\n            ".join(select_fields)
    else:
        select_clause = """hour_end,
            coast_actual,
            coast_expected,
            east_actual,
            east_expected,
            far_west_actual,
            far_west_expected,
            north_actual,
            north_expected,
            north_c_actual,
            north_c_expected,
            southern_actual,
            southern_expected,
            south_c_actual,
            south_c_expected,
            west_actual,
            west_expected,
            ercot_actual,
            ercot_expected"""

    query = f"""
        SELECT
            {select_clause}
        FROM {compare_table}
        WHERE 1=1
    """
    params = []

    if start_date:
        query += " AND hour_end >= %s"
        params.append(start_date)
    if end_date:
        query += " AND hour_end <= %s"
        params.append(end_date)

    query += " ORDER BY hour_end"

    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                logger.info(f"[GET /load/comparison] Query: {query}")
                logger.info(f"[GET /load/comparison] Params: {params}")
                cursor.execute(query, params)
                results = cursor.fetchall()
                logger.info(f"[GET /load/comparison] Returned {len(results)} rows")
                return results
    except Exception as e:
        logger.error(f"[GET /load/comparison] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/forecast/metrics", response_model=List[ForecastMetrics], tags=["Forecast"])
def get_forecast_metrics(
    start_date: Optional[datetime] = Query(None, description="Start date for analysis period"),
    end_date: Optional[datetime] = Query(None, description="End date for analysis period"),
    region: Optional[str] = Query(None, description="Filter results by specific region(s). Comma-separated for multiple regions."),
    model: str = Query("statistical", description="Model type to use for metrics calculation. Options: 'statistical' (default) or 'xgb'")
):
    """
    Retrieves statistical metrics comparing forecasted vs actual electricity demand
    for each ERCOT region, including MSE, MAE, MAPE, and R-squared values.
    Uses either staging.ercot_load_wide_compare (statistical model) or staging.ercot_load_wide_compare_xgb (XGBoost model).
    Always returns all metrics (n, mse, mae, mape_pct, r2).
    """
    # Validate model parameter
    valid_models = ['statistical', 'xgb']
    if model not in valid_models:
        raise HTTPException(
            status_code=400,
            detail=f"Invalid model: {model}. Valid options are: {', '.join(valid_models)}"
        )

    # Select the appropriate comparison table based on model
    compare_table = "staging.ercot_load_wide_compare" if model == "statistical" else "staging.ercot_load_wide_compare_xgb"

    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Check if the selected comparison table exists
                table_name = compare_table.split('.')[1]
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'staging'
                        AND table_name = %s
                    );
                """, (table_name,))
                table_exists = cursor.fetchone()['exists']

                if not table_exists:
                    raise HTTPException(
                        status_code=501,
                        detail=f"{compare_table} table not yet implemented. Please create the table first."
                    )

                # Build the base query with date filtering in the source data
                base_filter = "WHERE 1=1"
                base_params = []

                if start_date:
                    base_filter += f" AND hour_end >= %s"
                    base_params.append(start_date)
                if end_date:
                    base_filter += f" AND hour_end <= %s"
                    base_params.append(end_date)

                query = f"""
                    WITH filtered_data AS (
                      SELECT * FROM {compare_table}
                      {base_filter}
                    ),
                    pairs AS (
                      SELECT 'coast' AS region, coast_actual AS y, coast_expected AS yhat
                      FROM filtered_data
                      UNION ALL SELECT 'east', east_actual, east_expected
                      FROM filtered_data
                      UNION ALL SELECT 'far_west', far_west_actual, far_west_expected
                      FROM filtered_data
                      UNION ALL SELECT 'north', north_actual, north_expected
                      FROM filtered_data
                      UNION ALL SELECT 'north_c', north_c_actual, north_c_expected
                      FROM filtered_data
                      UNION ALL SELECT 'southern', southern_actual, southern_expected
                      FROM filtered_data
                      UNION ALL SELECT 'south_c', south_c_actual, south_c_expected
                      FROM filtered_data
                      UNION ALL SELECT 'west', west_actual, west_expected
                      FROM filtered_data
                      UNION ALL SELECT 'ercot', ercot_actual, ercot_expected
                      FROM filtered_data
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
                """
                params = base_params.copy()

                if region:
                    regions = [r.strip() for r in region.split(',')]
                    query += " WHERE p.region = ANY(%s)"
                    params.append(regions)

                query += " GROUP BY p.region ORDER BY p.region"

                logger.info(f"[GET /forecast/metrics] Query: {query}")
                logger.info(f"[GET /forecast/metrics] Params: {params}")
                cursor.execute(query, params)
                results = cursor.fetchall()
                logger.info(f"[GET /forecast/metrics] Returned {len(results)} rows")
                return results
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[GET /forecast/metrics] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/weather/heatwaves", response_model=List[HeatwaveStreak], tags=["Weather Analysis"])
def get_heatwave_streaks(
    zone: Optional[str] = Query(None, description="Filter by specific ERCOT zone(s). Comma-separated for multiple zones."),
    min_temp_f: float = Query(100.0, description="Minimum temperature threshold in Fahrenheit for heatwave definition"),
    min_days: int = Query(3, ge=1, description="Minimum consecutive days required to qualify as a heatwave"),
    start_date: Optional[date] = Query(None, description="Filter heatwaves starting on or after this date"),
    end_date: Optional[date] = Query(None, description="Filter heatwaves ending on or before this date")
):
    """
    Identifies heatwave periods (consecutive days with max temp >= threshold) for ERCOT zones.
    Returns streak start/end dates, duration, and average peak load during each heatwave.

    A heatwave is defined as consecutive days where the daily maximum temperature meets or exceeds
    the specified threshold, with a minimum number of consecutive days required.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = f"""
                    WITH weather_zone_daily AS (
                      SELECT
                        (wh.time AT TIME ZONE 'UTC')::date AS day_utc,
                        szm.zone,
                        MAX( (wh.temperature_2m_c * 9.0/5.0) + 32.0 ) AS temp_max_f
                      FROM weather_hourly wh
                      JOIN station_zone_map szm
                        ON szm.station_id = wh.station_id
                      GROUP BY (wh.time AT TIME ZONE 'UTC')::date, szm.zone
                    ),
                    hot_only AS (
                      -- Keep only hot days >= threshold
                      SELECT
                        zone,
                        day_utc,
                        temp_max_f
                      FROM weather_zone_daily
                      WHERE temp_max_f >= %s
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
                      -- Keep only streaks with length >= min_days
                      SELECT
                        zone,
                        streak_id,
                        MIN(day_utc) AS streak_start,
                        MAX(day_utc) AS streak_end,
                        COUNT(*)     AS streak_days
                      FROM streaks
                      GROUP BY zone, streak_id
                      HAVING COUNT(*) >= %s
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
                    WHERE 1=1
                """

                params = [min_temp_f, min_days]

                if zone:
                    zones = [z.strip() for z in zone.split(',')]
                    query += " AND s.zone = ANY(%s)"
                    params.append(zones)
                if start_date:
                    query += " AND s.streak_start >= %s"
                    params.append(start_date)
                if end_date:
                    query += " AND s.streak_end <= %s"
                    params.append(end_date)

                query += """
                    GROUP BY
                      s.zone, s.streak_start, s.streak_end, s.streak_days
                    ORDER BY
                      s.zone, s.streak_start
                """

                logger.info(f"[GET /weather/heatwaves] Query: {query}")
                logger.info(f"[GET /weather/heatwaves] Params: {params}")
                cursor.execute(query, params)
                results = cursor.fetchall()
                logger.info(f"[GET /weather/heatwaves] Returned {len(results)} rows")
                return results
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[GET /weather/heatwaves] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/weather/precipitation", response_model=List[PrecipitationImpact], tags=["Weather Analysis"])
def get_precipitation_load_impact(
    zone: Optional[str] = Query(None, description="Filter by specific ERCOT zone(s). Comma-separated for multiple zones."),
    start_date: Optional[date] = Query(None, description="Start date for analysis period"),
    end_date: Optional[date] = Query(None, description="End date for analysis period")
):
    """
    Analyzes the impact of precipitation on electricity demand by comparing average
    daily load on rainy days versus dry days for each ERCOT zone.

    A rainy day is defined as any day where total precipitation > 0mm.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = """
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
                    WHERE 1=1
                """
                params = []

                if zone:
                    zones = [z.strip() for z in zone.split(',')]
                    query += " AND wzd.zone = ANY(%s)"
                    params.append(zones)
                if start_date:
                    query += " AND wzd.day_utc >= %s"
                    params.append(start_date)
                if end_date:
                    query += " AND wzd.day_utc <= %s"
                    params.append(end_date)

                query += " GROUP BY wzd.zone, wzd.rainy_day ORDER BY wzd.zone, wzd.rainy_day DESC"

                logger.info(f"[GET /weather/precipitation] Query: {query}")
                logger.info(f"[GET /weather/precipitation] Params: {params}")
                cursor.execute(query, params)
                results = cursor.fetchall()
                logger.info(f"[GET /weather/precipitation] Returned {len(results)} rows")
                return results
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[GET /weather/precipitation] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/load/peak-load-extreme-heat", response_model=List[ExtremeHeatLoad], tags=["Load Data"])
def get_peak_load_extreme_heat(
    zone: Optional[str] = Query(None, description="Filter by specific ERCOT zone(s). Comma-separated for multiple zones."),
    start_date: Optional[date] = Query(None, description="Start date for analysis period (UTC)"),
    end_date: Optional[date] = Query(None, description="End date for analysis period (UTC)"),
    threshold: float = Query(99, ge=0, le=100, description="Percentile threshold for defining extreme heat (0-100)")
):
    """
    Analyzes electricity demand on the hottest days by calculating the median daily
    peak load for extreme heat conditions. Returns the median peak load, threshold
    temperature, and number of extreme heat days.

    Extreme heat is defined as days where the daily maximum temperature exceeds
    the specified percentile threshold for that zone.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Convert threshold percentage to decimal for percentile_cont
                percentile_decimal = threshold / 100.0

                query = f"""
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
                        percentile_cont(%s) WITHIN GROUP (ORDER BY temp_max_f) AS p_threshold_temp_f
                      FROM weather_zone_daily
                      GROUP BY zone
                    )
                    SELECT
                      wzd.zone,
                      percentile_cont(0.5) WITHIN GROUP (ORDER BY dpl.daily_peak_mw) AS median_peak_load_mw,
                      COUNT(*) AS num_extreme_heat_days,
                      %s AS threshold_percentile,
                      hc.p_threshold_temp_f AS threshold_temp_f
                    FROM weather_zone_daily wzd
                    JOIN hot_cutoff hc USING (zone)
                    JOIN daily_peak_load dpl USING (zone, day_utc)
                    WHERE wzd.temp_max_f >= hc.p_threshold_temp_f
                """

                params = [percentile_decimal, threshold]

                # Add date filters if provided
                if start_date:
                    query += " AND wzd.day_utc >= %s"
                    params.append(start_date)
                if end_date:
                    query += " AND wzd.day_utc <= %s"
                    params.append(end_date)

                # Add zone filter if provided
                if zone:
                    zones = [z.strip() for z in zone.split(',')]
                    query += " AND wzd.zone = ANY(%s)"
                    params.append(zones)

                query += " GROUP BY wzd.zone, hc.p_threshold_temp_f ORDER BY wzd.zone"

                logger.info(f"[GET /load/peak-load-extreme-heat] Query: {query}")
                logger.info(f"[GET /load/peak-load-extreme-heat] Params: {params}")
                cursor.execute(query, params)
                results = cursor.fetchall()
                logger.info(f"[GET /load/peak-load-extreme-heat] Returned {len(results)} rows")
                return results
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[GET /load/peak-load-extreme-heat] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/load/outliers/weather-conditions", response_model=LoadOutlierWeatherResponse, tags=["Load Data"])
def get_load_outliers_weather_conditions(
    start_date: Optional[date] = Query(None, description="Start date for analysis period (UTC)"),
    end_date: Optional[date] = Query(None, description="End date for analysis period (UTC)"),
    month: Optional[str] = Query(None, description="Filter to specific month(s). Comma-separated values (YYYY-MM format)."),
    outlier_type: Optional[Literal["high", "low"]] = Query(None, description="Filter by outlier type (high or low)"),
    std_dev_threshold: float = Query(3, ge=1, le=5, description="Standard deviation threshold for defining outliers")
):
    """
    Identifies days with unusually high or low electricity demand (outliers defined as
    daily average load beyond ±N standard deviations from the monthly mean) and analyzes
    the average weather conditions on those outlier days.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                query = f"""
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
                          WHEN dl.daily_avg_mw > ms.mu + {std_dev_threshold}*ms.sigma THEN 'high'
                          WHEN dl.daily_avg_mw < ms.mu - {std_dev_threshold}*ms.sigma THEN 'low'
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
                        AVG(wh.cloud_cover_mid_percent)      AS cloud_cover_pct_avg
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
                """
                params = []

                if start_date:
                    query += " AND od.month_start >= %s"
                    params.append(start_date)
                if end_date:
                    query += " AND od.month_start <= %s"
                    params.append(end_date)
                if month:
                    months = [m.strip() + "-01" for m in month.split(',')]
                    query += " AND od.month_start = ANY(%s)"
                    params.append(months)
                if outlier_type:
                    query += " AND od.outlier_group = %s"
                    params.append(outlier_type)

                query += " GROUP BY od.month_start, od.outlier_group ORDER BY od.month_start, od.outlier_group DESC"

                logger.info(f"[GET /load/outliers/weather-conditions] Query: {query}")
                logger.info(f"[GET /load/outliers/weather-conditions] Params: {params}")
                cursor.execute(query, params)
                results = cursor.fetchall()
                logger.info(f"[GET /load/outliers/weather-conditions] Returned {len(results)} rows")

                return {
                    "data": results,
                    "metadata": {
                        "std_dev_threshold": std_dev_threshold,
                        "description": f"Outliers defined as days with average load beyond ±{std_dev_threshold} standard deviations from monthly mean"
                    }
                }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[GET /load/outliers/weather-conditions] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

@app.get("/load/outliers", response_model=LoadOutlierResponse, tags=["Load Data"])
def get_load_outliers(
    start_date: Optional[datetime] = Query(None, description="Start date for analysis period"),
    end_date: Optional[datetime] = Query(None, description="End date for analysis period"),
    region: Optional[str] = Query(None, description="Filter by specific region(s). Comma-separated for multiple regions. Options: coast, east, far_west, north, north_c, southern, south_c, west, ercot"),
    outlier_type: Optional[Literal["high", "low"]] = Query(None, description="Filter by outlier type (high or low)"),
    std_dev_threshold: float = Query(3.0, ge=1.0, le=5.0, description="Standard deviation threshold for defining outliers (default: 3)"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records to return")
):
    """
    Identifies outlier electricity load values using statistical analysis (±N standard deviations).

    An outlier is defined as a load value that falls outside the range:
    - High outlier: load > mean + (N × standard deviation)
    - Low outlier: load < mean - (N × standard deviation)

    Returns hourly load data points that qualify as outliers along with their statistical metrics.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Parse regions if provided
                selected_regions = None
                if region:
                    selected_regions = [r.strip() for r in region.split(',')]
                    valid_regions = ['coast', 'east', 'far_west', 'north', 'north_c', 'southern', 'south_c', 'west', 'ercot']
                    invalid_regions = [r for r in selected_regions if r not in valid_regions]
                    if invalid_regions:
                        raise HTTPException(
                            status_code=400,
                            detail=f"Invalid region(s): {', '.join(invalid_regions)}. Valid options are: {', '.join(valid_regions)}"
                        )

                # Build the query to detect outliers
                query = """
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
                """

                params = []

                # Add date filters to filtered_load CTE
                if start_date:
                    query += " AND hour_end >= %s"
                    params.append(start_date)
                if end_date:
                    query += " AND hour_end <= %s"
                    params.append(end_date)

                query += """
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
                          WHEN fl.load_mw > rs.mean + %s * rs.std_dev THEN 'high'
                          WHEN fl.load_mw < rs.mean - %s * rs.std_dev THEN 'low'
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
                """

                params.extend([std_dev_threshold, std_dev_threshold])

                # Add region filter if provided
                if selected_regions:
                    query += " AND region = ANY(%s)"
                    params.append(selected_regions)

                # Add outlier type filter if provided
                if outlier_type:
                    query += " AND outlier_type = %s"
                    params.append(outlier_type)

                query += " ORDER BY hour_end DESC, region LIMIT %s"
                params.append(limit)

                logger.info(f"[GET /load/outliers] Query: {query}")
                logger.info(f"[GET /load/outliers] Params: {params}")
                cursor.execute(query, params)
                results = cursor.fetchall()
                logger.info(f"[GET /load/outliers] Returned {len(results)} rows")

                return {
                    "data": results,
                    "metadata": {
                        "std_dev_threshold": std_dev_threshold,
                        "description": f"Outliers defined as load values beyond ±{std_dev_threshold} standard deviations from the mean",
                        "date_range": {
                            "start": start_date.isoformat() if start_date else None,
                            "end": end_date.isoformat() if end_date else None
                        }
                    }
                }
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[GET /load/outliers] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
 