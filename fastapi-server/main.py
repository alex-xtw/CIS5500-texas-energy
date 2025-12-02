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
    zone_code: str = Field(description="ERCOT zone code")
    streak_start: date = Field(description="Start date of the heatwave streak")
    streak_end: date = Field(description="End date of the heatwave streak")
    streak_days: int = Field(ge=1, description="Number of consecutive days in the heatwave")
    max_temp_f: float = Field(description="Maximum temperature reached during the heatwave (Fahrenheit)")
    avg_peak_load_mw: float = Field(description="Average daily peak load during the heatwave period (MW)")

class PrecipitationImpact(BaseModel):
    zone_code: str = Field(description="ERCOT zone code")
    rainy_day: bool = Field(description="Whether this row represents rainy days (true) or dry days (false)")
    avg_load_mw: float = Field(description="Average daily electricity load for this weather condition (MW)")
    num_days: int = Field(ge=0, description="Number of days in the dataset with this weather condition")

class ExtremeHeatLoad(BaseModel):
    zone_code: str = Field(description="ERCOT zone code")
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
    end_date: Optional[datetime] = Query(None, description="End date"),
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records to return")
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

    query += " ORDER BY hour_end LIMIT %s"
    params.append(limit)

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
    limit: int = Query(1000, ge=1, le=10000, description="Maximum number of records to return")
):
    """
    Retrieves both expected and actual electricity demand data for all ERCOT regions.
    Joins data from staging.ercot_load_wide_expected and public.ercot_load tables.
    Returns time-series data comparing forecasted vs actual loads ordered chronologically.
    When region filter is applied, only returns data for the specified region(s).
    """
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
        select_fields = ["COALESCE(a.hour_end, e.hour_end) as hour_end"]
        for reg in selected_regions:
            select_fields.append(f"a.{reg} as {reg}_actual")
            select_fields.append(f"e.{reg} as {reg}_expected")
        select_clause = ",\n            ".join(select_fields)
    else:
        select_clause = """COALESCE(a.hour_end, e.hour_end) as hour_end,
            a.coast as coast_actual,
            e.coast as coast_expected,
            a.east as east_actual,
            e.east as east_expected,
            a.far_west as far_west_actual,
            e.far_west as far_west_expected,
            a.north as north_actual,
            e.north as north_expected,
            a.north_c as north_c_actual,
            e.north_c as north_c_expected,
            a.southern as southern_actual,
            e.southern as southern_expected,
            a.south_c as south_c_actual,
            e.south_c as south_c_expected,
            a.west as west_actual,
            e.west as west_expected,
            a.ercot as ercot_actual,
            e.ercot as ercot_expected"""

    query = f"""
        SELECT
            {select_clause}
        FROM public.ercot_load a
        FULL OUTER JOIN staging.ercot_load_wide_expected e ON a.hour_end = e.hour_end
        WHERE 1=1
    """
    params = []

    if start_date:
        query += " AND COALESCE(a.hour_end, e.hour_end) >= %s"
        params.append(start_date)
    if end_date:
        query += " AND COALESCE(a.hour_end, e.hour_end) <= %s"
        params.append(end_date)

    query += " ORDER BY COALESCE(a.hour_end, e.hour_end) LIMIT %s"
    params.append(limit)

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
    region: Optional[str] = Query(None, description="Filter results by specific region(s). Comma-separated for multiple regions.")
):
    """
    Retrieves statistical metrics comparing forecasted vs actual electricity demand
    for each ERCOT region, including MSE, MAE, MAPE, and R-squared values.
    Calculates metrics dynamically from staging.ercot_load_wide_compare table.
    Always returns all metrics (n, mse, mae, mape_pct, r2).
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Check if ercot_load_wide_compare table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'staging'
                        AND table_name = 'ercot_load_wide_compare'
                    );
                """)
                table_exists = cursor.fetchone()['exists']

                if not table_exists:
                    raise HTTPException(
                        status_code=501,
                        detail="staging.ercot_load_wide_compare table not yet implemented. Please create the table first."
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
                      SELECT * FROM staging.ercot_load_wide_compare
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

    Note: Requires heatwave_streaks table/view to be created in the database.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = 'heatwave_streaks'
                    );
                """)
                table_exists = cursor.fetchone()['exists']

                if not table_exists:
                    raise HTTPException(
                        status_code=501,
                        detail="heatwave_streaks table not yet implemented. Please create the table or view first."
                    )

                query = """
                    SELECT zone_code, streak_start, streak_end, streak_days, max_temp_f, avg_peak_load_mw
                    FROM heatwave_streaks
                    WHERE max_temp_f >= %s AND streak_days >= %s
                """
                params = [min_temp_f, min_days]

                if zone:
                    zones = [z.strip() for z in zone.split(',')]
                    query += " AND zone_code = ANY(%s)"
                    params.append(zones)
                if start_date:
                    query += " AND streak_start >= %s"
                    params.append(start_date)
                if end_date:
                    query += " AND streak_end <= %s"
                    params.append(end_date)

                query += " ORDER BY zone_code, streak_start"

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

    Note: Requires precipitation_impact table/view to be created in the database.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = 'precipitation_impact'
                    );
                """)
                table_exists = cursor.fetchone()['exists']

                if not table_exists:
                    raise HTTPException(
                        status_code=501,
                        detail="precipitation_impact table not yet implemented. Please create the table or view first."
                    )

                query = """
                    SELECT zone_code, rainy_day, avg_load_mw, num_days
                    FROM precipitation_impact
                    WHERE 1=1
                """
                params = []

                if zone:
                    zones = [z.strip() for z in zone.split(',')]
                    query += " AND zone_code = ANY(%s)"
                    params.append(zones)
                if start_date:
                    query += " AND date >= %s"
                    params.append(start_date)
                if end_date:
                    query += " AND date <= %s"
                    params.append(end_date)

                query += " ORDER BY zone_code, rainy_day DESC"

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
    zone: Optional[str] = Query(None, description="Filter by specific ERCOT zone. If omitted, returns data for all zones."),
    start_date: Optional[date] = Query(None, description="Start date for analysis period (UTC)"),
    end_date: Optional[date] = Query(None, description="End date for analysis period (UTC)"),
    threshold: float = Query(99, ge=0, le=100, description="Percentile threshold for defining extreme heat (0-100)")
):
    """
    Analyzes electricity demand on the hottest days by calculating the median daily
    peak load for extreme heat conditions. Returns the median peak load, threshold
    temperature, and number of extreme heat days.

    Note: Requires extreme_heat_load table/view to be created in the database.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = 'extreme_heat_load'
                    );
                """)
                table_exists = cursor.fetchone()['exists']

                if not table_exists:
                    raise HTTPException(
                        status_code=501,
                        detail="extreme_heat_load table not yet implemented. Please create the table or view first."
                    )

                query = """
                    SELECT zone_code, median_peak_load_mw, num_extreme_heat_days,
                           threshold_percentile, threshold_temp_f
                    FROM extreme_heat_load
                    WHERE threshold_percentile = %s
                """
                params = [threshold]

                if zone:
                    query += " AND zone_code = %s"
                    params.append(zone)
                if start_date:
                    query += " AND date >= %s"
                    params.append(start_date)
                if end_date:
                    query += " AND date <= %s"
                    params.append(end_date)

                query += " ORDER BY zone_code"

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

    Note: Requires load_outlier_weather table/view to be created in the database.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = 'load_outlier_weather'
                    );
                """)
                table_exists = cursor.fetchone()['exists']

                if not table_exists:
                    raise HTTPException(
                        status_code=501,
                        detail="load_outlier_weather table not yet implemented. Please create the table or view first."
                    )

                query = """
                    SELECT month_start, outlier_group, num_days, avg_temp_c, avg_rh_pct,
                           avg_precip_mm, avg_wind_kmh, avg_pressure_hpa, avg_cloud_cover_pct
                    FROM load_outlier_weather
                    WHERE std_dev_threshold = %s
                """
                params = [std_dev_threshold]

                if start_date:
                    query += " AND month_start >= %s"
                    params.append(start_date)
                if end_date:
                    query += " AND month_start <= %s"
                    params.append(end_date)
                if month:
                    months = [m.strip() + "-01" for m in month.split(',')]
                    query += " AND month_start = ANY(%s)"
                    params.append(months)
                if outlier_type:
                    query += " AND outlier_group = %s"
                    params.append(outlier_type)

                query += " ORDER BY month_start, outlier_group"

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
 
    

#7 
@app.get("/load/daily-outliers", response_model=LoadOutlierWeatherResponse, tags=["Load Data"])
def get_daily_load_outliers(
    start_month: Optional[str] = Query(None, description="Start month in YYYY-MM format"),
    end_month: Optional[str] = Query(None, description="End month in YYYY-MM format"),
    outlier_type: Optional[Literal["high", "low"]] = Query(None, description="Filter by outlier type (high or low)"),
    std_dev_threshold: float = Query(3.0, ge=1.0, le=5.0, description="Standard deviation threshold for defining outliers (1-5)")
):
    """
    Identifies daily electricity load outliers within each month (defined as ±N standard deviations
    from the monthly mean) and analyzes average weather conditions during those outlier days.
    
    Returns for each month and outlier group:
    - Number of outlier days
    - Average temperature, humidity, precipitation, wind speed, pressure, and cloud cover
    
    Note: Requires ercot_load and weather_hourly tables to exist in the database.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Build the query with the provided threshold
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
                        AVG(daily_avg_mw) AS mu,
                        STDDEV_SAMP(daily_avg_mw) AS sigma
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
                        AVG(wh.temperature_2m_c) AS temp_c_avg,
                        AVG(wh.relative_humidity_2m_percent) AS rh_pct_avg,
                        SUM(wh.precipitation_mm) AS precip_mm_sum,
                        AVG(wh.wind_speed_10m_kmh) AS wind_10m_kmh_avg,
                        AVG(wh.pressure_msl_hpa) AS pressure_hpa_avg,
                        AVG(wh.cloud_cover_percent) AS cloud_cover_pct_avg
                      FROM weather_hourly wh
                      GROUP BY (wh.time AT TIME ZONE 'UTC')::date
                    )
                    SELECT
                      od.month_start,
                      od.outlier_group,
                      COUNT(*) AS num_days,
                      AVG(dw.temp_c_avg) AS avg_temp_c,
                      AVG(dw.rh_pct_avg) AS avg_rh_pct,
                      AVG(dw.precip_mm_sum) AS avg_precip_mm,
                      AVG(dw.wind_10m_kmh_avg) AS avg_wind_kmh,
                      AVG(dw.pressure_hpa_avg) AS avg_pressure_hpa,
                      AVG(dw.cloud_cover_pct_avg) AS avg_cloud_cover_pct
                    FROM outlier_days od
                    JOIN daily_weather dw ON dw.day_utc = od.day_utc
                    WHERE od.outlier_group IS NOT NULL
                """
                
                params = []
                
                if start_month:
                    query += " AND od.month_start >= %s"
                    params.append(start_month + "-01")
                if end_month:
                    query += " AND od.month_start <= %s"
                    params.append(end_month + "-01")
                if outlier_type:
                    query += " AND od.outlier_group = %s"
                    params.append(outlier_type)
                
                query += " GROUP BY od.month_start, od.outlier_group ORDER BY od.month_start, od.outlier_group DESC"
                
                logger.info(f"[GET /load/daily-outliers] Query: {query}")
                logger.info(f"[GET /load/daily-outliers] Params: {params}")
                cursor.execute(query, params)
                results = cursor.fetchall()
                logger.info(f"[GET /load/daily-outliers] Returned {len(results)} rows")
                
                return {
                    "data": results,
                    "metadata": {
                        "std_dev_threshold": std_dev_threshold,
                        "description": f"Daily load outliers defined as average daily load beyond ±{std_dev_threshold}σ from monthly mean"
                    }
                }
    except Exception as e:
        logger.error(f"[GET /load/daily-outliers] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

#6
@app.get("/load/extreme-heat-analysis", response_model=List[ExtremeHeatLoad], tags=["Load Data"])
def get_extreme_heat_load_analysis(
    zone: Optional[str] = Query(None, description="Filter by specific ERCOT zone(s). Comma-separated for multiple zones."),
    percentile_threshold: float = Query(99.0, ge=50, le=100, description="Percentile threshold for extreme heat definition (50-100)")
):
    """
    Analyzes electricity demand during extreme heat conditions by identifying the hottest days
    in each zone (defined by percentile threshold of daily max temperature) and calculating
    the median peak load during those extreme heat days.
    
    Returns for each zone:
    - Median daily peak load during extreme heat days (MW)
    - Number of days qualifying as extreme heat
    - Temperature threshold (percentile) and actual temperature value
    
    Note: Requires weather_hourly, station_zone_map, and ercot_load tables to exist in the database.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Check if required tables exist
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = 'weather_hourly'
                    )
                    AND EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = 'station_zone_map'
                    )
                    AND EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = 'ercot_load'
                    );
                """)
                tables_exist = cursor.fetchone()['exists']

                if not tables_exist:
                    raise HTTPException(
                        status_code=501,
                        detail="Required tables (weather_hourly, station_zone_map, ercot_load) not found in database."
                    )

                query = f"""
                    WITH weather_zone_daily AS (
                      SELECT
                        (wh.time AT TIME ZONE 'UTC')::date AS day_utc,
                        szm.zone_code,
                        MAX((wh.temperature_2m_c * 9.0/5.0) + 32.0) AS temp_max_f
                      FROM weather_hourly wh
                      JOIN station_zone_map szm
                        ON szm.station_id = wh.station_id
                      GROUP BY (wh.time AT TIME ZONE 'UTC')::date, szm.zone_code
                    ),
                    daily_peak_load AS (
                      SELECT
                        (el.hour_end AT TIME ZONE 'UTC')::date AS day_utc,
                        z.zone_code,
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
                      ) AS z(zone_code, load_mw)
                      GROUP BY (el.hour_end AT TIME ZONE 'UTC')::date, z.zone_code
                    ),
                    hot_cutoff AS (
                      SELECT
                        zone_code,
                        percentile_cont(%s) WITHIN GROUP (ORDER BY temp_max_f) AS p_threshold_temp_f
                      FROM weather_zone_daily
                      GROUP BY zone_code
                    )
                    SELECT
                      wzd.zone_code,
                      percentile_cont(0.5) WITHIN GROUP (ORDER BY dpl.daily_peak_mw) AS median_peak_load_mw,
                      COUNT(*) AS num_extreme_heat_days,
                      %s AS threshold_percentile,
                      hc.p_threshold_temp_f AS threshold_temp_f
                    FROM weather_zone_daily wzd
                    JOIN hot_cutoff hc USING (zone_code)
                    JOIN daily_peak_load dpl USING (zone_code, day_utc)
                    WHERE wzd.temp_max_f >= hc.p_threshold_temp_f
                """
                
                params = [(percentile_threshold / 100.0), percentile_threshold]
                
                if zone:
                    zones = [z.strip() for z in zone.split(',')]
                    query += " AND wzd.zone_code = ANY(%s)"
                    params.append(zones)
                
                query += " GROUP BY wzd.zone_code, hc.p_threshold_temp_f ORDER BY wzd.zone_code"
                
                logger.info(f"[GET /load/extreme-heat-analysis] Query: {query}")
                logger.info(f"[GET /load/extreme-heat-analysis] Params: {params}")
                cursor.execute(query, params)
                results = cursor.fetchall()
                logger.info(f"[GET /load/extreme-heat-analysis] Returned {len(results)} rows")
                
                return results
    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"[GET /load/extreme-heat-analysis] Error: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))
