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

@app.get("/forecast/metrics", response_model=List[ForecastMetrics], tags=["Forecast"])
def get_forecast_metrics(
    region: Optional[str] = Query(None, description="Filter results by specific region(s). Comma-separated for multiple regions."),
    metric: Optional[str] = Query(None, description="Filter to return only specific metric(s). Comma-separated for multiple metrics.")
):
    """
    Retrieves statistical metrics comparing forecasted vs actual electricity demand
    for each ERCOT region, including MSE, MAE, MAPE, and R-squared values.

    Note: Requires forecast_metrics table/view to be created in the database.
    """
    try:
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=RealDictCursor) as cursor:
                # Check if forecast_metrics table exists
                cursor.execute("""
                    SELECT EXISTS (
                        SELECT FROM information_schema.tables
                        WHERE table_schema = 'public'
                        AND table_name = 'forecast_metrics'
                    );
                """)
                table_exists = cursor.fetchone()['exists']

                if not table_exists:
                    raise HTTPException(
                        status_code=501,
                        detail="forecast_metrics table not yet implemented. Please create the table or view first."
                    )

                query = "SELECT region, n, mse, mae, mape_pct, r2 FROM forecast_metrics WHERE 1=1"
                params = []

                if region:
                    regions = [r.strip() for r in region.split(',')]
                    query += " AND region = ANY(%s)"
                    params.append(regions)

                logger.info(f"[GET /forecast/metrics] Query: {query}")
                logger.info(f"[GET /forecast/metrics] Params: {params}")
                cursor.execute(query, params)
                results = cursor.fetchall()

                if metric:
                    metrics = [m.strip() for m in metric.split(',')]
                    filtered_results = []
                    for row in results:
                        filtered_row = {"region": row["region"], "n": row["n"]}
                        for m in metrics:
                            if m in row:
                                filtered_row[m] = row[m]
                        filtered_results.append(filtered_row)
                    logger.info(f"[GET /forecast/metrics] Returned {len(filtered_results)} filtered rows")
                    return filtered_results

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
