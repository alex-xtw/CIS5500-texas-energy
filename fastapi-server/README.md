# FastAPI Server

## Setup

1. Create a virtual environment:
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

2. Install dependencies:
```bash
pip install -r requirements.txt
```

3. Configure environment variables:
```bash
cp .env.example .env
# Edit .env with your database credentials if needed
```

4. Run the server:
```bash
fastapi dev main.py  # Development mode with auto-reload
# or
fastapi run main.py  # Production mode
```

5. Access the API:
- API: http://localhost:8000
- Interactive docs: http://localhost:8000/docs
- Alternative docs: http://localhost:8000/redoc

## Endpoints

### System
- `GET /` - Health check
- `GET /health` - Database connection check

### Load Data
- `GET /load/hourly` - Get hourly aggregated load data across all ERCOT regions
  - Query params: `start_date`, `end_date`, `limit`
  - Uses: `ercot_load` table
- `GET /load/peak-load-extreme-heat` - Get median peak load during extreme heat days by zone
  - Query params: `zone`, `start_date`, `end_date`, `threshold`
  - Requires: `extreme_heat_load` table/view
- `GET /load/outliers/weather-conditions` - Analyze weather conditions on load outlier days
  - Query params: `start_date`, `end_date`, `month`, `outlier_type`, `std_dev_threshold`
  - Requires: `load_outlier_weather` table/view

### Forecast
- `GET /forecast/metrics` - Get forecast accuracy metrics by region
  - Query params: `region`, `metric`
  - Requires: `forecast_metrics` table/view

### Weather Analysis
- `GET /weather/heatwaves` - Get heatwave streaks by ERCOT zone
  - Query params: `zone`, `min_temp_f`, `min_days`, `start_date`, `end_date`
  - Requires: `heatwave_streaks` table/view
- `GET /weather/precipitation` - Compare electricity load on rainy vs. dry days by zone
  - Query params: `zone`, `start_date`, `end_date`
  - Requires: `precipitation_impact` table/view

## Database Tables

The API uses the following database tables:

**Existing tables:**
- `ercot_load` - Hourly electricity load data for ERCOT regions
- `weather_hourly` - Hourly weather data
- `weather_station` - Weather station metadata

**Required tables/views for full API functionality:**
- `forecast_metrics` - Forecast accuracy metrics
- `heatwave_streaks` - Heatwave period data
- `precipitation_impact` - Precipitation impact analysis
- `extreme_heat_load` - Extreme heat load statistics
- `load_outlier_weather` - Weather conditions on outlier days

Note: Endpoints requiring tables/views that don't exist will return a 501 status code with a descriptive error message.
