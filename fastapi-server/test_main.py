import pytest
from fastapi.testclient import TestClient
from unittest.mock import patch, MagicMock, Mock
from datetime import datetime, date
import psycopg2
from main import (
    app,
    get_db_connection,
    root,
    health_check,
    get_hourly_load,
    get_load_comparison,
    get_forecast_metrics,
    get_heatwave_streaks,
    get_precipitation_load_impact,
    get_peak_load_extreme_heat,
    get_load_outliers_weather_conditions,
    get_load_outliers
)

# Create test client
client = TestClient(app)


class TestRootEndpoint:
    """Tests for the root endpoint"""

    def test_root_returns_status_ok(self):
        """Test that root endpoint returns status ok"""
        response = client.get("/")
        assert response.status_code == 200
        assert response.json() == {"status": "ok", "message": "CIS5500 Texas Energy API"}


class TestHealthCheckEndpoint:
    """Tests for the health check endpoint"""

    @patch('main.get_db_connection')
    def test_health_check_success(self, mock_get_db):
        """Test health check with successful database connection"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db.return_value.__enter__.return_value = mock_conn

        response = client.get("/health")
        assert response.status_code == 200
        assert response.json() == {"status": "healthy", "database": "connected"}
        mock_cursor.execute.assert_called_once_with("SELECT 1")

    @patch('main.get_db_connection')
    def test_health_check_failure(self, mock_get_db):
        """Test health check with database connection failure"""
        mock_get_db.return_value.__enter__.side_effect = Exception("Connection failed")

        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "unhealthy"
        assert "Connection failed" in response.json()["error"]


class TestGetHourlyLoad:
    """Tests for the /load/hourly endpoint"""

    @patch('main.get_db_connection')
    def test_get_hourly_load_no_filters(self, mock_get_db):
        """Test getting hourly load without filters"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {
                "hour_end": datetime(2024, 1, 1, 1, 0),
                "coast": 5000.0,
                "east": 3000.0,
                "far_west": 2000.0,
                "north": 4000.0,
                "north_c": 6000.0,
                "southern": 5500.0,
                "south_c": 7000.0,
                "west": 3500.0,
                "ercot": 36000.0
            }
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db.return_value.__enter__.return_value = mock_conn

        response = client.get("/load/hourly")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["coast"] == 5000.0
        assert data[0]["ercot"] == 36000.0

    @patch('main.get_db_connection')
    def test_get_hourly_load_with_date_filters(self, mock_get_db):
        """Test getting hourly load with date filters"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db.return_value.__enter__.return_value = mock_conn

        response = client.get(
            "/load/hourly",
            params={
                "start_date": "2024-01-01T00:00:00",
                "end_date": "2024-01-31T23:59:59"
            }
        )
        assert response.status_code == 200
        mock_cursor.execute.assert_called_once()

    @patch('main.get_db_connection')
    def test_get_hourly_load_database_error(self, mock_get_db):
        """Test handling of database errors"""
        mock_get_db.return_value.__enter__.side_effect = Exception("Database error")

        response = client.get("/load/hourly")
        assert response.status_code == 500
        assert "Database error" in response.json()["detail"]


class TestGetLoadComparison:
    """Tests for the /load/comparison endpoint"""

    @patch('main.get_db_connection')
    def test_get_load_comparison_statistical_model(self, mock_get_db):
        """Test load comparison with statistical model"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {
                "hour_end": datetime(2024, 1, 1, 1, 0),
                "coast_actual": 5000.0,
                "coast_expected": 4900.0,
                "ercot_actual": 36000.0,
                "ercot_expected": 35500.0
            }
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db.return_value.__enter__.return_value = mock_conn

        response = client.get("/load/comparison", params={"model": "statistical"})
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert "coast_actual" in data[0]
        assert "coast_expected" in data[0]

    @patch('main.get_db_connection')
    def test_get_load_comparison_xgb_model(self, mock_get_db):
        """Test load comparison with XGBoost model"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db.return_value.__enter__.return_value = mock_conn

        response = client.get("/load/comparison", params={"model": "xgb"})
        assert response.status_code == 200

    def test_get_load_comparison_invalid_model(self):
        """Test with invalid model parameter"""
        response = client.get("/load/comparison", params={"model": "invalid_model"})
        assert response.status_code == 400
        assert "Invalid model" in response.json()["detail"]

    @patch('main.get_db_connection')
    def test_get_load_comparison_with_region_filter(self, mock_get_db):
        """Test load comparison with region filter"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db.return_value.__enter__.return_value = mock_conn

        response = client.get("/load/comparison", params={"region": "coast,east"})
        assert response.status_code == 200

    def test_get_load_comparison_invalid_region(self):
        """Test with invalid region parameter"""
        response = client.get("/load/comparison", params={"region": "invalid_region"})
        assert response.status_code == 400
        assert "Invalid region" in response.json()["detail"]


class TestGetForecastMetrics:
    """Tests for the /forecast/metrics endpoint"""

    @patch('main.get_db_connection')
    def test_get_forecast_metrics_success(self, mock_get_db):
        """Test successful forecast metrics retrieval"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"exists": True}
        mock_cursor.fetchall.return_value = [
            {
                "region": "coast",
                "n": 100,
                "mse": 150000.0,
                "mae": 300.0,
                "mape_pct": 5.2,
                "r2": 0.95
            }
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db.return_value.__enter__.return_value = mock_conn

        response = client.get("/forecast/metrics")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["region"] == "coast"
        assert data[0]["r2"] == 0.95

    @patch('main.get_db_connection')
    def test_get_forecast_metrics_table_not_exists(self, mock_get_db):
        """Test when comparison table doesn't exist"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"exists": False}
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db.return_value.__enter__.return_value = mock_conn

        response = client.get("/forecast/metrics")
        assert response.status_code == 501
        assert "not yet implemented" in response.json()["detail"]

    @patch('main.get_db_connection')
    def test_get_forecast_metrics_with_filters(self, mock_get_db):
        """Test forecast metrics with date and region filters"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchone.return_value = {"exists": True}
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db.return_value.__enter__.return_value = mock_conn

        response = client.get(
            "/forecast/metrics",
            params={
                "start_date": "2024-01-01T00:00:00",
                "end_date": "2024-01-31T23:59:59",
                "region": "coast,east"
            }
        )
        assert response.status_code == 200

    def test_get_forecast_metrics_invalid_model(self):
        """Test with invalid model parameter"""
        response = client.get("/forecast/metrics", params={"model": "invalid"})
        assert response.status_code == 400


class TestGetHeatwaveStreaks:
    """Tests for the /weather/heatwaves endpoint"""

    @patch('main.get_db_connection')
    def test_get_heatwave_streaks_default_params(self, mock_get_db):
        """Test heatwave streaks with default parameters"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {
                "zone": "coast",
                "streak_start": date(2024, 7, 1),
                "streak_end": date(2024, 7, 5),
                "streak_days": 5,
                "avg_peak_load_mw": 6500.0
            }
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db.return_value.__enter__.return_value = mock_conn

        response = client.get("/weather/heatwaves")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["streak_days"] == 5

    @patch('main.get_db_connection')
    def test_get_heatwave_streaks_with_filters(self, mock_get_db):
        """Test heatwave streaks with custom filters"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db.return_value.__enter__.return_value = mock_conn

        response = client.get(
            "/weather/heatwaves",
            params={
                "zone": "coast,east",
                "min_temp_f": 105.0,
                "min_days": 5,
                "start_date": "2024-06-01",
                "end_date": "2024-08-31"
            }
        )
        assert response.status_code == 200

    def test_get_heatwave_streaks_invalid_min_days(self):
        """Test with invalid min_days parameter"""
        response = client.get("/weather/heatwaves", params={"min_days": 0})
        assert response.status_code == 422


class TestGetPrecipitationLoadImpact:
    """Tests for the /weather/precipitation endpoint"""

    @patch('main.get_db_connection')
    def test_get_precipitation_impact_success(self, mock_get_db):
        """Test precipitation impact analysis"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {
                "zone": "coast",
                "rainy_day": True,
                "avg_load_mw": 5200.0,
                "num_days": 45
            },
            {
                "zone": "coast",
                "rainy_day": False,
                "avg_load_mw": 5500.0,
                "num_days": 120
            }
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db.return_value.__enter__.return_value = mock_conn

        response = client.get("/weather/precipitation")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 2
        assert data[0]["rainy_day"] == True

    @patch('main.get_db_connection')
    def test_get_precipitation_impact_with_filters(self, mock_get_db):
        """Test precipitation impact with filters"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db.return_value.__enter__.return_value = mock_conn

        response = client.get(
            "/weather/precipitation",
            params={
                "zone": "coast",
                "start_date": "2024-01-01",
                "end_date": "2024-12-31"
            }
        )
        assert response.status_code == 200


class TestGetPeakLoadExtremeHeat:
    """Tests for the /load/peak-load-extreme-heat endpoint"""

    @patch('main.get_db_connection')
    def test_get_peak_load_extreme_heat_default(self, mock_get_db):
        """Test extreme heat analysis with default threshold"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {
                "zone": "coast",
                "median_peak_load_mw": 7200.0,
                "num_extreme_heat_days": 15,
                "threshold_percentile": 99.0,
                "threshold_temp_f": 102.5
            }
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db.return_value.__enter__.return_value = mock_conn

        response = client.get("/load/peak-load-extreme-heat")
        assert response.status_code == 200
        data = response.json()
        assert len(data) == 1
        assert data[0]["threshold_percentile"] == 99.0

    @patch('main.get_db_connection')
    def test_get_peak_load_extreme_heat_custom_threshold(self, mock_get_db):
        """Test with custom percentile threshold"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db.return_value.__enter__.return_value = mock_conn

        response = client.get("/load/peak-load-extreme-heat", params={"threshold": 95})
        assert response.status_code == 200

    def test_get_peak_load_extreme_heat_invalid_threshold(self):
        """Test with invalid threshold value"""
        response = client.get("/load/peak-load-extreme-heat", params={"threshold": 150})
        assert response.status_code == 422


class TestGetLoadOutliersWeatherConditions:
    """Tests for the /load/outliers/weather-conditions endpoint"""

    @patch('main.get_db_connection')
    def test_get_load_outliers_weather_default(self, mock_get_db):
        """Test outlier weather conditions with defaults"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {
                "month_start": date(2024, 1, 1),
                "outlier_group": "high",
                "num_days": 3,
                "avg_temp_c": 35.2,
                "avg_rh_pct": 45.0,
                "avg_precip_mm": 0.5,
                "avg_wind_kmh": 15.0,
                "avg_pressure_hpa": 1013.0,
                "avg_cloud_cover_pct": 30.0
            }
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db.return_value.__enter__.return_value = mock_conn

        response = client.get("/load/outliers/weather-conditions")
        assert response.status_code == 200
        result = response.json()
        assert "data" in result
        assert "metadata" in result
        assert len(result["data"]) == 1
        assert result["metadata"]["std_dev_threshold"] == 3

    @patch('main.get_db_connection')
    def test_get_load_outliers_weather_with_filters(self, mock_get_db):
        """Test outlier weather conditions with filters"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db.return_value.__enter__.return_value = mock_conn

        response = client.get(
            "/load/outliers/weather-conditions",
            params={
                "month": "2024-01,2024-02",
                "outlier_type": "high",
                "std_dev_threshold": 2.5
            }
        )
        assert response.status_code == 200

    def test_get_load_outliers_weather_invalid_threshold(self):
        """Test with invalid standard deviation threshold"""
        response = client.get(
            "/load/outliers/weather-conditions",
            params={"std_dev_threshold": 0.5}
        )
        assert response.status_code == 422


class TestGetLoadOutliers:
    """Tests for the /load/outliers endpoint"""

    @patch('main.get_db_connection')
    def test_get_load_outliers_default(self, mock_get_db):
        """Test load outliers with default parameters"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = [
            {
                "hour_end": datetime(2024, 7, 15, 14, 0),
                "region": "coast",
                "load_mw": 8500.0,
                "mean": 5500.0,
                "std_dev": 800.0,
                "z_score": 3.75,
                "outlier_type": "high"
            }
        ]
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db.return_value.__enter__.return_value = mock_conn

        response = client.get("/load/outliers")
        assert response.status_code == 200
        result = response.json()
        assert "data" in result
        assert "metadata" in result
        assert len(result["data"]) == 1
        assert result["data"][0]["outlier_type"] == "high"

    @patch('main.get_db_connection')
    def test_get_load_outliers_with_region_filter(self, mock_get_db):
        """Test outliers with region filter"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db.return_value.__enter__.return_value = mock_conn

        response = client.get("/load/outliers", params={"region": "coast,east"})
        assert response.status_code == 200

    def test_get_load_outliers_invalid_region(self):
        """Test with invalid region"""
        response = client.get("/load/outliers", params={"region": "invalid"})
        assert response.status_code == 400

    @patch('main.get_db_connection')
    def test_get_load_outliers_with_type_filter(self, mock_get_db):
        """Test outliers with outlier type filter"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db.return_value.__enter__.return_value = mock_conn

        response = client.get("/load/outliers", params={"outlier_type": "high"})
        assert response.status_code == 200

    @patch('main.get_db_connection')
    def test_get_load_outliers_custom_threshold(self, mock_get_db):
        """Test outliers with custom threshold"""
        mock_conn = MagicMock()
        mock_cursor = MagicMock()
        mock_cursor.fetchall.return_value = []
        mock_conn.cursor.return_value.__enter__.return_value = mock_cursor
        mock_get_db.return_value.__enter__.return_value = mock_conn

        response = client.get("/load/outliers", params={"std_dev_threshold": 2.5})
        assert response.status_code == 200

    def test_get_load_outliers_invalid_threshold(self):
        """Test with invalid threshold"""
        response = client.get("/load/outliers", params={"std_dev_threshold": 0.5})
        assert response.status_code == 422

    def test_get_load_outliers_invalid_limit(self):
        """Test with invalid limit"""
        response = client.get("/load/outliers", params={"limit": 20000})
        assert response.status_code == 422


class TestDatabaseConnection:
    """Tests for database connection context manager"""

    @patch('psycopg2.connect')
    def test_get_db_connection_success(self, mock_connect):
        """Test successful database connection"""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        with get_db_connection() as conn:
            assert conn == mock_conn

        mock_conn.close.assert_called_once()

    @patch('psycopg2.connect')
    def test_get_db_connection_closes_on_error(self, mock_connect):
        """Test that connection closes even on error"""
        mock_conn = MagicMock()
        mock_connect.return_value = mock_conn

        try:
            with get_db_connection() as conn:
                raise Exception("Test error")
        except Exception:
            pass

        mock_conn.close.assert_called_once()


class TestErrorHandling:
    """Tests for error handling across endpoints"""

    @patch('main.get_db_connection')
    def test_database_error_returns_500(self, mock_get_db):
        """Test that database errors return 500 status"""
        mock_get_db.return_value.__enter__.side_effect = psycopg2.Error("DB Error")

        response = client.get("/load/hourly")
        assert response.status_code == 500

    def test_invalid_datetime_format(self):
        """Test invalid datetime format returns 422"""
        response = client.get("/load/hourly", params={"start_date": "invalid-date"})
        assert response.status_code == 422

    def test_invalid_date_format(self):
        """Test invalid date format returns 422"""
        response = client.get("/weather/heatwaves", params={"start_date": "not-a-date"})
        assert response.status_code == 422


class TestPydanticModels:
    """Tests for Pydantic model validation"""

    def test_hourly_load_data_validation(self):
        """Test HourlyLoadData model validation"""
        from main import HourlyLoadData

        data = {
            "hour_end": datetime(2024, 1, 1, 1, 0),
            "coast": 5000.0,
            "east": 3000.0,
            "far_west": 2000.0,
            "north": 4000.0,
            "north_c": 6000.0,
            "southern": 5500.0,
            "south_c": 7000.0,
            "west": 3500.0,
            "ercot": 36000.0
        }

        model = HourlyLoadData(**data)
        assert model.coast == 5000.0
        assert model.ercot == 36000.0

    def test_forecast_metrics_validation(self):
        """Test ForecastMetrics model validation with R2 bounds"""
        from main import ForecastMetrics

        # Valid R2 value
        data = {
            "region": "coast",
            "n": 100,
            "mse": 150000.0,
            "mae": 300.0,
            "mape_pct": 5.2,
            "r2": 0.95
        }
        model = ForecastMetrics(**data)
        assert model.r2 == 0.95

        # R2 out of bounds should fail
        with pytest.raises(Exception):
            ForecastMetrics(**{**data, "r2": 1.5})

    def test_heatwave_streak_validation(self):
        """Test HeatwaveStreak model validation"""
        from main import HeatwaveStreak

        data = {
            "zone": "coast",
            "streak_start": date(2024, 7, 1),
            "streak_end": date(2024, 7, 5),
            "streak_days": 5,
            "avg_peak_load_mw": 6500.0
        }

        model = HeatwaveStreak(**data)
        assert model.streak_days == 5


class TestCORSMiddleware:
    """Tests for CORS middleware configuration"""

    def test_cors_headers_present(self):
        """Test that CORS headers are present in response with Origin header"""
        response = client.get("/", headers={"Origin": "http://localhost:3000"})
        assert "access-control-allow-origin" in response.headers


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
