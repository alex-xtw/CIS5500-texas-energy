import os
import pandas as pd
import numpy as np
from dotenv import load_dotenv
from sqlalchemy import create_engine
from prophet import Prophet
from statsmodels.tsa.arima.model import ARIMA
import matplotlib.pyplot as plt
from sklearn.metrics import mean_absolute_error, mean_squared_error


# Load environment variables
load_dotenv()

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT")
DB_USER = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")
DB_NAME = os.getenv("DB_NAME")

# Connect to PostgreSQL
def get_db_connection():
    url = f"postgresql://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
    engine = create_engine(url)
    return engine


# Load data from ercot_load table
def load_ercot_data():
    engine = get_db_connection()
    query = """
        SELECT hour_end, ercot
        FROM ercot_load
        WHERE HOUR_END >= '2009-01-01'
        ORDER BY hour_end;
    """
    df = pd.read_sql(query, engine)
    df["hour_end"] = pd.to_datetime(df["hour_end"])
    return df


# Prepare data for Prophet
def prepare_prophet(df):
    prophet_df = df.rename(columns={"hour_end": "ds", "ercot": "y"})
    return prophet_df


# Train Prophet model
def run_prophet(df):
    model = Prophet()
    model.fit(df)

    future = df["ds"]
    forecast = model.predict(future)

    return model, forecast


# Train ARIMA model
def run_arima(df):
    series = df["ercot"].astype(float)
    model = ARIMA(series, order=(5,1,2))
    model_fit = model.fit()
    forecast = model_fit.predict(start=0, end=len(df)-1)
    return model_fit, forecast

# Compute metrics for Prophet
def compute_metrics(actual, predicted):
    mae = mean_absolute_error(actual, predicted)
    rmse = np.sqrt(mean_squared_error(actual, predicted))
    return mae, rmse


# Main execution
def main():
    print("Loading data from database...")
    df = load_ercot_data()

    # Split for evaluation
    train = df
    test = df

    # Prophet Forecasting
    print("Running Prophet model...")
    prophet_train = prepare_prophet(train)
    prophet_model, prophet_forecast = run_prophet(prophet_train)
    prophet_pred = prophet_forecast["yhat"].values
    prophet_mae, prophet_rmse = compute_metrics(test["ercot"].values, prophet_pred)

    print(f"Prophet MAE: {prophet_mae:.2f}")
    print(f"Prophet RMSE: {prophet_rmse:.2f}")


    # ARIMA Forecasting
    print("Running ARIMA model...")
    arima_model, arima_pred = run_arima(train)
    arima_mae, arima_rmse = compute_metrics(test["ercot"].values, arima_pred.values)
    print(f"ARIMA MAE: {arima_mae:.2f}")
    print(f"ARIMA RMSE: {arima_rmse:.2f}")

    # Plot results
    plt.figure(figsize=(14, 7))
    plt.plot(df["hour_end"], df["ercot"], label="Actual Load", alpha=0.6)
    plt.plot(test["hour_end"], prophet_pred, label="Prophet Forecast", linestyle="--")
    plt.plot(test["hour_end"], arima_pred, label="ARIMA Forecast", linestyle="--")
    plt.title("ERCOT Load Forecasting: Prophet and ARIMA")
    plt.xlabel("Time")
    plt.ylabel("Load (MW)")
    plt.legend()
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    main()
