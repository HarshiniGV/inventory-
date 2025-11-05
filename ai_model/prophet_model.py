# --- CONFIG ---
# Dataset: https://www.kaggle.com/datasets/vivek468/superstore-dataset-final
import pandas as pd
import numpy as np
from prophet import Prophet
import pickle
import os
from datetime import datetime, timedelta
import psycopg2
from sqlalchemy import create_engine, text
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import logging
import sys

# PostgreSQL configuration
DB_CONFIG = {
    "host": "localhost",
    "database": "inventory_db",  # Matches POSTGRES_DB in docker-compose
    "user": "inventory_user",  # Matches POSTGRES_USER in docker-compose
    "password": "inventory123",  # Matches POSTGRES_PASSWORD in docker-compose
    "port": "5433",
}

# Hadoop configuration
HADOOP_CONFIG = {
    "namenode_host": "localhost",  # Hadoop namenode host
    "namenode_port": 9870,  # Hadoop namenode WebHDFS port
    "webhdfs_port": 9870,  # WebHDFS port
    "datanode_port": 9865,  # Hadoop datanode WebHDFS port (mapped from 9864)
    "hdfs_port": 9000,  # HDFS client port
    "user": "root",  # Hadoop user (using root for our Docker setup)
}

# Set up logging
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "prophet_predictions.log")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
)

DATASET_PATH = os.path.join(
    os.path.dirname(__file__), "../data/Sample - Superstore.csv"
)
CATEGORY = "Furniture"
MODEL_DIR = os.path.join(os.path.dirname(__file__), "models")
RESULTS_PATH = os.path.join(os.path.dirname(__file__), "predictions.csv")
os.makedirs(MODEL_DIR, exist_ok=True)


def load_and_prepare_data():
    """Load data from Hadoop and prepare it for training"""
    try:
        logging.info("Starting data load from Hadoop")
        # WebHDFS endpoints
        namenode_url = (
            f"http://{HADOOP_CONFIG['namenode_host']}:{HADOOP_CONFIG['webhdfs_port']}"
        )
        hadoop_path = "/user/root/store_data.parquet"
        webhdfs_url = f"{namenode_url}/webhdfs/v1{hadoop_path}?op=OPEN&user.name={HADOOP_CONFIG['user']}"

        logging.info(f"Accessing WebHDFS URL: {webhdfs_url}")

        # Get redirect URL
        resp = requests.get(webhdfs_url, allow_redirects=False)
        if resp.status_code == 307:
            # Fix the redirect URL
            redirect_url = fix_redirect_url(resp.headers["Location"])
            logging.info(f"Redirecting to DataNode URL: {redirect_url}")

            # Download the parquet file
            logging.info("Downloading data from Hadoop")
            download_resp = requests.get(redirect_url)
            if download_resp.status_code == 200:
                # Save temporarily and read with pyarrow
                with open("temp_download.parquet", "wb") as f:
                    f.write(download_resp.content)

                # Read parquet file
                table = pq.read_table("temp_download.parquet")
                df = table.to_pandas()

                # Clean up
                os.remove("temp_download.parquet")

                # Process the data
                df = df[df["Category"] == CATEGORY]
                df = df[["Product ID", "Product Name", "Order Date", "Quantity"]]
                df["Order Date"] = pd.to_datetime(df["Order Date"])
                agg = (
                    df.groupby(["Product ID", "Product Name", "Order Date"])
                    .agg({"Quantity": "sum"})
                    .reset_index()
                )
                return agg
            else:
                raise Exception(f"Failed to download file: {download_resp.text}")
        else:
            raise Exception(f"Failed to get file location: {resp.text}")

    except Exception as e:
        logging.error(f"Error loading data from Hadoop: {str(e)}")
        raise


def generate_mock_data(agg, n_products=3, n_days=7):
    # Select products with sufficient historical data
    product_counts = agg.groupby("Product ID").size()
    qualified_products = product_counts[product_counts >= 10].index

    if len(qualified_products) == 0:
        logging.warning("No products with sufficient historical data found!")
        qualified_products = agg["Product ID"].unique()

    unique_products = (
        agg[agg["Product ID"].isin(qualified_products)][["Product ID", "Product Name"]]
        .drop_duplicates()
        .sample(min(7, len(qualified_products)), random_state=42)
    )

    logging.info(f"Selected {len(unique_products)} products for prediction")

    mock_data = []
    today = agg["Order Date"].max() + timedelta(days=1)

    for _, row in unique_products.iterrows():
        product_id = row["Product ID"]
        product_name = row["Product Name"]

        # Get historical mean and std for this product
        hist_stats = agg[agg["Product ID"] == product_id]["Quantity"].agg(
            ["mean", "std"]
        )
        mean_qty = max(1, hist_stats["mean"])
        std_qty = max(1, hist_stats["std"])

        for i in range(7):
            mock_date = today + timedelta(days=i)
            # Generate quantity based on historical patterns
            quantity = max(1, int(np.random.normal(mean_qty, std_qty / 2)))
            mock_data.append(
                {
                    "Product ID": product_id,
                    "Product Name": product_name,
                    "Order Date": mock_date,
                    "Quantity": quantity,
                }
            )

    mock_df = pd.DataFrame(mock_data)
    return mock_df


def predict_min_and_restock(agg, mock_df):
    mock_df["min_quantity_7d"] = 0
    mock_df["restock_60d"] = 0

    for product_id in mock_df["Product ID"].unique():
        hist = agg[agg["Product ID"] == product_id][["Order Date", "Quantity"]].rename(
            columns={"Order Date": "ds", "Quantity": "y"}
        )

        # Add logging to understand the data
        logging.info(f"\nProcessing Product ID: {product_id}")
        logging.info(f"Historical data points: {len(hist)}")
        logging.info(f"Date range: {hist['ds'].min()} to {hist['ds'].max()}")
        logging.info(f"Total historical quantity: {hist['y'].sum()}")

        if len(hist) < 2:
            logging.warning(f"Insufficient data for Product ID {product_id}. Skipping.")
            continue

        # Sort data by date
        hist = hist.sort_values("ds")

        # Check for zero or negative values
        if (hist["y"] <= 0).all():
            logging.warning(
                f"All quantities are zero or negative for Product ID {product_id}"
            )
            continue

        try:
            m = Prophet(
                yearly_seasonality=True,
                weekly_seasonality=True,
                daily_seasonality=False,
                seasonality_mode="multiplicative",
            )
            m.fit(hist)

            future = m.make_future_dataframe(periods=60)
            forecast = m.predict(future)

            # Log predictions
            logging.info(
                f"7-day forecast values: {forecast.tail(60).head(7)['yhat'].values}"
            )
            logging.info(f"60-day forecast values: {forecast.tail(60)['yhat'].values}")

            min_qty = max(0, forecast.tail(60).head(7)["yhat"].sum())
            restock_qty = max(0, forecast.tail(60)["yhat"].sum())

            # Round to nearest whole number
            min_qty = int(round(min_qty))
            restock_qty = int(round(restock_qty))

            logging.info(f"Calculated min_qty_7d: {min_qty}")
            logging.info(f"Calculated restock_60d: {restock_qty}")

            mock_df.loc[mock_df["Product ID"] == product_id, "min_quantity_7d"] = (
                min_qty
            )
            mock_df.loc[mock_df["Product ID"] == product_id, "restock_60d"] = (
                restock_qty
            )

        except Exception as e:
            logging.error(f"Error predicting for Product ID {product_id}: {str(e)}")
            continue

    return mock_df


def fix_redirect_url(redirect_url):
    """Fix the redirect URL to use localhost instead of container hostname"""
    import re

    # Replace container hostname with localhost and use the correct port
    return re.sub(
        r"http://[^:]+:9864",
        f'http://localhost:{HADOOP_CONFIG["datanode_port"]}',
        redirect_url,
    )


def save_to_postgres(df):
    """Save DataFrame to PostgreSQL database"""
    try:
        # Create connection string
        conn_string = f"postgresql://{DB_CONFIG['user']}:{DB_CONFIG['password']}@{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['database']}"
        engine = create_engine(conn_string)

        # Create connection and create table
        with engine.connect() as connection:
            # Create the table with the correct schema
            connection.execute(
                text(
                    """
                CREATE TABLE IF NOT EXISTS furniture_predictions (
                    "Product ID" VARCHAR(255),
                    "Product Name" TEXT,
                    min_quantity_7d INTEGER,
                    restock_60d INTEGER
                );
            """
                )
            )
            connection.commit()

        # Save the dataframe to the table
        df.to_sql("furniture_predictions", engine, if_exists="replace", index=False)
        print(
            f"Successfully saved predictions to PostgreSQL table: furniture_predictions"
        )
    except Exception as e:
        print(f"Error saving to PostgreSQL: {str(e)}")
        print("Please ensure PostgreSQL is running and the database exists.")


def main():
    try:
        logging.info("Starting weekly inventory predictions")
        logging.info(f"Running predictions for category: {CATEGORY}")

        # Load and prepare data
        logging.info("Loading and preparing data...")
        agg = load_and_prepare_data()
        logging.info(f"Loaded data with {len(agg)} records")

        # Generate predictions
        logging.info("Generating predictions...")
        mock_df = generate_mock_data(agg)
        mock_df = predict_min_and_restock(agg, mock_df)

        # Summarize predictions
        summary = (
            mock_df.groupby(["Product ID", "Product Name"])[
                ["min_quantity_7d", "restock_60d"]
            ]
            .first()
            .reset_index()
        )
        logging.info("\nSummary predictions per product:")
        print(summary)

        # Save results to CSV
        summary.to_csv(RESULTS_PATH, index=False)
        logging.info(f"Predictions saved to {RESULTS_PATH}")

        # Save to PostgreSQL
        logging.info("Saving predictions to PostgreSQL...")
        save_to_postgres(summary)

        logging.info("Weekly prediction process completed successfully")

    except Exception as e:
        logging.error(f"Error during prediction process: {str(e)}", exc_info=True)
        raise  # Re-raise the exception for the batch script to catch


if __name__ == "__main__":
    main()
