# producer_db.py
import psycopg2
from kafka import KafkaProducer
import json
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import os
import logging
from datetime import datetime
import time

# Set up logging
LOG_DIR = "logs"
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "producer.log")

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler(LOG_FILE),
        logging.StreamHandler()
    ]
)

# Hadoop configuration
HADOOP_CONFIG = {
    "namenode_host": "localhost",  # Using localhost since we're running from host machine
    "namenode_port": 9870,
    "webhdfs_port": 9870,
    "datanode_port": 9865,
    "hdfs_port": 9000,
    "user": "root"
}

def fix_redirect_url(redirect_url):
    """Fix the redirect URL to use localhost instead of container hostname"""
    import re
    return re.sub(
        r'http://[^:]+:9864',
        f'http://localhost:{HADOOP_CONFIG["datanode_port"]}',
        redirect_url
    )

def save_transaction_to_hadoop(transaction_data):
    """Save transaction data to Hadoop as Parquet file"""
    try:
        # Add a delay to allow the datanode to initialize if needed
        time.sleep(1)

        # Create DataFrame from transaction
        df = pd.DataFrame([transaction_data])
        
        # Convert to PyArrow Table
        table = pa.Table.from_pandas(df)
        
        # Generate filename with timestamp
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        hadoop_path = f"/user/root/transactions/transaction_{timestamp}.parquet"
        
        # Save as temporary Parquet file
        temp_file = f"temp_transaction_{timestamp}.parquet"
        pq.write_table(table, temp_file)
        
        # WebHDFS endpoints
        namenode_url = f"http://{HADOOP_CONFIG['namenode_host']}:{HADOOP_CONFIG['webhdfs_port']}"
        webhdfs_url = f"{namenode_url}/webhdfs/v1{hadoop_path}"
        
        # Create directory if it doesn't exist
        dir_path = "/user/root/transactions"
        mkdir_url = f"{namenode_url}/webhdfs/v1{dir_path}?op=MKDIRS&user.name={HADOOP_CONFIG['user']}"
        mkdir_resp = requests.put(mkdir_url)
        logging.info(f"Create directory response: {mkdir_resp.status_code}")
        
        # Upload file to HDFS
        put_url = f"{webhdfs_url}?op=CREATE&user.name={HADOOP_CONFIG['user']}&overwrite=true"
        
        # Get redirect URL
        resp = requests.put(put_url, allow_redirects=False)
        if resp.status_code == 307:
            redirect_url = fix_redirect_url(resp.headers['Location'])
            
            # Upload the file
            with open(temp_file, 'rb') as f:
                upload_resp = requests.put(
                    redirect_url,
                    data=f,
                    headers={'Content-Type': 'application/octet-stream'}
                )
                if upload_resp.status_code == 201:
                    logging.info(f"Successfully saved transaction to Hadoop: {hadoop_path}")
                else:
                    logging.error(f"Failed to upload transaction: {upload_resp.text}")
        
        # Clean up temporary file
        os.remove(temp_file)
        
    except Exception as e:
        logging.error(f"Error saving to Hadoop: {str(e)}")
        raise

# PostgreSQL connection
try:
    conn = psycopg2.connect(
        host="localhost",
        port=5433,
        database="inventory_db",
        user="inventory_user",
        password="inventory123",
    )
    print("Connected to PostgreSQL successfully!")
    cursor = conn.cursor()
except Exception as e:
    print(f"Error connecting to PostgreSQL: {e}")
    raise

# Kafka producer for alerts
producer = KafkaProducer(
    bootstrap_servers="localhost:9092",
    value_serializer=lambda v: json.dumps(v).encode("utf-8"),
)

# Ensure table exists
cursor.execute(
    """
CREATE TABLE IF NOT EXISTS inventory (
    item_id SERIAL PRIMARY KEY,
    item_name VARCHAR(50) UNIQUE,
    quantity INT
)
"""
)
conn.commit()

while True:
    item_name = input("Enter item name: ")
    action = input("Enter action (buy/sell): ").lower()
    if action not in ["buy", "sell"]:
        print("Invalid action!")
        continue
    try:
        qty = int(input("Enter quantity: "))
    except ValueError:
        print("Quantity must be an integer!")
        continue

    # Check current quantity
    cursor.execute("SELECT quantity FROM inventory WHERE item_name=%s", (item_name,))
    result = cursor.fetchone()
    if result:
        current_qty = result[0]
        new_qty = current_qty + qty if action == "buy" else current_qty - qty
        cursor.execute(
            "UPDATE inventory SET quantity=%s WHERE item_name=%s", (new_qty, item_name)
        )
    else:
        new_qty = qty if action == "buy" else 0
        cursor.execute(
            "INSERT INTO inventory (item_name, quantity) VALUES (%s, %s)",
            (item_name, new_qty),
        )

    conn.commit()
    print(f"{action.upper()} {qty} {item_name}, new quantity: {new_qty}")

    # Save transaction to Hadoop
    transaction_data = {
        'timestamp': datetime.now().isoformat(),
        'item_name': item_name,
        'action': action,
        'quantity': qty,
        'new_quantity': new_qty
    }
    try:
        logging.info(f"Attempting to save transaction to Hadoop: {transaction_data}")
        save_transaction_to_hadoop(transaction_data)
        logging.info("Transaction saved to Hadoop successfully")
    except Exception as e:
        logging.error(f"Failed to save transaction to Hadoop: {str(e)}", exc_info=True)

    # Only send alert to Kafka if quantity < 10
    if new_qty < 10:
        alert_msg = {"item_name": item_name, "quantity": new_qty}
        producer.send("inventory-alerts", value=alert_msg)
        print(f"Alert sent to Kafka: {alert_msg}")
