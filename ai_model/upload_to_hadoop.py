import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import requests
import os
import logging
import sys
import time

# Hadoop configuration
HADOOP_CONFIG = {
    "namenode_host": "localhost",  
    "namenode_port": 9870,
    "webhdfs_port": 9870,  
    "datanode_port": 9865,  
    "hdfs_port": 9000, 
    "user": "root", 
}

# Set up logging
LOG_DIR = os.path.join(os.path.dirname(__file__), "logs")
os.makedirs(LOG_DIR, exist_ok=True)
LOG_FILE = os.path.join(LOG_DIR, "hadoop_upload.log")

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    handlers=[logging.FileHandler(LOG_FILE), logging.StreamHandler(sys.stdout)],
)


def fix_redirect_url(redirect_url):
    """Fix the redirect URL to use localhost instead of container hostname"""
    import re

    return re.sub(
        r"http://[^:]+:9864",
        f'http://localhost:{HADOOP_CONFIG["datanode_port"]}',
        redirect_url,
    )


def upload_csv_to_hadoop(csv_path, hadoop_path):
    """Upload CSV file to Hadoop as Parquet file using WebHDFS"""
    
    time.sleep(10)

    try:
        logging.info(f"Reading CSV file from {csv_path}")
        df = pd.read_csv(csv_path, encoding="latin1")

        table = pa.Table.from_pandas(df)

        
        local_path = "temp_store_data.parquet"
        pq.write_table(table, local_path)

       
        namenode_url = (
            f"http://{HADOOP_CONFIG['namenode_host']}:{HADOOP_CONFIG['webhdfs_port']}"
        )
        webhdfs_url = f"{namenode_url}/webhdfs/v1{hadoop_path}"

      
        dir_path = os.path.dirname(hadoop_path)
        if dir_path:
            mkdir_url = f"{namenode_url}/webhdfs/v1{dir_path}?op=MKDIRS&user.name={HADOOP_CONFIG['user']}"
            logging.info(f"Creating directory: {dir_path}")
            mkdir_resp = requests.put(mkdir_url)
            if mkdir_resp.status_code not in [200, 201]:
                logging.error(f"Failed to create directory: {mkdir_resp.text}")
                raise Exception(f"Failed to create directory: {mkdir_resp.text}")
            logging.info(f"Successfully created directory: {dir_path}")

      
        put_url = (
            f"{webhdfs_url}?op=CREATE&user.name={HADOOP_CONFIG['user']}&overwrite=true"
        )

       
        resp = requests.put(put_url, allow_redirects=False)
        if resp.status_code == 307:
       
            redirect_url = fix_redirect_url(resp.headers["Location"])
            logging.info(f"Uploading file to: {redirect_url}")

            
            with open(local_path, "rb") as f:
                logging.info(f"Uploading file to Hadoop path: {hadoop_path}")
                upload_resp = requests.put(
                    redirect_url,
                    data=f,
                    headers={"Content-Type": "application/octet-stream"},
                )
                if upload_resp.status_code == 201:
                    logging.info(
                        f"Successfully uploaded store data to Hadoop: {hadoop_path}"
                    )
                    logging.info(f"File size: {os.path.getsize(local_path)} bytes")
                    logging.info(
                        f"You can verify it at: {namenode_url}/explorer.html#/{hadoop_path}"
                    )
                    logging.info(
                        f"Or check using command: docker exec namenode hdfs dfs -ls {hadoop_path}"
                    )
                else:
                    logging.error(
                        f"Upload failed with status code: {upload_resp.status_code}"
                    )
                    logging.error(f"Response: {upload_resp.text}")
                    raise Exception(f"Failed to upload file: {upload_resp.text}")
        else:
            raise Exception(f"Failed to initiate file creation: {resp.text}")

      
        os.remove(local_path)

    except Exception as e:
        logging.error(f"Error uploading to Hadoop: {str(e)}")
        raise


if __name__ == "__main__":
    DATASET_PATH = os.path.join(
        os.path.dirname(__file__), "../data/Sample - Superstore.csv"
    )
    HADOOP_FILE_PATH = "/user/root/store_data.parquet"

    upload_csv_to_hadoop(DATASET_PATH, HADOOP_FILE_PATH)
