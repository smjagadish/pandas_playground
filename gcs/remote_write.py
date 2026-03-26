# All use of this code is permitted to explicit consent from the owner of the repo.

import gcsfs as gcs
import duckdb as duckdb
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import logging
from dotenv import load_dotenv
from pathlib import Path
import os

PROJECT_ID = 'gen-lang-client-0854937028'
load_dotenv()
logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

def init_duckdb(path=':memory:'):
    """Initialize a DuckDB connection with GCS (Google Cloud Storage) support.

    Installs the httpfs extension and configures GCS credentials from
    environment variables GCS_KEY_ID and GCS_SECRET.

    Args:
        path: Database path. Defaults to ':memory:' for an in-memory database.

    Returns:
        A configured DuckDB connection, or None if the path is invalid.

    Raises:
        Exception: If GCS credential environment variables are not set.
    """
    logger.info(f' initializing the db')
    if not path == ':memory:' and not path.exists():
        logging.info('db path is incorrect, try again later')
        return None
    else:
        con=duckdb.connect(path)
        con.execute("INSTALL httpfs; LOAD httpfs;")
        
        # Get credentials from environment variables
        gcs_key_id = os.getenv('GCS_KEY_ID')
        gcs_secret = os.getenv('GCS_SECRET')
        
        if not gcs_key_id or not gcs_secret:
            raise Exception("GCS_KEY_ID and GCS_SECRET environment variables must be set")
        
        con.execute(f"""
        CREATE OR REPLACE SECRET (
            TYPE GCS,
            KEY_ID '{gcs_key_id}',
            SECRET '{gcs_secret}'
        );
    """)
        '''con.execute("SET gcs_key_file='/home/esmjaga/adk-key.json';")
        con.execute("SET gcs_service_account='true';") '''
        logger.info('db initialized successfully')
        return con

def read_data(path:Path):
    """Read an Excel file and apply date transformations.

    Parses 'TG5 Actual Date' to datetime, drops rows with missing dates,
    and adds 'month' and 'year' columns.

    Args:
        path: Path to the Excel file.

    Returns:
        A transformed pandas DataFrame, or None if the path is invalid.
    """
    if not path.exists():
        logger.info(f' the data source in {str(path)} is invalid, try with correct path')
        return None
    else:
        df = pd.read_excel(path)
        logger.info('data read successfully. will now do transformation')
        df['TG5 Actual Date'] = pd.to_datetime(df['TG5 Actual Date'],errors='coerce')
        df = df.dropna(subset=['TG5 Actual Date'])

        df['month'] = df['TG5 Actual Date'].dt.month
        df['year'] = df['TG5 Actual Date'].dt.year 
        logger.info(f' Transformation is complete') 
        return df

def write_parquet(df,bucket_name:str):
    """Write a DataFrame to partitioned Parquet files on Google Cloud Storage.

    Partitions by year, month, and Product Area.

    Args:
        df: The pandas DataFrame to write.
        bucket_name: The GCS bucket name for logging purposes.
    """
    logging.info(f'preparing to write parquet file')
    pas = pa.Table.from_pandas(df)
    fs = gcs.GCSFileSystem(project=PROJECT_ID)
    pq.write_to_dataset(pas, root_path='gs://smj_adk_data/parquet/', partition_cols=['year', 'month','Product Area'],filesystem=fs,existing_data_behavior='overwrite_or_ignore')
    logger.info(f'parquet file written successfully to {bucket_name}')

def query_data(duckdb_connection,bucket_name:str):
    """Query remote Parquet files on GCS using DuckDB.

    Fetches records for January 2026 (limit 10) and logs each row.

    Args:
        duckdb_connection: An active DuckDB connection with GCS configured.
        bucket_name: The GCS bucket name for logging purposes.
    """
    logging.info(f'querying the remote parquet file using duckdb')
    res = duckdb_connection.execute("SELECT * FROM 'gs://smj_adk_data/parquet/*/*/*/*.parquet' WHERE year = 2026  and month = 1 limit 10").fetchdf()
    logger.info(f'query executed successfully')
    rows = res.to_dict('records')
    for row in rows:
        logger.info(f' the content of this record is{row.items()}')

if __name__ == '__main__':
    con = init_duckdb()
    if con is None:
        raise Exception("could not initialize db")
    sd = input("please enter the source data location: ")
    if sd == '':
        raise Exception("no value entered for source data")
    sd = Path(sd)
    df = read_data(sd)
    if df is None:
        raise Exception('could not read data')
    write_parquet(df, 'smj_adk_data')
    query_data(con, 'smj_adk_data')
