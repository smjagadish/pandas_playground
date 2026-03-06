import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb 
from pathlib import Path
import logging


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def init_duckdb(path=':memory:'):
    con = duckdb.connect(path)
    return con

def readData(path:Path):
    if not path.exists():
        logger.info('file does not exist , retry later')
    else:
        df = pd.read_csv(path)
        df = df.drop(columns=['CVSS Base Score', 'CVSS Vector','Ericsson Contextualized CVSS Base Score',
        'Ericsson Contextualized CVSS Vector','Threat','Affected Compound Component',
        'More Information'], errors='ignore')
        logger.info('data frame prepared')
    return df

def write_parquet(df):
    tables = pa.Table.from_pandas(df)
    pq.write_to_dataset(
        tables,
        root_path = './data',
        partition_cols = ['Product Name', 'Product Version']
    )
    logger.info('parquet files written)')

def queryDB(con):
    logger.info('querying the db for all vulns with medium severity')
    res = con.execute("SELECT * from read_parquet('./data/*/*/*.parquet') where Severity = 'medium'").df()
    if res.empty:
        logger.info('No vulnerabilities found with medium severity')
    else:
        logger.info(f'Found {len(res)} vulnerabilities with medium severity')
        logger.info(f'First CVE ID is: {res.iloc[0, 3]}')


if __name__ == '__main__':
    con = init_duckdb()
    path = Path('/mnt/c/Users/esmjaga/Downloads/T-174655_1_17474-AXB25019-1_G_pcc-1_39-STIssuesReport.csv')
    df = readData(path)
    write_parquet(df)
    queryDB(con)

    
