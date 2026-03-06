import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import duckdb


def init_duckdb(db_path=":memory:"):
    """
    Initialize DuckDB connection.
    
    Args:
        db_path: Path to database file or ":memory:" for in-memory database
    
    Returns:
        duckdb.DuckDBPyConnection: Database connection
    """
    conn = duckdb.connect(db_path)
    return conn


def main():
    # Initialize DuckDB connection (in-memory)
    conn = init_duckdb()
    
    print("DuckDB initialized successfully!")
    print(f"DuckDB version: {conn.execute('SELECT version()').fetchone()[0]}")
    
    # Example: Create a sample dataframe
    df = pd.DataFrame({
        'id': [1, 2, 3],
        'name': ['Alice', 'Bob', 'Charlie'],
        'age': [25, 30, 35]
    })
    
    print("\nDataFrame created:")
    print(df)
    
    # Convert to PyArrow table and write to parquet
    table = pa.Table.from_pandas(df)
    pq.write_table(table, "users.parquet")
    print("\nData written to users.parquet")
    
    # Read parquet file with DuckDB
    result = conn.execute("SELECT * FROM 'users.parquet' WHERE age > 25").df()
    print("\nQuery result from parquet file:")
    print(result)
    
    # Close connection
    conn.close()
    
    print("\nHello from pandas-playground!")


if __name__ == "__main__":
    main()
