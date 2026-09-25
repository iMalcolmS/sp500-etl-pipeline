import duckdb

def load_to_warehouse(bucket_name, warehouse_path="warehouse.duckdb"):
    con = duckdb.connect(warehouse_path)
    con.sql("INSTALL aws")
    con.sql("LOAD aws")
    con.sql("CREATE OR REPLACE SECRET (TYPE s3, PROVIDER credential_chain)")
    con.sql(f"""
        CREATE OR REPLACE TABLE raw_prices AS
        SELECT * FROM read_parquet('s3://{bucket_name}/raw/*.parquet')
    """)
    count = con.sql("SELECT COUNT(*) FROM raw_prices").fetchone()[0]
    print(f"Loaded {count} rows into raw_prices")
    con.close()

if __name__ == "__main__":
    load_to_warehouse("sp500-etl-raw-mcs2026")
