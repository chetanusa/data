import os
import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas
from dotenv import load_dotenv

load_dotenv()

def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
    )

def create_table(conn):
    query = """
    CREATE TABLE IF NOT EXISTS MOVIES (
        MOVIE_ID INTEGER PRIMARY KEY,
        TITLE VARCHAR,
        OVERVIEW TEXT,
        RELEASE_DATE DATE,
        POPULARITY FLOAT,
        RATING FLOAT,
        RATING_COUNT INTEGER,
        POPULARITY_TIER VARCHAR,
        RATING_LABEL VARCHAR
    );
    """
    conn.cursor().execute(query)
    print("Snowflake table ready")

def load_to_snowflake(parquet_path="transformed_movies"):
    df = pd.read_parquet(parquet_path)

    # Drop genre_ids (complex type, handle separately)
    if "genre_ids" in df.columns:
        df = df.drop(columns=["genre_ids"])

    # Uppercase column names for Snowflake
    df.columns = [c.upper() for c in df.columns]

    conn = get_snowflake_conn()
    create_table(conn)

    success, nchunks, nrows, _ = write_pandas(conn, df, "MOVIES")
    print(f"Loaded {nrows} rows into Snowflake MOVIES table")
    conn.close()

if __name__ == "__main__":
    load_to_snowflake()