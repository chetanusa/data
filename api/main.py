from fastapi import FastAPI, Query
import snowflake.connector
import os
from dotenv import load_dotenv

load_dotenv()

app = FastAPI(title="TMDB Movie API", version="1.0")

def get_conn():
    return snowflake.connector.connect(
        account=os.getenv("SNOWFLAKE_ACCOUNT"),
        user=os.getenv("SNOWFLAKE_USER"),
        password=os.getenv("SNOWFLAKE_PASSWORD"),
        database=os.getenv("SNOWFLAKE_DATABASE"),
        schema=os.getenv("SNOWFLAKE_SCHEMA"),
        warehouse=os.getenv("SNOWFLAKE_WAREHOUSE")
    )

@app.get("/movies")
def get_movies(limit: int = Query(20, le=100), rating_label: str = None, popularity_tier: str = None):
    conn = get_conn()
    cur = conn.cursor()

    query = "SELECT MOVIE_ID, TITLE, RELEASE_DATE, RATING, POPULARITY, RATING_LABEL, POPULARITY_TIER FROM MOVIES WHERE 1=1"
    if rating_label:
        query += f" AND RATING_LABEL = '{rating_label}'"
    if popularity_tier:
        query += f" AND POPULARITY_TIER = '{popularity_tier}'"
    query += f" LIMIT {limit}"

    cur.execute(query)
    rows = cur.fetchall()
    columns = [desc[0] for desc in cur.description]
    conn.close()

    return [dict(zip(columns, row)) for row in rows]

@app.get("/movies/{movie_id}")
def get_movie(movie_id: int):
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(f"SELECT * FROM MOVIES WHERE MOVIE_ID = {movie_id}")
    row = cur.fetchone()
    columns = [desc[0] for desc in cur.description]
    conn.close()
    if row:
        return dict(zip(columns, row))
    return {"error": "Movie not found"}

@app.get("/stats")
def get_stats():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT 
            COUNT(*) as total_movies,
            AVG(RATING) as avg_rating,
            AVG(POPULARITY) as avg_popularity,
            MAX(RATING) as max_rating,
            MIN(RATING) as min_rating
        FROM MOVIES
    """)
    row = cur.fetchone()
    columns = [desc[0] for desc in cur.description]
    conn.close()
    return dict(zip(columns, row))