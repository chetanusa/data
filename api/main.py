from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import snowflake.connector
import os
from typing import Optional, List
from pydantic import BaseModel
from datetime import datetime
from dotenv import load_dotenv
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response
import time

# Prometheus metrics
api_requests = Counter('api_requests_total', 'Total API requests', ['endpoint', 'method'])
api_request_duration = Histogram('api_request_duration_seconds', 'API request duration')

load_dotenv() 

app = FastAPI(
    title="TMDB Movie API",
    description="REST API for querying TMDB movie data from Snowflake",
    version="1.0.0"
)

# Enable CORS
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Pydantic models for response validation
class Movie(BaseModel):
    movie_id: int
    title: str
    overview: Optional[str]
    release_date: Optional[str]
    popularity: float
    rating: float
    rating_count: int
    genre_ids: Optional[str]
    popularity_tier: str
    rating_label: str

class MovieStats(BaseModel):
    total_movies: int
    avg_rating: float
    top_genre: str
    latest_release: str

# Snowflake connection
def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
    )


@app.get("/metrics")
def metrics():
    """Prometheus metrics endpoint"""
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)
@app.get("/")
def root():
    return {
        "message": "TMDB Movie API",
        "endpoints": {
            "/movies": "Get all movies with pagination",
            "/movies/{movie_id}": "Get specific movie by ID",
            "/movies/top-rated": "Get top rated movies",
            "/movies/search": "Search movies by title",
            "/stats": "Get movie statistics"
        }
    }

@app.get("/movies", response_model=List[Movie])
def get_movies(
    skip: int = Query(0, ge=0, description="Number of records to skip"),
    limit: int = Query(10, ge=1, le=100, description="Max records to return")
):
    """Get all movies with pagination"""
    try:
        conn = get_snowflake_conn()
        cursor = conn.cursor()
        
        query = f"""
        SELECT 
        MOVIE_ID, TITLE, OVERVIEW, RELEASE_DATE, 
        POPULARITY, RATING, RATING_COUNT,
        POPULARITY_TIER, RATING_LABEL
        FROM MOVIES
        ORDER BY POPULARITY DESC
        LIMIT {limit} OFFSET {skip}
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        movies = []
        for row in results:
            movies.append(Movie(
                movie_id=row[0],
                title=row[1],
                overview=row[2],
                release_date=str(row[3]) if row[3] else None,
                popularity=row[4],
                rating=row[5],
                rating_count=row[6],
                genre_ids=None,  # No genre data in Snowflake
                popularity_tier=row[7],
                rating_label=row[8]
            ))
        
        cursor.close()
        conn.close()
        
        return movies
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.middleware("http")
async def track_metrics(request, call_next):
    start_time = time.time()
    response = await call_next(request)
    duration = time.time() - start_time
    
    api_requests.labels(endpoint=request.url.path, method=request.method).inc()
    api_request_duration.observe(duration)
    
    return response

@app.get("/movies/{movie_id}", response_model=Movie)
def get_movie(movie_id: int):
    """Get a specific movie by ID"""
    try:
        conn = get_snowflake_conn()
        cursor = conn.cursor()
        
        query = f"""
        SELECT 
            MOVIE_ID, TITLE, OVERVIEW, RELEASE_DATE, 
            POPULARITY, RATING, RATING_COUNT, 
            POPULARITY_TIER, RATING_LABEL
        FROM MOVIES
        WHERE MOVIE_ID = {movie_id}
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        
        if not result:
            cursor.close()
            conn.close()
            raise HTTPException(status_code=404, detail="Movie not found")
        
        cursor.close()
        conn.close()
        
        return Movie(
            movie_id=result[0],
            title=result[1],
            overview=result[2],
            release_date=str(result[3]) if result[3] else None,
            popularity=result[4],
            rating=result[5],
            rating_count=result[6],
            genre_ids=None,
            popularity_tier=result[7],
            rating_label=result[8]
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/movies/filter/top-rated", response_model=List[Movie])
def get_top_rated(limit: int = Query(10, ge=1, le=50)):
    """Get top rated movies"""
    try:
        conn = get_snowflake_conn()
        cursor = conn.cursor()
        
        query = f"""
        SELECT 
            MOVIE_ID, TITLE, OVERVIEW, RELEASE_DATE, 
            POPULARITY, RATING, RATING_COUNT, 
            POPULARITY_TIER, RATING_LABEL
        FROM MOVIES
        WHERE RATING_COUNT > 1000
        ORDER BY RATING DESC
        LIMIT {limit}
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        movies = []
        for row in results:
            movies.append(Movie(
                movie_id=row[0],
                title=row[1],
                overview=row[2],
                release_date=str(row[3]) if row[3] else None,
                popularity=row[4],
                rating=row[5],
                rating_count=row[6],
                genre_ids=None,  # No genre data in Snowflake
                popularity_tier=row[7],
                rating_label=row[8]
            ))
        
        cursor.close()
        conn.close()
        
        return movies
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/movies/search/", response_model=List[Movie])
def search_movies(
    title: str = Query(..., min_length=1, description="Search by movie title"),
    limit: int = Query(10, ge=1, le=50)
):
    """Search movies by title"""
    try:
        conn = get_snowflake_conn()
        cursor = conn.cursor()
        
        query = f"""
        SELECT 
            MOVIE_ID, TITLE, OVERVIEW, RELEASE_DATE, 
            POPULARITY, RATING, RATING_COUNT, 
            POPULARITY_TIER, RATING_LABEL
        FROM MOVIES
        WHERE UPPER(TITLE) LIKE UPPER('%{title}%')
        ORDER BY POPULARITY DESC
        LIMIT {limit}
        """
        
        cursor.execute(query)
        results = cursor.fetchall()
        
        movies = []
        for row in results:
            movies.append(Movie(
                movie_id=row[0],
                title=row[1],
                overview=row[2],
                release_date=str(row[3]) if row[3] else None,
                popularity=row[4],
                rating=row[5],
                rating_count=row[6],
                genre_ids=None,  # No genre data in Snowflake
                popularity_tier=row[7],
                rating_label=row[8]
            ))
        
        cursor.close()
        conn.close()
        
        return movies
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/stats", response_model=MovieStats)
def get_stats():
    """Get movie database statistics"""
    try:
        conn = get_snowflake_conn()
        cursor = conn.cursor()
        
        cursor.execute("SELECT COUNT(*) FROM MOVIES")
        total = cursor.fetchone()[0]
        
        cursor.execute("SELECT AVG(RATING) FROM MOVIES")
        avg_rating = round(cursor.fetchone()[0], 2)
        
        cursor.execute("""
            SELECT MAX(RELEASE_DATE) FROM MOVIES
        """)
        latest_release = cursor.fetchone()[0]
        
        cursor.close()
        conn.close()
        
        return MovieStats(
            total_movies=total,
            avg_rating=avg_rating,
            top_genre="Action",  # Placeholder
            latest_release=str(latest_release)
        )
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Database error: {str(e)}")

@app.get("/health")
def health_check():
    """Health check endpoint"""
    try:
        conn = get_snowflake_conn()
        cursor = conn.cursor()
        cursor.execute("SELECT 1")
        cursor.close()
        conn.close()
        return {"status": "healthy", "database": "connected"}
    except Exception as e:
        return {"status": "unhealthy", "error": str(e)}