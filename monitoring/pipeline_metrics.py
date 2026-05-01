from prometheus_client import start_http_server, Gauge, Counter
import snowflake.connector
import os
import time
from dotenv import load_dotenv

load_dotenv()

# Define metrics
total_movies = Gauge('tmdb_total_movies', 'Total movies in database')
avg_rating = Gauge('tmdb_avg_rating', 'Average movie rating')
high_rated_movies = Gauge('tmdb_high_rated_movies', 'Movies with rating >= 8')
pipeline_runs = Counter('tmdb_pipeline_runs_total', 'Total pipeline runs')
data_quality_checks = Counter('tmdb_data_quality_checks', 'Data quality check results', ['status'])

def get_snowflake_conn():
    return snowflake.connector.connect(
        account=os.getenv('SNOWFLAKE_ACCOUNT'),
        user=os.getenv('SNOWFLAKE_USER'),
        password=os.getenv('SNOWFLAKE_PASSWORD'),
        database=os.getenv('SNOWFLAKE_DATABASE'),
        schema=os.getenv('SNOWFLAKE_SCHEMA'),
        warehouse=os.getenv('SNOWFLAKE_WAREHOUSE')
    )

def collect_metrics():
    """Collect metrics from Snowflake"""
    try:
        conn = get_snowflake_conn()
        cursor = conn.cursor()
        
        # Total movies
        cursor.execute("SELECT COUNT(*) FROM MOVIES")
        count = cursor.fetchone()[0]
        total_movies.set(count)
        
        # Average rating
        cursor.execute("SELECT AVG(RATING) FROM MOVIES")
        avg = cursor.fetchone()[0]
        avg_rating.set(float(avg))
        
        # High-rated movies
        cursor.execute("SELECT COUNT(*) FROM MOVIES WHERE RATING >= 8")
        high_rated = cursor.fetchone()[0]
        high_rated_movies.set(high_rated)
        
        cursor.close()
        conn.close()
        
        print(f"✅ Metrics updated: {count} movies, avg rating: {avg:.2f}")
        
    except Exception as e:
        print(f"❌ Error collecting metrics: {e}")

if __name__ == '__main__':
    # Start metrics server on port 8002
    start_http_server(8002)
    print("📊 Pipeline metrics server started on port 8002")
    print("Metrics available at http://localhost:8002")
    
    # Collect metrics every 60 seconds
    while True:
        collect_metrics()
        time.sleep(60)