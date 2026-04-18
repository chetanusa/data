# 🎬 TMDB Movie Data Platform

End-to-end data engineering pipeline for TMDB movie data.

## Architecture
- **Ingestion**: Apache Kafka
- **Transformation**: PySpark
- **Warehouse**: Snowflake
- **Orchestration**: Apache Airflow
- **Monitoring**: Grafana + Prometheus
- **API**: FastAPI

## Setup

1. Clone the repo
2. Copy `.env.example` to `.env` and fill in your credentials
3. Run `docker-compose up -d`
4. Access Airflow at http://localhost:8080 (admin/admin)

## Skills Demonstrated
- Apache Kafka, PySpark, Snowflake, Airflow, Docker, Python, FastAPI, PostgreSQL