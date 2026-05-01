# 🎬 TMDB Movie Data Platform

A production-grade data engineering and AI/ML platform for processing TMDB movie data with real-time streaming, automated orchestration, and intelligent chatbot capabilities.

---

## 📋 Table of Contents
- [Architecture](#architecture)
- [Features](#features)
- [Tech Stack](#tech-stack)
- [Quick Start](#quick-start)
- [Project Structure](#project-structure)
- [API Endpoints](#api-endpoints)
- [AI Chatbot](#ai-chatbot)
- [Monitoring](#monitoring)
- [Data Quality](#data-quality)
- [CI/CD](#cicd)
- [Skills Demonstrated](#skills-demonstrated)

---

## 🏗️ Architecture
**Data Flow:**
1. **Ingestion**: Fetch movie data from TMDB API → Kafka topics
2. **Streaming**: Consume from Kafka → Real-time processing
3. **Transformation**: PySpark data cleaning & enrichment
4. **Storage**: Load into Snowflake data warehouse
5. **Orchestration**: Airflow DAG runs daily at midnight
6. **Serving**: FastAPI exposes RESTful endpoints
7. **AI/ML**: RAG-powered chatbot with vector search

---

## ✨ Features

### Data Engineering
- ✅ Real-time data ingestion with Apache Kafka
- ✅ Distributed data processing with PySpark
- ✅ Cloud data warehouse (Snowflake)
- ✅ Automated orchestration with Airflow
- ✅ RESTful API with FastAPI
- ✅ Containerized deployment with Docker

### AI/ML
- ✅ RAG-powered movie chatbot using GPT-4
- ✅ Vector embeddings with FAISS
- ✅ Conversational AI with LangChain
- ✅ Interactive Streamlit UI

### DevOps & Monitoring
- ✅ Data quality validation checks
- ✅ CI/CD pipeline with GitHub Actions
- ✅ Real-time monitoring with Prometheus
- ✅ Grafana dashboards for observability
- ✅ Automated testing with pytest

---

## 🛠️ Tech Stack

**Data Engineering:**
- Apache Kafka (Streaming)
- PySpark (Processing)
- Snowflake (Warehouse)
- Apache Airflow (Orchestration)
- PostgreSQL (Metadata)
- Docker & Docker Compose

**API & Backend:**
- FastAPI
- Pydantic
- Python 3.12

**AI/ML:**
- OpenAI GPT-4
- LangChain
- FAISS (Vector DB)
- Streamlit

**Monitoring & DevOps:**
- Prometheus
- Grafana
- GitHub Actions
- pytest

---

## 🚀 Quick Start

### Prerequisites
- Docker Desktop installed
- Python 3.12+
- TMDB API Key ([Get one here](https://www.themoviedb.org/settings/api))
- Snowflake Account ([Free trial](https://signup.snowflake.com/))
- OpenAI API Key ([Get one here](https://platform.openai.com/api-keys))

### Installation

1. **Clone the repository**
```bash