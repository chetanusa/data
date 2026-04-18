import requests
import json
import time
import os
from kafka import KafkaProducer
from dotenv import load_dotenv

load_dotenv()

API_KEY = os.getenv("TMDB_API_KEY")
KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")
BASE_URL = "https://api.themoviedb.org/3"

producer = KafkaProducer(
    bootstrap_servers=KAFKA_BROKER,
    value_serializer=lambda v: json.dumps(v).encode("utf-8")
)

def fetch_and_produce(pages=10):
    for page in range(1, pages + 1):
        url = f"{BASE_URL}/movie/popular"
        params = {"api_key": API_KEY, "page": page}
        response = requests.get(url, params=params)

        if response.status_code == 200:
            movies = response.json().get("results", [])
            for movie in movies:
                producer.send(KAFKA_TOPIC, value=movie)
                print(f"Produced: {movie['title']}")
            time.sleep(0.5)
        else:
            print(f"Failed page {page}: {response.status_code}")

    producer.flush()
    print("All movies produced to Kafka!")

if __name__ == "__main__":
    fetch_and_produce(pages=10)