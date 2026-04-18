import json
import os
from kafka import KafkaConsumer
from dotenv import load_dotenv

load_dotenv()

KAFKA_BROKER = os.getenv("KAFKA_BROKER")
KAFKA_TOPIC = os.getenv("KAFKA_TOPIC")

consumer = KafkaConsumer(
    KAFKA_TOPIC,
    bootstrap_servers=KAFKA_BROKER,
    auto_offset_reset="earliest",
    value_deserializer=lambda v: json.loads(v.decode("utf-8")),
    consumer_timeout_ms=10000
)

def consume_to_json(output_path="raw_movies.json"):
    movies = []
    for message in consumer:
        movie = message.value
        movies.append(movie)
        print(f"Consumed: {movie['title']}")

    with open(output_path, "w") as f:
        json.dump(movies, f)

    print(f"Saved {len(movies)} movies to {output_path}")
    return output_path

if __name__ == "__main__":
    consume_to_json()