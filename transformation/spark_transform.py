from pyspark.sql import SparkSession
from pyspark.sql.functions import col, ceil, to_date, when
from pyspark.sql.types import IntegerType, FloatType

def run_transform(input_path="raw_movies.json", output_path="transformed_movies"):
    spark = SparkSession.builder \
        .appName("TMDB Movie Transformation") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Read raw JSON
    df = spark.read.option("multiline", "true").json(input_path)

    # Select and rename columns
    df = df.select(
        col("id").cast(IntegerType()).alias("movie_id"),
        col("title"),
        col("overview"),
        col("release_date"),
        col("popularity").cast(FloatType()),
        col("vote_average").cast(FloatType()).alias("rating"),
        col("vote_count").cast(IntegerType()).alias("rating_count"),
        col("genre_ids")
    )

    # Clean data
    df = df.filter(col("title").isNotNull())
    df = df.filter(col("release_date").isNotNull() & (col("release_date") != ""))
    df = df.withColumn("release_date", to_date(col("release_date"), "yyyy-MM-dd"))
    df = df.dropDuplicates(["movie_id"])

    # Enrich - add popularity tier
    df = df.withColumn("popularity_tier",
        when(col("popularity") >= 100, "High")
        .when(col("popularity") >= 50, "Medium")
        .otherwise("Low")
    )

    # Enrich - rating label
    df = df.withColumn("rating_label",
        when(col("rating") >= 8.0, "Excellent")
        .when(col("rating") >= 6.0, "Good")
        .when(col("rating") >= 4.0, "Average")
        .otherwise("Poor")
    )

    print(f"Transformed {df.count()} movies")
    df.show(5)

    # Save as parquet
    df.write.mode("overwrite").parquet(output_path)
    print(f"Saved transformed data to {output_path}")

    spark.stop()
    return output_path

if __name__ == "__main__":
    run_transform()