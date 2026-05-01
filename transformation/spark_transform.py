from pyspark.sql import SparkSession
from pyspark.sql.functions import col, ceil, to_date, when, count
from pyspark.sql.types import IntegerType, FloatType
import sys

def run_data_quality_checks(df, stage="raw"):
    """
    Run data quality checks on the dataframe
    Returns: (bool, list of errors)
    """
    print(f"\n{'='*50}")
    print(f"🔍 Running Data Quality Checks - {stage.upper()}")
    print(f"{'='*50}")
    
    errors = []
    total_rows = df.count()
    
    # Check 1: Total row count
    print(f"✓ Total rows: {total_rows}")
    if total_rows == 0:
        errors.append("❌ CRITICAL: No data found!")
    
    # Check 2: Check for null values in critical fields
    critical_fields = ["movie_id", "title"]
    for field in critical_fields:
        if field in df.columns:
            null_count = df.filter(col(field).isNull()).count()
            if null_count > 0:
                errors.append(f"❌ Found {null_count} null values in {field}")
            else:
                print(f"✓ No null values in {field}")
    
    # Check 3: Validate rating range (0-10)
    if "rating" in df.columns:
        invalid_ratings = df.filter((col("rating") < 0) | (col("rating") > 10)).count()
        if invalid_ratings > 0:
            errors.append(f"❌ Found {invalid_ratings} movies with invalid ratings (outside 0-10 range)")
        else:
            print(f"✓ All ratings valid (0-10 range)")
    
    # Check 4: Validate popularity is positive
    if "popularity" in df.columns:
        invalid_popularity = df.filter(col("popularity") < 0).count()
        if invalid_popularity > 0:
            errors.append(f"❌ Found {invalid_popularity} movies with negative popularity")
        else:
            print(f"✓ All popularity scores are positive")
    
    # Check 5: Check for duplicates
    if "movie_id" in df.columns:
        duplicate_count = df.groupBy("movie_id").count().filter(col("count") > 1).count()
        if duplicate_count > 0:
            errors.append(f"❌ Found {duplicate_count} duplicate movie IDs")
        else:
            print(f"✓ No duplicate movie IDs")
    
    # Check 6: Validate rating_count is non-negative
    if "rating_count" in df.columns:
        invalid_rating_count = df.filter(col("rating_count") < 0).count()
        if invalid_rating_count > 0:
            errors.append(f"❌ Found {invalid_rating_count} movies with negative rating counts")
        else:
            print(f"✓ All rating counts are valid")
    
    # Summary
    print(f"\n{'='*50}")
    if errors:
        print("❌ DATA QUALITY CHECKS FAILED")
        for error in errors:
            print(f"  {error}")
        print(f"{'='*50}\n")
        return False, errors
    else:
        print("✅ ALL DATA QUALITY CHECKS PASSED")
        print(f"{'='*50}\n")
        return True, []

def run_transform(input_path="raw_movies.json", output_path="transformed_movies"):
    spark = SparkSession.builder \
        .appName("TMDB Movie Transformation") \
        .getOrCreate()
    spark.sparkContext.setLogLevel("WARN")

    # Read raw JSON
    df = spark.read.option("multiline", "true").json(input_path)
    
    # ✨ NEW: Run data quality checks on RAW data
    print("\n🔍 STAGE 1: Raw Data Quality Checks")
    passed, errors = run_data_quality_checks(df, stage="raw")
    if not passed:
        print("⚠️ WARNING: Raw data has quality issues, but continuing with cleaning...")

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

    # ✨ NEW: Run data quality checks on TRANSFORMED data
    print("\n🔍 STAGE 2: Transformed Data Quality Checks")
    passed, errors = run_data_quality_checks(df, stage="transformed")
    if not passed:
        print("🚨 CRITICAL: Transformed data failed quality checks!")
        print("Stopping pipeline to prevent bad data in warehouse.")
        spark.stop()
        sys.exit(1)  # Exit with error code

    print(f"\n✅ Transformed {df.count()} movies successfully")
    df.show(5)

    # Save as parquet
    df.write.mode("overwrite").parquet(output_path)
    print(f"Saved transformed data to {output_path}")
    
    spark.stop()
    return output_path

if __name__ == "__main__":
    run_transform()