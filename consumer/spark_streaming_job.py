from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window, count
from pyspark.sql.types import StructType, StringType, TimestampType

# Schema of the incoming JSON data
schema = StructType() \
    .add("event_time", TimestampType()) \
    .add("user_id", StringType()) \
    .add("url", StringType()) \
    .add("region", StringType()) \
    .add("browser", StringType())

def run_spark_job():
    print("⚡ Starting PySpark Structured Streaming Job...")
    
    spark = SparkSession.builder \
        .appName("RealTimeClickAnalysis") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    # Read from Kafka
    # Note: Using 'rate' source for demo if no Kafka is available, 
    # but code structure assumes Kafka.
    df = spark \
      .readStream \
      .format("kafka") \
      .option("kafka.bootstrap.servers", "localhost:9092") \
      .option("subscribe", "user_clicks") \
      .load()

    # Parse JSON
    parsed_df = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

    # Aggregation: Count clicks per URL every 10 seconds
    windowed_counts = parsed_df \
        .groupBy(
            window(col("event_time"), "10 seconds"),
            col("url")
        ) \
        .count() \
        .orderBy("window")

    # Output to Console
    query = windowed_counts \
        .writeStream \
        .outputMode("complete") \
        .format("console") \
        .option("truncate", "false") \
        .start()

    query.awaitTermination()

if __name__ == "__main__":
    run_spark_job()
