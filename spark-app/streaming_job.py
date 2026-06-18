from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col
from pyspark.sql.types import (
    StructType,
    IntegerType,
    DoubleType,
    StringType
)

# ==========================================
# Create Spark Session
# ==========================================
spark = SparkSession.builder \
    .appName("Fraud Detection Streaming") \
    .master("spark://spark-master:7077") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# ==========================================
# Read Stream from Kafka
# ==========================================
df = spark.readStream \
    .format("kafka") \
    .option(
        "kafka.bootstrap.servers",
        "kafka1:29092,kafka2:29093,kafka3:29094"
    ) \
    .option("subscribe", "raw_transactions") \
    .option("startingOffsets", "latest") \
    .load()

# Convert Kafka value to string
json_df = df.selectExpr(
    "CAST(value AS STRING) as json"
)

# ==========================================
# Define JSON Schema
# ==========================================
schema = StructType() \
    .add("transaction_id", IntegerType()) \
    .add("user_id", IntegerType()) \
    .add("amount", DoubleType()) \
    .add("currency", StringType()) \
    .add("timestamp", StringType()) \
    .add("country", StringType()) \
    .add("merchant", StringType())

# ==========================================
# Parse JSON
# ==========================================
parsed_df = json_df.select(
    from_json(
        col("json"),
        schema
    ).alias("data")
).select("data.*")

# ==========================================
# Data Cleaning
# ==========================================
clean_df = parsed_df.filter(
    col("amount").isNotNull()
)

clean_df = clean_df.filter(
    col("amount") > 0
)

clean_df = clean_df.filter(
    col("country").isNotNull()
)

clean_df = clean_df.filter(
    col("merchant").isNotNull()
)

# ==========================================
# Display Stream
# ==========================================
query = clean_df.writeStream \
    .format("console") \
    .option("truncate", "false") \
    .outputMode("append") \
    .start()

query.awaitTermination()