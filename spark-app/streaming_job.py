from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when, hour, to_json, struct
from pyspark.sql.types import StructType, IntegerType, DoubleType, StringType

# ==========================================
# 1. Spark Session
# ==========================================
spark = SparkSession.builder \
    .appName("Fraud Detection Streaming") \
    .master("spark://spark-master:7077") \
    .config("spark.executor.memory", "1g") \
    .config("spark.executor.cores", "1") \
    .config("spark.sql.shuffle.partitions", "2") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")


# ==========================================
# 2. Read from Kafka (INPUT)
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


# ==========================================
# 3. Convert Kafka value → JSON string
# ==========================================
json_df = df.selectExpr("CAST(value AS STRING) as json")


# ==========================================
# 4. Schema
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
# 5. Parse JSON
# ==========================================
parsed_df = json_df.select(
    from_json(col("json"), schema).alias("data")
).select("data.*")


# ==========================================
# 6. Cleaning
# ==========================================
clean_df = parsed_df \
    .filter(col("amount").isNotNull()) \
    .filter(col("amount") > 0) \
    .filter(col("country").isNotNull()) \
    .filter(col("merchant").isNotNull())


# ==========================================
# 7. Feature Engineering
# ==========================================
enriched_df = clean_df.withColumn("hour", hour(col("timestamp")))

enriched_df = enriched_df.withColumn(
    "high_amount_flag",
    when(col("amount") > 1500, 1).otherwise(0)
)

enriched_df = enriched_df.withColumn(
    "risk_country_flag",
    when(col("country").isin("CN", "RU"), 1).otherwise(0)
)

enriched_df = enriched_df.withColumn(
    "fraud_score",
    col("high_amount_flag") + col("risk_country_flag")
)


# ==========================================
# 8. Fraud Detection Filter
# ==========================================
fraud_df = enriched_df.filter(col("fraud_score") >= 1)


# ==========================================
# 9. Convert to Kafka format (IMPORTANT)
# ==========================================
kafka_df = fraud_df.select(
    to_json(struct(
        col("transaction_id"),
        col("user_id"),
        col("amount"),
        col("currency"),
        col("timestamp"),
        col("country"),
        col("merchant"),
        col("fraud_score")
    )).alias("value")
)


# ==========================================
# 10. Write to Kafka (OUTPUT)
# ==========================================
query = kafka_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers",
            "kafka1:29092,kafka2:29093,kafka3:29094") \
    .option("topic", "fraud_alerts") \
    .option("checkpointLocation", "/tmp/checkpoints/fraud_job") \
    .outputMode("append") \
    .start()

query.awaitTermination()