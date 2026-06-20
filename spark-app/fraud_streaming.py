"""
Spark Streaming لتطبيق كشف الاحتيال على المعاملات
يقرأ من Kafka (raw_transactions) ويكتب النتائج إلى Kafka (fraud_alerts)
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *
from pyspark.sql.window import Window
import os

# ============================================
# 1. إعداد Spark Session
# ============================================
spark = SparkSession.builder \
    .appName("FraudDetectionStreaming") \
    .config("spark.master", "spark://spark-master:7077") \
    .config("spark.executor.memory", "2g") \
    .config("spark.sql.shuffle.partitions", "4") \
    .config("spark.streaming.stopGracefullyOnShutdown", "true") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("🚀 بدء تشغيل Spark Streaming...")

# ============================================
# 2. قراءة البيانات من Kafka
# ============================================
input_df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:29092,kafka2:29093,kafka3:29094") \
    .option("subscribe", "raw_transactions") \
    .option("startingOffsets", "latest") \
    .option("failOnDataLoss", "false") \
    .load()

# ============================================
# 3. تعريف Schema (هيكل البيانات)
# ============================================
schema = StructType() \
    .add("transaction_id", IntegerType()) \
    .add("user_id", IntegerType()) \
    .add("amount", DoubleType()) \
    .add("currency", StringType()) \
    .add("timestamp", StringType()) \
    .add("country", StringType()) \
    .add("merchant", StringType()) \
    .add("device_id", StringType()) \
    .add("ip_address", StringType()) \
    .add("is_fraud", IntegerType())

# ============================================
# 4. تحليل JSON من Kafka
# ============================================
parsed_df = input_df.select(
    from_json(col("value").cast("string"), schema).alias("data")
).select("data.*")

# ============================================
# 5. تنظيف البيانات (إزالة القيم الفارغة)
# ============================================
clean_df = parsed_df \
    .filter(col("amount").isNotNull() & (col("amount") > 0)) \
    .filter(col("user_id").isNotNull()) \
    .filter(col("country").isNotNull())

# ============================================
# 6. استخراج الوقت
# ============================================
clean_df = clean_df.withColumn("event_time", col("timestamp").cast("timestamp"))
clean_df = clean_df.withColumn("hour", hour("event_time"))
clean_df = clean_df.withColumn("day", dayofmonth("event_time"))

# ============================================
# 7. قواعد كشف الاحتيال
# ============================================
# قاعدة 1: المبالغ الكبيرة (> 1500)
enriched_df = clean_df.withColumn(
    "high_amount_flag",
    when(col("amount") > 1500, 1).otherwise(0)
)

# قاعدة 2: الدول الخطيرة
risk_countries = ["CN", "RU", "KP", "IR"]
enriched_df = enriched_df.withColumn(
    "risk_country_flag",
    when(col("country").isin(risk_countries), 1).otherwise(0)
)

# قاعدة 3: التجار غير المعروفين
enriched_df = enriched_df.withColumn(
    "unknown_merchant_flag",
    when(col("merchant") == "Unknown Merchant", 1).otherwise(0)
)

# حساب درجة المخاطرة (0-3)
enriched_df = enriched_df.withColumn(
    "fraud_score",
    col("high_amount_flag") + col("risk_country_flag") + col("unknown_merchant_flag")
)

# تصنيف المعاملة حسب درجة المخاطرة
enriched_df = enriched_df.withColumn(
    "fraud_level",
    when(col("fraud_score") == 0, "safe")
    .when(col("fraud_score") == 1, "low_risk")
    .when(col("fraud_score") == 2, "medium_risk")
    .otherwise("high_risk")
)

# ============================================
# 8. تصفية المعاملات المشبوهة فقط (fraud_score >= 1)
# ============================================
fraud_df = enriched_df.filter(col("fraud_score") >= 1)

# ============================================
# 9. إرسال النتائج إلى Kafka (fraud_alerts)
# ============================================
output_df = fraud_df.select(
    to_json(struct(
        "transaction_id",
        "user_id",
        "amount",
        "currency",
        "country",
        "merchant",
        "fraud_score",
        "fraud_level",
        "event_time",
        "device_id",
        "ip_address"
    )).alias("value")
)

query = output_df.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka1:29092,kafka2:29093,kafka3:29094") \
    .option("topic", "fraud_alerts") \
    .option("checkpointLocation", "/tmp/checkpoints/fraud_streaming") \
    .outputMode("append") \
    .trigger(processingTime="10 seconds") \
    .start()

print("⏳ Spark Streaming يعمل... ينتظر البيانات من Kafka")
print("📤 سيتم إرسال المعاملات المشبوهة إلى موضوع: fraud_alerts")

query.awaitTermination()