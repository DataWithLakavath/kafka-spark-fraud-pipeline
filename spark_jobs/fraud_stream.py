from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, when, lit
from pyspark.sql.types import StructType, StructField, IntegerType, DoubleType, StringType

spark = SparkSession.builder \
    .appName("FraudDetection") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

schema = StructType([
    StructField("user_id", IntegerType(), True),
    StructField("amount", IntegerType(), True),
    StructField("location", StringType(), True),
    StructField("time", DoubleType(), True)
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .option("startingOffsets", "latest") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING) as json_string")

parsed_df = json_df.select(
    from_json(col("json_string"), schema).alias("data")
).select("data.*")

fraud_df = parsed_df.withColumn(
    "fraud_flag",
    when(col("amount") > 3000, lit("Fraud")).otherwise(lit("Normal"))
)

alerts_df = fraud_df.filter(col("fraud_flag") == "Fraud")

query = alerts_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .option("truncate", False) \
    .start()

query.awaitTermination()