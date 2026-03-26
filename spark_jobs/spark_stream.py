from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

spark = SparkSession.builder \
    .appName("FraudDetection") \
    .master("local[*]") \
    .getOrCreate()

schema = StructType([
    StructField("user_id", IntegerType()),
    StructField("amount", IntegerType()),
    StructField("location", StringType()),
    StructField("time", DoubleType())
])

df = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "transactions") \
    .load()

json_df = df.selectExpr("CAST(value AS STRING)")

parsed_df = json_df.select(
    from_json(col("value"), schema).alias("data")
).select("data.*")

fraud_df = parsed_df.withColumn(
    "fraud_flag",
    when(col("amount") > 3000, "Fraud").otherwise("Normal")
)

query = fraud_df.writeStream \
    .format("console") \
    .outputMode("append") \
    .start()

query.awaitTermination()