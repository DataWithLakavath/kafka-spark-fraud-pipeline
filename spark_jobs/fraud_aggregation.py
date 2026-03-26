from pyspark.sql import SparkSession
from pyspark.sql.functions import count, avg

spark = SparkSession.builder.getOrCreate()

df = spark.read.parquet("data_lake/processed/transactions")

agg = df.groupBy("merchant").agg(
    count("*").alias("total_transactions"),
    avg("amount").alias("avg_amount")
)

agg.write.mode("overwrite").parquet("data_lake/analytics/fraud_summary")

print("Aggregation completed")

spark.stop()