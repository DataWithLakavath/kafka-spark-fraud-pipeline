from pyspark.sql import SparkSession
import os

print("Starting Spark Batch Job...")

spark = SparkSession.builder \
    .appName("FraudBatch") \
    .master("local[*]") \
    .getOrCreate()

project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))

input_path = os.path.join(project_root, "sample_data", "transactions.csv")
output_path = os.path.join(project_root, "data_lake", "processed", "transactions")

print("Input:", input_path)
print("Output:", output_path)

df = spark.read.option("header", True).csv(input_path)

print("Data Loaded")
df.show(5)

print("Row count:", df.count())
print("Writing to:", output_path)

df.write.mode("overwrite").parquet(output_path)

print("WRITE FINISHED")

spark.stop()