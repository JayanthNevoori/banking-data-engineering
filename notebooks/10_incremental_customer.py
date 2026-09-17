from pyspark.sql import functions as F

# Read source customer data
source_customer_df = spark.read.parquet(
    "/mnt/data/banking/bronze/customer"
)

# Read existing Silver customer data
target_customer_df = spark.read.format("delta").load(
    "/mnt/data/banking/silver/customer"
)

# Find the latest processed timestamp
watermark = target_customer_df.agg(
    F.max("modified_date").alias("max_modified_date")
).collect()[0]["max_modified_date"]

# Select only newly modified records
incremental_customer_df = source_customer_df.filter(
    F.col("modified_date") > F.lit(watermark)
)

# Append incremental records
incremental_customer_df.write \
    .format("delta") \
    .mode("append") \
    .save("/mnt/data/banking/silver/customer")