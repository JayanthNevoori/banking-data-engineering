from pyspark.sql import functions as F

# Read Bronze transaction data
transaction_df = spark.read.parquet(
    "/mnt/data/banking/bronze/transaction"
)

# Clean and standardize transaction data
silver_transaction_df = (
    transaction_df
    .withColumn(
        "transaction_type",
        F.upper(F.trim(F.col("transaction_type")))
    )
    .withColumn(
        "transaction_status",
        F.upper(F.trim(F.col("transaction_status")))
    )
    .withColumn(
        "channel",
        F.upper(F.trim(F.col("channel")))
    )
    .withColumn(
        "transaction_amount",
        F.col("transaction_amount").cast("decimal(18,2)")
    )
    .withColumn(
        "merchant",
        F.trim(F.col("merchant"))
    )
)

# Write Silver Transaction as Delta
silver_transaction_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/mnt/data/banking/silver/transaction")