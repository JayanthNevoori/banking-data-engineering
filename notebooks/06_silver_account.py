from pyspark.sql import functions as F

# Read Bronze account data
account_df = spark.read.parquet(
    "/mnt/data/banking/bronze/account"
)

# Clean and standardize account data
silver_account_df = (
    account_df
    .withColumn(
        "account_type",
        F.upper(F.trim(F.col("account_type")))
    )
    .withColumn(
        "account_status",
        F.upper(F.trim(F.col("account_status")))
    )
    .withColumn(
        "balance",
        F.col("balance").cast("decimal(18,2)")
    )
)

# Write Silver Account as Delta
silver_account_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/mnt/data/banking/silver/account")