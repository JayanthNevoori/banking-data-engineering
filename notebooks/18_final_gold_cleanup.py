from pyspark.sql import functions as F

gold_path = "/mnt/data/banking/gold/banking_transactions"

# Read integrated Gold data
gold_df = spark.read.format("delta").load(gold_path)

# Final cleanup
final_gold_df = (
    gold_df
    .filter(F.col("transaction_id").isNotNull())
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("account_id").isNotNull())
    .filter(F.col("transaction_status") == "SUCCESS")
    .withColumn(
        "transaction_amount",
        F.col("transaction_amount").cast("decimal(18,2)")
    )
    .select(
        "customer_id",
        "full_name",
        "account_id",
        "account_type",
        "transaction_id",
        "transaction_date",
        "transaction_type",
        "transaction_amount",
        "transaction_status",
        "channel",
        "merchant"
    )
)

# Write final Gold dataset
final_gold_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/mnt/data/banking/gold/final_banking_transactions")
    