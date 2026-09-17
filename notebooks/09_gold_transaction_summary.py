from pyspark.sql import functions as F

# Read integrated banking data
banking_df = spark.read.format("delta").load(
    "/mnt/data/banking/gold/banking_transactions"
)

# Create transaction summary
gold_transaction_summary = (
    banking_df
    .groupBy(
        "customer_id",
        "full_name",
        "account_type",
        "transaction_type",
        "transaction_status"
    )
    .agg(
        F.count("transaction_id").alias("transaction_count"),
        F.sum("transaction_amount").alias("total_transaction_amount"),
        F.avg("transaction_amount").alias("average_transaction_amount")
    )
)

# Write Gold summary
gold_transaction_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/mnt/data/banking/gold/transaction_summary")