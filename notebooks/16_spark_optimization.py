from pyspark.sql import functions as F

customer_df = spark.read.format("delta").load(
    "/mnt/data/banking/silver/customer"
)

transaction_df = spark.read.format("delta").load(
    "/mnt/data/banking/silver/transaction"
)

# Filter early
filtered_transaction_df = transaction_df.filter(
    F.col("transaction_status") == "SUCCESS"
)

# Repartition before large processing
transaction_repartitioned_df = filtered_transaction_df.repartition(
    "account_id"
)

# Broadcast the smaller Customer dataset
result_df = transaction_repartitioned_df.join(
    F.broadcast(customer_df),
    "customer_id",
    "inner"
)

# Select only required columns
final_df = result_df.select(
    "customer_id",
    "full_name",
    "account_id",
    "transaction_id",
    "transaction_amount",
    "transaction_status"
)