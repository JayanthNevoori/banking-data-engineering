from pyspark.sql import functions as F

# Read Silver datasets
customer_df = spark.read.format("delta").load(
    "/mnt/data/banking/silver/customer"
)

account_df = spark.read.format("delta").load(
    "/mnt/data/banking/silver/account"
)

transaction_df = spark.read.format("delta").load(
    "/mnt/data/banking/silver/transaction"
)

# Join Customer with Account
customer_account_df = (
    customer_df.alias("c")
    .join(
        account_df.alias("a"),
        F.col("c.customer_id") == F.col("a.customer_id"),
        "inner"
    )
    .select(
        F.col("c.customer_id"),
        F.col("c.full_name"),
        F.col("c.city"),
        F.col("c.state"),
        F.col("a.account_id"),
        F.col("a.account_type"),
        F.col("a.account_status"),
        F.col("a.balance")
    )
)

# Join with Transaction
banking_transaction_df = (
    customer_account_df.alias("ca")
    .join(
        transaction_df.alias("t"),
        F.col("ca.account_id") == F.col("t.account_id"),
        "inner"
    )
    .select(
        "ca.customer_id",
        "ca.full_name",
        "ca.city",
        "ca.state",
        "ca.account_id",
        "ca.account_type",
        "ca.account_status",
        "ca.balance",
        "t.transaction_id",
        "t.transaction_date",
        "t.transaction_type",
        "t.transaction_amount",
        "t.transaction_status",
        "t.channel",
        "t.merchant"
    )
)

# Write integrated data
banking_transaction_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/mnt/data/banking/gold/banking_transactions")