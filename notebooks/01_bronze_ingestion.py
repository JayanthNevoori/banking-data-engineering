from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("Banking Bronze Ingestion") \
    .getOrCreate()

# Source paths
customer_source = "/mnt/data/banking/customer"
branch_source = "/mnt/data/banking/branch"
account_source = "/mnt/data/banking/account"
transaction_source = "/mnt/data/banking/transaction"

# Bronze paths
customer_bronze = "/mnt/data/banking/bronze/customer"
branch_bronze = "/mnt/data/banking/bronze/branch"
account_bronze = "/mnt/data/banking/bronze/account"
transaction_bronze = "/mnt/data/banking/bronze/transaction"

# Read source data
customer_df = spark.read.option("header", "true").csv(customer_source)
branch_df = spark.read.option("header", "true").csv(branch_source)
account_df = spark.read.option("header", "true").csv(account_source)
transaction_df = spark.read.option("header", "true").csv(transaction_source)

# Write raw data to Bronze
customer_df.write.mode("overwrite").parquet(customer_bronze)
branch_df.write.mode("overwrite").parquet(branch_bronze)
account_df.write.mode("overwrite").parquet(account_bronze)
transaction_df.write.mode("overwrite").parquet(transaction_bronze)