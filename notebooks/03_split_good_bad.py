from pyspark.sql import functions as F

# Read DQ-validated customer data
customer_dq_df = spark.read.parquet(
    "/mnt/data/banking/dq/customer"
)

# GOOD records
good_customer_df = customer_dq_df.filter(
    F.col("dq_status") == "GOOD"
)

# BAD records
bad_customer_df = customer_dq_df.filter(
    F.col("dq_status") == "BAD"
)

# Store GOOD records
good_customer_df.write.mode("overwrite").parquet(
    "/mnt/data/banking/dq/good/customer"
)

# Store BAD records
bad_customer_df.write.mode("overwrite").parquet(
    "/mnt/data/banking/dq/bad/customer"
)