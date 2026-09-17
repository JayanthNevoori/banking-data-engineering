from pyspark.sql import functions as F

# Read BAD customer records
bad_customer_df = spark.read.parquet(
    "/mnt/data/banking/dq/bad/customer"
)

# Create error log
error_log_df = (
    bad_customer_df
    .select(
        "customer_id",
        "first_name",
        "last_name",
        "email",
        "phone",
        "error_reason"
    )
    .withColumn(
        "error_logged_at",
        F.current_timestamp()
    )
)

# Store error log
error_log_df.write.mode("append").parquet(
    "/mnt/data/banking/dq/error_log/customer"
)