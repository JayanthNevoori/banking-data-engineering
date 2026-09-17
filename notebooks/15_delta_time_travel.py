from pyspark.sql import functions as F

customer_path = "/mnt/data/banking/silver/customer"

# Read the current Delta table
current_df = (
    spark.read
    .format("delta")
    .load(customer_path)
)

# Read an older version
previous_df = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .load(customer_path)
)