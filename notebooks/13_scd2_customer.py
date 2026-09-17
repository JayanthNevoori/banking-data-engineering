from delta.tables import DeltaTable
from pyspark.sql import functions as F

target_path = "/mnt/data/banking/silver/customer_scd2"

# Current customer data
source_df = spark.read.format("delta").load(
    "/mnt/data/banking/silver/customer"
)

# Prepare SCD2 columns
source_scd2_df = (
    source_df
    .withColumn("effective_start_date", F.current_timestamp())
    .withColumn("effective_end_date", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
)

# Initial load
source_scd2_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save(target_path)