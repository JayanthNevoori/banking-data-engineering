from delta.tables import DeltaTable
from pyspark.sql import functions as F

target_path = "/mnt/data/banking/silver/customer_scd2"

# Read latest customer data
source_df = spark.read.format("delta").load(
    "/mnt/data/banking/silver/customer"
)

# Read current SCD2 records
target_df = spark.read.format("delta").load(
    target_path
).filter(
    F.col("is_current") == True
)

# Find changed customers
changed_df = (
    source_df.alias("s")
    .join(
        target_df.alias("t"),
        F.col("s.customer_id") == F.col("t.customer_id"),
        "inner"
    )
    .filter(
        (F.coalesce(F.col("s.email"), F.lit("")) !=
         F.coalesce(F.col("t.email"), F.lit(""))))
        |
        (F.coalesce(F.col("s.city"), F.lit("")) !=
         F.coalesce(F.col("t.city"), F.lit("")))
        |
        (F.coalesce(F.col("s.state"), F.lit("")) !=
         F.coalesce(F.col("t.state"), F.lit("")))
    )
    .select("s.*")
)

# Expire old records
delta_target = DeltaTable.forPath(
    spark,
    target_path
)

delta_target.alias("t").merge(
    changed_df.alias("s"),
    "t.customer_id = s.customer_id AND t.is_current = true"
).whenMatchedUpdate(
    set={
        "effective_end_date": "current_timestamp()",
        "is_current": "false"
    }
).execute()

# Insert new versions
new_versions_df = (
    changed_df
    .withColumn(
        "effective_start_date",
        F.current_timestamp()
    )
    .withColumn(
        "effective_end_date",
        F.lit(None).cast("timestamp")
    )
    .withColumn(
        "is_current",
        F.lit(True)
    )
)

new_versions_df.write \
    .format("delta") \
    .mode("append") \
    .save(target_path)