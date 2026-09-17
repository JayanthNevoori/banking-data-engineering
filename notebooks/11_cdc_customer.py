from pyspark.sql import functions as F

# Read current source snapshot
source_df = spark.read.parquet(
    "/mnt/data/banking/bronze/customer"
)

# Read previous target snapshot
target_df = spark.read.format("delta").load(
    "/mnt/data/banking/silver/customer"
)

# Compare source and target using customer_id
cdc_df = (
    source_df.alias("s")
    .join(
        target_df.alias("t"),
        F.col("s.customer_id") == F.col("t.customer_id"),
        "full_outer"
    )
    .withColumn(
        "cdc_operation",
        F.when(F.col("t.customer_id").isNull(), "INSERT")
         .when(F.col("s.customer_id").isNull(), "DELETE")
         .when(
             F.col("s.modified_date") > F.col("t.modified_date"),
             "UPDATE"
         )
         .otherwise("NO_CHANGE")
    )
)

# Keep only actual changes
changes_df = cdc_df.filter(
    F.col("cdc_operation") != "NO_CHANGE"
)

changes_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/mnt/data/banking/cdc/customer")