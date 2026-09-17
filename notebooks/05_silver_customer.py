from pyspark.sql import functions as F

# Read GOOD customer records
good_customer_df = spark.read.parquet(
    "/mnt/data/banking/dq/good/customer"
)

# Silver transformation
silver_customer_df = (
    good_customer_df
    .withColumn(
        "full_name",
        F.concat_ws(
            " ",
            F.col("first_name"),
            F.col("last_name")
        )
    )
    .withColumn(
        "email",
        F.lower(F.trim(F.col("email")))
    )
    .withColumn(
        "city",
        F.initcap(F.trim(F.col("city")))
    )
    .withColumn(
        "state",
        F.initcap(F.trim(F.col("state")))
    )
)

# Write Silver data
silver_customer_df.write \
    .format("delta") \
    .mode("overwrite") \
    .save("/mnt/data/banking/silver/customer")