from pyspark.sql import functions as F

source_path = "/mnt/data/banking/bronze/customer"
target_path = "/mnt/data/banking/silver/customer"

try:
    # Read source data
    customer_df = spark.read.format("parquet").load(source_path)

    # Basic validation
    if customer_df.limit(1).count() == 0:
        raise ValueError("Source customer data is empty")

    # Example transformation
    processed_df = (
        customer_df
        .withColumn(
            "full_name",
            F.concat_ws(
                " ",
                F.col("first_name"),
                F.col("last_name")
            )
        )
    )

    # Write target
    processed_df.write \
        .format("delta") \
        .mode("append") \
        .save(target_path)

except Exception as e:
    error_message = str(e)

    print(f"Customer pipeline failed: {error_message}")

    raise