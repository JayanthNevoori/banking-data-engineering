from pyspark.sql import functions as F

# Read Bronze customer data
customer_df = spark.read.parquet(
    "/mnt/data/banking/bronze/customer"
)

# Data Quality validation
customer_dq_df = (
    customer_df
    .withColumn(
        "error_reason",
        F.when(
            F.col("customer_id").isNull(),
            "customer_id is null"
        )
        .when(
            F.col("first_name").isNull(),
            "first_name is null"
        )
        .when(
            F.col("email").isNotNull()
            & ~F.col("email").rlike(
                r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
            ),
            "invalid email"
        )
        .when(
            F.col("phone").isNotNull()
            & ~F.col("phone").rlike(r"^[0-9]{10}$"),
            "invalid phone"
        )
    )
    .withColumn(
        "dq_status",
        F.when(F.col("error_reason").isNull(), "GOOD")
         .otherwise("BAD")
    )
)

customer_dq_df.write.mode("overwrite").parquet(
    "/mnt/data/banking/dq/customer"
)


# Banking customer data quality validation