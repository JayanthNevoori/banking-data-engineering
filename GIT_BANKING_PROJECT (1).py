# Databricks notebook source
# MAGIC %sql
# MAGIC
# MAGIC CREATE DATABASE IF NOT EXISTS banking_project;
# MAGIC SHOW DATABASES;

# COMMAND ----------

# MAGIC %md Step 2 — Create Source Tables
# MAGIC
# MAGIC Now we create the 4 main banking source tables inside banking_project.

# COMMAND ----------

# MAGIC %sql
# MAGIC USE banking_project;
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS customer (
# MAGIC     customer_id STRING,
# MAGIC     first_name STRING,
# MAGIC     last_name STRING,
# MAGIC     date_of_birth DATE,
# MAGIC     gender STRING,
# MAGIC     phone STRING,
# MAGIC     email STRING,
# MAGIC     city STRING,
# MAGIC     state STRING,
# MAGIC     customer_type STRING,
# MAGIC     created_date DATE,
# MAGIC     modified_date TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS branch (
# MAGIC     branch_id STRING,
# MAGIC     branch_name STRING,
# MAGIC     city STRING,
# MAGIC     state STRING,
# MAGIC     branch_type STRING
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS account (
# MAGIC     account_id STRING,
# MAGIC     customer_id STRING,
# MAGIC     branch_id STRING,
# MAGIC     account_type STRING,
# MAGIC     account_status STRING,
# MAGIC     opening_date DATE,
# MAGIC     closing_date DATE,
# MAGIC     balance DECIMAL(18,2),
# MAGIC     modified_date TIMESTAMP
# MAGIC );
# MAGIC
# MAGIC CREATE TABLE IF NOT EXISTS transaction_data (
# MAGIC     transaction_id STRING,
# MAGIC     account_id STRING,
# MAGIC     transaction_date DATE,
# MAGIC     transaction_type STRING,
# MAGIC     transaction_amount DECIMAL(18,2),
# MAGIC     transaction_status STRING,
# MAGIC     channel STRING,
# MAGIC     merchant STRING,
# MAGIC     modified_date TIMESTAMP
# MAGIC );

# COMMAND ----------

#Bronze Ingestion
# Read source tables
customer_df = spark.table("banking_project.customer")
branch_df = spark.table("banking_project.branch")
account_df = spark.table("banking_project.account")
transaction_df = spark.table("banking_project.transaction_data")

# Write to Bronze
customer_df.write.format("delta").mode("overwrite") \
    .saveAsTable("banking_project.bronze_customer")

branch_df.write.format("delta").mode("overwrite") \
    .saveAsTable("banking_project.bronze_branch")

account_df.write.format("delta").mode("overwrite") \
    .saveAsTable("banking_project.bronze_account")

transaction_df.write.format("delta").mode("overwrite") \
    .saveAsTable("banking_project.bronze_transaction")

# COMMAND ----------

#Step 5 — Customer Data Quality
from pyspark.sql import functions as F

customer_df = spark.table("banking_project.bronze_customer")

valid_customer = customer_df.filter(
    F.col("customer_id").isNotNull()
    & F.col("first_name").isNotNull()
    & F.col("last_name").isNotNull()
    & F.col("phone").rlike(r"^[6-9][0-9]{9}$")
    & F.col("email").rlike(
        r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
    )
)

# COMMAND ----------

#Step 6 — Customer DQ Error Handling
from pyspark.sql import functions as F

customer_dq = (
    customer_df
    .withColumn(
        "error_reason",
        F.concat_ws(
            ", ",
            F.when(F.col("customer_id").isNull(), "NULL_CUSTOMER_ID"),
            F.when(F.col("first_name").isNull(), "NULL_FIRST_NAME"),
            F.when(F.col("last_name").isNull(), "NULL_LAST_NAME"),
            F.when(
                F.col("phone").isNull(),
                "NULL_PHONE"
            ).when(
                ~F.col("phone").rlike(r"^[6-9][0-9]{9}$"),
                "INVALID_PHONE"
            ),
            F.when(
                F.col("email").isNull(),
                "NULL_EMAIL"
            ).when(
                ~F.col("email").rlike(
                    r"^[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}$"
                ),
                "INVALID_EMAIL"
            )
        )
    )
    .withColumn(
        "dq_status",
        F.when(F.col("error_reason") == "", "GOOD")
         .otherwise("BAD")
    )
)

# COMMAND ----------

#Step 7 — Separate GOOD and BAD Records

# COMMAND ----------

good_customer = customer_dq.filter(
    F.col("dq_status") == "GOOD"
)

bad_customer = customer_dq.filter(
    F.col("dq_status") == "BAD"
)

# COMMAND ----------

#Step 8 — Save BAD Records to DQ Error Log
dq_error_log = (
    bad_customer
    .select(
        F.col("customer_id").alias("record_id"),
        F.lit("customer").alias("table_name"),
        F.lit("DATA_QUALITY").alias("rule_name"),
        F.col("error_reason"),
        F.current_timestamp().alias("processed_date")
    )
)

dq_error_log.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable("banking_project.dq_error_log")

# COMMAND ----------

#Step 9: Save GOOD Customer data to Silver Delta.
silver_customer = (
    good_customer
    .drop("dq_status", "error_reason")
)

silver_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("banking_project.silver_customer")

# COMMAND ----------

# DBTITLE 1,Cell 11
#Account Silver Processing.
account_df = spark.table("banking_project.account")

silver_account = (
    account_df
    .filter(F.col("account_id").isNotNull())
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("account_status").isin(
        "ACTIVE", "CLOSED", "BLOCKED", "DORMANT"
    ))
)

silver_account.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("banking_project.silver_account")

# COMMAND ----------

# DBTITLE 1,Cell 12
#Step 11 — Transaction Silver Processing
transaction_df = spark.table(
    "banking_project.transaction_data"
)

silver_transaction = (
    transaction_df
    .filter(F.col("transaction_id").isNotNull())
    .filter(F.col("account_id").isNotNull())
    .filter(F.col("transaction_amount") > 0)
)
silver_transaction.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "banking_project.transaction"
    )

# COMMAND ----------

# DBTITLE 1,Cell 13
#Step 12 — Integrate Customer + Account + Transaction
customer_df = spark.table("banking_project.silver_customer")
account_df = spark.table("banking_project.silver_account")
transaction_df = spark.table("banking_project.transaction")

customer_account_transaction = (
    transaction_df
    .join(
        account_df,
        "account_id",
        "inner"
    )
    .join(
        customer_df,
        "customer_id",
        "inner"
    )
    .select(
        "transaction_id",
        "transaction_date",
        "transaction_type",
        "transaction_amount",
        "transaction_status",
        "channel",
        "merchant",
        "account_id",
        "account_type",
        "customer_id",
        F.concat(F.col("first_name"), F.lit(" "), F.col("last_name")).alias("full_name"),
        "city",
        "state"
    )
)

# COMMAND ----------

#Step 13 — Create Business-Ready Gold Dataset
gold_transaction_summary = (
    customer_account_transaction
    .groupBy(
        "customer_id",
        "full_name",
        "account_type",
        "city",
        "state"
    )
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.sum("transaction_amount").alias("total_transaction_amount")
    )
)
gold_transaction_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable(
        "banking_project.gold_transaction_summary"
    )

# COMMAND ----------

#Step 14 — Incremental Customer Load + Watermark
from pyspark.sql import functions as F

source_customer = spark.table(
    "banking_project.bronze_customer"
)

last_processed = (
    spark.table("banking_project.silver_customer")
    .agg(F.max("modified_date").alias("watermark"))
    .first()["watermark"]
)

incremental_customer = source_customer.filter(
    F.col("modified_date") > last_processed
)
incremental_customer.write \
    .format("delta") \
    .mode("append") \
    .saveAsTable(
        "banking_project.silver_customer"
    )

# COMMAND ----------

#Step 15 — CDC: Identify INSERT / UPDATE / DELETE
from pyspark.sql import functions as F

source = spark.table("banking_project.bronze_customer")
target = spark.table("banking_project.silver_customer")

cdc_data = (
    source.alias("s")
    .join(
        target.alias("t"),
        F.col("s.customer_id") == F.col("t.customer_id"),
        "full_outer"
    )
    .withColumn(
        "cdc_operation",
        F.when(F.col("t.customer_id").isNull(), "INSERT")
         .when(F.col("s.customer_id").isNull(), "DELETE")
         .when(
             (F.col("s.first_name") != F.col("t.first_name")) |
             (F.col("s.last_name") != F.col("t.last_name")) |
             (F.col("s.phone") != F.col("t.phone")) |
             (F.col("s.email") != F.col("t.email")) |
             (F.col("s.city") != F.col("t.city")) |
             (F.col("s.state") != F.col("t.state")),
             "UPDATE"
         )
    )
)

# COMMAND ----------

# DBTITLE 1,Cell 17
#Step 16 — Apply CDC Changes Using Delta MERGE
from delta.tables import DeltaTable

from pyspark.sql.window import Window

target = DeltaTable.forName(spark, "banking_project.silver_customer")

# Deduplicate source changes - keep latest per customer_id
source_changes = (
    cdc_data
    .filter(F.col("cdc_operation").isNotNull())
    .select(
        F.col("s.customer_id").alias("customer_id"),
        F.col("s.first_name").alias("first_name"),
        F.col("s.last_name").alias("last_name"),
        F.col("s.date_of_birth").alias("date_of_birth"),
        F.col("s.gender").alias("gender"),
        F.col("s.phone").alias("phone"),
        F.col("s.email").alias("email"),
        F.col("s.city").alias("city"),
        F.col("s.state").alias("state"),
        F.col("s.customer_type").alias("customer_type"),
        F.col("s.created_date").alias("created_date"),
        F.col("s.modified_date").alias("modified_date"),
        F.col("cdc_operation")
    )
    .withColumn("row_num", F.row_number().over(
        Window.partitionBy("customer_id").orderBy(F.col("modified_date").desc())
    ))
    .filter(F.col("row_num") == 1)
    .drop("row_num")
)

(
    target.alias("t")
    .merge(
        source_changes.alias("s"),
        "t.customer_id = s.customer_id"
    )
    .whenMatchedUpdate(
        condition="s.cdc_operation = 'UPDATE'",
        set={
            "first_name": "s.first_name",
            "last_name": "s.last_name",
            "phone": "s.phone",
            "email": "s.email",
            "city": "s.city",
            "state": "s.state",
            "modified_date": "s.modified_date"
        }
    )
    .whenNotMatchedInsert(
        values={
            "customer_id": "s.customer_id",
            "first_name": "s.first_name",
            "last_name": "s.last_name",
            "date_of_birth": "s.date_of_birth",
            "gender": "s.gender",
            "phone": "s.phone",
            "email": "s.email",
            "city": "s.city",
            "state": "s.state",
            "customer_type": "s.customer_type",
            "created_date": "s.created_date",
            "modified_date": "s.modified_date"
        }
    )
    .execute()
)

# COMMAND ----------

#Step 17 — SCD Type 2 Implementation
from pyspark.sql import functions as F

silver_customer_scd = (
    spark.table("banking_project.silver_customer")
    .withColumn("effective_start_date", F.col("modified_date"))
    .withColumn("effective_end_date", F.lit(None).cast("timestamp"))
    .withColumn("is_current", F.lit(True))
)

silver_customer_scd.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("banking_project.silver_customer_scd")

# COMMAND ----------

#Step 18 — Apply SCD Type 2 Changes
from delta.tables import DeltaTable
from pyspark.sql import functions as F

target = DeltaTable.forName(
    spark,
    "banking_project.silver_customer_scd"
)

updates = (
    source_changes
    .filter(F.col("cdc_operation") == "UPDATE")
)
(
    target.alias("t")
    .merge(
        updates.alias("s"),
        """
        t.customer_id = s.customer_id
        AND t.is_current = true
        """
    )
    .whenMatchedUpdate(
        set={
            "effective_end_date": "s.modified_date",
            "is_current": "false"
        }
    )
    .execute()
)
(
    target.alias("t")
    .merge(
        updates.alias("s"),
        """
        t.customer_id = s.customer_id
        AND t.is_current = true
        """
    )
    .whenMatchedUpdate(
        set={
            "effective_end_date": "s.modified_date",
            "is_current": "false"
        }
    )
    .execute()
)


# COMMAND ----------

#Step 19 — Delta Time Travel Verification
from delta.tables import DeltaTable

history = DeltaTable.forName(
    spark,
    "banking_project.silver_customer_scd"
).history()




# COMMAND ----------

old_version = (
    spark.read
    .format("delta")
    .option("versionAsOf", 0)
    .table("banking_project.silver_customer_scd")
)



# COMMAND ----------

#Step 20 — Final Spark Optimization
from pyspark.sql import functions as F

# Enable Adaptive Query Execution
# spark.conf.set("spark.sql.adaptive.enabled", "true")
# spark.conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
# spark.conf.set("spark.sql.adaptive.skewJoin.enabled", "true")

customer = (
    spark.table("banking_project.silver_customer")
    .select(
        "customer_id",
        "full_name",
        "city",
        "state",
        "customer_type"
    )
)

account = (
    spark.table("banking_project.silver_account")
    .select(
        "account_id",
        "customer_id",
        "account_type",
        "account_status",
        "balance"
    )
    .filter(F.col("account_status") == "ACTIVE")
)

optimized_data = (
    account
    .join(
        F.broadcast(customer),
        "customer_id",
        "inner"
    )
)

# COMMAND ----------

#Step 21 — Production Error Handling + Logging
from pyspark.sql import functions as F

try:
    source_customer = spark.table(
        "banking_project.bronze_customer"
    )

    source_customer.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(
            "banking_project.silver_customer"
        )

except Exception as e:

    error_message = str(e)

    error_log = spark.createDataFrame(
        [
            (
                "customer_load",
                "FAILED",
                error_message
            )
        ],
        [
            "job_name",
            "status",
            "error_message"
        ]
    ).withColumn(
        "error_time",
        F.current_timestamp()
    )

    error_log.write \
        .format("delta") \
        .mode("append") \
        .saveAsTable(
            "banking_project.pipeline_error_log"
        )

    raise

# COMMAND ----------

#Step 22 — Final Production Cleanup + GitHub Structure
from pyspark.sql import functions as F

customer_df = spark.table("banking_project.bronze_customer")

silver_customer = (
    customer_df
    .filter(F.col("customer_id").isNotNull())
    .filter(F.col("first_name").isNotNull())
    .filter(F.col("last_name").isNotNull())
    .withColumn(
        "full_name",
        F.concat_ws(" ", F.col("first_name"), F.col("last_name"))
    )
)

silver_customer.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("banking_project.silver_customer")

# COMMAND ----------

# DBTITLE 1,Cell 25
#22.2 Final Gold Dataset
from pyspark.sql import functions as F

customer_df = spark.table("banking_project.silver_customer")
account_df = spark.table("banking_project.silver_account")
transaction_df = spark.table("banking_project.transaction")

gold_transaction_summary = (
    transaction_df
    .join(account_df, "account_id", "inner")
    .join(customer_df, "customer_id", "inner")
    .groupBy(
        "customer_id",
        "full_name",
        "account_type",
        "city",
        "state"
    )
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.sum("transaction_amount").alias("total_transaction_amount")
    )
)

gold_transaction_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("banking_project.gold_transaction_summary")

# COMMAND ----------

# DBTITLE 1,Cell 26
#22.2 Final Gold Dataset
from pyspark.sql import functions as F

customer_df = spark.table("banking_project.silver_customer")
account_df = spark.table("banking_project.silver_account")
transaction_df = spark.table("banking_project.transaction")

gold_transaction_summary = (
    transaction_df
    .join(account_df, "account_id", "inner")
    .join(customer_df, "customer_id", "inner")
    .groupBy(
        "customer_id",
        "full_name",
        "account_type",
        "city",
        "state"
    )
    .agg(
        F.count("transaction_id").alias("total_transactions"),
        F.sum("transaction_amount").alias("total_transaction_amount")
    )
)

gold_transaction_summary.write \
    .format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable("banking_project.gold_transaction_summary")

# COMMAND ----------

# MAGIC %md
# MAGIC Customer       Account Type    Transactions    Total Amount
# MAGIC -----------------------------------------------------------
# MAGIC Jayanth        SAVINGS         12              85,000
# MAGIC Vamshi         CURRENT          8              52,000
# MAGIC Sarika         SAVINGS         15             120,000

# COMMAND ----------

# MAGIC %md
# MAGIC 22.3 GitHub Final Structure
# MAGIC
# MAGIC Mana project ni GitHub lo ila maintain cheyyachu:
# MAGIC
# MAGIC banking-data-engineering/
# MAGIC │
# MAGIC ├── README.md
# MAGIC │
# MAGIC ├── sql/
# MAGIC │   ├── 01_create_database.sql
# MAGIC │   ├── 02_create_source_tables.sql
# MAGIC │   └── 03_insert_sample_data.sql
# MAGIC │
# MAGIC ├── notebooks/
# MAGIC │   ├── 04_bronze_ingestion.py
# MAGIC │   ├── 05_customer_dq.py
# MAGIC │   ├── 06_customer_dq_errors.py
# MAGIC │   ├── 07_split_good_bad.py
# MAGIC │   ├── 08_dq_error_log.py
# MAGIC │   ├── 09_silver_customer.py
# MAGIC │   ├── 10_silver_account.py
# MAGIC │   ├── 11_silver_transaction.py
# MAGIC │   ├── 12_integrate_banking_data.py
# MAGIC │   ├── 13_gold_transaction_summary.py
# MAGIC │   ├── 14_incremental_customer.py
# MAGIC │   ├── 15_cdc_customer.py
# MAGIC │   ├── 16_cdc_delta_merge.py
# MAGIC │   ├── 17_scd2_initial_setup.py
# MAGIC │   ├── 18_scd2_apply_changes.py
# MAGIC │   ├── 19_delta_time_travel.py
# MAGIC │   ├── 20_spark_optimization.py
# MAGIC │   ├── 21_error_handling.py
# MAGIC │   └── 22_final_gold_cleanup.py
# MAGIC │
# MAGIC └── docs/
# MAGIC     └── architecture.md
# MAGIC Final architecture
# MAGIC On-Prem Banking DB
# MAGIC         ↓
# MAGIC Azure Data Factory
# MAGIC         ↓
# MAGIC Self Hosted Integration Runtime
# MAGIC         ↓
# MAGIC ADLS Gen2
# MAGIC         ↓
# MAGIC Bronze
# MAGIC         ↓
# MAGIC Azure Databricks
# MAGIC         ↓
# MAGIC PySpark + DQ
# MAGIC         ↓
# MAGIC Silver / Delta Lake
# MAGIC         ↓
# MAGIC CDC + SCD2
# MAGIC         ↓
# MAGIC Gold
# MAGIC         ↓
# MAGIC Azure Synapse
# MAGIC         ↓
# MAGIC Power BI
# MAGIC Interview one-liner
# MAGIC
# MAGIC "I built an end-to-end banking data engineering pipeline where ADF handles ingestion, ADLS stores the data, Databricks and PySpark perform transformation and data quality, Delta Lake manages reliable Silver and Gold datasets, Synapse serves analytics, and Power BI provides reporting."
# MAGIC
# MAGIC Important: display(), debugging print(), unnecessary SELECT *, hardcoded secrets — GitHub production code lo avoid chestham.
# MAGIC
# MAGIC With this, our core Databricks coding project is complete. Next major work is the Azure integration layer: ADF → SHIR → ADLS → Databricks → Synapse → Power BI, which connects this Databricks work into a complete real-world project.