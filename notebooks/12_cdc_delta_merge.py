from delta.tables import DeltaTable
from pyspark.sql import functions as F

target_path = "/mnt/data/banking/silver/customer"
cdc_path = "/mnt/data/banking/cdc/customer"

# Load CDC changes
cdc_df = spark.read.format("delta").load(cdc_path)

# Load target Delta table
customer_delta = DeltaTable.forPath(
    spark,
    target_path
)

# Apply INSERT and UPDATE
customer_delta.alias("t").merge(
    cdc_df.alias("s"),
    "t.customer_id = s.customer_id"
).whenMatchedUpdate(
    condition="s.cdc_operation = 'UPDATE'",
    set={
        "first_name": "s.first_name",
        "last_name": "s.last_name",
        "email": "s.email",
        "phone": "s.phone",
        "city": "s.city",
        "state": "s.state",
        "modified_date": "s.modified_date"
    }
).whenNotMatchedInsert(
    condition="s.cdc_operation = 'INSERT'",
    values={
        "customer_id": "s.customer_id",
        "first_name": "s.first_name",
        "last_name": "s.last_name",
        "email": "s.email",
        "phone": "s.phone",
        "city": "s.city",
        "state": "s.state",
        "modified_date": "s.modified_date"
    }
).execute()

# Apply DELETE separately
delete_ids = (
    cdc_df
    .filter(F.col("cdc_operation") == "DELETE")
    .select("customer_id")
)

customer_delta.alias("t").merge(
    delete_ids.alias("s"),
    "t.customer_id = s.customer_id"
).whenMatchedDelete().execute()