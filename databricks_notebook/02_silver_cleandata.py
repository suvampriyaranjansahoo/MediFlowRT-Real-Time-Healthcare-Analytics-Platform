from pyspark.sql.types import *
from pyspark.sql.functions import *

#ADLS configuration -- pulled from a Databricks secret scope (see
#01_bronze_rawdata.py for setup), never hardcoded in the notebook
SECRET_SCOPE = "patient-flow-kv"
storage_account_name = "<<Storageaccount_name>>"

spark.conf.set(
  f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
  dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-account-key")
)

bronze_path = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"
silver_path = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"

#read from bronze
bronze_df = (
    spark.readStream
    .format("delta")
    .load(bronze_path)
)

#Define Schema
schema = StructType([
    StructField("patient_id", StringType()),
    StructField("gender", StringType()),
    StructField("age", IntegerType()),
    StructField("department", StringType()),
    StructField("admission_time", StringType()),
    StructField("discharge_time", StringType()),
    StructField("bed_id", IntegerType()),
    StructField("hospital_id", IntegerType())
])

#Parse it to dataframe
parsed_df = bronze_df.withColumn("data", from_json(col("raw_json"), schema)).select("data.*")

#convert type to Timestamp
clean_df = parsed_df.withColumn("admission_time", to_timestamp("admission_time"))
clean_df = clean_df.withColumn("discharge_time", to_timestamp("discharge_time"))

# --- Data quality: FLAG invalid records instead of fabricating replacement values ---
# Rule 1: admission_time missing or set in the future is invalid
clean_df = clean_df.withColumn(
    "is_admission_time_valid",
    ~(col("admission_time").isNull() | (col("admission_time") > current_timestamp()))
)

# Rule 2: age outside a plausible human range is invalid (also catch <=0, not just >100)
clean_df = clean_df.withColumn(
    "is_age_valid",
    col("age").isNotNull() & (col("age") > 0) & (col("age") <= 100)
)

# Null out only the specific invalid field (preserves the rest of the record) and
# record why the row was flagged, instead of inventing a fake replacement value.
clean_df = (
    clean_df
    .withColumn("admission_time", when(col("is_admission_time_valid"), col("admission_time")).otherwise(lit(None)))
    .withColumn("age", when(col("is_age_valid"), col("age")).otherwise(lit(None)))
    .withColumn(
        "data_quality_flag",
        concat_ws(
            ",",
            when(~col("is_admission_time_valid"), lit("invalid_admission_time")),
            when(~col("is_age_valid"), lit("invalid_age"))
        )
    )
    .withColumn(
        "is_valid_record",
        col("is_admission_time_valid") & col("is_age_valid")
    )
    .drop("is_admission_time_valid", "is_age_valid")
)

#schema evolution guard (kept defensive in case upstream JSON drops a field)
expected_cols = ["patient_id", "gender", "age", "department", "admission_time",
                  "discharge_time", "bed_id", "hospital_id"]

for col_name in expected_cols:
    if col_name not in clean_df.columns:
        clean_df = clean_df.withColumn(col_name, lit(None))

#Write to silver table (quarantined/flagged rows are included; downstream Gold
#layer or a separate quarantine sink can filter on is_valid_record as needed)
silver_query = (
    clean_df.writeStream
    .format("delta")
    .outputMode("append")
    .option("mergeSchema", "true")
    .option("checkpointLocation", silver_path + "_checkpoint")
    .start(silver_path)
)

silver_query.awaitTermination()
