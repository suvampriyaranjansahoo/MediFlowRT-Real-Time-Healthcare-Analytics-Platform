from pyspark.sql import functions as F
from pyspark.sql.functions import lit, col, expr, current_timestamp, to_timestamp, sha2, concat_ws, coalesce
from delta.tables import DeltaTable
from pyspark.sql import Window

#ADLS configuration -- pulled from a Databricks secret scope (see
#01_bronze_rawdata.py for setup), never hardcoded in the notebook
SECRET_SCOPE = "patient-flow-kv"
storage_account_name = "<<Storageaccount_name>>"

spark.conf.set(
  f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
  dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-account-key")
)

# Paths
silver_path = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"
gold_dim_patient = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"
gold_dim_department = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"
gold_fact = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"


def next_surrogate_keys(df, key_col="surrogate_key", existing_max=0):
    """
    Generates collision-free, sequential surrogate keys.
    monotonically_increasing_id() is NOT safe to call independently across
    separate write operations (initial load vs. incremental insert) because
    the ranges it produces depend on partitioning and are not guaranteed to
    avoid overlap between separate DataFrame executions. This uses
    row_number() offset by the current max key in the target table instead,
    so keys never collide across runs.

    Note: row_number() over an unpartitioned window is a single-partition
    operation and won't scale to very large incremental batches. For high
    volume, swap this for an ID-generation service or a partitioned
    zipWithIndex-based approach.
    """
    w = Window.orderBy(F.lit(1))
    return df.withColumn(key_col, F.row_number().over(w) + F.lit(existing_max))


def get_max_key(path, key_col="surrogate_key"):
    if not DeltaTable.isDeltaTable(spark, path):
        return 0
    max_val = spark.read.format("delta").load(path).agg(F.max(col(key_col))).collect()[0][0]
    return max_val if max_val is not None else 0


# Read silver data, keep only records that passed data-quality checks in the
# silver step (see is_valid_record). Bad records stay quarantined in silver
# instead of silently flowing into the curated Gold/BI layer.
silver_df_all = spark.read.format("delta").load(silver_path)
silver_df = silver_df_all.filter(col("is_valid_record") == True)

# Define window for latest admission per patient
w = Window.partitionBy("patient_id").orderBy(F.col("admission_time").desc())

silver_df = (
    silver_df
    .withColumn("row_num", F.row_number().over(w))  # Rank by latest admission_time
    .filter(F.col("row_num") == 1)                  # Keep only latest row
    .drop("row_num")
)

# ---------------- Patient Dimension Table (SCD Type 2) ----------------
incoming_patient = (
    silver_df
    .select("patient_id", "gender", "age")
    .withColumn("effective_from", current_timestamp())
)

if not DeltaTable.isDeltaTable(spark, gold_dim_patient):
    # initialize table with schema and empty data, keys start at 1
    (
        next_surrogate_keys(incoming_patient, existing_max=0)
        .withColumn("effective_to", lit(None).cast("timestamp"))
        .withColumn("is_current", lit(True))
        .write.format("delta").mode("overwrite").save(gold_dim_patient)
    )

target_patient = DeltaTable.forPath(spark, gold_dim_patient)

# hash to detect attribute changes
incoming_patient = incoming_patient.withColumn(
    "_hash",
    F.sha2(F.concat_ws("||", F.coalesce(col("gender"), lit("NA")), F.coalesce(col("age").cast("string"), lit("NA"))), 256)
)

target_patient_df = spark.read.format("delta").load(gold_dim_patient).withColumn(
    "_target_hash",
    F.sha2(F.concat_ws("||", F.coalesce(col("gender"), lit("NA")), F.coalesce(col("age").cast("string"), lit("NA"))), 256)
).select("surrogate_key", "patient_id", "gender", "age", "is_current", "_target_hash", "effective_from", "effective_to")

incoming_patient.createOrReplaceTempView("incoming_patient_tmp")
target_patient_df.createOrReplaceTempView("target_patient_tmp")

# 1) Mark old current rows as not current where changed
changes_df = spark.sql("""
SELECT t.surrogate_key, t.patient_id
FROM target_patient_tmp t
JOIN incoming_patient_tmp i
  ON t.patient_id = i.patient_id
WHERE t.is_current = true AND t._target_hash <> i._hash
""")

changed_keys = [row['surrogate_key'] for row in changes_df.collect()]

if changed_keys:
    target_patient.update(
        condition=expr("is_current = true AND surrogate_key IN ({})".format(",".join([str(k) for k in changed_keys]))),
        set={
            "is_current": expr("false"),
            "effective_to": expr("current_timestamp()")
        }
    )

# 2) Insert new rows for changed & new records, keys offset from current max
inserts_df = spark.sql("""
SELECT i.patient_id, i.gender, i.age, i.effective_from, i._hash
FROM incoming_patient_tmp i
LEFT JOIN target_patient_tmp t
  ON i.patient_id = t.patient_id AND t.is_current = true
WHERE t.patient_id IS NULL OR t._target_hash <> i._hash
""")

if inserts_df.count() > 0:
    current_max_key = get_max_key(gold_dim_patient)
    inserts_final = (
        next_surrogate_keys(inserts_df, existing_max=current_max_key)
        .withColumn("effective_to", lit(None).cast("timestamp"))
        .withColumn("is_current", lit(True))
        .select("surrogate_key", "patient_id", "gender", "age", "effective_from", "effective_to", "is_current")
    )
    inserts_final.write.format("delta").mode("append").save(gold_dim_patient)


# ---------------- Department Dimension Table ----------------
# Department list is small/slow-changing, so a full rebuild with fresh
# sequential keys each run is fine here (unlike the patient dimension).
incoming_dept = (
    silver_df
    .select("department", "hospital_id")
    .dropDuplicates(["department", "hospital_id"])
)

incoming_dept = next_surrogate_keys(incoming_dept, existing_max=0)

(
    incoming_dept
    .select("surrogate_key", "department", "hospital_id")
    .write.format("delta").mode("overwrite").save(gold_dim_department)
)


# ---------------- Fact Table ----------------
dim_patient_df = (
    spark.read.format("delta").load(gold_dim_patient)
    .filter(col("is_current") == True)
    .select(col("surrogate_key").alias("surrogate_key_patient"), "patient_id", "gender", "age")
)

dim_dept_df = (
    spark.read.format("delta").load(gold_dim_department)
    .select(col("surrogate_key").alias("surrogate_key_dept"), "department", "hospital_id")
)

fact_base = (
    silver_df
    .select("patient_id", "department", "hospital_id", "admission_time", "discharge_time", "bed_id")
    .withColumn("admission_date", F.to_date("admission_time"))
)

fact_enriched = (
    fact_base
    .join(dim_patient_df, on="patient_id", how="left")
    .join(dim_dept_df, on=["department", "hospital_id"], how="left")
)

fact_enriched = (
    fact_enriched
    .withColumn("length_of_stay_hours",
                (F.unix_timestamp(col("discharge_time")) - F.unix_timestamp(col("admission_time"))) / 3600.0)
    .withColumn("is_currently_admitted", F.when(col("discharge_time") > current_timestamp(), lit(True)).otherwise(lit(False)))
    .withColumn("event_ingestion_time", current_timestamp())
)

fact_final = next_surrogate_keys(fact_enriched, key_col="fact_id", existing_max=0).select(
    "fact_id",
    col("surrogate_key_patient").alias("patient_sk"),
    col("surrogate_key_dept").alias("department_sk"),
    "admission_time",
    "discharge_time",
    "admission_date",
    "length_of_stay_hours",
    "is_currently_admitted",
    "bed_id",
    "event_ingestion_time"
)

# Persist fact table partitioned by admission_date (matches the original
# comment's intent, which the previous version stated but never applied)
(
    fact_final.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("admission_date")
    .save(gold_fact)
)

# Quick sanity checks
print("Patient dim count:", spark.read.format("delta").load(gold_dim_patient).count())
print("Department dim count:", spark.read.format("delta").load(gold_dim_department).count())
print("Fact rows:", spark.read.format("delta").load(gold_fact).count())
