from pyspark.sql.functions import *

# ---------------------------------------------------------------
# Secrets are pulled from a Databricks secret scope backed by Azure Key
# Vault, instead of being hardcoded/pasted into the notebook. Set this up
# once via:
#   databricks secrets create-scope --scope patient-flow-kv
#   databricks secrets put --scope patient-flow-kv --key eventhub-conn-str
#   databricks secrets put --scope patient-flow-kv --key storage-account-key
# (or link the scope directly to an existing Key Vault instance).
# Nothing sensitive lives in source control this way -- only the scope/key
# *names* below, which are not secrets themselves.
# ---------------------------------------------------------------
SECRET_SCOPE = "patient-flow-kv"

event_hub_namespace = "<<Namespace_hostname>>"
event_hub_name = "<<Eventhub_Name>>"
event_hub_conn_str = dbutils.secrets.get(scope=SECRET_SCOPE, key="eventhub-conn-str")

kafka_options = {
    'kafka.bootstrap.servers': f"{event_hub_namespace}:9093",
    'subscribe': event_hub_name,
    'kafka.security.protocol': 'SASL_SSL',
    'kafka.sasl.mechanism': 'PLAIN',
    'kafka.sasl.jaas.config': f'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username="$ConnectionString" password="{event_hub_conn_str}";',
    'startingOffsets': 'latest',
    'failOnDataLoss': 'false'
}

# Read from Event Hub
raw_df = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

# Cast raw Kafka payload to JSON string, and keep the Kafka metadata
# (offset/partition/timestamp) so bronze preserves full lineage instead of
# discarding it at parse time.
json_df = raw_df.selectExpr(
    "CAST(value AS STRING) as raw_json",
    "topic",
    "partition",
    "offset",
    "timestamp as kafka_timestamp"
).withColumn("ingestion_time", current_timestamp())

# ADLS configuration -- also pulled from the secret scope, never hardcoded
storage_account_name = "<<Storageaccount_name>>"
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    dbutils.secrets.get(scope=SECRET_SCOPE, key="storage-account-key")
)

bronze_path = "abfss://<<container>>@<<Storageaccount_name>>.core.windows.net/<<path>>"

# Write stream to bronze, keeping a handle to the query so it can be
# monitored / stopped gracefully instead of firing and forgetting.
bronze_query = (
    json_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", "dbfs:/mnt/bronze/_checkpoints/patient_flow")
    .start(bronze_path)
)

bronze_query.awaitTermination()
