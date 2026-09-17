# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import sys, os
sys.path.append(os.path.abspath("../src"))

import yaml
from pyspark.sql import functions as F
from nyc311.schema import bronze_schema, BRONZE_COLUMNS

with open("../conf/config.yaml") as f:
    cfg = yaml.safe_load(f)

catalog, schema = cfg["catalog"], cfg["schema"]
landing = f"/Volumes/{catalog}/{schema}/{cfg['landing_volume']}"
bronze_table = f"{catalog}.{schema}.bronze_complaints"
checkpoint = f"{landing}/_checkpoints/bronze"

# COMMAND ----------

# Incremental landing -> bronze via Auto Loader.
# The checkpoint tracks which page_*.json.gz files have already been ingested, so a
# rerun only appends files that appeared since the last run (no duplicate appends).
# trigger(availableNow=True) = batch semantics: process all new files, then stop.
# The ingest_date=*/ glob scopes the read to data folders, skipping _watermark/ and
# _checkpoints/. Auto Loader adds a _rescued_data column: any source field missing
# from bronze_schema() lands there instead of being silently dropped.
stream = (spark.readStream
    .format("cloudFiles")
    .option("cloudFiles.format", "json")
    .option("cloudFiles.schemaLocation", checkpoint)
    .schema(bronze_schema())
    .load(f"{landing}/ingest_date=*/")
    .withColumn("_ingested_at", F.current_timestamp())
    .withColumn("_source_file", F.col("_metadata.file_path")))

query = (stream.writeStream
    .option("checkpointLocation", checkpoint)
    .trigger(availableNow=True)
    .toTable(bronze_table))

query.awaitTermination()

# COMMAND ----------

# One-time check: does schema inference on the raw landing files see any column our
# explicit BRONZE_COLUMNS list doesn't declare? Anything in the first set is being
# routed to _rescued_data rather than a real column - decide per field whether to add it.
inferred = set(spark.read.json(f"{landing}/ingest_date=*/").columns)
print("in source but not in BRONZE_COLUMNS:", sorted(inferred - set(BRONZE_COLUMNS)))
print("in BRONZE_COLUMNS but not in this sample:", sorted(set(BRONZE_COLUMNS) - inferred))

# COMMAND ----------

# Count reconciliation: bronze row count vs. the record total run_ingestion() reported.
# Expect bronze >= ingestion total - at-least-once ingestion can re-land dupes, which
# silver's deduplicate step absorbs. A shortfall means landing files were missed.
print("bronze_complaints rows:", spark.read.table(bronze_table).count())