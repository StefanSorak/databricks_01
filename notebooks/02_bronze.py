# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import sys, os
sys.path.append(os.path.abspath("../src"))

import yaml
from pyspark.sql import functions as F
from nyc311.schema import bronze_schema

with open("../conf/config.yaml") as f:
    cfg = yaml.safe_load(f)

catalog, schema = cfg["catalog"], cfg["schema"]
landing = f"/Volumes/{catalog}/{schema}/{cfg['landing_volume']}"
bronze_table = f"{catalog}.{schema}.bronze_complaints"
checkpoint = f"{landing}/_checkpoints/bronze"

# Auto Loader: checkpoint tracks ingested files, so reruns only append new ones.
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

# Reconciliation: expect >= ingestion's reported total (dupes get absorbed by silver's dedupe).
print("bronze_complaints rows:", spark.read.table(bronze_table).count())