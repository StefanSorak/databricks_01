# Databricks notebook source

dbutils.library.restartPython()

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath("../src"))

import yaml
from pyspark.sql import functions as F
from delta.tables import DeltaTable
from nyc311.transformations import build_silver

with open("../conf/config.yaml") as f:
    cfg = yaml.safe_load(f)

catalog, schema = cfg["catalog"], cfg["schema"]

df = spark.read.table(f"{catalog}.{schema}.bronze_complaints")
df = build_silver(df)

# --- BR-5 evidence: bounds-violation count (flagged, not dropped) ---
total = df.count()
invalid = df.filter(~F.col("_duration_valid")).count()
print(f"BR-5: {invalid}/{total} rows flagged with implausible resolution duration (bounds violation — flagged, not dropped)")

# --- Idempotent upsert into silver_complaints, keyed on complaint_id ---
target_table = f"{catalog}.{schema}.silver_complaints"

if not spark.catalog.tableExists(target_table):
    df.write.format("delta").saveAsTable(target_table)
    print(f"Created {target_table} with {total} rows")
else:
    target = DeltaTable.forName(spark, target_table)
    (target.alias("t")
        .merge(df.alias("s"), "t.complaint_id = s.complaint_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"Merged into {target_table}")
