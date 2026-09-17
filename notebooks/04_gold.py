# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
dbutils.library.restartPython()

# COMMAND ----------

import sys, os
sys.path.append(os.path.abspath("../src"))

from delta.tables import DeltaTable
import yaml

with open("../conf/config.yaml") as f:
    cfg = yaml.safe_load(f)

catalog, schema = cfg["catalog"], cfg["schema"]

df = spark.read.table(f"{catalog}.{schema}.silver_complaints")

# COMMAND ----------

# BR-1 - Agency Performance
from nyc311.aggregations import gold_agency_performance


df = gold_agency_performance(df)

# --- Idempotent upsert into br_1_agency_performance, keyed on agency, complaint_type, month ---
target_table = f"{catalog}.{schema}.br_1_agency_performance"

if not spark.catalog.tableExists(target_table):
    df.write.format("delta").saveAsTable(target_table)
else:
    target = DeltaTable.forName(spark, target_table)
    (target.alias("t")
        .merge(df.alias("s"), "t.agency = s.agency AND t.complaint_type = s.complaint_type AND t.month <=> s.month")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"Merged into {target_table}")
