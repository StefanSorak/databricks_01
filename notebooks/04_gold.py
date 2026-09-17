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


df_agency_performance = gold_agency_performance(df)

# --- Idempotent upsert into gold_agency_performance, keyed on agency, complaint_type, month ---
target_table = f"{catalog}.{schema}.gold_agency_performance"

try:
    target = DeltaTable.forName(spark, target_table)
    table_exists = True
except Exception:
    table_exists = False

if not table_exists:
    df_agency_performance.write.format("delta").saveAsTable(target_table)
    print(f"Table created {target_table}")
else:
    (target.alias("t")
        .merge(df_agency_performance.alias("s"), "t.agency = s.agency AND t.complaint_type = s.complaint_type AND t.month <=> s.month")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"Merged into {target_table}")

# COMMAND ----------


# BR-2 - Borough equity
from nyc311.aggregations import gold_borough_metrics

df_borough_metrics = gold_borough_metrics(df)

# --- Idempotent upsert into gold_borough_metrics, keyed on borough, complaint_type ---
target_table = f"{catalog}.{schema}.gold_borough_metrics"

try:
    target = DeltaTable.forName(spark, target_table)
    table_exists = True
except Exception:
    table_exists = False

if not table_exists:
    df_borough_metrics.write.format("delta").saveAsTable(target_table)
    print(f"Table created {target_table}")
else:
    target = DeltaTable.forName(spark, target_table)
    (target.alias("t")
        .merge(df_borough_metrics.alias("s"), "t.borough <=> s.borough AND t.complaint_type <=> s.complaint_type")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"Merged into {target_table}")