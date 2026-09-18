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
from pyspark.sql import functions as F
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
    (target.alias("t")
        .merge(df_agency_performance.alias("s"), "t.agency = s.agency AND t.complaint_type = s.complaint_type AND t.month <=> s.month")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"Merged into {target_table}")
except Exception:
    df_agency_performance.write.format("delta").saveAsTable(target_table)
    print(f"Table created {target_table}")

# COMMAND ----------


# BR-2 - Borough equity
from nyc311.aggregations import gold_borough_metrics

df_borough_metrics = gold_borough_metrics(df)

# --- Idempotent upsert into gold_borough_metrics, keyed on borough, complaint_type ---
target_table = f"{catalog}.{schema}.gold_borough_metrics"

try:
    target = DeltaTable.forName(spark, target_table)
    (target.alias("t")
        .merge(df_borough_metrics.alias("s"), "t.borough <=> s.borough AND t.complaint_type <=> s.complaint_type")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"Merged into {target_table}")
except Exception:
    df_borough_metrics.write.format("delta").saveAsTable(target_table)
    print(f"Table created {target_table}")

# COMMAND ----------

# BR-3 - Weather data (fetch + land as a table; join/aggregation not built yet)
from datetime import date
from nyc311.weather import fetch_daily_weather
from nyc311.aggregations import gold_weather_date

weather_rows = fetch_daily_weather(cfg["start_date"][:10], date.today().isoformat())
weather_df = spark.createDataFrame(weather_rows).withColumn("date", F.to_date("date"))

# --- Always a full window refetch, not an incremental append, so overwrite is
# correct here (unlike the MERGE tables above): there's no watermark, and the
# table only ever needs to hold "today's view of the whole window," not an
# accumulated history. Overwrite mode also creates the table on first run for free. ---
target_table = f"{catalog}.{schema}.weather_daily"
weather_df.write.format("delta").mode("overwrite").saveAsTable(target_table)
print(f"Wrote {weather_df.count()} rows to {target_table}")

df_weather_date = gold_weather_date(df, weather_df)
target_table = f"{catalog}.{schema}.gold_weather_date"

try:
    target = DeltaTable.forName(spark, target_table)
    (target.alias("t")
        .merge(df_weather_date.alias("s"), "t.date = s.date")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"Merged into {target_table}")
except Exception:
    df_weather_date.write.format("delta").saveAsTable(target_table)
    print(f"Table created {target_table}")

# COMMAND ----------

# BR-4 - Channel shift
from nyc311.aggregations import gold_channel_trends

df_channel_trends = gold_channel_trends(df)

# --- Idempotent upsert into gold_channel_trends, keyed on intake_channel, month ---
target_table = f"{catalog}.{schema}.gold_channel_trends"

try:
    target = DeltaTable.forName(spark, target_table)
    (target.alias("t")
        .merge(df_channel_trends.alias("s"), "t.intake_channel <=> s.intake_channel AND t.month <=> s.month")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute())
    print(f"Merged into {target_table}")
except Exception:
    df_channel_trends.write.format("delta").saveAsTable(target_table)
    print(f"Table created {target_table}")
