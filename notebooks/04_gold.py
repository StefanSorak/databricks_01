# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
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

# BR-6: column comments, reapplied every run so a dropped/recreated table gets them back
spark.sql(f"COMMENT ON TABLE {target_table} IS 'Median/p90 resolution time and open-complaint aging, per agency and complaint type, monthly. (BR-1)'")
column_comments = {
    "agency": "NYC agency code (e.g. HPD, DOT)",
    "complaint_type": "Complaint category",
    "month": "Resolution month (closed_at truncated to month); null for agency/type pairs with no closed complaints yet",
    "closed_count": "Complaints closed that month for that agency/type; 0 if none",
    "median_response_time_hours": "Median created-to-closed hours, that month",
    "p90_response_hours": "90th-percentile resolution hours, that month",
    "open_count": "Currently-open complaints for that agency/type (a snapshot, not month-scoped)",
    "median_open_age_hours": "Median hours-open for those currently-open complaints",
}
for col, comment in column_comments.items():
    spark.sql(f"ALTER TABLE {target_table} ALTER COLUMN {col} COMMENT '{comment}'")

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

# BR-6: column comments, reapplied every run so a dropped/recreated table gets them back
spark.sql(f"COMMENT ON TABLE {target_table} IS 'Resolution time and open-complaint aging, per borough and complaint type. Missing/Unspecified borough counted, not dropped. (BR-2)'")
column_comments = {
    "borough": "Can be null or Unspecified; kept per BR-2, never dropped",
    "complaint_type": "Occasionally null in source data",
    "closed_count": "Complaints closed for that borough and complaint_type; 0 if none",
    "median_response_time_hours": "Median created-to-closed hours",
    "p90_response_hours": "90th-percentile resolution hours",
    "open_count": "Currently-open complaints for that borough and complaint_type",
    "median_open_age_hours": "Median hours-open for those currently-open complaints",
}
for col, comment in column_comments.items():
    spark.sql(f"ALTER TABLE {target_table} ALTER COLUMN {col} COMMENT '{comment}'")

# COMMAND ----------

# BR-3 - Weather data
from datetime import date
from nyc311.weather import fetch_daily_weather
from nyc311.aggregations import gold_weather_date

weather_rows = fetch_daily_weather(cfg["start_date"][:10], date.today().isoformat())
weather_df = spark.createDataFrame(weather_rows).withColumn("date", F.to_date("date"))

# Overwrite, not MERGE: full window refetch every run, no incremental history to preserve.
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

# BR-6: column comments, reapplied every run so a dropped/recreated table gets them back
spark.sql(f"COMMENT ON TABLE {target_table} IS 'Daily HEAT/HOT WATER complaint volume joined with NYC temperature. (BR-3)'")
column_comments = {
    "date": "Calendar day (America/New_York)",
    "temperature_2m_max": "Daily max NYC temperature, degrees Celsius",
    "temperature_2m_min": "Daily min NYC temperature, degrees Celsius",
    "temperature_2m_mean": "Daily mean NYC temperature, degrees Celsius",
    "complaint_count": "HEAT/HOT WATER complaints created that day; 0 if none",
}
for col, comment in column_comments.items():
    spark.sql(f"ALTER TABLE {target_table} ALTER COLUMN {col} COMMENT '{comment}'")

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

# BR-6: column comments, reapplied every run so a dropped/recreated table gets them back
spark.sql(f"COMMENT ON TABLE {target_table} IS 'Complaint volume by intake channel over time. (BR-4)'")
column_comments = {
    "intake_channel": "Mobile, Phone, Online, Other, or null",
    "month": "Creation month (created_at truncated to month)",
    "complaint_count": "Complaints filed that month via that channel",
}
for col, comment in column_comments.items():
    spark.sql(f"ALTER TABLE {target_table} ALTER COLUMN {col} COMMENT '{comment}'")
