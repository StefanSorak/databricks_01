# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import sys, os

sys.path.append(os.path.abspath("../src"))

import yaml
from datetime import datetime, timezone
from delta.tables import DeltaTable
from nyc311.transformations import build_silver
from nyc311.quality import run_quality_checks

with open("../conf/config.yaml") as f:
    cfg = yaml.safe_load(f)

catalog, schema = cfg["catalog"], cfg["schema"]

df = spark.read.table(f"{catalog}.{schema}.bronze_complaints")
df = build_silver(df)

checks = run_quality_checks(df, "complaint_id")
total = checks["total_rows"]

if checks["duplicate_count"]:
    raise ValueError(f"BR-5 violation: {checks['duplicate_count']} duplicate complaint_id(s) found in silver — dedup should guarantee zero")

print(
    f"BR-5: {checks['bounds_violation_count']}/{total} rows flagged with implausible resolution duration (bounds violation — flagged, not dropped)"
)

# BR-5: append-only quality log (not MERGE — each run is a new log entry, not a snapshot).
quality_row = {**checks, "run_at": datetime.now(timezone.utc)}
spark.createDataFrame([quality_row]).write.format("delta").mode("append").saveAsTable(
    f"{catalog}.{schema}.quality_checks"
)

# --- Idempotent upsert into silver_complaints, keyed on complaint_id ---
target_table = f"{catalog}.{schema}.silver_complaints"

try:
    target = DeltaTable.forName(spark, target_table)
    (
        target.alias("t")
        .merge(df.alias("s"), "t.complaint_id = s.complaint_id")
        .whenMatchedUpdateAll()
        .whenNotMatchedInsertAll()
        .execute()
    )
    print(f"Merged into {target_table}")
except Exception:
    df.write.format("delta").saveAsTable(target_table)
    print(f"Created {target_table} with {total} rows")
