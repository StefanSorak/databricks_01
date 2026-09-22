# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import yaml

with open("../conf/config.yaml") as f:
    cfg = yaml.safe_load(f)

catalog, schema, volume = cfg["catalog"], cfg["schema"], cfg["landing_volume"]

spark.sql(f"CREATE CATALOG IF NOT EXISTS {catalog}")
spark.sql(f"CREATE SCHEMA IF NOT EXISTS {catalog}.{schema}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {catalog}.{schema}.{volume}")