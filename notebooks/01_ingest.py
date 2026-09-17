# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "5"
# ///
import yaml
from src.nyc311.api_client import run_ingestion

with open("../conf/config.yaml") as f:
    cfg = yaml.safe_load(f)

landing_root = f"/Volumes/{cfg['catalog']}/{cfg['schema']}/{cfg['landing_volume']}"
result = run_ingestion(landing_root, default_start=cfg["start_date"])
print(result)