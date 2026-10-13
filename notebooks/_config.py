# Databricks notebook source
# MAGIC %md
# MAGIC # Shared config
# MAGIC Run with `%run ./_config` at the top of every notebook. Change names here only.

# COMMAND ----------

import re

# Databricks Free Edition gives you a catalog called "workspace".
CATALOG = "workspace"
SCHEMA = "cfpb"
RAW_VOLUME = "raw"

RAW_DIR = f"/Volumes/{CATALOG}/{SCHEMA}/{RAW_VOLUME}"

BRONZE_TABLE = f"{CATALOG}.{SCHEMA}.complaints_bronze"
SILVER_TABLE = f"{CATALOG}.{SCHEMA}.complaints_silver"
DQ_TABLE = f"{CATALOG}.{SCHEMA}.dq_results"
GOLD_WEEKLY = f"{CATALOG}.{SCHEMA}.gold_weekly_volume"
GOLD_COMPANY = f"{CATALOG}.{SCHEMA}.gold_company_summary"
GOLD_STATE = f"{CATALOG}.{SCHEMA}.gold_state_summary"
GOLD_ISSUE = f"{CATALOG}.{SCHEMA}.gold_issue_summary"

CFPB_ZIP_URL = "https://files.consumerfinance.gov/ccdb/complaints.csv.zip"


def to_snake(name: str) -> str:
    """'Timely response?' -> 'timely_response', 'Sub-product' -> 'sub_product'."""
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")
