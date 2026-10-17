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

# --- Narratives archive -------------------------------------------------------
# In Aug 2026 the CFPB stopped publishing complaint narratives in the live database
# (release notes, Release 24). It published an archive of everything through
# Aug 14, 2026 here:
# https://www.consumerfinance.gov/foia-requests/foia-electronic-reading-room/cfpb-consumer-complaint-database-narratives-archive/
ARCHIVE_DIR = f"{RAW_DIR}/narratives_archive"
NARRATIVES_TABLE = f"{CATALOG}.{SCHEMA}.narratives_bronze"

_ARCHIVE_BASE = "https://files.consumerfinance.gov/f/documents/"
# Newest first. Start with the last ~2 years (exports 13-21); add older ones later if you want more data.
ARCHIVE_FILES = [
    "CCDB_Export_21_August_2026.zip",
    "CCDB_Export_20_July_2026.zip",
    "CCDB_Export_19_June_2026.zip",
    "CCDB_Export_18_May_2026.zip",
    "CCDB_Export_17_April_2026.zip",
    "CCDB_Export_16_March_2026.zip",
    "CCDB_Export_15_January_2026_through_February_2026.zip",
    "CCDB_Export_14_November_2025_through_December_2025.zip",
    "CCDB_Export_13_September_2025_through_October_2025.zip",
    # "CCDB_Export_12_July_2025_through_August_2025.zip",
    # "CCDB_Export_11_May_2025_through_June_2025.zip",
    # "CCDB_Export_10_March_2025_through_April_2025.zip",
    # "CCDB_Export_9_January_2025_through_February_2025.zip",
    # "CCDB_Export_8_November_2024_through_December_2024.zip",
    # "CCDB_Export_7_August_2024_through_October_2024.zip",
    # "CCDB_Export_6_April_2024_through_July_2024.zip",
    # "CCDB_Export_5_September_2023_through_March_2024.zip",
    # "CCDB_Export_4_November_2022_through_August_2023.zip",
    # "CCDB_Export_3_May_2021_through_October_2022.zip",
    # "CCDB_Export_2_May_2018_through_April_2021.zip",
    # "CCDB_Export_1_December_2011_through_April_2018.zip",
]
ARCHIVE_URLS = [_ARCHIVE_BASE + f for f in ARCHIVE_FILES]


def to_snake(name: str) -> str:
    """'Timely response?' -> 'timely_response', 'Sub-product' -> 'sub_product'."""
    s = name.strip().lower()
    s = re.sub(r"[^a-z0-9]+", "_", s)
    return s.strip("_")
