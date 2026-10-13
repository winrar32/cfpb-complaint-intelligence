# Databricks notebook source
# MAGIC %md
# MAGIC # 01 · Ingest CFPB complaints → Bronze
# MAGIC
# MAGIC **Goal:** land the raw CFPB Consumer Complaint Database in a Unity Catalog volume and load it, unchanged, into a Bronze Delta table.
# MAGIC
# MAGIC Bronze = raw data + ingestion metadata. No cleaning here, so we can always rebuild Silver from it.
# MAGIC
# MAGIC Source: https://www.consumerfinance.gov/data-research/consumer-complaints/

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md ## 1. Create the schema and a volume for raw files

# COMMAND ----------

spark.sql(f"CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}")
spark.sql(f"CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{RAW_VOLUME}")
print("Raw files will live in:", RAW_DIR)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Get the raw file
# MAGIC
# MAGIC **Option A (try first):** download directly from the CFPB.
# MAGIC Databricks Free Edition only allows outbound traffic to some trusted domains, so this may fail.
# MAGIC
# MAGIC **Option B (if A fails):** download `complaints.csv.zip` on your laptop from
# MAGIC https://files.consumerfinance.gov/ccdb/complaints.csv.zip, unzip it, then in Databricks go to
# MAGIC **Catalog → workspace → cfpb → raw → Upload to this volume** and upload `complaints.csv`.
# MAGIC If the full file is too big to upload, filter on the CFPB search page (e.g., last 3 years, "has narrative")
# MAGIC and use its **Export data → CSV** button instead. Then skip to step 3.

# COMMAND ----------

import os, zipfile, requests

zip_path = f"{RAW_DIR}/complaints.csv.zip"
csv_path = f"{RAW_DIR}/complaints.csv"

if os.path.exists(csv_path):
    print("complaints.csv already present, skipping download.")
else:
    try:
        with requests.get(CFPB_ZIP_URL, stream=True, timeout=60) as r:
            r.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)
        with zipfile.ZipFile(zip_path) as z:
            z.extractall(RAW_DIR)
        os.remove(zip_path)
        print("Downloaded and unzipped to", RAW_DIR)
    except Exception as e:
        print("Direct download failed (expected on Free Edition if the domain is blocked):", e)
        print("Use Option B above, then re-run from step 3.")

display(dbutils.fs.ls(RAW_DIR))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read the CSV with Spark
# MAGIC
# MAGIC Complaint narratives contain commas, quotes and line breaks, so we need `multiLine` and proper quote/escape handling.
# MAGIC Everything is read as strings in Bronze; types are fixed in Silver.

# COMMAND ----------

from pyspark.sql import functions as F

csv_files = [f.path for f in dbutils.fs.ls(RAW_DIR) if f.name.lower().endswith(".csv")]
assert csv_files, f"No CSV found in {RAW_DIR}. Upload complaints.csv first (step 2)."

raw = (
    spark.read
    .option("header", True)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .option("inferSchema", False)
    .csv(csv_files)
)

# snake_case column names: "Consumer complaint narrative" -> consumer_complaint_narrative
raw = raw.toDF(*[to_snake(c) for c in raw.columns])

bronze = (
    raw
    .withColumn("_source_file", F.col("_metadata.file_path"))
    .withColumn("_ingested_at", F.current_timestamp())
)

print("Columns:", bronze.columns)

# COMMAND ----------

# MAGIC %md ## 4. Write the Bronze Delta table

# COMMAND ----------

(
    bronze.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable(BRONZE_TABLE)
)

spark.sql(f"COMMENT ON TABLE {BRONZE_TABLE} IS 'Raw CFPB consumer complaints, all columns as strings, plus ingestion metadata.'")

n = spark.table(BRONZE_TABLE).count()
print(f"Wrote {n:,} rows to {BRONZE_TABLE}")

# COMMAND ----------

# MAGIC %md ## 5. Quick sanity check

# COMMAND ----------

display(spark.table(BRONZE_TABLE).limit(10))

# COMMAND ----------

expected = {
    "date_received", "product", "issue", "consumer_complaint_narrative", "company",
    "state", "company_response_to_consumer", "timely_response", "complaint_id",
}
missing = expected - set(spark.table(BRONZE_TABLE).columns)
if missing:
    print("⚠️ Missing expected columns (check the CSV header):", missing)
else:
    print("✅ All expected columns present.")
