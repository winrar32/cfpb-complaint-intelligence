# Databricks notebook source
# MAGIC %md
# MAGIC # 01b · Ingest the CFPB narratives archive → Bronze
# MAGIC
# MAGIC **Why this notebook exists:** in August 2026 the CFPB stopped publishing complaint narratives in the live
# MAGIC database (API release notes, Release 24), so `complaints.csv` no longer has the `Consumer complaint narrative`
# MAGIC column. The CFPB published an archive of all complaints with narratives received through **Aug 14, 2026**.
# MAGIC
# MAGIC This notebook downloads the archive exports listed in `_config` (`ARCHIVE_FILES`), keeps only
# MAGIC `complaint_id` + narrative, and writes `narratives_bronze`. Notebook 02 joins it back onto the complaints.
# MAGIC
# MAGIC Source: https://www.consumerfinance.gov/foia-requests/foia-electronic-reading-room/cfpb-consumer-complaint-database-narratives-archive/

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

# MAGIC %md ## 1. Download and unzip each archive export (skips files already present)

# COMMAND ----------

import os, zipfile, requests

os.makedirs(ARCHIVE_DIR, exist_ok=True)

for url in ARCHIVE_URLS:
    name = url.rsplit("/", 1)[-1]
    target_dir = f"{ARCHIVE_DIR}/{name.removesuffix('.zip')}"
    if os.path.isdir(target_dir) and os.listdir(target_dir):
        print("already extracted:", name)
        continue
    zip_path = f"{ARCHIVE_DIR}/{name}"
    try:
        with requests.get(url, stream=True, timeout=120) as r:
            r.raise_for_status()
            with open(zip_path, "wb") as f:
                for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)
        os.makedirs(target_dir, exist_ok=True)
        with zipfile.ZipFile(zip_path) as z:
            print(name, "contains:", z.namelist()[:5])
            z.extractall(target_dir)
        os.remove(zip_path)
        print("✅", name)
    except Exception as e:
        print("❌", name, "->", e)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Look at what's inside before trusting it
# MAGIC The archive page doesn't document the file layout, so check the file types and headers.

# COMMAND ----------

all_files = []
for root, _, files in os.walk(ARCHIVE_DIR):
    for fn in files:
        all_files.append(os.path.join(root, fn))

for p in all_files[:20]:
    print(f"{os.path.getsize(p)/1e6:8.1f} MB  {p}")

csvs = [p for p in all_files if p.lower().endswith(".csv")]
print(f"\n{len(csvs)} CSV files found")
if csvs:
    with open(csvs[0], encoding="utf-8", errors="replace") as f:
        print("Header of first CSV:\n", f.readline())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Read all archive CSVs, keep only ID + narrative
# MAGIC If step 2 shows a different format (e.g., JSON) or the narrative column has another name, adjust here.

# COMMAND ----------

from pyspark.sql import functions as F, Window

assert csvs, "No CSV files in the archive folders. Check step 2's output and paste it into the chat."

arch = (
    spark.read
    .option("header", True)
    .option("multiLine", True)
    .option("quote", '"')
    .option("escape", '"')
    .csv(csvs)
)
arch = arch.toDF(*[to_snake(c) for c in arch.columns])
print("Archive columns:", arch.columns)

narr_cols = [c for c in arch.columns if "narrative" in c]
assert "complaint_id" in arch.columns and narr_cols, f"Expected complaint_id and a narrative column, got: {arch.columns}"
NARR = narr_cols[0]

narratives = (
    arch
    .select(
        F.expr("try_cast(complaint_id AS BIGINT)").alias("complaint_id"),
        F.trim(F.col(NARR)).alias("consumer_complaint_narrative"),
        F.col("_metadata.file_path").alias("_source_file"),
    )
    .filter(F.col("complaint_id").isNotNull())
    .filter(F.col("consumer_complaint_narrative").isNotNull() & (F.col("consumer_complaint_narrative") != ""))
)

# The same complaint can appear in more than one export: keep one row per complaint.
w = Window.partitionBy("complaint_id").orderBy(F.col("_source_file").desc())
narratives = narratives.withColumn("_rn", F.row_number().over(w)).filter("_rn = 1").drop("_rn")

# COMMAND ----------

# MAGIC %md ## 4. Write `narratives_bronze`

# COMMAND ----------

(
    narratives
    .withColumn("_ingested_at", F.current_timestamp())
    .write.format("delta").mode("overwrite").option("overwriteSchema", True)
    .saveAsTable(NARRATIVES_TABLE)
)
spark.sql(f"COMMENT ON TABLE {NARRATIVES_TABLE} IS 'Complaint narratives from the CFPB narratives archive (complaints received through 2026-08-14).'")

n = spark.table(NARRATIVES_TABLE).count()
print(f"Wrote {n:,} narratives to {NARRATIVES_TABLE}")

# COMMAND ----------

# MAGIC %md ## 5. How many complaints in Bronze get a narrative?

# COMMAND ----------

bronze_ids = spark.table(BRONZE_TABLE).select(F.expr("try_cast(complaint_id AS BIGINT)").alias("complaint_id"))
matched = bronze_ids.join(spark.table(NARRATIVES_TABLE).select("complaint_id"), "complaint_id").count()
print(f"{matched:,} of {bronze_ids.count():,} bronze complaints have an archived narrative")
display(spark.table(NARRATIVES_TABLE).limit(5))
