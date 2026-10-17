# Databricks notebook source
# MAGIC %md
# MAGIC # 02 · Bronze → Silver: clean, type, dedupe, validate
# MAGIC
# MAGIC Silver = one trustworthy row per complaint, with proper types and a few derived columns used later for EDA and modeling.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F, Window

bronze = spark.table(BRONZE_TABLE)

# COMMAND ----------

# MAGIC %md ## 1. Standardize text: trim, turn empty strings into nulls

# COMMAND ----------

string_cols = [c for c, t in bronze.dtypes if t == "string" and not c.startswith("_")]

df = bronze
for c in string_cols:
    df = df.withColumn(c, F.when(F.trim(F.col(c)) == "", None).otherwise(F.trim(F.col(c))))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Parse dates
# MAGIC The bulk file uses `yyyy-MM-dd`; some exports use `MM/dd/yy`. Try each format.
# MAGIC Serverless compute runs Spark in ANSI mode, where a failed parse raises an error, so we use the `try_` functions, which return null instead.

# COMMAND ----------

def parse_date(col_name):
    return F.coalesce(*[
        F.expr(f"try_to_timestamp(`{col_name}`, '{fmt}')").cast("date")
        for fmt in ("yyyy-MM-dd", "MM/dd/yy", "MM/dd/yyyy")
    ])

df = (
    df
    .withColumn("date_received", parse_date("date_received"))
    .withColumn("date_sent_to_company", parse_date("date_sent_to_company"))
    .withColumn("complaint_id", F.expr("try_cast(complaint_id AS BIGINT)"))
)

# COMMAND ----------

# MAGIC %md ## 3. Deduplicate: keep the latest ingested row per complaint

# COMMAND ----------

w = Window.partitionBy("complaint_id").orderBy(F.col("_ingested_at").desc())
df = (
    df.filter(F.col("complaint_id").isNotNull())
      .withColumn("_rn", F.row_number().over(w))
      .filter("_rn = 1")
      .drop("_rn")
)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3b. Attach narratives from the archive
# MAGIC The live CSV no longer includes narratives (removed Aug 2026). Join them from `narratives_bronze`
# MAGIC (built by notebook 01b). Complaints received after Aug 14, 2026 will never have one.

# COMMAND ----------

if "consumer_complaint_narrative" in df.columns:
    print("Bronze already has narratives; skipping the archive join.")
elif spark.catalog.tableExists(NARRATIVES_TABLE):
    narr = spark.table(NARRATIVES_TABLE).select("complaint_id", "consumer_complaint_narrative")
    df = df.join(narr, "complaint_id", "left")
    print("Joined narratives from", NARRATIVES_TABLE)
else:
    print("⚠️ No narratives table found. Run 01b first; continuing without narratives.")
    df = df.withColumn("consumer_complaint_narrative", F.lit(None).cast("string"))

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Derived columns
# MAGIC - `has_narrative`, `narrative_words`: only consumers who consented have a public narrative (needed for NLP later)
# MAGIC - `days_to_company`: CFPB processing lag
# MAGIC - `is_monetary_relief`: the label for the outcome model (null while a complaint is still in progress)
# MAGIC - `is_timely`: company responded on time

# COMMAND ----------

narr = F.col("consumer_complaint_narrative")
resp = F.col("company_response_to_consumer")

df = (
    df
    .withColumn("has_narrative", narr.isNotNull())
    .withColumn("narrative_words", F.when(narr.isNotNull(), F.size(F.split(narr, r"\s+"))).otherwise(F.lit(0)))
    .withColumn("days_to_company", F.datediff("date_sent_to_company", "date_received"))
    .withColumn("received_year", F.year("date_received"))
    .withColumn("received_week", F.date_trunc("week", F.col("date_received")).cast("date"))
    .withColumn("is_timely", F.lower(F.col("timely_response")) == "yes")
    .withColumn(
        "is_monetary_relief",
        F.when(resp.isNull() | resp.startswith("In progress"), None)
         .otherwise(resp == "Closed with monetary relief"),
    )
    .withColumn("state", F.upper(F.col("state")))
)

silver_cols = [
    "complaint_id", "date_received", "date_sent_to_company", "received_year", "received_week",
    "product", "sub_product", "issue", "sub_issue", "company", "state", "zip_code", "tags",
    "submitted_via", "company_public_response", "company_response_to_consumer",
    "timely_response", "is_timely", "is_monetary_relief",
    "consumer_complaint_narrative", "has_narrative", "narrative_words", "days_to_company",
    "_source_file", "_ingested_at",
]
silver = df.select(*[c for c in silver_cols if c in df.columns])

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Data-quality checks
# MAGIC Results go to a `dq_results` table so you can show them in the README (and later in CI).
# MAGIC Hard failures stop the notebook; soft checks are just recorded.

# COMMAND ----------

from datetime import datetime

total = silver.count()
checks = []

def record(name, passed, value, hard=True):
    checks.append((name, bool(passed), str(value), hard, datetime.utcnow()))

dupes = total - silver.select("complaint_id").distinct().count()
record("complaint_id is unique", dupes == 0, dupes)

null_dates = silver.filter(F.col("date_received").isNull()).count()
record("date_received parsed (null rate < 0.1%)", null_dates / max(total, 1) < 0.001, null_dates)

future = silver.filter(F.col("date_received") > F.current_date()).count()
record("no complaints dated in the future", future == 0, future)

neg_lag = silver.filter(F.col("days_to_company") < 0).count()
record("days_to_company never negative", neg_lag == 0, neg_lag, hard=False)

narr_share = silver.filter("has_narrative").count() / max(total, 1)
record("share of complaints with narrative (info)", True, round(narr_share, 3), hard=False)

dq = spark.createDataFrame(checks, "check STRING, passed BOOLEAN, value STRING, hard BOOLEAN, run_at TIMESTAMP")
dq.write.format("delta").mode("append").saveAsTable(DQ_TABLE)
display(dq)

failed = [c for c in checks if c[3] and not c[1]]
assert not failed, f"Hard data-quality checks failed: {failed}"

# COMMAND ----------

# MAGIC %md ## 6. Write Silver

# COMMAND ----------

(
    silver.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", True)
    .saveAsTable(SILVER_TABLE)
)
spark.sql(f"COMMENT ON TABLE {SILVER_TABLE} IS 'Cleaned, typed, deduplicated CFPB complaints with derived features.'")
print(f"Wrote {total:,} rows to {SILVER_TABLE}")
