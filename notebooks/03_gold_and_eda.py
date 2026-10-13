# Databricks notebook source
# MAGIC %md
# MAGIC # 03 · Silver → Gold tables + exploratory data analysis
# MAGIC
# MAGIC Gold = small, aggregated tables built for questions people actually ask (and for dashboards and forecasting later).
# MAGIC Then we explore them. **Write your own findings in the markdown cells marked ✍️**. That's what makes this your project.

# COMMAND ----------

# MAGIC %run ./_config

# COMMAND ----------

from pyspark.sql import functions as F

silver = spark.table(SILVER_TABLE)
closed = silver.filter(F.col("is_monetary_relief").isNotNull())

# COMMAND ----------

# MAGIC %md ## 1. Gold tables

# COMMAND ----------

# Weekly volume per product: input for the forecasting notebook later
weekly = (
    silver.groupBy("received_week", "product")
    .agg(F.count("*").alias("complaints"))
    .orderBy("received_week", "product")
)

# Company scorecard
company = (
    silver.groupBy("company")
    .agg(
        F.count("*").alias("complaints"),
        F.avg(F.col("is_timely").cast("double")).alias("timely_rate"),
        F.avg(F.col("is_monetary_relief").cast("double")).alias("monetary_relief_rate"),
        F.avg(F.col("has_narrative").cast("double")).alias("narrative_share"),
        F.min("date_received").alias("first_complaint"),
        F.max("date_received").alias("last_complaint"),
    )
)

# State summary (join population later for per-capita rates)
state = (
    silver.filter(F.length("state") == 2)
    .groupBy("state")
    .agg(
        F.count("*").alias("complaints"),
        F.avg(F.col("is_monetary_relief").cast("double")).alias("monetary_relief_rate"),
    )
)

# Product x issue
issue = (
    silver.groupBy("product", "issue")
    .agg(
        F.count("*").alias("complaints"),
        F.avg(F.col("is_monetary_relief").cast("double")).alias("monetary_relief_rate"),
    )
)

for df, name in [(weekly, GOLD_WEEKLY), (company, GOLD_COMPANY), (state, GOLD_STATE), (issue, GOLD_ISSUE)]:
    df.write.format("delta").mode("overwrite").option("overwriteSchema", True).saveAsTable(name)
    print("wrote", name)

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. EDA
# MAGIC Aggregations run in Spark; only small results come to pandas for plotting.

# COMMAND ----------

import matplotlib.pyplot as plt

def bar(pdf, x, y, title, horizontal=True, fmt=None):
    fig, ax = plt.subplots(figsize=(9, 5))
    if horizontal:
        ax.barh(pdf[x][::-1], pdf[y][::-1])
    else:
        ax.bar(pdf[x], pdf[y])
    ax.set_title(title)
    if fmt:
        ax.xaxis.set_major_formatter(fmt)
    plt.tight_layout()
    display(fig)

# COMMAND ----------

# MAGIC %md ### 2.1 Overview

# COMMAND ----------

display(
    silver.agg(
        F.count("*").alias("complaints"),
        F.countDistinct("company").alias("companies"),
        F.min("date_received").alias("from"),
        F.max("date_received").alias("to"),
        F.round(F.avg(F.col("has_narrative").cast("double")), 3).alias("narrative_share"),
        F.round(F.avg(F.col("is_monetary_relief").cast("double")), 3).alias("monetary_relief_rate"),
    )
)

# COMMAND ----------

# MAGIC %md ### 2.2 Which products get the most complaints?

# COMMAND ----------

top_products = (
    silver.groupBy("product").count().orderBy(F.desc("count")).limit(12).toPandas()
)
bar(top_products, "product", "count", "Complaints by product (top 12)")

# COMMAND ----------

# MAGIC %md ### 2.3 How has volume changed over time?

# COMMAND ----------

monthly = (
    silver.groupBy(F.date_trunc("month", "date_received").alias("month"))
    .count().orderBy("month").toPandas()
)
fig, ax = plt.subplots(figsize=(10, 4))
ax.plot(monthly["month"], monthly["count"])
ax.set_title("Complaints per month")
plt.tight_layout()
display(fig)

# COMMAND ----------

# MAGIC %md ### 2.4 Where do consumers actually get money back?

# COMMAND ----------

relief_by_product = (
    closed.groupBy("product")
    .agg(F.count("*").alias("n"), F.avg(F.col("is_monetary_relief").cast("double")).alias("relief_rate"))
    .filter("n >= 1000")
    .orderBy(F.desc("relief_rate"))
    .toPandas()
)
bar(relief_by_product, "product", "relief_rate", "Share of closed complaints with monetary relief (products with 1,000+)")

# COMMAND ----------

# MAGIC %md ### 2.5 Companies: volume vs. timeliness (top 20 by volume)

# COMMAND ----------

display(
    spark.table(GOLD_COMPANY)
    .orderBy(F.desc("complaints"))
    .limit(20)
    .select("company", "complaints",
            F.round("timely_rate", 3).alias("timely_rate"),
            F.round("monetary_relief_rate", 3).alias("monetary_relief_rate"))
)

# COMMAND ----------

# MAGIC %md ### 2.6 Narrative length (matters for the NLP models: transformer inputs are capped at ~512 tokens)

# COMMAND ----------

lengths = (
    silver.filter("has_narrative")
    .select("narrative_words")
    .sample(fraction=0.05, seed=42)
    .toPandas()
)
fig, ax = plt.subplots(figsize=(9, 4))
ax.hist(lengths["narrative_words"].clip(upper=1500), bins=60)
ax.set_title("Narrative length in words (5% sample, clipped at 1,500)")
plt.tight_layout()
display(fig)
print(lengths["narrative_words"].describe(percentiles=[.5, .9, .95, .99]))

# COMMAND ----------

# MAGIC %md ### 2.7 Is monetary relief associated with having a narrative? (quick statistical check)

# COMMAND ----------

from scipy.stats import chi2_contingency

ct = (
    closed.groupBy("has_narrative", "is_monetary_relief").count()
    .toPandas()
    .pivot(index="has_narrative", columns="is_monetary_relief", values="count")
    .fillna(0)
)
chi2, p, dof, _ = chi2_contingency(ct.values)
print(ct)
print(f"chi2={chi2:,.1f}, p={p:.3g}")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ✍️ Findings (write these yourself)
# MAGIC 1. …
# MAGIC 2. …
# MAGIC 3. …
# MAGIC
# MAGIC **Implications for modeling:** class balance of `is_monetary_relief`, narrative coverage, any products to exclude, etc.
