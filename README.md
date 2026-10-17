# Consumer Complaint Intelligence Platform

An end-to-end Databricks lakehouse and ML project on the **CFPB Consumer Complaint Database**, millions of real complaints about banks, lenders and credit bureaus.

> Status: **Milestone 1 (lakehouse + EDA)**. Forecasting, NLP classification, outcome modeling, LLM summaries and serving come next.

## Architecture

```
CFPB CSV ──► Unity Catalog volume ──► Bronze (raw Delta) ──► Silver (clean, typed, validated) ──► Gold (aggregates)
                                                                   │
                                                                   ├─► EDA notebook
                                                                   ├─► Forecasting (Prophet / SARIMA)       [next]
                                                                   ├─► Issue classifier (TF-IDF → DistilBERT) [next]
                                                                   ├─► Monetary-relief model (XGBoost + SHAP) [next]
                                                                   └─► LLM theme summaries                   [next]
                                             all experiments tracked in MLflow · models in Unity Catalog registry
```

## Tables (`workspace.cfpb`)

| Table | Layer | What it holds |
|---|---|---|
| `complaints_bronze` | Bronze | Raw CSV, all strings, plus `_source_file`, `_ingested_at` |
| `narratives_bronze` | Bronze | `complaint_id` + narrative text from the CFPB narratives archive |
| `complaints_silver` | Silver | One row per complaint: typed dates, dedupe, `has_narrative`, `narrative_words`, `days_to_company`, `is_timely`, `is_monetary_relief` |
| `dq_results` | Quality | Pass/fail log of data-quality checks per run |
| `gold_weekly_volume` | Gold | Complaints per week × product (forecasting input) |
| `gold_company_summary` | Gold | Volume, timely-response rate, monetary-relief rate per company |
| `gold_state_summary` | Gold | Volume and relief rate per state |
| `gold_issue_summary` | Gold | Volume and relief rate per product × issue |

## How to run (Databricks Free Edition)

1. Sign up for **Databricks Free Edition** (free, serverless).
2. Push this repo to your GitHub, then in Databricks: **Workspace → Create → Git folder** and paste the repo URL.
3. Open `notebooks/01_ingest_bronze.py` and run it.
   - It tries to download the CFPB file directly. Free Edition limits outbound internet, so if that fails, download
     [complaints.csv.zip](https://files.consumerfinance.gov/ccdb/complaints.csv.zip) yourself, unzip it, and upload
     `complaints.csv` to **Catalog → workspace → cfpb → raw**. If the file is too large to upload, export a filtered CSV
     (e.g., last 3 years, with narrative) from the [CFPB search page](https://www.consumerfinance.gov/data-research/consumer-complaints/search/).
4. Run `01b_ingest_narratives_archive.py` (narratives from the CFPB archive), then `02_clean_silver.py`, then `03_gold_and_eda.py`.

Notebooks are stored in Databricks *source* format (`.py` with `# COMMAND ----------` separators), so they diff cleanly in Git.

## Results (fill in as you go)

| Metric | Value |
|---|---|
| Complaints processed | |
| Date range | |
| Share with narratives | |
| Monetary-relief rate (closed complaints) | |

## Roadmap

- [x] Bronze / Silver / Gold lakehouse with data-quality checks
- [x] EDA
- [ ] Weekly volume forecasting (Prophet vs. SARIMA vs. seasonal naive, MAPE)
- [ ] Issue classification: TF-IDF + LR → embeddings + XGBoost → fine-tuned DistilBERT (macro-F1)
- [ ] Monetary-relief outcome model with SHAP explanations
- [ ] LLM theme summaries per company / month
- [ ] MLflow tracking + Unity Catalog model registry + Model Serving endpoint
- [ ] Streamlit app
- [ ] CI/CD: GitHub Actions + Databricks Asset Bundles

## Data

Source: Consumer Financial Protection Bureau, [Consumer Complaint Database](https://www.consumerfinance.gov/data-research/consumer-complaints/). Raw data is not committed to this repo.

**Narratives:** in August 2026 the CFPB stopped publishing complaint narratives in the live database ([release notes](https://cfpb.github.io/api/ccdb/release-notes.html), Release 24). This project loads them from the CFPB's official [narratives archive](https://www.consumerfinance.gov/foia-requests/foia-electronic-reading-room/cfpb-consumer-complaint-database-narratives-archive/) (complaints received through Aug 14, 2026) in `01b_ingest_narratives_archive` and joins them by `complaint_id`. Complaints received after that date have structured fields only. Narratives were published only with consumer consent and scrubbed of personal information by the CFPB.
