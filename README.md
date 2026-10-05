# 🏥 Health Analytics Lakehouse (V2)

A production-grade **Azure Databricks + Delta Lake** platform that processes wearable-device health data (heart rate, workouts, gym check-ins, user profiles) through a **Medallion Architecture** (Bronze → Silver → Gold) with full CI/CD, multi-environment deployment, and Unity Catalog data governance.

![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-red?logo=databricks)
![Delta Lake](https://img.shields.io/badge/Delta-Lake-blue)
![PySpark](https://img.shields.io/badge/PySpark-3.5-orange?logo=apachespark)
![Azure DevOps](https://img.shields.io/badge/Azure_DevOps-CI%2FCD-0078D7?logo=azuredevops)
![License](https://img.shields.io/badge/License-MIT-green)

---

## 📌 Overview

Users wear a health-monitoring wristband that continuously streams heart rate, workout events, and gym check-ins. This platform ingests five event streams, cleans and enriches the data through Silver transforms, and produces two Gold outputs for Power BI:

| Gold Output | Description |
|---|---|
| `workout_bpm_summary` | Per-session min / avg / max heart rate with user demographics |
| `gym_summary` | Per-visit minutes in the gym vs. minutes actively exercising |

---

## 🏗️ Architecture

```
ADLS Gen2  abfss://raw@datazone.dfs.core.windows.net/
    │
    │  File arrival trigger (60 s settle, 5 min cooldown)
    ▼
┌──────────────────────────────────────────────┐
│  Job: Health Analytics — Decoupled Pipeline    │
│                                                │
│  Task 1 ─ bronze_ingestion                     │
│    setup() → column masks → PII tags           │
│    spark.read  →  Bronze Delta tables          │
│         ↓ SUCCEEDED                            │
│  Task 2 ─ silver_gold_transforms               │
│    6 Silver transforms → 2 Gold outputs        │
└──────────────────────────────────────────────┘
    │
    ▼
Unity Catalog  <catalog>.project_db
```

### Medallion Layers

| Layer | Tables | Notes |
|---|---|---|
| **Bronze** | `registered_users_bz`, `gym_logins_bz`, `kafka_multiplex_bz` | Raw files from ADLS; every row carries `load_time` + `source_file`; `kafka_multiplex_bz` partitioned by `topic`, `week_part` |
| **Silver** | `users`, `gym_logs`, `user_profile`, `user_bins`, `workouts`, `completed_workouts`, `workout_bpm`, `date_lookup` | Cleaned, typed, CDC delete-handled |
| **Gold** | `workout_bpm_summary` (table), `gym_summary` (view) | Power BI ready; no PII exposed |

### 5 Data Sources

| # | Source | Format | Bronze Table |
|---|---|---|---|
| 1 | Device registration | CSV | `registered_users_bz` |
| 2 | User profile CDC | JSON — Kafka `user_info` | `kafka_multiplex_bz` |
| 3 | Heart rate stream | JSON — Kafka `bpm` | `kafka_multiplex_bz` |
| 4 | Gym login / logout | CSV | `gym_logins_bz` |
| 5 | Workout start / stop | JSON — Kafka `workout` | `kafka_multiplex_bz` |

---

## 📁 Project Structure

```
Health-Analytics-V2/
├── databricks.yml                        # DABs bundle — dev / test / prod targets
├── resources/
│   └── health_analytics_pipeline.yml    # Job definition with ${var.catalog} substitution
├── conf/
│   └── create_test_prod_catalogs.sql    # One-time admin SQL to create test + prod catalogs
├── src/
│   ├── config.py          # Reads CATALOG_NAME / APP_ENV / DB_NAME from env vars
│   ├── setup.py           # DDL + apply_column_masks() + apply_column_tags()
│   ├── ingestion.py       # BronzeIngestor: spark.read batch (3 sources)
│   ├── transformation.py  # SilverTransformer: 6 methods + CDC delete fix
│   ├── analytics.py       # GoldAnalytics: workout_bpm_summary + gym_summary
│   └── utils.py           # Logging + schema validation helpers
├── tests/
│   ├── conftest.py              # Session-scoped Spark fixture; DatabricksSession fallback
│   ├── test_config.py           # Config env var overrides
│   ├── test_ingestion.py        # BronzeIngestor smoke test
│   ├── test_transformation.py   # Silver + CDC delete + masking + PII tag coverage
│   └── test_analytics.py        # GoldAnalytics smoke test
├── 01_Run_Pipeline.py            # Bronze entry point (setup → schema → ingest)
├── 02_Silver_Gold_Pipeline.py    # Silver + Gold entry point
├── azure_pipeline.yml            # Azure DevOps 3-stage CI/CD
├── pyproject.toml
└── LICENSE
```

---

## ⚙️ Configuration

`Config` reads all settings from environment variables — injected automatically by DABs per target:

| Variable | Default | Purpose |
|---|---|---|
| `CATALOG_NAME` | `dev_catalog` | Unity Catalog name (set by DABs) |
| `APP_ENV` | `dev` | Environment identifier |
| `DB_NAME` | `project_db` | Schema name |

Storage paths are derived from the `datazone` ADLS Gen2 account (`abfss://raw@…`, `abfss://delta@…`). Secrets (`db-password`, `kafka-key`) are fetched from the `health-secrets` Databricks secret scope. **Never commit credentials.**

---

## 🚀 Getting Started

**Prerequisites:**
- Databricks workspace with Unity Catalog + Serverless compute
- ADLS Gen2 account `datazone` with `raw` and `delta` containers
- UC external locations: `health_lake_location` (→ raw) and `datazone_delta_metadata` (→ delta)
- Azure Managed Identity with Storage Blob Data Contributor on both containers

**1. Clone into Databricks Git Folders**

**2. Drop source files** into the root of `abfss://raw@datazone.dfs.core.windows.net/`

**3. Run manually (dev)**
```bash
# Open in Databricks and run in order:
01_Run_Pipeline.py          # Bronze: schema setup + ADLS ingestion
02_Silver_Gold_Pipeline.py  # Silver transforms + Gold analytics
```

**4. Deploy via DABs**
```bash
pip install databricks-cli
databricks bundle deploy --target dev    # → dev_catalog.project_db
databricks bundle deploy --target test   # → test_catalog.project_db
databricks bundle deploy --target prod   # → prod_catalog.project_db
```

**Local development**
```bash
pip install pyspark pytest pytest-mock databricks-sdk
PYTHONPATH=. python -m pytest tests/ -v
```

---

## 🧪 Testing & CI/CD

`tests/` uses a mocked Spark session (`conftest.py`) — runs fully offline, no cluster needed.

| Test | What it covers |
|---|---|
| `test_config` | Env var overrides for `CATALOG_NAME` / `APP_ENV` / `DB_NAME` |
| `test_bronze_ingestor` | `BronzeIngestor` initialisation |
| `test_clean_users_logic` | `registration_timestamp` cast chain |
| `test_user_profile_cdc_delete` | CDC delete filter — deleted users excluded from Silver |
| `test_mac_address_masking_logic` | OUI prefix preserved, device bytes masked |
| `test_apply_column_masks_calls_correct_sql` | 4× `SET MASK` across all mac_address tables |
| `test_apply_column_tags_sql_coverage` | 13× `SET TAGS` across 5 tables, all 3 sensitivity tiers |

**Azure DevOps 3-stage pipeline (`azure_pipeline.yml`):**

| Stage | Trigger | Action |
|---|---|---|
| **CI** | Every push + PR → `main` / `develop` | Python 3.10, JDK 17, pytest, publish results |
| **Deploy Test** | Merge to `main` (non-PR) | `databricks bundle deploy --target test` → `test_catalog` |
| **Deploy Prod** | Push `v*` tag (e.g. `v1.2.0`) | `databricks bundle deploy --target prod` → `prod_catalog` |

> **Deploy Prod** is gated by the `production` ADO Environment — add an approval check to require manual sign-off.

**One-time ADO setup:**
1. **Pipelines → Variables**: `DATABRICKS_HOST` (plain) + `DATABRICKS_TOKEN` (🔒 secret)
2. **Pipelines → Environments**: create `test` and `production`; add approvers to `production`
3. Run `conf/create_test_prod_catalogs.sql` as workspace admin

---

## 📦 Environments (DABs)

`databricks.yml` defines three targets — each deploys `[env] Health Analytics Pipeline` with the correct Unity Catalog injected via `--catalog` / `--env` args:

| Target | Catalog | Trigger |
|---|---|---|
| `dev` | `dev_catalog` | Manual / local (default) |
| `test` | `test_catalog` | Azure DevOps on merge to `main` |
| `prod` | `prod_catalog` | Azure DevOps on `v*` tag + approval |

---

## 🔐 Data Governance

### Column masks — `mac_address`

Applied to `registered_users_bz`, `gym_logins_bz`, `users`, `gym_logs` via a UC SQL masking function. Automatically inherited by the `gym_summary` Gold view.

| User | Value seen |
|---|---|
| `data_engineers` or `admins` group | `AA:BB:CC:DD:EE:FF` (real) |
| Everyone else | `AA:BB:CC:XX:XX:XX` (OUI kept, device bytes hidden) |

### UC PII tags — 13 columns

Every PII column carries `pii=true`, `sensitivity`, and `data_class` tags.

| Column(s) | Table(s) | `sensitivity` | `data_class` |
|---|---|---|---|
| `mac_address` | 4 tables | `high` | `device_identifier` |
| `dob`, `street_address` | `user_profile` | `high` | `date_of_birth`, `home_address` |
| `first_name`, `last_name`, `sex`, `gender` | `user_profile` | `medium` | `personal_name`, `demographic` |
| `city`, `state`, `zip` | `user_profile` | `low` | `location` |

---

## 📊 Project Status

| Area | Status |
|---|---|
| Bronze ingestion — 5 data sources | ✅ Complete |
| Silver transforms — 6 methods + CDC delete | ✅ Complete |
| Gold analytics — 2 outputs verified live | ✅ Complete |
| Date dimension (`date_lookup`) | ✅ Complete |
| Decoupled Bronze / Silver+Gold jobs | ✅ Complete |
| DEV / TEST / PROD via DABs | ✅ Complete |
| Azure DevOps 3-stage CI/CD | ✅ Complete |
| Column masks on `mac_address` | ✅ Complete |
| UC PII tags — 13 columns | ✅ Complete |
| Real integration tests with sample data | 🔲 Placeholder tests only |
| Kafka producer / data simulator | 🔲 Not yet ported |

---

## 🙋 Author

**Giridhar Reddy T** · [LinkedIn](https://www.linkedin.com/in/giridhar-reddy-tatiparthi-272b94244/) · [GitHub](https://github.com/GiridharReddy-T) · [Portfolio](https://giridharreddy-t.github.io/)

## 📄 License

[MIT](LICENSE)
