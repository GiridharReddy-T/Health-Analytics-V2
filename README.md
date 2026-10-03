# 🏥 Real-Time Health Analytics Lakehouse Platform

> An end-to-end, production-grade real-time data engineering platform built on **Databricks Lakehouse** for a wearable health device company — continuously ingesting IoT health metrics from Kafka streams and batch sources, processing through a **Medallion Architecture** (Bronze → Silver → Gold), and delivering analytical-ready datasets for health and fitness center reporting. Fully automated with **Azure DevOps CI/CD** using Databricks Asset Bundles.

[![Databricks](https://img.shields.io/badge/Databricks-Lakehouse-red?logo=databricks)](https://databricks.com)
[![Delta Lake](https://img.shields.io/badge/Delta-Lake-blue)](https://delta.io)
[![Apache Kafka](https://img.shields.io/badge/Apache-Kafka-black?logo=apachekafka)](https://kafka.apache.org)
[![Azure DevOps](https://img.shields.io/badge/Azure-DevOps-blue?logo=azuredevops)](https://azure.microsoft.com/en-us/products/devops)
[![PySpark](https://img.shields.io/badge/PySpark-Structured%20Streaming-orange?logo=apachespark)](https://spark.apache.org)
[![Unity Catalog](https://img.shields.io/badge/Unity-Catalog-green)](https://docs.databricks.com/data-governance/unity-catalog)

---

## 📌 Project Overview

This project simulates a production-grade data engineering system for a **wearable device manufacturing company**. End users wear a health-monitoring wristband that continuously transmits health parameters — heart rate, workout sessions, and gym activity — to company servers.

The platform collects, processes, and delivers two key gold-layer analytical outputs:

- **Workout BPM Summary** — per-user workout heart rate analytics (min / avg / max BPM) enriched with user demographic bins
- **Gym Summary** — per-fitness-center visit duration and active exercise time reporting

The system is built on **Databricks Lakehouse** with **Delta Lake**, supports both **batch and streaming** workflows, and is deployed via a fully automated **CI/CD pipeline** using Azure DevOps and Databricks Asset Bundles (DAB).

---

## 📊 Data Sources & Datasets

| # | Dataset | Format | Transport | Description |
|---|---------|--------|-----------|-------------|
| 1 | Device Registration | CSV | Cloud Storage → Landing Zone | User ID, Device ID, MAC Address, Registration Timestamp |
| 2 | User Profile CDC Events | JSON | Kafka → Multiplex Topic | Insert / Update / Delete profile changes from mobile app |
| 3 | BPM Heart Rate Stream | JSON | Kafka → Multiplex Topic | Continuous IoT heartrate events from wearable device (253K records/set) |
| 4 | Workout Session Events | JSON | Kafka → Multiplex Topic | Start / Stop workout button press events |
| 5 | Gym Login / Logout Events | CSV | Cloud Storage → Landing Zone | Facility scanner entry and exit events |
| 6 | Date Lookup | JSON | Seed / Reference | 365-record date dimension table for partitioning and joins |

> **Design Note:** Kafka topics 2, 3, and 4 are ingested via a single **Kafka Multiplex** bronze table, partitioned by `topic` and `week_part` — reducing ingestion overhead while allowing efficient topic-based filtering in the silver layer.

---

## 🏗️ Architecture

```
SOURCE SYSTEMS
─────────────────────────────────────────────────────────────────────────
  Azure Cloud Storage  →  Device Registration CSV Files
  Azure Cloud Storage  →  Gym Login/Logout CSV Files
  Kafka Topics         →  User Profile CDC + BPM Stream + Workout Events
                          (Multiplexed into single ADLs landing zone)

MEDALLION LAKEHOUSE  (Databricks + Delta Lake + Unity Catalog)
─────────────────────────────────────────────────────────────────────────

  ┌──────────────────────────────────────────────────────────────┐
  │                      BRONZE LAYER                            │
  │   Raw ingestion using Spark Structured Streaming             │
  │   (Auto Loader / cloudFiles format)                          │
  │                                                              │
  │   registered_users_bz   → CSV batch files                    │
  │   gym_logins_bz         → CSV batch files                    │
  │   kafka_multiplex_bz    → JSON Kafka events                  │
  │   (Partitioned by topic + week_part)                         │
  │   Audit: load_time, source_file columns added                │
  └───────────────────────────┬──────────────────────────────────┘
                              │
                              ▼
  ┌──────────────────────────────────────────────────────────────┐
  │                      SILVER LAYER                            │
  │   Cleaned, Deduplicated, CDC Merged tables                   │
  │                                                              │
  │   Phase 1 (Parallel Streams):                                │
  │     users              ← dedup + MERGE (insert-only)         │
  │     gym_logs           ← MERGE (insert + conditional update) │
  │     user_profile       ← CDC MERGE via CDCUpserter           │
  │                           (new/update/delete, SCD Type 1)    │
  │     workouts           ← dedup + MERGE                       │
  │     heart_rate         ← dedup + MERGE + valid flag          │
  │                                                              │
  │   Phase 2 (Derived Tables):                                  │
  │     user_bins          ← stream-to-static join + age bins    │
  │     completed_workouts ← stream-stream join                  │
  │                          (start + stop events, 3hr timeout)  │
  │                                                              │
  │   Phase 3 (Enriched Join):                                   │
  │     workout_bpm        ← stream-stream join                  │
  │                          (BPM × completed_workouts window)   │
  └───────────────────────────┬──────────────────────────────────┘
                              │
                              ▼
  ┌──────────────────────────────────────────────────────────────┐
  │                       GOLD LAYER                             │
  │   Analytical-ready aggregated outputs                        │
  │                                                              │
  │   workout_bpm_summary → Streaming aggregation                │
  │                          min/avg/max BPM per session         │
  │                          enriched with user demographic bins │
  │   gym_summary         → SQL View                             │
  │                          visit duration + exercise time      │
  └──────────────────────────────────────────────────────────────┘

DATA CONSUMERS
─────────────────────────────────────────────────────────────────────────
  BI Dashboards  |  Health Reports  |  Fitness Center Analytics
```

---

## 🗂️ Delta Table Schema

### Bronze Layer

| Table | Key Columns | Notes |
|-------|-------------|-------|
| `registered_users_bz` | user_id, device_id, mac_address, registration_timestamp | + load_time, source_file |
| `gym_logins_bz` | mac_address, gym, login (double), logout (double) | Epoch timestamps as double |
| `kafka_multiplex_bz` | key, value, topic, partition, offset, timestamp, week_part | Partitioned by topic + week_part |

### Silver Layer

| Table | Key Columns | Notes |
|-------|-------------|-------|
| `users` | user_id, device_id, mac_address, registration_timestamp | Proper timestamp type |
| `gym_logs` | mac_address, gym, login, logout | Converted to timestamp |
| `user_profile` | user_id, dob, sex, gender, first_name, last_name, city, state, zip, updated | SCD Type 1 CDC |
| `heart_rate` | device_id, time, heartrate, valid | valid = heartrate > 0 |
| `workouts` | user_id, workout_id, time, action, session_id | start/stop events |
| `completed_workouts` | user_id, workout_id, session_id, start_time, end_time | Stream-stream join result |
| `workout_bpm` | user_id, workout_id, session_id, start_time, end_time, time, heartrate | BPM within workout window |
| `user_bins` | user_id, age (bin), gender, city, state | 10 age buckets |
| `date_lookup` | date, week, year, month, dayofweek, dayofmonth, dayofyear, week_part | 365 records |

### Gold Layer

| Table / View | Key Columns | Type |
|---|---|---|
| `workout_bpm_summary` | workout_id, session_id, user_id, age, gender, city, state, min_bpm, avg_bpm, max_bpm, num_recordings | Delta Table |
| `gym_summary` | date, gym, mac_address, workout_id, session_id, minutes_in_gym, minutes_exercising | SQL View |

---

## 🛠️ Tech Stack

| Layer | Technology |
|-------|------------|
| Data Processing | Apache Spark, PySpark |
| Streaming Engine | Spark Structured Streaming |
| Storage Format | Delta Lake |
| Ingestion | Auto Loader (`cloudFiles`) |
| Platform | Databricks Lakehouse (Azure) |
| Data Governance | Unity Catalog (multi-env catalog) |
| State Store | RocksDB State Store Provider |
| Orchestration | Databricks Workflows |
| CI/CD | Azure DevOps + Databricks Asset Bundles (DAB) |
| Infrastructure | Azure Data Lake Storage Gen2 (ADLS) |
| Languages | Python, SQL |
| Environments | DEV → PROD |

---

## 📁 Project Structure

```
Real-Time-Health-Analytics-Lakehouse-Platform/
│
├── databricks.yml                          # Databricks Asset Bundle config (CI/CD jobs)
├── azure-pipelines.yml                     # Azure DevOps pipeline definition
│
└── Capstone Project/
    └── Notebooks/
        ├── 01-config.py                    # Config class: external locations, DB name, tuning
        ├── 02-setup.py                     # SetupHelper: create / validate / cleanup all Delta tables
        ├── 03-history-loader.py            # HistoryLoader: seed date_lookup reference table
        ├── 04-bronze.py                    # Bronze class: Auto Loader streaming ingestion (3 streams)
        ├── 05-silver.py                    # Silver class: CDC merge, dedup, stream-stream joins (3 phases)
        ├── 06-gold.py                      # Gold class: BPM aggregation + gym_summary view
        ├── 07-run.py                       # Main orchestrator: full pipeline run (batch or stream mode)
        ├── 08-batch-test.py                # Integration test: batch mode end-to-end validation
        ├── 09-stream-test.py               # Integration test: streaming mode validation
        └── 10-producer.py                  # Producer: simulates incremental data arrival in landing zone
```

---

## 🔄 Data Flow & Processing Logic

### 🥉 Bronze Layer — Raw Ingestion (`04-bronze.py`)

- Uses **Auto Loader** (`cloudFiles` format) for incremental, exactly-once file detection from ADLS
- Adds `load_time` (audit timestamp) and `source_file` (lineage column) to every ingested record
- `kafka_multiplex_bz` is **partitioned by `topic` and `week_part`** for efficient downstream reads
- **Broadcast join** with `date_lookup` on ingestion to enrich Kafka records with week partition info
- Three concurrent streams assigned to named Spark scheduler pools (`bronze_p1`, `bronze_p2`)

### 🥈 Silver Layer — Phased Upserts (`05-silver.py`)

**Phase 1 — Core tables (5 parallel streams):**

| Table | Logic |
|-------|-------|
| `users` | Dedup on `(user_id, device_id)` with 30s watermark → insert-only MERGE |
| `gym_logs` | Dedup on `(mac_address, gym, login)` → MERGE with conditional logout update logic |
| `user_profile` | CDC MERGE via `CDCUpserter` — window ranking selects latest record per `user_id` before merging. Handles `new`, `update`, `delete` event types with SCD Type 1 |
| `workouts` | Dedup on `(user_id, time)` → insert-only MERGE |
| `heart_rate` | Dedup + BPM validity flag (`valid = heartrate > 0`) → insert-only MERGE |

**Phase 2 — Derived tables:**

| Table | Logic |
|-------|-------|
| `user_bins` | Stream-to-static join with user_profile → age binning into 10 buckets → SCD Type 1 MERGE |
| `completed_workouts` | **Stream-stream join** matching `start` and `stop` workout events with 3-hour state timeout |

**Phase 3 — Enriched join:**

| Table | Logic |
|-------|-------|
| `workout_bpm` | **Stream-stream join** matching BPM readings within `(start_time, end_time)` window of completed workouts. 3-hour watermark for automatic state cleanup |

### 🥇 Gold Layer — Aggregation (`06-gold.py`)

- **`workout_bpm_summary`** — Streaming group-by aggregation on `(user_id, workout_id, session_id, end_time)` with 30s watermark computing min/avg/max BPM. Joined with `user_bins` for demographic enrichment. Idempotent upsert via `Upserter` class — once a session is complete it doesn't change, so only new records are inserted
- **`gym_summary`** — SQL View joining `gym_logs` with `completed_workouts` and `users` to calculate minutes in gym vs. minutes exercising per visit

---

## ⚙️ Key Engineering Highlights

**Idempotent MERGE Operations**
All silver and gold writes use Delta Lake `MERGE` statements, ensuring exactly-once semantics even on pipeline reruns or failures — safe to re-execute without duplicating data.

**CDC Handling with Window Ranking (`CDCUpserter`)**
User profile CDC events (insert/update/delete) are processed using Spark window functions to select the latest record per `user_id` before merging — preventing stale updates from out-of-order Kafka events.

**Stream-Stream Joins with Watermarks**
Completed workouts are derived by joining two independent streams (`start` and `stop` events) with a **3-hour watermark** for automatic state cleanup. The same join pattern is applied to match BPM readings to their workout time window — both implemented with proper watermarking to bound state size.

**Kafka Multiplex Pattern**
Instead of separate bronze tables per Kafka topic, all Kafka data lands in a single multiplexed table partitioned by `topic` and `week_part` — reducing ingestion overhead and simplifying pipeline management while enabling efficient topic-based filtering downstream.

**Spark Scheduler Pools**
Concurrent streams are assigned to named scheduler pools (`bronze_p1`, `bronze_p2`, `silver_p1`, `silver_p2`, etc.) to control resource allocation and prioritization across parallel streaming queries.

**RocksDB State Store**
Streaming state is managed using the **RocksDB State Store Provider** (`spark.sql.streaming.stateStore.providerClass`) for improved performance with large stateful operations such as stream-stream joins.

**Multi-Environment Support via Unity Catalog**
Unity Catalog is used as the environment namespace — `dev`, `prod` — allowing the same codebase to run across all environments without code changes. Environment is controlled via Databricks widget parameters passed at runtime.

**Auto Loader with Lineage Tracking**
Auto Loader adds `_metadata.file_path` as `source_file` to every ingested record, enabling complete data lineage tracking from raw file to gold table.

---

## 🧪 Automated Integration Testing

### Batch Test (`08-batch-test.py`)

Full end-to-end integration test — runs automatically as part of the CI/CD pipeline:

1. Cleans up the environment (drops DB, deletes landing zone and checkpoints)
2. Runs full setup and history load (seeds `date_lookup`)
3. Produces **Set 1** of test data via `Producer` class (copies from `test_data/` to landing zone)
4. Runs the pipeline in batch mode (`07-run.py`)
5. Validates record counts at Bronze, Silver, and Gold layers
6. Produces **Set 2** (incremental load — simulates next batch)
7. Re-runs pipeline and validates with cumulative expected counts
8. Compares Gold layer output against expected Parquet reference files row-by-row

**Validation Counts:**

| Table | Set 1 | Set 2 |
|-------|-------|-------|
| registered_users_bz | 5 | 10 |
| gym_logins_bz | 8 | 16 |
| kafka_multiplex_bz (user_info) | 7 | 13 |
| kafka_multiplex_bz (workout) | 16 | 32 |
| kafka_multiplex_bz (bpm) | 253,801 | 507,602 |
| users (silver) | 5 | 10 |
| heart_rate (silver) | 253,801 | 507,602 |
| completed_workouts (silver) | 8 | 16 |
| workout_bpm (silver) | 3,968 | 8,192 |
| workout_bpm_summary (gold) | validated | 2 records |

### Stream Test (`09-stream-test.py`)

Continuous streaming mode validation:
- Programmatically creates and triggers a streaming job via Databricks SDK
- Waits for job to reach `RUNNING` state
- Produces data incrementally in sets, sleeping 2 minutes between each to allow microbatch pickup
- Validates all layers after each data set
- Cancels and deletes the streaming job automatically after validation

---

## 🚀 CI/CD Pipeline

### Databricks Asset Bundles (`databricks.yml`)

The project uses **Databricks Asset Bundles (DAB)** for modern, declarative CI/CD:

```yaml
# Two jobs defined:
health-analytics-pipeline   → runs 07-run.py (Bronze → Silver → Gold)
batch-integration-test      → runs 08-batch-test.py (full validation)

# Two targets:
dev   → development mode (default)
prod  → production mode with run_as identity
```

### Azure DevOps Pipeline (`azure-pipelines.yml`)

Triggers automatically on every push to `main` branch:

```
Push to main
      │
      ▼
Install Databricks CLI
      │
      ▼
databricks bundle validate    ← fails fast if config is wrong
      │
      ▼
databricks bundle deploy      ← uploads notebooks + creates/updates jobs
      │
      ▼
databricks bundle run         ← triggers health-analytics-pipeline
```

---

## 🚀 Getting Started

### Prerequisites

- Databricks Workspace with **Unity Catalog** enabled
- Two **External Locations** configured in Unity Catalog:
  - `data_zone` → ADLS Gen2 container for raw data landing zone
  - `checkpoint` → ADLS Gen2 container for streaming checkpoints
- An existing Databricks cluster (or configure a new cluster in `databricks.yml`)
- Azure DevOps organization (for CI/CD)
- Databricks CLI installed locally

### ADLS Setup

Upload test data files to your `data_zone` external location:

```
{data_zone}/
└── test_data/
    ├── 1-registered_users_1.csv
    ├── 1-registered_users_2.csv
    ├── 2-user_info_1.json
    ├── 2-user_info_2.json
    ├── 3-bpm_1.json
    ├── 3-bpm_2.json
    ├── 4-workout_1.json
    ├── 4-workout_2.json
    ├── 5-gym_logins_1.csv
    ├── 5-gym_logins_2.csv
    ├── 6-date-lookup.json       
    ├── 7-gym_summary_1.parquet
    └── 7-gym_summary_2.parquet
```

### Run Manually in Databricks

```python
# Run 07-run.py with widget parameters:
Environment    = "dev"         # Unity Catalog name
RunType        = "once"        # "once" for batch, "stream" for continuous
ProcessingTime = "5 seconds"   # Microbatch interval (streaming mode only)
```

### Deploy via Databricks Asset Bundles

```bash
# Install Databricks CLI
curl -fsSL https://raw.githubusercontent.com/databricks/setup-cli/main/install.sh | sudo sh

# Configure authentication
databricks configure
# Host:  https://adb-xxxxxxxx.azuredatabricks.net
# Token: dapixxxxxxxxxxxxxxxx

# Validate bundle
databricks bundle validate -t dev

# Deploy to dev
databricks bundle deploy -t dev

# Run integration tests
databricks bundle run batch-integration-test -t dev

# Run full pipeline
databricks bundle run health-analytics-pipeline -t dev

# Deploy to production
databricks bundle deploy -t prod
```

### Azure DevOps CI/CD Setup

1. Push repo to Azure Repos
2. Create pipeline pointing to `azure-pipelines.yml`
3. Add pipeline variables:

```
DATABRICKS_HOST   = https://adb-xxxxxxxx.azuredatabricks.net
DATABRICKS_TOKEN  = dapixxxxxxxxxxxxxxxx   ← mark as secret
BUNDLE_TARGET     = dev
```

4. Every push to `main` triggers automatic deploy → validate → run

---

## 📈 Business Value

- **Real-time health monitoring** — continuous BPM tracking with validity checks enables proactive health alerting for end users
- **Fitness center analytics** — accurate visit duration vs. active exercise time reporting per gym facility
- **Governed, auditable data** — Unity Catalog with complete lineage via `source_file` tracking on every record
- **Incremental processing** — pipeline handles new data batches without full reprocessing, minimizing compute cost
- **Multi-environment architecture** — safe promotion from DEV → PROD with the same codebase and no code changes
- **Automated quality gates** — integration tests validate every layer before promoting to production

---

## 🙋 Author

**Giridhar Reddy T**  
Data Engineer | Databricks | Spark | Delta Lake | Kafka | Azure

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-blue?logo=linkedin)](https://www.linkedin.com/in/giridhar-reddy-tatiparthi-272b94244/)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-black?logo=github)](https://github.com/GiridharReddy-T)
[![Portfolio](https://img.shields.io/badge/Portfolio-Visit-green)](https://giridharreddy-t.github.io/)
[![Ask DeepWiki](https://deepwiki.com/badge.svg)](https://deepwiki.com/GiridharReddy-T/Real-Time-Health-Analytics-Lakehouse-Platform)

---

## 📄 License

This project is licensed under the [MIT License](LICENSE).
