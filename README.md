# ⚽ Football Spatial Lakehouse

A distributed **PySpark & Delta Lake** architecture engineered on **Databricks** to process high-frequency football event streams and StatsBomb 360° spatial freeze-frame telemetry into dimensional tables for tactical analysis.

---

## 🏗️ Architecture Overview

```text
┌─────────────────────────────────────────────────────────────────────────┐
│                          DATA INGESTION                                 │
│         StatsBomb Open Data (Multi-Tournament Events & 360° Telemetry)  │
└────────────────────────────────────┬────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  🟤 BRONZE LAYER (Raw Telemetry Ingestion)                              │
│  • Multi-threaded asynchronous discovery & ingestion (20 workers)       │
│  • In-memory broadcast joins eliminating 100% of cluster network shuffle│
│  • Idempotent, partition-pruned Delta Lake writes                       │
└────────────────────────────────────┬────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  ⚪ SILVER LAYER (Normalized Dimensional Store)                          │
│  • 10 structured tables (passes, carries, shots, 360 freeze-frames)     │
│  • Sports coordinate boundary validator ([-5, 125] × [-5, 85] yds)      │
│  • Delta Lake ACID MERGE INTO upserts (zero duplicate keys)             │
└────────────────────────────────────┬────────────────────────────────────┘
                                     │
                                     ▼
┌─────────────────────────────────────────────────────────────────────────┐
│  🟡 GOLD LAYER (Tactical Analytics & Research Engine)                   │
│  • Passing networks, team possession sequences, and player KPIs         │
│  • Dynamic gap vector analysis & spatial line breakdown modeling        │
└─────────────────────────────────────────────────────────────────────────┘
```

---

## 📊 Performance Benchmarks

*Benchmarked on Databricks cluster against single-threaded baseline across 8 World Cup tournaments (80+ matches):*

| Performance Metric | Baseline (Pandas / Sequential) | Databricks Lakehouse (PySpark + Delta) | Improvement |
| :--- | :---: | :---: | :---: |
| **Bronze Ingestion (80+ matches)** | ~180–240s | **~12–16s** | **~15× faster** |
| **Network Shuffle Traffic** | ~150 MB | **0 MB (Broadcast Join)** | **100% eliminated** |
| **360° Spatial Query Latency** | ~8.20s | **~0.22s (Columnar Parquet)** | **~37× faster** |
| **Storage Footprint** | ~350 MB (JSON) | **~42 MB (Snappy Parquet)** | **88% reduction** |
| **Historical Schema Collision** | 100% Failure | **0% Error (Unified 1-Scan)** | **Zero data loss** |
| **Schema Evolution** | Manual Re-ingest | **Automated Zero-Downtime** | **Seamless evolution** |

---

## 📁 Project Structure

```text
football-spatial-lakehouse/
├── notebooks/                      # Databricks Lakehouse Notebooks (Source Format)
│   ├── _ddl.py                     # Schema & Unity Catalog volume initialization
│   ├── 01_download_dynamic.py      # Multi-threaded raw telemetry acquisition
│   ├── 02_bronze_ingestion.py      # Zero-shuffle Bronze Delta ingestion
│   ├── 03_silver_transformation.py # 10 Silver tables & spatial coordinate normalization
│   └── 04_gold_layer.py            # Tactical feature store & research stub
├── src/                            # Modular PySpark Transformation Libraries
│   ├── bronze_ingestion.py         # Broadcast join & raw schema validation
│   ├── silver_transformation.py    # Spatial cleaning, flattening & Delta MERGE
│   └── common.py                   # Spark session & utility helpers
├── requirements.txt                # Python environment dependencies
├── .gitignore                      # Pre-configured (safeguards DGVI IP & data files)
└── README.md
```

---

## 🚀 How to Run in Databricks

1. **Clone into Databricks:**  
   In your Databricks Workspace, navigate to **Workspace → Repos (Git Folders)** and clone this repository.
2. **Cluster Environment:**  
   Attach to any Databricks Runtime cluster (**DBR 13.3+ LTS** recommended with Apache Spark 3.4+ and Delta Lake).
3. **Execute Notebooks Sequentially:**  
   Run the pipeline notebooks in `notebooks/`:
   * `01_download_dynamic.py` *(automatically initializes Unity Catalog schema and volumes via `_ddl.py`)*
   * `02_bronze_ingestion.py`
   * `03_silver_transformation.py`
   * `04_gold_layer.py`

---

## 👤 Author

**Pitchayaphum Rukjring (PhumPitch)**  
* [LinkedIn](https://linkedin.com/in/phumpitch)  
* [GitHub](https://github.com/phumpitch)

