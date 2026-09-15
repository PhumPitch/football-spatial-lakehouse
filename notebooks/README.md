# Databricks Notebooks

Interactive notebooks for running the pipeline on a Databricks cluster with Unity Catalog.

## Execution Order
0. `_ddl` — Create schema and volume
1. `01_download_dynamic.ipynb` — Downloads raw match, event, and 360 tracking data.
2. `02_bronze_ingestion.ipynb` — Ingests raw JSON into partitioned Delta tables.
3. `03_silver_transformation.ipynb` — Cleans and transforms Bronze data into 10 Silver dimensional tables.
4. `04_gold_layer.ipynb` — Computes match KPIs and passing network metrics.

## Setup in Databricks
Install dependencies at the top of any notebook:
```python
%pip install -r ../requirements.txt --quiet
```

Shared helper functions can be imported directly from `src/`:
```python
from src.common import flatten_dataframe, clean_glitch_location
```
