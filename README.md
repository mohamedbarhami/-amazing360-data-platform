
# Amazing 360 — Enterprise Data Platform

End-to-end Data Engineering and Customer Analytics project using real e-commerce event data from the REES46 dataset.

## Project Overview

Amazing 360 aims to build a scalable data platform for analyzing customer behavior, product interactions, sales performance, and customer segmentation.

The project follows a Bronze / Silver / Gold architecture.

## Technology Stack

**Implemented**
- Python — Automated data ingestion
- DuckDB — Large-scale data processing
- Parquet + ZSTD — Optimized storage
- Data Quality — Validation and anomaly detection
- Git — Version control

**Planned**
- Docker and Apache Airflow — Pipeline orchestration
- PostgreSQL — Analytical storage
- dbt Core — SQL transformations and data marts
- Power BI — Business intelligence dashboards
- Machine Learning — Customer segmentation

## Pipeline Architecture

REES46 Dataset
    |
    v
Python Ingestion
    |
    v
Bronze (Raw CSV.gz)
    |
    v
DuckDB Processing
    |
    v
Silver (Parquet)
    |
    v
Data Quality Validation

Planned next: Airflow orchestration, PostgreSQL, dbt, Power BI, and ML.

## October 2019 — Processing Results

| Metric | Result |
|---|---:|
| Events processed | 42,448,764 |
| Product views | 40,779,399 |
| Cart events | 926,516 |
| Purchase events | 742,849 |
| Silver Parquet size | 1,146.04 MiB |
| Bronze-to-Silver processing time | 152.74 seconds |
| Validation checks passed | 17 / 17 |

## Data Quality Findings

| Metric | Result |
|---|---:|
| Missing category codes | 13,515,609 |
| Missing brands | 6,113,008 |
| Missing sessions | 2 |
| Zero-price events | 68,673 |
| Invalid or negative prices | 0 |
| Candidate duplicate rows | 30,220 |

Candidate duplicates are retained because identical event attributes do not necessarily indicate an ingestion error.

The 17 checks validate selected aggregate metrics and data constraints; they do not establish row-by-row equivalence.

## Project Status

- [x] Automated ingestion for October 2019
- [x] Source integrity validation
- [x] Bronze data audit
- [x] DuckDB Bronze-to-Silver transformation
- [x] Missing-value analysis
- [x] Candidate duplicate analysis
- [x] Zero-price analysis
- [x] Silver aggregate validation
- [ ] Generalize scripts across all seven months
- [ ] Docker and Apache Airflow
- [ ] PostgreSQL and dbt
- [ ] Power BI dashboards
- [ ] Customer segmentation and ML evaluation

## Dataset

Source: REES46 public e-commerce behavioral data.

Raw datasets, generated Parquet files, and private secrets are excluded from this Git repository.

## Development Environment

- Windows 11
- Python 3.11
- DuckDB
- VS Code
- Docker Desktop (for upcoming orchestration)
