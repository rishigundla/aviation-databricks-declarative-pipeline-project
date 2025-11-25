# Databricks LakeFlow Declarative Pipelines — Aviation Analytics Project

This repository showcases a complete end-to-end **Aviation Analytics** pipeline built using **Databricks LakeFlow Declarative Pipelines (DLT)** and **Workflows**, following the modern **Lakehouse Medallion Architecture**.

The project demonstrates how to design a reliable, declarative, and production-ready ETL pipeline that ingests aviation flight operations data (flights, airports, bookings, passengers), processes it using DLT Streaming Tables, and powers BI dashboards through **Gold-layer Materialized Views** for on-time performance and analytics.

---

## ️ LakeFlow Architecture

![Aviation LakeFlow Architecture](assets/aviation_lakeflow_architecture.png)

This architecture outlines the full lifecycle:

- **Auto Loader ingestion** (Bronze zone)
- **DLT pipeline** handling **Bronze → Silver → Gold** transformations
- **Data quality enforcement** through DLT expectations
- **Business aggregations** for aviation KPIs
- **Unified governance** with Unity Catalog

---

### LakeFlow Declarative Architecture (Streaming + Materialized Views)

This pipeline combines **continuous streaming ingestion** and **incremental transformations** with optimized **Materialized Views**:

- **Streaming Tables (Bronze → Silver)**  
  All early-stage transformations use `spark.readStream` and **DLT Streaming Tables**, enabling the pipeline to automatically process new flight data as it arrives (e.g., daily or intra-day flight updates).

- **Materialized Views (Gold Layer)**  
  The Gold layer is built using **DLT Materialized Views**, which maintain **pre-aggregated, analytics-ready aviation datasets** that refresh efficiently as upstream Streaming Tables update.

This design ensures **high freshness**, **low latency**, and **strong reliability** from raw flight events → operational insights.

---

## Project Overview

This project simulates an **aviation data engineering workflow** using:

- **Streaming ingestion** of flight operations data (flights, airports, bookings, passengers)
- **Cleansing & validation** using DLT expectations
- **Transformations & derived attributes**
- **SCD Type-2 processing** for slowly changing aviation dimensions
- **Business reporting layer** via **Materialized Views**

This project uses a **fully streaming Lakehouse pipeline** where:

- **Bronze Layer → Streaming Tables (Auto Loader)**
- **Silver Layer → Streaming Tables with SCD Type-2**
- **Gold Layer → Materialized Views (MV)**

All transformations up to Gold run **incrementally** using **DLT Streaming Tables**, ensuring the pipeline remains live, scalable, and ready for near–real-time aviation analytics. The Gold layer uses **DLT Materialized Views**, optimized for BI tools and dashboard performance.

---

## Tech Stack

| Layer          | Technology                              |
|----------------|------------------------------------------|
| Ingestion      | Databricks Auto Loader (`cloudFiles`)   |
| Processing     | LakeFlow — Delta Live Tables (DLT)      |
| Storage        | Databricks Volumes |
| Governance     | Unity Catalog                           |

---

## ️ Dataset Description

The project uses a **synthetic aviation dataset** containing:

- **Flights** – flight number, date,, origin, destination, scheduled/actual departure & arrival times, status (on-time, delayed, cancelled), delay minutes
- **Airports** – airport code, name, city, country, region, timezone
- **Bookings** – carrier code, airline name, country, alliance
- **Passengers** – distance, region pair, domestic vs international

Files are incrementally ingested using **Auto Loader** and processed through the **DLT pipeline** into curated layers for analytics.

---

## Lakehouse Medallion Architecture (Streaming + MVs)

### Bronze — Streaming Table

- Ingested via **Auto Loader** (`cloudFiles`)
- Contains **raw but schema-enforced** flight records
- **Continuous streaming updates**
- Core **schema & basic expectations** applied (non-null keys, valid timestamps)

### Silver — Streaming Table

- **Cleansed & enriched** aviation entities:
  - Normalized timestamps (local vs UTC)
  - Derived **delay metrics** (arrival/departure delay, cancelled flag, delay bucket)
  - Joined with **airport** and **airline** reference data
- **SCD1/SCD2** handling for slowly changing dimensions (e.g., airport/airline details)
- Built on top of the **continuous Bronze** output

### Gold — Materialized Views (MV)

- Pre-aggregated **aviation KPIs**, for example:
  - On-time performance by **route**, **airport**, **carrier**
  - Average arrival/departure delay by **day/week/month**
  - **Cancellation rates** by route and airline
  - Top delayed routes and peak delay time windows
- **Optimized for BI dashboards**
- Automatically **refreshed** as upstream Streaming Tables update

---

> ⭐ **Pipeline Design Summary**
>
> - **Landing = Streaming Table** (Auto Loader)
> - **Bronze = Streaming Table**
> - **Silver = Streaming Table**
> - **Gold = Materialized Views (MV)**
>
> This ensures your pipeline is **fully incremental** from raw ingestion to business reporting for aviation operations.

---

## DLT Pipeline Lineage

> _Add your DLT pipeline screenshot to `assets/` and update the path below._

![Aviation DLT Pipeline](assets/aviation_dlt_project_pipeline.png)

The lineage shows:

- Incremental **streaming ingestion** of flight data
- **Landing → Bronze → Silver → SCD1/SCD2 → Gold** layers
- **Materialized Views** powering aviation dashboards

---

## ⚙️ Workflow Automation

End-to-end orchestration is implemented using **Databricks Workflows**:

> _Add your workflow screenshot to `assets/` and update the path below._

![Aviation Workflow](assets/aviation_dlt_project_workflow.png)

The workflow ensures:

1. **DLT pipeline refresh**
2. **Materialized Views rebuild**
3. **Dashboard auto-refresh**

This creates a **production-style continuous data pipeline** for aviation analytics.

---

## Aviation Analytics Dashboard

The final BI layer is built using **Databricks AI/BI Dashboard** (or your BI tool of choice), visualizing:

- Total **flights** operated
- **On-time vs delayed vs cancelled** flight distribution
- Average **arrival & departure delay** trends
- **Route-level performance** (top routes by delay / volume)
- **Airport-level KPIs** (origin/destination performance)
- **Carrier comparison** (on-time rates, avg delays)
- Daily / monthly trends for key aviation metrics
- Drill-down views for **route, airline, airport, and date**

> _Add your dashboard screenshot to `assets/` and update the path below._

![Aviation Analytics Dashboard](assets/aviation_analytics_dashboard.png)

---

## Repository Structure

```text
aviation-databricks-declarative-pipeline-project/
│
├── assets/                              # Architecture, pipeline & dashboard screenshots
│   ├── aviation_lakeflow_architecture.png
│   ├── aviation_dlt_project_pipeline.png
│   ├── aviation_dlt_project_workflow.png
│   └── aviation_analytics_dashboard.png
│
├── datasets/                            # Sample aviation CSV/JSON datasets
│   ├── flights.csv
│   ├── airports.csv
│   └── airlines.csv
│
├── notebooks/                           # Databricks LakeFlow (DLT) notebooks
│   ├── 00_landing_layer.py              # Auto Loader + Landing DLT Streaming Tables
│   ├── 01_bronze_layer.py               # Bronze cleansing logic + expectations
│   ├── 02_silver_layer.py               # Silver transformations & enrichments
│   ├── 02_silver_scd_layer.py           # SCD Type-1 & Type-2 dimension handling
│   └── 03_gold_layer.py                 # Gold Materialized Views & aviation KPIs
│
└── README.md                            # Project documentation (this file)
