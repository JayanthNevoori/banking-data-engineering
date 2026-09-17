# Banking Data Engineering Project

## Overview

This project demonstrates an end-to-end banking data engineering pipeline.

## Architecture

On-Prem Banking Database
        ↓
Azure Data Factory
        ↓
ADLS Gen2
        ↓
Bronze Layer
        ↓
Azure Databricks / PySpark
        ↓
Data Quality
        ↓
Silver Layer
        ↓
CDC / SCD Type 2
        ↓
Gold Layer
        ↓
Azure Synapse
        ↓
Power BI

## Technologies

- Azure Data Factory

- ADLS Gen2
- Azure Databricks
- PySpark
- SQL
- Delta Lake
- Azure Synapse
- Power BI

## Key Features

- Bronze / Silver / Gold architecture
- Data Quality validation
- Incremental loading
- CDC
- SCD Type 2
- Delta Lake
- Spark optimization
- Error handling