# Banking Data Engineering Architecture

## End-to-End Flow

On-Prem Banking Database
        ↓
Azure Data Factory
        ↓
Self-Hosted Integration Runtime
        ↓
ADLS Gen2
        ↓
Bronze Layer
        ↓
Azure Databricks / PySpark
        ↓
Data Quality Validation
        ↓
Silver Layer
        ↓
CDC / Incremental Load / SCD Type 2
        ↓
Gold Layer
        ↓
Azure Synapse
        ↓
Power BI

## Architecture Components

### 1. Source
The banking data originates from the on-premises banking database.

### 2. Azure Data Factory
ADF is used for orchestration and data movement.

### 3. Self-Hosted Integration Runtime
SHIR provides secure connectivity between the on-premises environment and Azure.

### 4. ADLS Gen2
ADLS Gen2 stores the raw and processed banking data.

### 5. Bronze Layer
Stores the raw ingested data.

### 6. Databricks
Azure Databricks with PySpark performs data transformation and processing.

### 7. Data Quality
Data is validated for null values, duplicates, invalid records, and other quality issues.

### 8. Silver Layer
Contains cleaned and transformed banking data.

### 9. CDC and SCD Type 2
CDC handles source changes and SCD Type 2 maintains historical changes.

### 10. Gold Layer
Contains business-ready data for analytics.

### 11. Synapse
Azure Synapse is used for analytical querying and reporting.

### 12. Power BI
Power BI consumes the prepared data for dashboards and business reporting.