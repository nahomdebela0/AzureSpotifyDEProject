# Azure Spotify Data Engineering Project

An end-to-end Azure data engineering project that ingests Spotify data from Azure SQL with Azure Data Factory (ADF), stores it in ADLS Gen2 across bronze/silver/gold layers, and transforms it with Databricks and Delta Live Tables/Lakeflow.

## Topics Covered
Azure Architecture & Platform
- Azure architecture
- Azure networking basics
- Azure cost management and resource cleanup
  
Azure SQL Database
- Azure SQL Database
Azure Data Factory
- Azure Data Factory
- ADF dynamic expressions and parameterization
- Metadata-driven ADF pipelines
- ForEach looping and configuration-driven ingestion
- CDC and watermark-based incremental ingestion
- Backfills
- Pipeline error handling and retries
- Logic Apps integration
- ADF Monitor / Azure Monitor / Log Analytics
ADLS & Lakehouse Storage
- ADLS Gen2
- Medallion architecture: Bronze, Silver, Gold
- Parquet and Delta Lake
Databricks
- Azure Databricks
- Incremental loading in Databricks
- Databricks Asset Bundles
PySpark & Python
- PySpark transformations
- Python utilities
Spark Structured Streaming & Auto Loader
- Spark Structured Streaming
- Auto Loader
- Streaming checkpoints and recovery
- Idempotency, deduplication, and upsert concepts
Unity Catalog
- Unity Catalog
- Managed vs external tables
- Storage credentials, Managed Identity, and Access Connector
Data Quality
- Data quality expectations
Jinja2 & Metadata-Driven SQL
- Jinja2 templating
- Metadata-driven SQL generation
Data Modeling
- Star schema modeling
- Fact and dimension design
- Slowly Changing Dimensions
- SCD sequencing with sequence_by
Delta Live Tables / Lakeflow
- Delta Live Tables / Lakeflow Declarative Pipelines
Git & CI/CD
- Git / GitHub workflow
- CI/CD basics

## Architecture

Azure SQL → ADF incremental ingestion → ADLS Gen2 (bronze/silver/gold) → Databricks / PySpark → curated dimensions and facts

## Repository layout

- `dataset/`, `linkedService/`, `pipeline/`, `factory/`: ADF Git artifacts
- `source_scripts/`: source SQL and load scripts
- `spotify_dab/dev/files/`: Databricks Asset Bundle source, including `databricks.yml`, resources, and application code
- `cdc.json`, `loop_input`: ingestion configuration

<img width="726" height="307" alt="image" src="https://github.com/user-attachments/assets/63fa37fa-ffba-43d0-8dd6-01936ad1cb8c" />


<img width="955" height="349" alt="image" src="https://github.com/user-attachments/assets/fbebf716-d5e0-40b5-a04f-2f9b6af55f8c" />


The Asset Bundle source is the canonical Databricks implementation. Exported archives and deployment state are intentionally excluded.

## Security

Credentials are not stored in this repository. Configure ADF linked-service credentials through Azure Key Vault, managed identity, or the ADF connection UI. Configure Databricks authentication through the Databricks CLI or environment-specific secret management.

Before committing changes, scan for passwords, tokens, keys, connection strings, and exported deployment state.

## Notes

The ADF JSON files are deployment artifacts and may contain environment-specific resource names. Review those values before deploying to another subscription or region.
