# Azure Spotify Data Engineering Project

An end-to-end Azure data engineering project that ingests Spotify data from Azure SQL with Azure Data Factory (ADF), stores it in ADLS Gen2 across bronze/silver/gold layers, and transforms it with Databricks and Delta Live Tables/Lakeflow.

## Architecture

Azure SQL → ADF incremental ingestion → ADLS Gen2 (bronze/silver/gold) → Databricks / PySpark → curated dimensions and facts

## Repository layout

- `dataset/`, `linkedService/`, `pipeline/`, `factory/`: ADF Git artifacts
- `source_scripts/`: source SQL and load scripts
- `spotify_dab/dev/files/`: Databricks Asset Bundle source, including `databricks.yml`, resources, and application code
- `cdc.json`, `loop_input`: ingestion configuration

The Asset Bundle source is the canonical Databricks implementation. Exported archives and deployment state are intentionally excluded.

## Security

Credentials are not stored in this repository. Configure ADF linked-service credentials through Azure Key Vault, managed identity, or the ADF connection UI. Configure Databricks authentication through the Databricks CLI or environment-specific secret management.

Before committing changes, scan for passwords, tokens, keys, connection strings, and exported deployment state.

## Notes

The ADF JSON files are deployment artifacts and may contain environment-specific resource names. Review those values before deploying to another subscription or region.
