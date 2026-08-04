---
README.md
---

# Agentic Glue ETL Pipeline

Config-driven ETL pipeline using Groq LLM and PySpark for medallion architecture (Bronze → Silver → Gold). Supports **local development**, **AWS Glue**, and **Google Cloud Dataproc**.

## Quick Start

[!code-bash]
# Setup
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt

# Configure
cp .env.example .env
# Edit .env with your GROQ_API_KEY

# Run locally
python scripts/run_pipeline.py
[!/code-bash]

## Architecture

[!code]
Config (USE_CASE_CONFIG) → Code Generator → PySpark Code → Validator → Executor → Bronze/Silver/Gold
[!/code]

## Supported Cloud Providers

| Provider | Compute | Storage | Warehouse | Orchestration |
|----------|---------|---------|-----------|---------------|
| **Local** | PySpark (local) | Local filesystem | Parquet | Manual |
| **AWS** | Glue | S3 | Parquet | Glue Triggers |
| **GCP** | Dataproc | GCS | BigQuery/Parquet | Cloud Composer |

## Directory Structure

[!code]
├── agents/               # CodeGenerator, Validator, Executor
├── config/               # Settings and configuration
├── data/                 # Data lake (raw/bronze/silver/gold)
├── notebooks/            # Jupyter notebooks for development
├── scripts/              # run_pipeline.py, deploy_to_*.py
├── infra/                # Infrastructure as Code (AWS/GCP)
├── output/               # Generated code and logs
└── tests/                # Unit tests
[!/code]

## Configuration

Modify `USE_CASE_CONFIG` in `agents/code_generator_agent.py`:

[!code-python]
USE_CASE_CONFIG = {
    "business_domain": "Your Domain",
    "cloud_provider": "aws",          # "aws" or "gcp"
    "source": {
        "environment": "offline",     # "offline", "aws", "gcp"
        "type": "file"
    },
    "cleaning_rules": {...},
    "aggregations": {...},
}
[!/code-python]

## Usage

[!code-bash]
# Local development
python scripts/run_pipeline.py

# Deploy to AWS Glue
python scripts/deploy_to_glue.py --bucket my-bucket --job-name my-job

# Deploy to GCP Dataproc
python scripts/deploy_to_dataproc.py --project my-project --bucket my-bucket --cluster etl-cluster

# Deploy to GCP Cloud Composer
python scripts/deploy_to_cloud_composer.py --project my-project --environment etl-prod --dag-name my_dag
[!/code-bash]

## Layers

- **Bronze**: Raw ingestion with metadata columns
- **Silver**: Data cleaning, validation, deduplication
- **Gold**: Aggregations and business metrics

## Output Formats

- **Parquet** (default) - Local, AWS S3, or GCS
- **BigQuery** (GCP only) - Google Cloud Warehouse

## Switching Domains or Clouds

Create new config for IoT, Fintech, or different cloud:

[!code-python]
# For IoT on GCP
agent = CodeGeneratorAgent(use_case_overrides=USE_CASE_CONFIG_IOT_GCP)

# For Fintech on AWS
agent = CodeGeneratorAgent(use_case_overrides=USE_CASE_CONFIG_FINTECH_AWS)
[!/code-python]

## Documentation

- `USE_CASE_CONFIG_README.md` - Configuration reference
- `POSTGRES_SETUP.md` - PostgreSQL setup guide (optional)
- `GENERAL_INSTRUCTIONS.md` - Complete usage instructions

## License

MIT
