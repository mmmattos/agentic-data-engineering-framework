---
GENERAL_INSTRUCTIONS.md
---

# General Instructions - GCP Extension

## Project Overview

This is a config-driven ETL pipeline that uses an LLM (Groq) to generate PySpark code for medallion architecture data processing. It supports local development, AWS Glue, and Google Cloud Dataproc.

## Multi-Cloud Configuration

The framework uses a single `USE_CASE_CONFIG` to switch between cloud providers:

[!code-python]
USE_CASE_CONFIG = {
    "cloud_provider": "aws",  # "aws" or "gcp"
    "source": {
        "environment": "offline",  # "offline", "aws", "gcp"
        ...
    },
    "gold_output_format": "parquet",  # "parquet" or "bigquery" (GCP only)
    ...
}
[!/code-python]

## GCP Prerequisites

### 1. Install Google Cloud SDK

[!code-bash]
# macOS
brew install --cask google-cloud-sdk

# Linux
curl -sSL https://sdk.cloud.google.com | bash

# Verify
gcloud --version
[!/code-bash]

### 2. Authenticate

[!code-bash]
gcloud auth login
gcloud config set project your-gcp-project-id
[!/code-bash]

### 3. Enable Required APIs

[!code-bash]
gcloud services enable dataproc.googleapis.com
gcloud services enable bigquery.googleapis.com
gcloud services enable composer.googleapis.com
gcloud services enable storage.googleapis.com
[!/code-bash]

### 4. Create Service Account

[!code-bash]
gcloud iam service-accounts create etl-sa \
    --display-name="ETL Service Account"

gcloud projects add-iam-policy-binding your-project \
    --member="serviceAccount:etl-sa@your-project.iam.gserviceaccount.com" \
    --role="roles/dataproc.admin"

gcloud projects add-iam-policy-binding your-project \
    --member="serviceAccount:etl-sa@your-project.iam.gserviceaccount.com" \
    --role="roles/bigquery.admin"

gcloud projects add-iam-policy-binding your-project \
    --member="serviceAccount:etl-sa@your-project.iam.gserviceaccount.com" \
    --role="roles/storage.admin"

gcloud iam service-accounts keys create service-account-key.json \
    --iam-account=etl-sa@your-project.iam.gserviceaccount.com
[!/code-bash]

### 5. Set Environment Variables

Add to `.env`:

[!code-bash]
GCP_PROJECT=your-gcp-project-id
GCP_REGION=us-central1
GCP_CREDENTIALS_PATH=path/to/service-account-key.json
[!/code-bash]

## GCP Deployment Options

### Option 1: Dataproc (Spark Jobs)

Deploy and run on Dataproc:

[!code-bash]
python scripts/deploy_to_dataproc.py \
    --project your-gcp-project \
    --region us-central1 \
    --cluster etl-cluster \
    --bucket your-scripts-bucket \
    --job-name sales-pipeline
[!/code-bash]

**What it does:**
1. Packages agents into ZIP
2. Uploads to GCS
3. Submits Dataproc job

**Pre-create cluster:**

[!code-bash]
gcloud dataproc clusters create etl-cluster \
    --region us-central1 \
    --project your-gcp-project \
    --config-file=infra/gcp/dataproc_cluster.yaml
[!/code-bash]

### Option 2: Cloud Composer (Airflow)

Deploy DAG for scheduled orchestration:

[!code-bash]
python scripts/deploy_to_cloud_composer.py \
    --project your-gcp-project \
    --location us-central1 \
    --environment etl-prod \
    --dag-name agentic_etl_dag \
    --schedule "0 2 * * *"
[!/code-bash]

**What it does:**
1. Generates Airflow DAG
2. Uploads to Cloud Composer's GCS bucket
3. Updates environment variables

**Pre-create Composer environment:**

[!code-bash]
gcloud composer environments create etl-prod \
    --location us-central1 \
    --project your-gcp-project \
    --image-version composer-2.5.0-airflow-2.7.0
[!/code-bash]

## GCP Configuration Example

[!code-python]
USE_CASE_CONFIG_GCP = {
    "business_domain": "Sales Analytics",
    "cloud_provider": "gcp",
    "bronze_table_name": "sales_raw",
    "silver_table_name": "sales_clean",
    "gold_table_name": "sales_aggregated",
    "gold_output_format": "bigquery",  # Write to BigQuery

    "source": {
        "type": "event",
        "environment": "gcp",
        "gcs_bucket": "my-incoming-data",
        "gcs_prefix": "uploads/",
    },

    "gcp": {
        "compute": {
            "engine": "dataproc",
            "region": "us-central1",
            "cluster_name": "etl-cluster"
        },
        "warehouse": {
            "type": "bigquery",
            "project": "my-gcp-project",
            "dataset": "gold_analytics"
        },
        "storage": {
            "bucket": "my-data-lake",
            "bronze_path": "gs://my-data-lake/bronze/",
            "silver_path": "gs://my-data-lake/silver/",
            "gold_path": "gs://my-data-lake/gold/"
        }
    }
}
[!/code-python]

## Verifying Results on GCP

### BigQuery

[!code-sql]
SELECT * FROM `my-gcp-project.gold_analytics.revenue_by_product` LIMIT 10;
SELECT * FROM `my-gcp-project.gold_analytics.daily_trend` ORDER BY date DESC;
[!/code-sql]

### GCS (Parquet)

[!code-python]
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("gs://my-data-lake/gold/revenue_by_product/")
df.show()
[!/code-python]

## Cost Optimization

### Dataproc
- Use preemptible workers for batch jobs
- Enable autoscaling
- Set idle delete TTL (30 minutes)

### BigQuery
- Partition by date
- Cluster on high-cardinality columns
- Use slot reservations for predictable costs

### Cloud Composer
- Use environment size based on workload
- Schedule DAGs during off-peak hours
- Monitor resource usage

## Troubleshooting GCP

### Dataproc Job Fails

[!code-bash]
# Check job logs
gcloud dataproc jobs describe JOB_ID --region us-central1 --project your-project

# View cluster logs
gcloud logging read "resource.type=cloud_dataproc_cluster"
[!/code-bash]

### BigQuery Write Fails

Check service account permissions:
[!code-bash]
gcloud projects get-iam-policy your-project \
    --format=json | grep -A 10 "etl-sa"
[!/code-bash]

### Cloud Composer DAG Not Appearing

[!code-bash]
# Check DAG bucket
gcloud composer environments describe etl-prod \
    --location us-central1 --format="value(config.dagGcsPrefix)"

# Check DAG file exists
gsutil ls gs://DAG_BUCKET/dags/
[!/code-bash]

## Migration from AWS to GCP

1. Update `cloud_provider` to `"gcp"`
2. Update `source.environment` to `"gcp"`
3. Update S3 paths to GCS (`s3://` → `gs://`)
4. Optional: Set `gold_output_format` to `"bigquery"`
5. Update `.env` with GCP credentials
6. Deploy using `deploy_to_dataproc.py`

## Next Steps

1. Test with sample data locally
2. Deploy to Dataproc for staging
3. Validate BigQuery outputs
4. Schedule with Cloud Composer for production
