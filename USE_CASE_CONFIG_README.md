# USE_CASE_CONFIG Reference

Complete reference for configuring the pipeline for any business domain.

## Basic Configuration

\`\`\`python
USE_CASE_CONFIG = {
    # Required fields
    "business_domain": "Your Domain Name",
    "bronze_table_name": "table_raw",
    "silver_table_name": "table_clean", 
    "gold_table_name": "table_aggregated",
    "gold_output_format": "parquet",
    
    # Source configuration
    "source": {
        "type": "file",        # "file", "event", "database"
        "environment": "offline",  # "offline" or "aws"
        
        # For file type
        "format": "csv",       # csv, json, parquet
        "path": "data/raw/",
        "options": {
            "header": "true",
            "inferSchema": "true",
            "delimiter": ","
        },
        
        # For event type (AWS only)
        "s3_bucket": "my-bucket",
        "s3_prefix": "incoming/",
        "s3_suffix": ".csv",
        
        # For database type
        "database": {
            "connection": {
                "url": "${DB_URL}",
                "user": "${DB_USER}",
                "password": "${DB_PASSWORD}",
                "driver": "org.postgresql.Driver"
            },
            "query": "SELECT * FROM source_table",
            "incremental": {
                "enabled": True,
                "column": "created_at"
            },
            "batch_size": 10000
        }
    },
    
    # AWS Glue configuration
    "glue": {
        "job_run_queuing_enabled": True,
        "max_concurrent_runs": 5,
        "timeout_minutes": 60,
        "worker_type": "G.1X",
        "num_workers": 5,
        "max_retries": 0,
        "s3_trigger_enabled": False,
        "connections": []
    },
    
    # Schema documentation
    "schema_info": "column_name: type description",
    "available_columns": ["col1", "col2", "col3"],
    
    # Silver layer cleaning rules
    "cleaning_rules": {
        "null_drop_columns": ["required_col1", "required_col2"],
        "null_fill_columns": {"col": "default_value"},
        "deduplication_columns": ["unique_col1", "unique_col2"],
        "validation_rules": [
            {"column": "col", "rule": "col('col') > 0", "description": "Must be positive"}
        ],
        "data_type_fixes": {
            "date_col": "to_date(col('date_col'))",
            "int_col": "col('int_col').cast('int')"
        }
    },
    
    # Gold layer aggregations
    "aggregations": {
        "metric_name": {
            "type": "agg",  # or "group_by"
            "expression": "sum('column')",
            "alias": "metric_alias"
        },
        "group_by_metric": {
            "type": "group_by",
            "group_by": ["column1", "column2"],
            "aggregations": [
                {"column": "value", "function": "sum", "alias": "total"}
            ],
            "sort": ["total DESC"]
        }
    }
}
\`\`\`

## Domain Examples

### IoT Sensors
\`\`\`python
"source": {"type": "event", "environment": "aws", "s3_bucket": "iot-data"},
"cleaning_rules": {
    "null_drop_columns": ["device_id", "timestamp"],
    "validation_rules": [
        {"column": "temperature", "rule": "(col('temperature') >= -50) & (col('temperature') <= 100)"}
    ]
}
\`\`\`

### Fintech
\`\`\`python
"source": {"type": "database", "environment": "aws"},
"cleaning_rules": {
    "deduplication_columns": ["transaction_id"],
    "validation_rules": [
        {"column": "amount", "rule": "col('amount') > 0"}
    ]
}
\`\`\`

### Web Logs
\`\`\`python
"source": {"type": "event", "s3_bucket": "web-logs"},
"cleaning_rules": {
    "null_drop_columns": ["ip", "timestamp"],
    "validation_rules": [
        {"column": "status_code", "rule": "col('status_code').between(100, 599)"}
    ]
}
\`\`\`

## Switching Configurations

\`\`\`python
# In notebook or script
from agents.code_generator_agent import CodeGeneratorAgent

# Use custom config
agent = CodeGeneratorAgent(use_case_overrides=USE_CASE_CONFIG_IOT)

# Override specific values
agent = CodeGeneratorAgent(use_case_overrides={
    "business_domain": "Custom Domain",
    "bronze_table_name": "custom_raw"
})
\`\`\`
