"""Production Code Generator Agent - Config-driven for multi-domain and multi-cloud support."""

import os
import re
import json
from datetime import datetime
from typing import Dict, Optional, List, Tuple, Any
from dataclasses import dataclass, field
from langchain_groq import ChatGroq
from langchain_core.messages import HumanMessage, SystemMessage
from config.settings import config


@dataclass
class GenerationResult:
    """Container for code generation results with explanations."""
    success: bool
    layer: str
    code: str = ""
    cleaning_rules: Dict[str, Any] = field(default_factory=dict)
    aggregations: Dict[str, str] = field(default_factory=dict)
    explanations: Dict[str, str] = field(default_factory=dict)
    warnings: List[str] = field(default_factory=list)
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def summary(self) -> str:
        lines = []
        lines.append(f"{'='*60}")
        lines.append(f"📋 Generation Result - {self.layer.upper()} Layer")
        lines.append(f"{'='*60}")
        lines.append(f"✅ Success: {self.success}")
        
        if self.cleaning_rules:
            lines.append(f"\n📖 Cleaning Rules ({len(self.cleaning_rules)}):")
            for key, value in list(self.cleaning_rules.items())[:5]:
                lines.append(f"  - {key}: {value}")
        
        if self.aggregations:
            lines.append(f"\n📊 Aggregations ({len(self.aggregations)}):")
            for key, value in list(self.aggregations.items())[:5]:
                lines.append(f"  - {key}: {value}")
        
        if self.explanations:
            lines.append(f"\n💡 Explanations:")
            for key, value in list(self.explanations.items())[:3]:
                lines.append(f"  {key}: {str(value)[:80]}...")
        
        if self.warnings:
            lines.append(f"\n⚠️ Warnings:")
            for warning in self.warnings[:3]:
                lines.append(f"  - {warning}")
        
        lines.append(f"\n📝 Code Length: {len(self.code)} characters")
        lines.append(f"{'='*60}")
        return "\n".join(lines)


class CodeGeneratorAgent:
    """
    Config-driven agent. Modify USE_CASE_CONFIG below for any business domain or cloud provider.
    Supports: AWS (Glue) and GCP (Dataproc/BigQuery).
    """
    
    # ============================================================
    # SECTION 1: USE CASE CONFIGURATION (CUSTOMIZE FOR ANY DOMAIN)
    # ============================================================
    USE_CASE_CONFIG = {
        # === BASIC INFORMATION ===
        "business_domain": "Sales Analytics",
        
        # === CLOUD PROVIDER ===
        "cloud_provider": "aws",  # "aws" or "gcp"
        
        # === TABLE NAMES ===
        "bronze_table_name": "sales_raw",
        "silver_table_name": "sales_clean",
        "gold_table_name": "sales_aggregated",
        "gold_output_format": "parquet",  # "parquet" (AWS/GCP) or "bigquery" (GCP only)
        
        # === SOURCE CONFIGURATION ===
        "source": {
            "type": "file",              # "file", "event", "database"
            "environment": "offline",    # "offline", "aws", "gcp"
            
            # For file type
            "format": "csv",
            "path": "data/raw/",
            "options": {
                "header": "true",
                "inferSchema": "true",
                "delimiter": ","
            },
            
            # For event type (S3 trigger) - used when type="event" and environment="aws"
            "s3_bucket": "my-data-bucket",
            "s3_prefix": "incoming/",
            "s3_suffix": ".csv",
            "event_type": "s3:ObjectCreated:*",
            
            # For event type (GCS trigger) - used when type="event" and environment="gcp"
            "gcs_bucket": "my-data-bucket",
            "gcs_prefix": "incoming/",
            "gcs_suffix": ".csv",
            "pubsub_topic": "data-ingestion",
            
            # For database type - used when type="database"
            "database": {
                "connection": {
                    "url": "${DB_URL}",
                    "user": "${DB_USER}",
                    "password": "${DB_PASSWORD}",
                    "driver": "org.postgresql.Driver"
                },
                "query": "SELECT * FROM source_table WHERE processed = false",
                "incremental": {
                    "enabled": True,
                    "column": "created_at",
                    "last_value_table": "control_table"
                },
                "batch_size": 10000
            }
        },
        
        # === AWS GLUE JOB CONFIGURATION ===
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
        
        # === GCP CONFIGURATION ===
        "gcp": {
            # Compute
            "compute": {
                "engine": "dataproc",           # "dataproc" or "dataflow"
                "region": "us-central1",
                "cluster_name": "etl-cluster",
                "max_idle_time": "15m",
                "master_machine_type": "n1-standard-4",
                "worker_machine_type": "n1-standard-4",
                "num_workers": 2,
                "autoscaling": True,
                "image_version": "2.1"
            },
            
            # Warehouse
            "warehouse": {
                "type": "parquet",              # "bigquery" or "parquet"
                "project": "my-gcp-project",
                "dataset": "gold_analytics",
                "location": "US",
                "partitioning": {
                    "enabled": True,
                    "field": "date",
                    "type": "DAY"
                },
                "clustering": ["product_id", "region"],
                "write_disposition": "WRITE_TRUNCATE"
            },
            
            # Storage
            "storage": {
                "bucket": "my-data-lake",
                "bronze_path": "gs://my-data-lake/bronze/",
                "silver_path": "gs://my-data-lake/silver/",
                "gold_path": "gs://my-data-lake/gold/"
            },
            
            # Orchestration
            "orchestration": {
                "type": "cloud_composer",
                "location": "us-central1",
                "environment": "etl-prod",
                "dag_name": "agentic_etl_dag",
                "schedule_interval": "0 2 * * *",
                "retries": 3,
                "retry_delay": "5m",
                "email_on_failure": True
            }
        },
        
        # === SCHEMA INFORMATION ===
        "schema_info": """
            transaction_date: date
            product_id: string
            customer_email: string
            amount: decimal(10,2)
            quantity: integer
            region: string
        """,
        
        # === AVAILABLE COLUMNS ===
        "available_columns": [
            "transaction_date", "product_id", "customer_email",
            "amount", "quantity", "region", "ingestion_timestamp"
        ],
        
        # === SILVER LAYER: CLEANING RULES ===
        "cleaning_rules": {
            "null_drop_columns": ["product_id", "customer_email"],
            "null_fill_columns": {},
            "deduplication_columns": ["transaction_date", "product_id", "customer_email", "amount"],
            "validation_rules": [
                {"column": "transaction_date", "rule": "col('transaction_date') <= current_date()", "description": "No future dates"},
                {"column": "amount", "rule": "col('amount') > 0", "description": "Positive amounts only"},
                {"column": "quantity", "rule": "(col('quantity') >= 1) & (col('quantity') <= 100)", "description": "Quantity between 1-100"},
                {"column": "customer_email", "rule": "col('customer_email').rlike(r'^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\\.[a-zA-Z]{2,}$')", "description": "Valid email format"}
            ],
            "data_type_fixes": {
                "transaction_date": "to_date(col('transaction_date'))",
                "quantity": "col('quantity').cast('int')",
                "amount": "col('amount').cast('double')"
            }
        },
        
        # === GOLD LAYER: AGGREGATIONS ===
        "aggregations": {
            "total_revenue": {
                "type": "agg",
                "expression": "sum('amount')",
                "alias": "total_revenue"
            },
            "avg_order_value": {
                "type": "agg",
                "expression": "avg('amount')",
                "alias": "avg_order_value"
            },
            "revenue_by_product": {
                "type": "group_by",
                "group_by": ["product_id"],
                "aggregations": [{"column": "amount", "function": "sum", "alias": "revenue"}],
                "sort": ["revenue DESC"]
            },
            "revenue_by_region": {
                "type": "group_by",
                "group_by": ["region"],
                "aggregations": [{"column": "amount", "function": "sum", "alias": "revenue"}],
                "sort": ["revenue DESC"]
            },
            "daily_trend": {
                "type": "group_by",
                "group_by": ["transaction_date"],
                "date_column": "transaction_date",
                "aggregations": [{"column": "amount", "function": "sum", "alias": "daily_revenue"}],
                "sort": ["date ASC"]
            }
        },
        
        # === DATA QUALITY REQUIREMENTS ===
        "default_quality_requirements": """
            - No future transaction dates
            - No null product_ids
            - Positive amounts only
            - Remove duplicate transactions
            - Valid email format for customer_email
        """,
    }
    
    # ============================================================
    # SECTION 2: INITIALIZATION
    # ============================================================
    
    def __init__(self, use_case_overrides: Optional[Dict] = None):
        self.llm = ChatGroq(
            api_key=config.groq_api_key,
            model=config.groq_model,
            temperature=config.groq_temperature
        )
        self.output_dir = "output/generated_code"
        os.makedirs(self.output_dir, exist_ok=True)
        
        self.config = self.USE_CASE_CONFIG.copy()
        if use_case_overrides:
            self.config.update(use_case_overrides)
        
        self._validate_config()
        
        cloud = self.config.get("cloud_provider", "aws")
        print(f"✅ CodeGeneratorAgent initialized (Config-driven)")
        print(f"   Cloud Provider: {cloud}")
        print(f"   Business Domain: {self.config['business_domain']}")
        print(f"   Bronze Table: {self.config['bronze_table_name']}")
        print(f"   Silver Table: {self.config['silver_table_name']}")
        print(f"   Gold Table: {self.config['gold_table_name']}")
    
    def _validate_config(self):
        required_fields = [
            "business_domain", "bronze_table_name", "silver_table_name",
            "gold_table_name", "gold_output_format", "cloud_provider",
            "source", "glue", "gcp", "available_columns", "cleaning_rules", "aggregations"
        ]
        missing = [f for f in required_fields if f not in self.config]
        if missing:
            raise ValueError(f"Missing required config fields: {missing}")
        
        # Validate cloud provider
        cloud = self.config.get("cloud_provider", "aws")
        if cloud not in ["aws", "gcp"]:
            raise ValueError(f"Invalid cloud_provider: {cloud}. Must be 'aws' or 'gcp'")
        
        # Validate source configuration
        source = self.config.get("source", {})
        source_type = source.get("type")
        if source_type not in ["file", "event", "database"]:
            raise ValueError(f"Invalid source.type: {source_type}. Must be 'file', 'event', or 'database'")
        
        source_env = source.get("environment")
        if source_env not in ["offline", "aws", "gcp"]:
            raise ValueError(f"Invalid source.environment: {source_env}. Must be 'offline', 'aws', or 'gcp'")
        
        # For file source, validate format and path
        if source_type == "file":
            if "format" not in source:
                raise ValueError("File source missing 'format'")
            if "path" not in source:
                raise ValueError("File source missing 'path'")
        
        # Validate event source has required fields
        if source_type == "event":
            if source_env == "aws":
                required_event_fields = ["s3_bucket", "s3_prefix"]
                missing_event = [f for f in required_event_fields if f not in source]
                if missing_event:
                    raise ValueError(f"Event source (AWS) missing required fields: {missing_event}")
            elif source_env == "gcp":
                required_event_fields = ["gcs_bucket", "gcs_prefix"]
                missing_event = [f for f in required_event_fields if f not in source]
                if missing_event:
                    raise ValueError(f"Event source (GCP) missing required fields: {missing_event}")
        
        # Validate database source has required fields
        if source_type == "database":
            if "database" not in source:
                raise ValueError("Database source missing 'database' configuration")
            db_config = source["database"]
            required_db_fields = ["connection", "query"]
            missing_db = [f for f in required_db_fields if f not in db_config]
            if missing_db:
                raise ValueError(f"Database source missing required fields: {missing_db}")
        
        # Validate GCP config if cloud_provider is gcp
        if cloud == "gcp":
            gcp_config = self.config.get("gcp", {})
            required_gcp_fields = ["compute", "warehouse", "storage"]
            missing_gcp = [f for f in required_gcp_fields if f not in gcp_config]
            if missing_gcp:
                raise ValueError(f"GCP config missing required fields: {missing_gcp}")
        
        print(f"✅ Configuration validated")
        print(f"   Cloud Provider: {cloud}")
        print(f"   Source Type: {source_type}")
        print(f"   Environment: {source_env}")
    
    # ============================================================
    # SECTION 3: HELPER METHODS
    # ============================================================
    
    def _get_storage_path(self, path_type: str, layer: str = None) -> str:
        """
        Get storage path based on cloud provider and environment.
        
        Args:
            path_type: "raw", "bronze", "silver", "gold"
            layer: Optional layer name for building paths
        """
        cloud = self.config.get("cloud_provider", "aws")
        source_config = self.config.get("source", {})
        env = source_config.get("environment", "offline")
        
        # Offline always uses local paths
        if env == "offline":
            return f"data/{path_type}/"
        
        # AWS paths
        if cloud == "aws":
            if path_type == "raw":
                return source_config.get("path", "data/raw/")
            elif path_type == "bronze":
                return f"s3://{source_config.get('s3_bucket', 'my-bucket')}/bronze/"
            else:
                return f"s3://{source_config.get('s3_bucket', 'my-bucket')}/{path_type}/"
        
        # GCP paths
        if cloud == "gcp":
            gcp_storage = self.config.get("gcp", {}).get("storage", {})
            bucket = gcp_storage.get("bucket", "my-data-lake")
            
            if path_type == "raw":
                return source_config.get("path", "data/raw/")
            elif path_type == "bronze":
                return gcp_storage.get("bronze_path", f"gs://{bucket}/bronze/")
            elif path_type == "silver":
                return gcp_storage.get("silver_path", f"gs://{bucket}/silver/")
            elif path_type == "gold":
                return gcp_storage.get("gold_path", f"gs://{bucket}/gold/")
            else:
                return f"gs://{bucket}/{path_type}/"
        
        # Fallback
        return f"data/{path_type}/"
    
    # ============================================================
    # SECTION 4: BRONZE LAYER
    # ============================================================
    
    def generate_bronze_code(self, 
                            source_format: Optional[str] = None,
                            source_path: Optional[str] = None,
                            schema_hint: Optional[str] = None,
                            table_name: Optional[str] = None) -> str:
        """Generate Bronze code - supports file, event, and database sources."""
        
        table = table_name or self.config["bronze_table_name"]
        source_config = self.config.get("source", {})
        source_type = source_config.get("type", "file")
        environment = source_config.get("environment", "offline")
        cloud = self.config.get("cloud_provider", "aws")
        
        # Determine write mode based on source type
        write_mode = "append" if source_type == "event" else "overwrite"
        
        # Determine read path
        if source_type == "file":
            path = source_path or source_config.get("path", "data/raw/")
        elif source_type == "event":
            if environment == "aws":
                path = f"s3://{source_config.get('s3_bucket')}/{source_config.get('s3_prefix', '')}"
            elif environment == "gcp":
                path = f"gs://{source_config.get('gcs_bucket')}/{source_config.get('gcs_prefix', '')}"
            else:
                path = "data/raw/"
        else:
            path = "data/raw/"
        
        fmt = source_format or source_config.get("format", "csv")
        options = source_config.get("options", {"header": "true", "inferSchema": "true"})
        
        # Build options string
        opts_lines = []
        option_items = list(options.items())
        for i, (k, v) in enumerate(option_items):
            if i == len(option_items) - 1:
                opts_lines.append(f'    .option("{k}", "{v}")')
            else:
                opts_lines.append(f'    .option("{k}", "{v}")\\')
        opts_str = "\n".join(opts_lines)
        
        # Determine if we need input_file_name() for source tracking
        use_input_file = source_type == "event" and environment in ["aws", "gcp"]
        
        # Bronze code template
        if source_type == "database":
            # Database source code
            db_config = source_config.get("database", {})
            conn = db_config.get("connection", {})
            query = db_config.get("query", "SELECT * FROM source_table")
            batch_size = db_config.get("batch_size", 10000)
            
            url = conn.get("url", "${DB_URL}")
            user = conn.get("user", "${DB_USER}")
            password = conn.get("password", "${DB_PASSWORD}")
            driver = conn.get("driver", "org.postgresql.Driver")
            
            code = f"""from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit
import os

spark = SparkSession.builder \\
    .appName("Bronze_{table}") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .getOrCreate()

# Read from database
jdbc_url = os.getenv("DB_URL", "{url}")
jdbc_user = os.getenv("DB_USER", "{user}")
jdbc_password = os.getenv("DB_PASSWORD", "{password}")

df = spark.read \\
    .format("jdbc") \\
    .option("url", jdbc_url) \\
    .option("dbtable", "({query}) AS source") \\
    .option("user", jdbc_user) \\
    .option("password", jdbc_password) \\
    .option("driver", "{driver}") \\
    .option("fetchsize", "{batch_size}") \\
    .load()

# Add metadata columns
df = df \\
    .withColumn("ingestion_timestamp", current_timestamp()) \\
    .withColumn("source_query", lit("{query}"))

# Write to Bronze
output_path = "data/bronze/{table}/"
df.write \\
    .mode("{write_mode}") \\
    .option("compression", "snappy") \\
    .parquet(output_path)

print(f"Bronze complete: {{df.count()}} records")
print(f"Write mode: {write_mode}")
"""
        else:
            # File or Event source
            source_file_col = "input_file_name()" if use_input_file else f'lit("{path}")'
            
            code = f"""from pyspark.sql import SparkSession
from pyspark.sql.functions import current_timestamp, lit, input_file_name

spark = SparkSession.builder \\
    .appName("Bronze_{table}") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .getOrCreate()

# Read from source
df = spark.read \\
{opts_str} \\
    .{fmt}("{path}")

# Add metadata columns
df = df \\
    .withColumn("ingestion_timestamp", current_timestamp()) \\
    .withColumn("source_file", {source_file_col})

# Write to Bronze
output_path = "data/bronze/{table}/"
df.write \\
    .mode("{write_mode}") \\
    .option("compression", "snappy") \\
    .parquet(output_path)

print(f"Bronze complete: {{df.count()}} records")
print(f"Write mode: {write_mode}")
print(f"Source: {path}")
"""
        
        return code
    
    # ============================================================
    # SECTION 5: SILVER LAYER
    # ============================================================
    
    def generate_silver_code(self, 
                            bronze_table_path: Optional[str] = None,
                            table_name: Optional[str] = None) -> str:
        """Generate Silver code from config cleaning_rules."""
        
        table = table_name or self.config["silver_table_name"]
        
        # Determine Bronze path based on cloud provider
        if bronze_table_path is None:
            cloud = self.config.get("cloud_provider", "aws")
            source_config = self.config.get("source", {})
            env = source_config.get("environment", "offline")
            
            if env == "offline":
                bronze_table_path = f"data/bronze/{self.config['bronze_table_name']}/"
            elif cloud == "aws":
                bucket = source_config.get('s3_bucket', 'my-bucket')
                bronze_table_path = f"s3://{bucket}/bronze/{self.config['bronze_table_name']}/"
            elif cloud == "gcp":
                gcp_storage = self.config.get("gcp", {}).get("storage", {})
                bucket = gcp_storage.get("bucket", "my-data-lake")
                bronze_table_path = gcp_storage.get("bronze_path", f"gs://{bucket}/bronze/")
            else:
                bronze_table_path = f"data/bronze/{self.config['bronze_table_name']}/"
        
        rules = self.config["cleaning_rules"]
        
        # Build null dropping
        null_drop = ""
        if rules.get("null_drop_columns"):
            cols = ", ".join([f"'{c}'" for c in rules["null_drop_columns"]])
            null_drop = f'\ndf = df.dropna(subset=[{cols}])'
        
        # Build data type fixes
        type_fixes = ""
        for col_name, fix_expr in rules.get("data_type_fixes", {}).items():
            type_fixes += f'\ndf = df.withColumn("{col_name}", {fix_expr})'
        
        # Build validation filters
        validations = ""
        for v in rules.get("validation_rules", []):
            validations += f'\ndf = df.filter({v["rule"]})  # {v["description"]}'
        
        # Build deduplication
        dedup = ""
        if rules.get("deduplication_columns"):
            cols = ", ".join([f"'{c}'" for c in rules["deduplication_columns"]])
            dedup = f'\ndf = df.dropDuplicates([{cols}])'
        
        code = f"""from pyspark.sql import SparkSession
from pyspark.sql.functions import col, to_date, current_date

spark = SparkSession.builder \\
    .appName("Silver_{table}") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .getOrCreate()

# Read from Bronze
df = spark.read.parquet("{bronze_table_path}"){type_fixes}{null_drop}{validations}{dedup}

# Write to Silver
output_path = "data/silver/{table}/"
df.write.mode("overwrite").parquet(output_path)

print(f"Silver complete: {{df.count()}} records")
"""
        return code
    
    # ============================================================
    # SECTION 6: GOLD LAYER
    # ============================================================
    
    def generate_gold_code(self, 
                          silver_table_path: Optional[str] = None,
                          table_name: Optional[str] = None) -> str:
        """Generate Gold code - supports Parquet or BigQuery output."""
        
        table = table_name or self.config["gold_table_name"]
        cloud = self.config.get("cloud_provider", "aws")
        source_config = self.config.get("source", {})
        env = source_config.get("environment", "offline")
        
        # Determine Silver path based on cloud provider
        if silver_table_path is None:
            if env == "offline":
                silver_table_path = f"data/silver/{self.config['silver_table_name']}/"
            elif cloud == "aws":
                bucket = source_config.get('s3_bucket', 'my-bucket')
                silver_table_path = f"s3://{bucket}/silver/{self.config['silver_table_name']}/"
            elif cloud == "gcp":
                gcp_storage = self.config.get("gcp", {}).get("storage", {})
                bucket = gcp_storage.get("bucket", "my-data-lake")
                silver_table_path = gcp_storage.get("silver_path", f"gs://{bucket}/silver/")
            else:
                silver_table_path = f"data/silver/{self.config['silver_table_name']}/"
        
        aggs = self.config["aggregations"]
        
        # Build metrics collection
        metrics_code = []
        outputs_code = []
        metrics_dict_items = []
        
        for name, agg_config in aggs.items():
            agg_type = agg_config.get("type", "agg")
            
            if agg_type == "agg":
                alias = agg_config["alias"]
                expr = agg_config["expression"]
                metrics_code.append(f'{alias} = df.agg({expr}).collect()[0][0]')
                metrics_dict_items.append(f"'{name}': {alias}")
                
            elif agg_type == "group_by":
                agg_exprs = []
                for a in agg_config["aggregations"]:
                    agg_exprs.append(f"{a['function']}('{a['column']}').alias('{a['alias']}')")
                agg_str = ", ".join(agg_exprs)
                
                df_name = name
                
                if "date_column" in agg_config:
                    date_col = agg_config['date_column']
                    outputs_code.append(f"""
# {name.replace('_', ' ').title()}
{df_name} = df.withColumn('date', to_date(col('{date_col}'))) \\
    .groupBy('date').agg({agg_str})""")
                else:
                    group_cols = ", ".join([f"'{c}'" for c in agg_config["group_by"]])
                    outputs_code.append(f"""
# {name.replace('_', ' ').title()}
{df_name} = df.groupBy({group_cols}).agg({agg_str})""")
                
                if agg_config.get("sort"):
                    sort_exprs = []
                    for s in agg_config["sort"]:
                        parts = s.split()
                        col_name = parts[0].strip("'")
                        direction = parts[1] if len(parts) > 1 else "ASC"
                        if direction.upper() == "DESC":
                            sort_exprs.append(f"col('{col_name}').desc()")
                        else:
                            sort_exprs.append(f"col('{col_name}').asc()")
                    sort_str = ", ".join(sort_exprs)
                    outputs_code[-1] += f'\n{df_name} = {df_name}.orderBy({sort_str})'
        
        metrics_str = "\n".join(metrics_code)
        outputs_str = "\n".join(outputs_code)
        metrics_dict_str = "{" + ", ".join(metrics_dict_items) + "}" if metrics_dict_items else "{}"
        
        # Determine output format and path
        output_format = self.config.get("gold_output_format", "parquet")
        
        # GCP + BigQuery support
        if cloud == "gcp" and output_format == "bigquery":
            gcp_warehouse = self.config.get("gcp", {}).get("warehouse", {})
            project = gcp_warehouse.get("project", "my-gcp-project")
            dataset = gcp_warehouse.get("dataset", "gold_analytics")
            write_disposition = gcp_warehouse.get("write_disposition", "WRITE_TRUNCATE")
            
            write_code = f"""
# Write to BigQuery
print("Writing to BigQuery...")
for name, agg_config in {self.config['aggregations']}.items():
    if agg_config.get("type") == "group_by":
        df_name = name
        if df_name in locals():
            table_id = f"{{project}}.{{dataset}}.{{name}}"
            locals()[df_name].write \\
                .format("bigquery") \\
                .option("table", table_id) \\
                .option("writeMethod", "direct") \\
                .mode("{write_disposition.lower()}") \\
                .save()
            print(f"  ✓ Wrote {{name}} to BigQuery: {{table_id}}")
    elif agg_config.get("type") == "agg":
        if name in metrics:
            value = metrics[name]
            metrics_df = spark.createDataFrame([(value,)], [agg_config['alias']])
            table_id = f"{{project}}.{{dataset}}.{{name}}"
            metrics_df.write \\
                .format("bigquery") \\
                .option("table", table_id) \\
                .option("writeMethod", "direct") \\
                .mode("{write_disposition.lower()}") \\
                .save()
            print(f"  ✓ Wrote {{name}} to BigQuery: {{table_id}}")
"""
        else:
            # Parquet output (local or cloud storage)
            if env == "offline":
                gold_path = f"data/gold/{table}/"
            elif cloud == "aws":
                bucket = source_config.get('s3_bucket', 'my-bucket')
                gold_path = f"s3://{bucket}/gold/{table}/"
            elif cloud == "gcp":
                gcp_storage = self.config.get("gcp", {}).get("storage", {})
                bucket = gcp_storage.get("bucket", "my-data-lake")
                gold_path = gcp_storage.get("gold_path", f"gs://{bucket}/gold/")
            else:
                gold_path = f"data/gold/{table}/"
            
            write_code = f"""
# Write to Parquet
print("Writing to Parquet...")
for name, agg_config in {self.config['aggregations']}.items():
    if agg_config.get("type") == "group_by":
        df_name = name
        if df_name in locals():
            locals()[df_name].write.mode("overwrite").parquet(f"{gold_path}{{name}}")
            print(f"  ✓ Wrote {{name}} to {{gold_path}}{{name}}")
    elif agg_config.get("type") == "agg":
        if name in metrics:
            value = metrics[name]
            metrics_df = spark.createDataFrame([(value,)], [agg_config['alias']])
            metrics_df.write.mode("overwrite").parquet(f"{gold_path}{{name}}")
            print(f"  ✓ Wrote {{name}} to {{gold_path}}{{name}}")
"""
        
        code = f"""from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, avg, count, to_date

spark = SparkSession.builder \\
    .appName("Gold_{table}") \\
    .config("spark.sql.adaptive.enabled", "true") \\
    .getOrCreate()

# Read from Silver
df = spark.read.parquet("{silver_table_path}")

# Calculate metrics
{metrics_str}

# Dictionary of metrics for easy access
metrics = {metrics_dict_str}

# Generate output DataFrames
{outputs_str}

{write_code}

print(f"Gold complete: {{df.count()}} input records")
print(f"Output format: {output_format}")
"""
        return code
    
    # ============================================================
    # SECTION 7: LEGACY METHODS
    # ============================================================
    
    def generate_silver_code_with_auto_rules(self, data_description: str, **kwargs) -> GenerationResult:
        result = GenerationResult(success=True, layer="silver")
        result.code = self.generate_silver_code()
        result.cleaning_rules = self.config["cleaning_rules"]
        result.explanations = {"note": "Config-driven - modify USE_CASE_CONFIG['cleaning_rules']"}
        self._save_code(result.code, "silver", "config_driven")
        return result
    
    def generate_gold_code_with_auto_metrics(self, business_questions: List[str], **kwargs) -> GenerationResult:
        result = GenerationResult(success=True, layer="gold")
        result.code = self.generate_gold_code()
        result.aggregations = self.config["aggregations"]
        result.explanations = {"note": "Config-driven - modify USE_CASE_CONFIG['aggregations']"}
        self._save_code(result.code, "gold", "config_driven")
        return result
    
    def _clean_code(self, code: str) -> str:
        code = re.sub(r'```python\n?', '', code)
        code = re.sub(r'```\n?', '', code)
        lines = code.split('\n')
        lines = [line.lstrip() for line in lines]
        return '\n'.join(lines).strip()
    
    def _save_code(self, code: str, layer: str, identifier: str):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{layer}_{identifier}_{timestamp}.py"
        filepath = os.path.join(self.output_dir, filename)
        
        with open(filepath, 'w') as f:
            f.write(f"# Generated by CodeGeneratorAgent (Config-driven)\n")
            f.write(f"# Cloud Provider: {self.config.get('cloud_provider', 'aws')}\n")
            f.write(f"# Business Domain: {self.config['business_domain']}\n")
            f.write(f"# Layer: {layer.upper()}\n")
            f.write(f"# Timestamp: {datetime.now().isoformat()}\n")
            f.write("# " + "="*60 + "\n\n")
            f.write(code)
        
        print(f"✅ Code saved to: {filepath}")
    def _save_metadata(self, result: GenerationResult, layer: str):
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{layer}_metadata_{timestamp}.json"
        filepath = os.path.join(self.output_dir, filename)
        
        metadata = {
            "business_domain": self.config["business_domain"],
            "cloud_provider": self.config.get("cloud_provider", "aws"),
            "layer": layer,
            "timestamp": timestamp,
            "success": result.success,
            "cleaning_rules": result.cleaning_rules,
            "aggregations": result.aggregations,
            "explanations": result.explanations,
            "warnings": result.warnings
        }
        
        with open(filepath, 'w') as f:
            json.dump(metadata, f, indent=2)
        
        print(f"📊 Metadata saved to: {filepath}") 


# ============================================================
# SECTION 8: EXAMPLE USAGE
# ============================================================

if __name__ == "__main__":
    agent = CodeGeneratorAgent()
    
    print("\n" + "="*60)
    print("🔍 Testing Bronze Generation")
    print("="*60)
    bronze_code = agent.generate_bronze_code()
    print(f"✅ Bronze code generated ({len(bronze_code)} chars)")
    
    print("\n" + "="*60)
    print("🔍 Testing Silver Generation (Config-driven)")
    print("="*60)
    silver_code = agent.generate_silver_code()
    print(f"✅ Silver code generated ({len(silver_code)} chars)")
    
    print("\n" + "="*60)
    print("🔍 Testing Gold Generation (Config-driven)")
    print("="*60)
    gold_code = agent.generate_gold_code()
    print(f"✅ Gold code generated ({len(gold_code)} chars)")
