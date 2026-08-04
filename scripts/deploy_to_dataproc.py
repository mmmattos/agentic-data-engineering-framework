#!/usr/bin/env python3
"""
Deploy Agentic Glue ETL Pipeline to Google Cloud Dataproc.

Usage:
    python scripts/deploy_to_dataproc.py \
        --project my-gcp-project \
        --region us-central1 \
        --cluster etl-cluster \
        --bucket my-scripts-bucket \
        --job-name sales-pipeline

    python scripts/deploy_to_dataproc.py \
        --project my-gcp-project \
        --region us-central1 \
        --cluster etl-cluster \
        --bucket my-scripts-bucket \
        --job-name sales-pipeline \
        --update
"""

import os
import sys
import json
import zipfile
import argparse
import subprocess
import tempfile
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import config


class DataprocDeployer:
    """Handles packaging and deployment of agentic ETL pipeline to GCP Dataproc."""
    
    def __init__(self, 
                 project: str,
                 region: str = "us-central1",
                 cluster_name: str = "etl-cluster"):
        """
        Initialize deployer.
        
        Args:
            project: GCP project ID
            region: GCP region
            cluster_name: Dataproc cluster name
        """
        self.project = project
        self.region = region
        self.cluster_name = cluster_name
        self.project_root = Path(__file__).parent.parent
        
        # Files to include in deployment package
        self.include_files = [
            "agents/code_generator_agent.py",
            "agents/validator_agent.py",
            "agents/executor_agent.py",
            "config/settings.py",
            "pipelines/bronze/__init__.py",
            "pipelines/silver/__init__.py",
            "pipelines/gold/__init__.py",
        ]
        
        # Exclude patterns
        self.exclude_patterns = [
            "*.pyc",
            "__pycache__",
            ".venv",
            "notebooks",
            "tests",
            "output",
            "data",
            "*.ipynb",
            ".env",
            ".git"
        ]
    
    def check_gcloud_cli(self) -> bool:
        """Verify gcloud CLI is installed and configured."""
        try:
            result = subprocess.run(
                ["gcloud", "--version"],
                capture_output=True,
                text=True,
                timeout=5
            )
            if result.returncode == 0:
                print(f"✅ gcloud CLI found")
                return True
        except (FileNotFoundError, subprocess.TimeoutExpired):
            print("❌ gcloud CLI not found. Install: https://cloud.google.com/sdk/docs/install")
            return False
        return False
    
    def check_gcp_auth(self) -> bool:
        """Verify GCP authentication is configured."""
        try:
            result = subprocess.run(
                ["gcloud", "auth", "list", "--format=json"],
                capture_output=True,
                text=True,
                timeout=10
            )
            if result.returncode == 0:
                accounts = json.loads(result.stdout)
                if accounts:
                    print(f"✅ Authenticated: {accounts[0].get('account', 'unknown')}")
                    return True
        except:
            pass
        print("❌ Not authenticated. Run: gcloud auth login")
        return False
    
    def check_cluster_exists(self) -> bool:
        """Check if Dataproc cluster exists."""
        cmd = [
            "gcloud", "dataproc", "clusters", "describe",
            self.cluster_name,
            "--region", self.region,
            "--project", self.project,
            "--format=json"
        ]
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.returncode == 0
    
    def create_deployment_package(self, output_name: str = "dataproc_deployment") -> str:
        """
        Create ZIP package for Dataproc deployment.
        
        Returns:
            Path to created ZIP file
        """
        print("\n📦 Creating deployment package...")
        
        deploy_dir = self.project_root / "deploy"
        deploy_dir.mkdir(exist_ok=True)
        
        zip_path = deploy_dir / f"{output_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in self.include_files:
                full_path = self.project_root / file_path
                if full_path.exists():
                    arcname = str(file_path)
                    zipf.write(full_path, arcname)
                    print(f"  ✅ Added: {file_path}")
                else:
                    print(f"  ⚠️  Not found: {file_path}")
            
            # Create __init__.py in root
            init_content = """# Agentic Glue ETL Package (GCP Dataproc)
__version__ = "1.0.0"
"""
            zipf.writestr("__init__.py", init_content)
            print("  ✅ Added: __init__.py")
        
        print(f"\n✅ Package created: {zip_path}")
        print(f"   Size: {zip_path.stat().st_size / 1024:.1f} KB")
        
        return str(zip_path)
    
    def upload_to_gcs(self, zip_path: str, bucket_name: str, prefix: str = "dataproc-scripts/") -> str:
        """
        Upload deployment package to GCS.
        
        Returns:
            GCS URI of uploaded file
        """
        print(f"\n☁️  Uploading to GCS: gs://{bucket_name}/...")
        
        file_name = os.path.basename(zip_path)
        gcs_key = f"{prefix}{file_name}"
        gcs_uri = f"gs://{bucket_name}/{gcs_key}"
        
        cmd = [
            "gcloud", "storage", "cp",
            zip_path,
            gcs_uri,
            "--project", self.project
        ]
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Upload failed: {result.stderr}")
            return None
        
        print(f"✅ Uploaded to: {gcs_uri}")
        return gcs_uri
    
    def create_or_update_job(self,
                            job_name: str,
                            gcs_script_uri: str,
                            main_class: str = "org.apache.spark.deploy.PythonRunner",
                            args: Optional[List[str]] = None) -> bool:
        """
        Create or submit Dataproc job.
        
        Args:
            job_name: Job name
            gcs_script_uri: GCS URI of deployment package
            main_class: Main class for Spark job
            args: Additional arguments for the job
            
        Returns:
            True if successful
        """
        print(f"\n🔧 Submitting Dataproc job: {job_name}")
        
        # Build job configuration
        job_config = {
            "job": {
                "placement": {
                    "clusterName": self.cluster_name
                },
                "reference": {
                    "jobId": job_name
                },
                "sparkJob": {
                    "mainClass": main_class,
                    "args": [
                        "--script", gcs_script_uri,
                        "--job-name", job_name
                    ] + (args or []),
                    "jarFileUris": [
                        "gs://spark-lib/bigquery/spark-bigquery-latest_2.12.jar"
                    ],
                    "properties": {
                        "spark.sql.adaptive.enabled": "true",
                        "spark.sql.adaptive.coalescePartitions.enabled": "true",
                        "spark.sql.execution.arrow.pyspark.enabled": "true"
                    }
                }
            }
        }
        
        # Create temp file for job config
        with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
            json.dump(job_config, f, indent=2)
            temp_file = f.name
        
        try:
            cmd = [
                "gcloud", "dataproc", "jobs", "submit",
                "--project", self.project,
                "--region", self.region,
                "--job", temp_file
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True)
            
            if result.returncode != 0:
                print(f"❌ Job submission failed: {result.stderr}")
                return False
            
            print(f"✅ Job submitted: {job_name}")
            print(result.stdout)
            return True
            
        finally:
            os.unlink(temp_file)
    
    def create_pipeline_script(self, output_path: str = "scripts/dataproc_pipeline.py") -> str:
        """
        Generate the main pipeline script for Dataproc execution.
        
        Returns:
            Path to generated script
        """
        pipeline_script = '''"""
GCP Dataproc Pipeline Entry Point - Executes Bronze/Silver/Gold layers.
Generated by agentic-glue-etl deployment script for GCP Dataproc.
"""

import sys
import os
import logging
from datetime import datetime

from pyspark.sql import SparkSession
from pyspark.conf import SparkConf

# Import agent modules
from agents.code_generator_agent import CodeGeneratorAgent
from agents.validator_agent import ValidatorAgent
from agents.executor_agent import ExecutorAgent

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def create_spark_session():
    """Create optimized Spark session for Dataproc."""
    conf = SparkConf()
    conf.set("spark.sql.adaptive.enabled", "true")
    conf.set("spark.sql.adaptive.coalescePartitions.enabled", "true")
    conf.set("spark.sql.execution.arrow.pyspark.enabled", "true")
    conf.set("spark.hadoop.fs.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem")
    conf.set("spark.hadoop.fs.AbstractFileSystem.gs.impl", "com.google.cloud.hadoop.fs.gcs.GoogleHadoopFS")
    
    spark = SparkSession.builder \\
        .appName("AgenticGlueETL_GCP") \\
        .config(conf=conf) \\
        .getOrCreate()
    
    logger.info("✅ Spark session created for Dataproc")
    return spark

def execute_pipeline():
    """Execute the complete ETL pipeline on Dataproc."""
    logger.info("🚀 Starting Agentic ETL Pipeline on GCP Dataproc")
    
    # Create Spark session
    spark = create_spark_session()
    
    # Initialize agents
    code_gen = CodeGeneratorAgent()
    validator = ValidatorAgent()
    executor = ExecutorAgent(spark_session=spark)
    
    results = {}
    
    # Bronze Layer
    try:
        logger.info("📦 Executing Bronze Layer")
        bronze_code = code_gen.generate_bronze_code()
        results['bronze'] = executor.execute_code(bronze_code, "bronze")
        logger.info(f"Bronze complete: {results['bronze'].record_count} records")
    except Exception as e:
        logger.error(f"Bronze failed: {e}")
        raise
    
    # Silver Layer
    try:
        logger.info("✨ Executing Silver Layer")
        silver_code = code_gen.generate_silver_code()
        results['silver'] = executor.execute_code(silver_code, "silver")
        logger.info(f"Silver complete: {results['silver'].record_count} records")
    except Exception as e:
        logger.error(f"Silver failed: {e}")
        raise
    
    # Gold Layer
    try:
        logger.info("🏆 Executing Gold Layer")
        gold_code = code_gen.generate_gold_code()
        results['gold'] = executor.execute_code(gold_code, "gold")
        logger.info(f"Gold complete: {results['gold'].record_count} records")
    except Exception as e:
        logger.error(f"Gold failed: {e}")
        raise
    
    # Summary
    logger.info("="*60)
    logger.info("🏆 PIPELINE EXECUTION SUMMARY")
    logger.info("="*60)
    for layer, result in results.items():
        status = "✅" if result.success else "❌"
        records = f", records: {result.record_count:,}" if result.record_count else ""
        logger.info(f"{status} {layer.upper()}: {result.execution_time_seconds:.2f}s{records}")
    
    executor.cleanup()
    logger.info("✅ Pipeline complete!")
    return results

if __name__ == "__main__":
    execute_pipeline()
'''
        
        script_path = self.project_root / output_path
        script_path.parent.mkdir(exist_ok=True)
        
        with open(script_path, 'w') as f:
            f.write(pipeline_script)
        
        print(f"✅ Pipeline script created: {script_path}")
        return str(script_path)
    
    def deploy(self,
              bucket_name: str,
              job_name: str = "agentic-dataproc-pipeline") -> bool:
        """
        Complete deployment workflow.
        """
        print("\n" + "="*60)
        print("🚀 Starting GCP Dataproc Deployment")
        print("="*60)
        
        # Check gcloud CLI
        if not self.check_gcloud_cli():
            return False
        
        # Check authentication
        if not self.check_gcp_auth():
            return False
        
        # Check if cluster exists
        if not self.check_cluster_exists():
            print(f"❌ Cluster '{self.cluster_name}' not found in {self.region}")
            print(f"   Create it with:")
            print(f"   gcloud dataproc clusters create {self.cluster_name} --region {self.region} --project {self.project}")
            return False
        
        # Create pipeline script
        self.create_pipeline_script()
        
        # Create deployment package
        zip_path = self.create_deployment_package(job_name)
        
        # Upload to GCS
        gcs_uri = self.upload_to_gcs(zip_path, bucket_name)
        if not gcs_uri:
            return False
        
        # Submit Dataproc job
        success = self.create_or_update_job(
            job_name=job_name,
            gcs_script_uri=gcs_uri
        )
        
        if success:
            print("\n" + "="*60)
            print("🎉 Deployment Complete!")
            print("="*60)
            print(f"\nNext steps:")
            print(f"1. Check job status in GCP Console:")
            print(f"   https://console.cloud.google.com/dataproc/jobs?project={self.project}")
            print(f"2. View logs: gcloud dataproc jobs wait --project {self.project} --region {self.region}")
            print(f"\nJob name: {job_name}")
            print(f"Cluster: {self.cluster_name}")
            print(f"Script location: {gcs_uri}")
        else:
            print("\n❌ Deployment failed")
        
        return success


def main():
    parser = argparse.ArgumentParser(description="Deploy Agentic ETL to GCP Dataproc")
    parser.add_argument("--project", help="GCP project ID", required=True)
    parser.add_argument("--region", help="GCP region", default="us-central1")
    parser.add_argument("--cluster", help="Dataproc cluster name", default="etl-cluster")
    parser.add_argument("--bucket", help="GCS bucket for scripts", required=True)
    parser.add_argument("--job-name", help="Job name", default="agentic-dataproc-pipeline")
    
    args = parser.parse_args()
    
    deployer = DataprocDeployer(
        project=args.project,
        region=args.region,
        cluster_name=args.cluster
    )
    
    success = deployer.deploy(
        bucket_name=args.bucket,
        job_name=args.job_name
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
