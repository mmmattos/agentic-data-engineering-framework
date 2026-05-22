#!/usr/bin/env python3
"""
AWS Glue Deployment Script - Package and deploy agentic ETL pipeline to AWS Glue.

Usage:
    python scripts/deploy_to_glue.py --profile my-aws-profile --bucket my-glue-scripts-bucket
    python scripts/deploy_to_glue.py --help
"""

import os
import sys
import zipfile
import argparse
import subprocess
import json
from pathlib import Path
from datetime import datetime
from typing import List, Dict, Optional

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from config.settings import config


class GlueDeployer:
    """Handles packaging and deployment of agentic ETL pipeline to AWS Glue."""
    
    def __init__(self, profile_name: str = None, region: str = "us-east-1"):
        """
        Initialize deployer.
        
        Args:
            profile_name: AWS profile name (uses default if None)
            region: AWS region
        """
        self.profile_name = profile_name
        self.region = region
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
        
        # Files to exclude
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
    
    def check_aws_cli(self) -> bool:
        """Verify AWS CLI is installed and configured."""
        try:
            result = subprocess.run(
                ["aws", "--version"],
                capture_output=True,
                text=True
            )
            if result.returncode == 0:
                print(f"✅ AWS CLI found: {result.stdout.strip()}")
                return True
        except FileNotFoundError:
            print("❌ AWS CLI not found. Please install: https://aws.amazon.com/cli/")
            return False
        return False
    
    def check_glue_job_exists(self, job_name: str) -> bool:
        """Check if Glue job already exists."""
        cmd = [
            "aws", "glue", "get-job",
            "--job-name", job_name
        ]
        
        if self.profile_name:
            cmd.extend(["--profile", self.profile_name])
        cmd.extend(["--region", self.region])
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        return result.returncode == 0
    
    def create_deployment_package(self, output_name: str = "glue_deployment") -> str:
        """
        Create ZIP package for AWS Glue deployment.
        
        Returns:
            Path to created ZIP file
        """
        print("\n📦 Creating deployment package...")
        
        # Create temporary directory
        deploy_dir = self.project_root / "deploy"
        deploy_dir.mkdir(exist_ok=True)
        
        zip_path = deploy_dir / f"{output_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.zip"
        
        with zipfile.ZipFile(zip_path, 'w', zipfile.ZIP_DEFLATED) as zipf:
            for file_path in self.include_files:
                full_path = self.project_root / file_path
                if full_path.exists():
                    # Add file with its relative path
                    arcname = str(file_path)
                    zipf.write(full_path, arcname)
                    print(f"  ✅ Added: {file_path}")
                else:
                    print(f"  ⚠️  Not found: {file_path}")
            
            # Create __init__.py in root if not exists
            init_content = """# Agentic Glue ETL Package
__version__ = "1.0.0"
"""
            zipf.writestr("__init__.py", init_content)
            print("  ✅ Added: __init__.py")
        
        print(f"\n✅ Package created: {zip_path}")
        print(f"   Size: {zip_path.stat().st_size / 1024:.1f} KB")
        
        return str(zip_path)
    
    def upload_to_s3(self, zip_path: str, bucket_name: str, prefix: str = "glue-scripts/") -> str:
        """
        Upload deployment package to S3.
        
        Returns:
            S3 URI of uploaded file
        """
        print(f"\n☁️  Uploading to S3: s3://{bucket_name}/...")
        
        file_name = os.path.basename(zip_path)
        s3_key = f"{prefix}{file_name}"
        
        cmd = ["aws", "s3", "cp", zip_path, f"s3://{bucket_name}/{s3_key}"]
        
        if self.profile_name:
            cmd.extend(["--profile", self.profile_name])
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Upload failed: {result.stderr}")
            return None
        
        s3_uri = f"s3://{bucket_name}/{s3_key}"
        print(f"✅ Uploaded to: {s3_uri}")
        
        return s3_uri
    
    def create_or_update_job(self, 
                            job_name: str,
                            s3_script_uri: str,
                            glue_version: str = "4.0",
                            worker_type: str = "G.1X",
                            num_workers: int = 5,
                            timeout_minutes: int = 60) -> bool:
        """
        Create or update AWS Glue job.
        
        Args:
            job_name: Glue job name
            s3_script_uri: S3 URI of deployment package
            glue_version: Glue version (2.0, 3.0, 4.0)
            worker_type: Worker type (G.1X, G.2X, G.4X, G.8X)
            num_workers: Number of workers
            timeout_minutes: Job timeout
        """
        print(f"\n🔧 Configuring Glue job: {job_name}")
        
        # Job configuration
        job_config = {
            "Name": job_name,
            "Role": "AWSGlueServiceRole",  # Update with your role
            "GlueVersion": glue_version,
            "WorkerType": worker_type,
            "NumberOfWorkers": num_workers,
            "Timeout": timeout_minutes,
            "Command": {
                "Name": "glueetl",
                "ScriptLocation": s3_script_uri,
                "PythonVersion": "3"
            },
            "DefaultArguments": {
                "--job-language": "python",
                "--enable-glue-datacatalog": "true",
                "--enable-metrics": "true",
                "--enable-continuous-cloudwatch-log": "true",
                "--job-bookmark-option": "job-bookmark-enable",
                "--TempDir": f"s3://{s3_script_uri.split('/')[2]}/temporary/"
            },
            "MaxRetries": 2,
            "Connections": {
                "Connections": []  # Add if needed
            }
        }
        
        # Check if job exists
        if self.check_glue_job_exists(job_name):
            print(f"⚠️  Job exists, updating...")
            cmd = ["aws", "glue", "update-job", "--job-name", job_name]
            cmd.extend(["--job-update", json.dumps(job_config)])
        else:
            print(f"📝 Creating new job...")
            cmd = ["aws", "glue", "create-job"]
            cmd.extend(["--job-name", job_name])
            cmd.extend(["--role", job_config["Role"]])
            cmd.extend(["--command", json.dumps(job_config["Command"])])
            cmd.extend(["--glue-version", glue_version])
            cmd.extend(["--worker-type", worker_type])
            cmd.extend(["--number-of-workers", str(num_workers)])
            cmd.extend(["--timeout", str(timeout_minutes)])
        
        if self.profile_name:
            cmd.extend(["--profile", self.profile_name])
        cmd.extend(["--region", self.region])
        
        result = subprocess.run(cmd, capture_output=True, text=True)
        
        if result.returncode != 0:
            print(f"❌ Glue job configuration failed: {result.stderr}")
            return False
        
        print(f"✅ Glue job configured: {job_name}")
        return True
    
    def create_pipeline_script(self, output_path: str = "scripts/glue_pipeline.py") -> str:
        """
        Generate the main pipeline script for Glue execution.
        
        Returns:
            Path to generated script
        """
        pipeline_script = '''"""
AWS Glue Pipeline Entry Point - Executes Bronze/Silver/Gold layers.
Generated by agentic-glue-etl deployment script.
"""

import sys
import logging
from datetime import datetime
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job
from awsglue.utils import getResolvedOptions

# Import agent modules
from agents.code_generator_agent import CodeGeneratorAgent
from agents.validator_agent import ValidatorAgent
from agents.executor_agent import ExecutorAgent

# Setup logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Get job parameters
args = getResolvedOptions(
    sys.argv, 
    ['JOB_NAME', 'BRONZE_CODE', 'SILVER_CODE', 'GOLD_CODE']
)

# Initialize Glue context
sc = SparkContext()
glueContext = GlueContext(sc)
spark = glueContext.spark_session
job = Job(glueContext)
job.init(args['JOB_NAME'], args)

def execute_pipeline():
    """Execute the complete ETL pipeline."""
    logger.info("Starting Agentic Glue ETL Pipeline")
    
    # Initialize agents
    code_gen = CodeGeneratorAgent()
    validator = ValidatorAgent()
    executor = ExecutorAgent(spark_session=spark)
    
    results = {}
    
    # Bronze Layer
    try:
        logger.info("📦 Executing Bronze Layer")
        bronze_code = args['BRONZE_CODE']
        results['bronze'] = executor.execute_code(bronze_code, "bronze")
        logger.info(f"Bronze complete: {results['bronze'].record_count} records")
    except Exception as e:
        logger.error(f"Bronze failed: {e}")
        raise
    
    # Silver Layer
    try:
        logger.info("✨ Executing Silver Layer")
        silver_code = args['SILVER_CODE']
        results['silver'] = executor.execute_code(silver_code, "silver")
        logger.info(f"Silver complete: {results['silver'].record_count} records")
    except Exception as e:
        logger.error(f"Silver failed: {e}")
        raise
    
    # Gold Layer
    try:
        logger.info("🏆 Executing Gold Layer")
        gold_code = args['GOLD_CODE']
        results['gold'] = executor.execute_code(gold_code, "gold")
        logger.info(f"Gold complete: {results['gold'].record_count} records")
    except Exception as e:
        logger.error(f"Gold failed: {e}")
        raise
    
    logger.info("✅ Pipeline execution complete")
    return results

if __name__ == "__main__":
    execute_pipeline()
    job.commit()
'''
        
        script_path = self.project_root / output_path
        script_path.parent.mkdir(exist_ok=True)
        
        with open(script_path, 'w') as f:
            f.write(pipeline_script)
        
        print(f"✅ Pipeline script created: {script_path}")
        return str(script_path)
    
    def deploy(self, 
              bucket_name: str,
              job_name: str = "agentic-glue-pipeline",
              glue_version: str = "4.0",
              worker_type: str = "G.1X",
              num_workers: int = 5) -> bool:
        """
        Complete deployment workflow.
        """
        print("\n" + "="*60)
        print("🚀 Starting AWS Glue Deployment")
        print("="*60)
        
        # Check AWS CLI
        if not self.check_aws_cli():
            return False
        
        # Create deployment package
        zip_path = self.create_deployment_package(job_name)
        
        # Upload to S3
        s3_uri = self.upload_to_s3(zip_path, bucket_name)
        if not s3_uri:
            return False
        
        # Configure Glue job
        success = self.create_or_update_job(
            job_name=job_name,
            s3_script_uri=s3_uri,
            glue_version=glue_version,
            worker_type=worker_type,
            num_workers=num_workers
        )
        
        if success:
            print("\n" + "="*60)
            print("🎉 Deployment Complete!")
            print("="*60)
            print(f"\nNext steps:")
            print(f"1. Update IAM role in Glue job configuration")
            print(f"2. Set up connections to data sources")
            print(f"3. Run Glue job with appropriate parameters")
            print(f"\nJob name: {job_name}")
            print(f"Script location: {s3_uri}")
        else:
            print("\n❌ Deployment failed")
        
        return success


def main():
    parser = argparse.ArgumentParser(description="Deploy Agentic Glue ETL to AWS Glue")
    parser.add_argument("--profile", help="AWS profile name", default=None)
    parser.add_argument("--region", help="AWS region", default="us-east-1")
    parser.add_argument("--bucket", help="S3 bucket for scripts", required=True)
    parser.add_argument("--job-name", help="Glue job name", default="agentic-glue-pipeline")
    parser.add_argument("--glue-version", help="Glue version", default="4.0")
    parser.add_argument("--worker-type", help="Worker type", default="G.1X")
    parser.add_argument("--num-workers", help="Number of workers", type=int, default=5)
    
    args = parser.parse_args()
    
    deployer = GlueDeployer(profile_name=args.profile, region=args.region)
    
    success = deployer.deploy(
        bucket_name=args.bucket,
        job_name=args.job_name,
        glue_version=args.glue_version,
        worker_type=args.worker_type,
        num_workers=args.num_workers
    )
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
