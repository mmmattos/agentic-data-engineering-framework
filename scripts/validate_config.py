#!/usr/bin/env python3
"""
Configuration Validator - Validates USE_CASE_CONFIG before deployment.

Usage:
    python scripts/validate_config.py
    python scripts/validate_config.py --config my_config.py
"""

import os
import sys
import json
import argparse
from pathlib import Path
from typing import List, Tuple, Dict, Any

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from agents.code_generator_agent import CodeGeneratorAgent


class ConfigValidator:
    """Validate USE_CASE_CONFIG for AWS deployment."""
    
    def __init__(self, config: Dict[str, Any]):
        self.config = config
        self.issues: List[Tuple[str, str]] = []
        self.warnings: List[Tuple[str, str]] = []
    
    def validate_all(self) -> bool:
        """Run all validations."""
        print("\n" + "="*60)
        print("🔍 AWS Configuration Validator")
        print("="*60)
        
        self._validate_source()
        self._validate_glue_config()
        self._validate_cleaning_rules()
        self._validate_aggregations()
        
        self._print_report()
        return len(self.issues) == 0
    
    def _validate_source(self):
        """Validate source configuration."""
        print("\n📂 Validating Source Configuration...")
        
        source = self.config.get("source", {})
        
        # Check source type
        source_type = source.get("type")
        if source_type not in ["file", "event", "database"]:
            self.issues.append(("Source", f"Invalid type: {source_type}", "Use 'file', 'event', or 'database'"))
            return
        
        env = source.get("environment", "offline")
        
        # For AWS deployment
        if env == "aws":
            if source_type == "event":
                s3_bucket = source.get("s3_bucket")
                if not s3_bucket:
                    self.issues.append(("Source", "Missing s3_bucket for event source", "Set source.s3_bucket"))
                elif not self._check_s3_bucket(s3_bucket):
                    self.warnings.append(("Source", f"S3 bucket may not exist: {s3_bucket}"))
                
                s3_prefix = source.get("s3_prefix", "")
                print(f"  ✅ Event source: s3://{s3_bucket}/{s3_prefix}")
            
            elif source_type == "database":
                db_config = source.get("database", {})
                conn = db_config.get("connection", {})
                url = conn.get("url", "")
                if not url or url.startswith("${"):
                    self.warnings.append(("Source", "Database URL uses environment variable", "Ensure DB_URL is set in AWS Glue"))
                print(f"  ✅ Database source: {url}")
            
            elif source_type == "file":
                path = source.get("path", "")
                if path.startswith("s3://"):
                    print(f"  ✅ File source: {path}")
                else:
                    self.warnings.append(("Source", f"File path is local: {path}", "Use s3:// for AWS deployment"))
        else:
            print(f"  ✅ Offline mode (environment: {env})")
    
    def _validate_glue_config(self):
        """Validate Glue job configuration."""
        print("\n⚡ Validating Glue Configuration...")
        
        glue = self.config.get("glue", {})
        
        # Check required fields
        required = ["job_run_queuing_enabled", "max_concurrent_runs", "timeout_minutes", "worker_type", "num_workers"]
        for field in required:
            if field not in glue:
                self.warnings.append(("Glue", f"Missing {field}, using default", None))
        
        # Validate worker type
        worker_type = glue.get("worker_type", "G.1X")
        valid_workers = ["G.1X", "G.2X", "G.4X", "G.8X", "Standard"]
        if worker_type not in valid_workers:
            self.issues.append(("Glue", f"Invalid worker_type: {worker_type}", f"Use one of {valid_workers}"))
        else:
            print(f"  ✅ Worker type: {worker_type}")
        
        # Validate concurrent runs
        max_runs = glue.get("max_concurrent_runs", 5)
        if max_runs < 1 or max_runs > 50:
            self.issues.append(("Glue", f"max_concurrent_runs must be 1-50, got {max_runs}", None))
        else:
            print(f"  ✅ Max concurrent runs: {max_runs}")
        
        # Validate job queuing
        queuing = glue.get("job_run_queuing_enabled", True)
        print(f"  ✅ Job run queuing: {'Enabled' if queuing else 'Disabled'}")
        
        # Check S3 trigger
        if glue.get("s3_trigger_enabled", False):
            print(f"  ✅ S3 trigger enabled")
    
    def _validate_cleaning_rules(self):
        """Validate cleaning rules."""
        print("\n🧹 Validating Cleaning Rules...")
        
        rules = self.config.get("cleaning_rules", {})
        
        # Check required rule sections
        if "null_drop_columns" not in rules:
            self.warnings.append(("Cleaning", "No null_drop_columns defined", "Add to cleaning_rules"))
        else:
            print(f"  ✅ Null drop columns: {len(rules['null_drop_columns'])} columns")
        
        if "deduplication_columns" not in rules:
            self.warnings.append(("Cleaning", "No deduplication_columns defined", "Add to cleaning_rules"))
        else:
            print(f"  ✅ Deduplication columns: {len(rules['deduplication_columns'])} columns")
        
        if "validation_rules" in rules:
            print(f"  ✅ Validation rules: {len(rules['validation_rules'])} rules")
        
        if "data_type_fixes" in rules:
            print(f"  ✅ Data type fixes: {len(rules['data_type_fixes'])} columns")
    
    def _validate_aggregations(self):
        """Validate aggregations."""
        print("\n📊 Validating Aggregations...")
        
        aggs = self.config.get("aggregations", {})
        
        if not aggs:
            self.issues.append(("Aggregations", "No aggregations defined", "Add at least one aggregation"))
            return
        
        for name, agg in aggs.items():
            agg_type = agg.get("type")
            if agg_type not in ["agg", "group_by"]:
                self.warnings.append(("Aggregations", f"{name}: invalid type {agg_type}", None))
            elif agg_type == "agg":
                if "expression" not in agg:
                    self.issues.append(("Aggregations", f"{name}: missing expression", None))
            elif agg_type == "group_by":
                if "group_by" not in agg:
                    self.issues.append(("Aggregations", f"{name}: missing group_by", None))
                if "aggregations" not in agg:
                    self.issues.append(("Aggregations", f"{name}: missing aggregations", None))
        
        print(f"  ✅ {len(aggs)} aggregations defined")
    
    def _check_s3_bucket(self, bucket_name: str) -> bool:
        """Check if S3 bucket exists (requires AWS CLI)."""
        import subprocess
        try:
            result = subprocess.run(
                ["aws", "s3", "ls", f"s3://{bucket_name}", "--max-items", "1"],
                capture_output=True, text=True, timeout=5
            )
            return result.returncode == 0
        except:
            return False  # Assume exists if can't check
    
    def _print_report(self):
        """Print validation report."""
        print("\n" + "="*60)
        print("📋 VALIDATION REPORT")
        print("="*60)
        
        if self.issues:
            print(f"\n❌ ISSUES ({len(self.issues)}):")
            for category, issue, suggestion in self.issues:
                print(f"\n  [{category}] {issue}")
                if suggestion:
                    print(f"    💡 Fix: {suggestion}")
        
        if self.warnings:
            print(f"\n⚠️  WARNINGS ({len(self.warnings)}):")
            for category, warning, _ in self.warnings:
                print(f"\n  [{category}] {warning}")
        
        if not self.issues and not self.warnings:
            print("\n✅ All validations passed!")
        elif not self.issues:
            print("\n✅ No critical issues (warnings only)")
        else:
            print(f"\n❌ {len(self.issues)} critical issue(s) found")
        
        print("="*60)


def main():
    parser = argparse.ArgumentParser(description="Validate USE_CASE_CONFIG for AWS deployment")
    parser.add_argument("--config", help="Path to config file (uses agent config if not provided)")
    
    args = parser.parse_args()
    
    if args.config:
        # Load config from file
        import importlib.util
        spec = importlib.util.spec_from_file_location("user_config", args.config)
        user_config = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(user_config)
        config_dict = user_config.USE_CASE_CONFIG
    else:
        # Use agent's default config
        agent = CodeGeneratorAgent()
        config_dict = agent.config
    
    validator = ConfigValidator(config_dict)
    success = validator.validate_all()
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
