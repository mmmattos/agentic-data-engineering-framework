#!/usr/bin/env python3
"""
Run Agentic Glue ETL Pipeline from command line.

Usage:
    python scripts/run_pipeline.py
    python scripts/run_pipeline.py --input data/raw/sales.csv
    python scripts/run_pipeline.py --layer bronze
    python scripts/run_pipeline.py --layer bronze --silver --gold
"""

import os
import sys
import argparse
import shutil
from pathlib import Path

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from agents.code_generator_agent import CodeGeneratorAgent
from agents.validator_agent import ValidatorAgent
from agents.executor_agent import ExecutorAgent
from config.settings import config


def setup_directories():
    """Ensure required directories exist."""
    dirs = [
        "data/raw",
        "data/bronze",
        "data/silver", 
        "data/gold",
        "output/generated_code",
        "output/logs",
        "data/processed"  # For archive of processed files
    ]
    for d in dirs:
        os.makedirs(d, exist_ok=True)


def archive_processed_file(file_path: str):
    """Move processed file to archive directory."""
    if file_path and os.path.exists(file_path):
        archive_dir = "data/processed"
        os.makedirs(archive_dir, exist_ok=True)
        archive_path = os.path.join(archive_dir, os.path.basename(file_path))
        shutil.move(file_path, archive_path)
        print(f"📦 Archived: {file_path} -> {archive_path}")


def run_pipeline(input_file: str = None, layers: list = None):
    """
    Run the ETL pipeline for specified layers.
    
    Args:
        input_file: Optional input file path (for event source)
        layers: List of layers to run ['bronze', 'silver', 'gold']
    """
    print("\n" + "="*60)
    print("🚀 Starting Agentic Glue ETL Pipeline")
    print("="*60)
    
    if input_file:
        print(f"📄 Input file: {input_file}")
    
    if layers:
        print(f"📋 Layers: {', '.join(layers)}")
    else:
        layers = ['bronze', 'silver', 'gold']
        print(f"📋 Layers: all (bronze, silver, gold)")
    
    # Initialize agents
    print("\n🔧 Initializing agents...")
    code_gen = CodeGeneratorAgent()
    validator = ValidatorAgent()
    executor = ExecutorAgent()
    
    results = {}
    
    # Bronze Layer
    if 'bronze' in layers:
        print("\n" + "="*60)
        print("📦 Bronze Layer")
        print("="*60)
        
        bronze_code = code_gen.generate_bronze_code()
        # After generating bronze_code, add:
        
        # Validate
        bronze_validation = validator.validate(bronze_code, layer="bronze")
        if bronze_validation.errors:
            print(f"❌ Bronze validation failed: {bronze_validation.errors}")
            return False
        
        # Execute
        bronze_result = executor.execute_code(bronze_code, "bronze")
        print(bronze_result.summary())
        
        if not bronze_result.success:
            print("❌ Pipeline stopped: Bronze layer failed")
            return False
        
        results['bronze'] = bronze_result
        
        # Archive input file after successful Bronze
        if input_file:
            archive_processed_file(input_file)
    
    # Silver Layer
    if 'silver' in layers:
        print("\n" + "="*60)
        print("✨ Silver Layer")
        print("="*60)
        
        silver_code = code_gen.generate_silver_code()
        
        # Validate
        silver_validation = validator.validate(silver_code, layer="silver")
        if silver_validation.errors:
            print(f"❌ Silver validation failed: {silver_validation.errors}")
            return False
        
        # Execute
        silver_result = executor.execute_code(silver_code, "silver")
        print(silver_result.summary())
        
        if not silver_result.success:
            print("❌ Pipeline stopped: Silver layer failed")
            return False
        
        results['silver'] = silver_result
    
    # Gold Layer
    if 'gold' in layers:
        print("\n" + "="*60)
        print("🏆 Gold Layer")
        print("="*60)
        
        gold_code = code_gen.generate_gold_code()
        
        # Validate
        gold_validation = validator.validate(gold_code, layer="gold")
        if gold_validation.errors:
            print(f"❌ Gold validation failed: {gold_validation.errors}")
            return False
        
        # Execute
        gold_result = executor.execute_code(gold_code, "gold")
        print(gold_result.summary())
        
        if not gold_result.success:
            print("❌ Pipeline stopped: Gold layer failed")
            return False
        
        results['gold'] = gold_result
    
    # Summary
    print("\n" + "="*60)
    print("🏆 PIPELINE EXECUTION SUMMARY")
    print("="*60)
    for layer, result in results.items():
        status = "✅" if result.success else "❌"
        records = f", records: {result.record_count:,}" if result.record_count else ""
        print(f"{status} {layer.upper()}: {result.execution_time_seconds:.2f}s{records}")
    
    print("\n📂 Output locations:")
    print("   - Bronze: data/bronze/")
    print("   - Silver: data/silver/")
    print("   - Gold:   data/gold/")
    print("   - Code:   output/generated_code/")
    
    executor.cleanup()
    
    print("\n✅ Pipeline complete!")
    return True


def main():
    parser = argparse.ArgumentParser(description="Run Agentic Glue ETL Pipeline")
    parser.add_argument("--input", help="Input file path (for event source)")
    parser.add_argument("--layer", action="append", choices=['bronze', 'silver', 'gold'], 
                        help="Layer to run (can specify multiple times)")
    parser.add_argument("--bronze", action="store_true", help="Run bronze layer only")
    parser.add_argument("--silver", action="store_true", help="Run silver layer only")
    parser.add_argument("--gold", action="store_true", help="Run gold layer only")
    
    args = parser.parse_args()
    
    # Determine which layers to run
    layers = []
    if args.layer:
        layers = args.layer
    elif args.bronze:
        layers = ['bronze']
    elif args.silver:
        layers = ['silver']
    elif args.gold:
        layers = ['gold']
    else:
        layers = ['bronze', 'silver', 'gold']
    
    # Setup directories
    setup_directories()
    
    # Run pipeline
    success = run_pipeline(input_file=args.input, layers=layers)
    
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
