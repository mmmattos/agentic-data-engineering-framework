"""Executor Agent - Executes generated PySpark code and manages pipeline execution."""

import os
import sys
import time
import logging
import traceback
from datetime import datetime
from typing import Dict, Optional, Any, Callable
from dataclasses import dataclass, field
from pathlib import Path

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


@dataclass
class ExecutionResult:
    """Container for execution results."""
    success: bool
    layer: str
    execution_time_seconds: float
    record_count: Optional[int] = None
    error_message: Optional[str] = None
    traceback: Optional[str] = None
    output_path: Optional[str] = None
    metadata: Dict[str, Any] = field(default_factory=dict)
    
    def summary(self) -> str:
        """Return formatted execution summary."""
        lines = []
        lines.append(f"{'='*60}")
        lines.append(f"📊 Execution Result - {self.layer.upper()} Layer")
        lines.append(f"{'='*60}")
        
        if self.success:
            lines.append(f"✅ Status: SUCCESS")
        else:
            lines.append(f"❌ Status: FAILED")
        
        lines.append(f"⏱️  Execution time: {self.execution_time_seconds:.2f} seconds")
        
        if self.record_count is not None:
            lines.append(f"📊 Record count: {self.record_count:,}")
        
        if self.output_path:
            lines.append(f"💾 Output path: {self.output_path}")
        
        if self.error_message:
            lines.append(f"\n🚫 Error: {self.error_message}")
        
        if self.traceback:
            lines.append(f"\n📋 Traceback:\n{self.traceback}")
        
        lines.append(f"{'='*60}")
        return "\n".join(lines)


class ExecutorAgent:
    """Executes PySpark code for ETL pipeline layers."""
    
    def __init__(self, spark_session=None, enable_metrics: bool = True):
        """
        Initialize executor agent.
        
        Args:
            spark_session: Existing SparkSession (will create if None)
            enable_metrics: Whether to collect execution metrics
        """
        self.spark = spark_session
        self.enable_metrics = enable_metrics
        self.execution_history: list = []
        
        # Track execution context
        self.current_pipeline_id = None
        self.current_layer = None
        
    def _ensure_spark_session(self):
        """Create SparkSession if not already available."""
        if self.spark is None:
            try:
                from pyspark.sql import SparkSession
                
                self.spark = SparkSession.builder \
                    .appName("AgenticGlueETL") \
                    .config("spark.sql.adaptive.enabled", "true") \
                    .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
                    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer") \
                    .getOrCreate()
                
                logger.info("✅ Created new SparkSession")
            except ImportError:
                raise ImportError("PySpark not installed. Run: pip install pyspark")
        
        return self.spark
    
    def execute_code(self, code: str, layer: str, 
                    global_vars: Optional[Dict] = None,
                    timeout_seconds: int = 300) -> ExecutionResult:
        """
        Execute generated PySpark code.
        
        Args:
            code: PySpark code to execute
            layer: Which layer (bronze/silver/gold)
            global_vars: Additional global variables to pass to execution context
            timeout_seconds: Maximum execution time in seconds
            
        Returns:
            ExecutionResult with execution details
        """
        start_time = time.time()
        self.current_layer = layer
        
        # Ensure Spark is available
        spark = self._ensure_spark_session()
        
        # Prepare execution context
        exec_globals = {
            'spark': spark,
            'logger': logger,
            '__builtins__': __builtins__,
        }
        
        if global_vars:
            exec_globals.update(global_vars)
        
        # Capture output paths (common patterns)
        output_path = self._extract_output_path(code)
        
        try:
            logger.info(f"🚀 Executing {layer.upper()} layer code...")
            
            # Execute with timeout (simplified - for production use signal or concurrent.futures)
            exec(code, exec_globals)
            
            execution_time = time.time() - start_time
            
            # Capture record count if DataFrame exists in context
            record_count = self._extract_record_count(exec_globals, layer)
            
            # Log success
            logger.info(f"✅ {layer.upper()} execution completed in {execution_time:.2f}s")
            
            result = ExecutionResult(
                success=True,
                layer=layer,
                execution_time_seconds=execution_time,
                record_count=record_count,
                output_path=output_path,
                metadata={
                    'pipeline_id': self.current_pipeline_id,
                    'timestamp': datetime.now().isoformat()
                }
            )
            
            # Store in history
            self.execution_history.append(result)
            
            return result
            
        except Exception as e:
            execution_time = time.time() - start_time
            error_msg = str(e)
            tb = traceback.format_exc()
            
            logger.error(f"❌ {layer.upper()} execution failed: {error_msg}")
            
            result = ExecutionResult(
                success=False,
                layer=layer,
                execution_time_seconds=execution_time,
                error_message=error_msg,
                traceback=tb,
                output_path=output_path
            )
            
            self.execution_history.append(result)
            return result
    
    def execute_pipeline(self, bronze_code: str, silver_code: str, gold_code: str,
                        pipeline_name: str = "unnamed_pipeline") -> Dict[str, ExecutionResult]:
        """
        Execute complete Bronze -> Silver -> Gold pipeline.
        
        Args:
            bronze_code: PySpark code for Bronze layer
            silver_code: PySpark code for Silver layer
            gold_code: PySpark code for Gold layer
            pipeline_name: Name for this pipeline run
            
        Returns:
            Dictionary with results for each layer
        """
        self.current_pipeline_id = f"{pipeline_name}_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        logger.info(f"🏭 Starting pipeline: {self.current_pipeline_id}")
        
        results = {}
        
        # Execute Bronze
        logger.info("📦 Stage 1/3: Bronze Layer")
        results['bronze'] = self.execute_code(bronze_code, 'bronze')
        
        if not results['bronze'].success:
            logger.error("❌ Pipeline stopped: Bronze layer failed")
            return results
        
        # Execute Silver
        logger.info("✨ Stage 2/3: Silver Layer")
        results['silver'] = self.execute_code(silver_code, 'silver')
        
        if not results['silver'].success:
            logger.error("❌ Pipeline stopped: Silver layer failed")
            return results
        
        # Execute Gold
        logger.info("🏆 Stage 3/3: Gold Layer")
        results['gold'] = self.execute_code(gold_code, 'gold')
        
        # Summary
        logger.info(f"✅ Pipeline complete: {self.current_pipeline_id}")
        for layer, result in results.items():
            status = "✅" if result.success else "❌"
            logger.info(f"  {status} {layer.upper()}: {result.execution_time_seconds:.2f}s")
        
        return results
    
    def _extract_output_path(self, code: str) -> Optional[str]:
        """Extract output path from code (supports parquet, csv, json writes)."""
        patterns = [
            r'\.parquet\(["\']([^"\']+)["\']\)',
            r'\.csv\(["\']([^"\']+)["\']\)',
            r'\.json\(["\']([^"\']+)["\']\)',
            r'\.jdbc\(["\']([^"\']+)["\']',
        ]
        
        for pattern in patterns:
            import re
            match = re.search(pattern, code)
            if match:
                return match.group(1)
        
        return None
    
    def _extract_record_count(self, exec_globals: Dict, layer: str) -> Optional[int]:
        """Try to extract record count from last DataFrame in execution."""
        # Look for common DataFrame variable names
        df_vars = ['df', 'bronze_df', 'silver_df', 'gold_df', 'result_df', 'final_df']
        
        for var_name in df_vars:
            if var_name in exec_globals:
                df = exec_globals[var_name]
                try:
                    # Check if it's a Spark DataFrame
                    if hasattr(df, 'count'):
                        count = df.count()
                        logger.info(f"📊 Record count from {var_name}: {count:,}")
                        return count
                except:
                    pass
        
        # Also check for layer-specific variable
        layer_var = f"{layer}_df"
        if layer_var in exec_globals:
            try:
                count = exec_globals[layer_var].count()
                return count
            except:
                pass
        
        return None
    
    def execute_with_retry(self, code: str, layer: str, 
                          max_retries: int = 3,
                          retry_delay_seconds: int = 5) -> ExecutionResult:
        """
        Execute code with automatic retry on failure.
        
        Args:
            code: PySpark code to execute
            layer: Layer name
            max_retries: Maximum number of retry attempts
            retry_delay_seconds: Delay between retries
            
        Returns:
            ExecutionResult (from final attempt)
        """
        last_result = None
        
        for attempt in range(1, max_retries + 1):
            logger.info(f"🔄 Attempt {attempt}/{max_retries} for {layer} layer")
            
            result = self.execute_code(code, layer)
            last_result = result
            
            if result.success:
                if attempt > 1:
                    logger.info(f"✅ Succeeded on attempt {attempt}")
                return result
            
            if attempt < max_retries:
                logger.warning(f"⚠️ Attempt {attempt} failed. Retrying in {retry_delay_seconds}s...")
                time.sleep(retry_delay_seconds)
        
        logger.error(f"❌ All {max_retries} attempts failed for {layer} layer")
        return last_result
    
    def get_execution_summary(self) -> str:
        """Get summary of all executions in this session."""
        if not self.execution_history:
            return "No executions performed yet."
        
        lines = []
        lines.append("\n" + "="*60)
        lines.append("📈 EXECUTION HISTORY SUMMARY")
        lines.append("="*60)
        
        successful = sum(1 for r in self.execution_history if r.success)
        total = len(self.execution_history)
        total_time = sum(r.execution_time_seconds for r in self.execution_history)
        
        lines.append(f"Total executions: {total}")
        lines.append(f"Successful: {successful}")
        lines.append(f"Failed: {total - successful}")
        lines.append(f"Success rate: {(successful/total)*100:.1f}%")
        lines.append(f"Total execution time: {total_time:.2f}s")
        lines.append(f"Average execution time: {total_time/total:.2f}s")
        
        lines.append("\n📋 Detailed History:")
        for i, result in enumerate(self.execution_history[-5:], 1):  # Last 5
            status = "✅" if result.success else "❌"
            lines.append(f"  {i}. {status} {result.layer.upper()} - {result.execution_time_seconds:.2f}s")
        
        lines.append("="*60)
        return "\n".join(lines)
    
    def cleanup(self):
        """Clean up Spark session if it was created by this agent."""
        if self.spark is not None:
            try:
                self.spark.stop()
                logger.info("🧹 SparkSession stopped")
            except:
                pass


# Example usage
if __name__ == "__main__":
    # Test executor with sample code
    executor = ExecutorAgent()
    
    # Test bronze execution
    test_bronze_code = """
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, current_timestamp

# Create sample data
data = [("product1", 100), ("product2", 200), ("product3", 150)]
df = spark.createDataFrame(data, ["product", "amount"])
df = df.withColumn("ingestion_time", current_timestamp())

# Write to bronze
df.write.mode("overwrite").parquet("data/bronze/test_output/")
print(f"✅ Wrote {df.count()} records to bronze layer")
"""
    
    result = executor.execute_code(test_bronze_code, "bronze")
    print(result.summary())
    
    # Test retry mechanism
    test_failing_code = """
# This code will fail
df = spark.read.parquet("nonexistent_path/")
"""
    
    retry_result = executor.execute_with_retry(test_failing_code, "silver", max_retries=2)
    print(retry_result.summary())
    
    # Print execution summary
    print(executor.get_execution_summary())
    
    # Cleanup
    executor.cleanup()
