"""Validator Agent - Updated for config-driven architecture."""

import re
from typing import List, Tuple, Optional, Dict
from dataclasses import dataclass, field


@dataclass
class ValidationResult:
    """Container for validation results."""
    is_valid: bool
    errors: List[str] = field(default_factory=list)
    warnings: List[str] = field(default_factory=list)
    suggestions: List[str] = field(default_factory=list)
    
    def summary(self) -> str:
        lines = []
        if self.is_valid:
            lines.append("✅ Validation PASSED")
        else:
            lines.append("❌ Validation FAILED")
        
        if self.errors:
            lines.append(f"\n🚫 Errors ({len(self.errors)}):")
            for error in self.errors[:5]:
                lines.append(f"  - {error}")
        
        if self.warnings:
            lines.append(f"\n⚠️  Warnings ({len(self.warnings)}):")
            for warning in self.warnings[:5]:
                lines.append(f"  - {warning}")
        
        if self.suggestions:
            lines.append(f"\n💡 Suggestions ({len(self.suggestions)}):")
            for suggestion in self.suggestions[:3]:
                lines.append(f"  - {suggestion}")
        
        return "\n".join(lines)


class ValidatorAgent:
    """Validates PySpark code - config-aware, less strict."""
    
    def __init__(self, spark_session_required: bool = True):
        self.spark_session_required = spark_session_required
        
        # Only critical anti-patterns
        self.anti_patterns = [
            (r"\.show\(\)", "Contains .show() - remove for production"),
            (r"for .* in .*\.rdd\.", "Looping over RDD - use DataFrame operations"),
        ]
        
        # Optional imports (not required)
        self.suggested_imports = [
            "from pyspark.sql import SparkSession",
            "from pyspark.sql.functions import",
        ]
    
    def validate(self, code: str, layer: str = "unknown") -> ValidationResult:
        """Validate generated PySpark code."""
        result = ValidationResult(is_valid=True)
        
        # 1. Syntax validation (critical)
        syntax_valid, syntax_error = self._check_syntax(code)
        if not syntax_valid:
            result.is_valid = False
            result.errors.append(f"Syntax error: {syntax_error}")
            return result
        
        # 2. Check for SparkSession (suggestion only)
        self._check_spark_session(code, result)
        
        # 3. Layer-specific checks (less strict)
        if layer.lower() == "gold":
            self._validate_gold(code, result)
        elif layer.lower() == "silver":
            self._validate_silver(code, result)
        elif layer.lower() == "bronze":
            self._validate_bronze(code, result)
        
        # 4. Anti-patterns (warnings only)
        self._check_anti_patterns(code, result)
        
        return result
    
    def _check_syntax(self, code: str) -> Tuple[bool, Optional[str]]:
        try:
            compile(code, '<string>', 'exec')
            return True, None
        except SyntaxError as e:
            return False, str(e)
    
    def _check_spark_session(self, code: str, result: ValidationResult):
        if "SparkSession" not in code and "spark =" not in code:
            result.suggestions.append("Add SparkSession initialization")
    
    def _validate_bronze(self, code: str, result: ValidationResult):
        """Bronze layer suggestions (not errors)."""
        if "ingestion_timestamp" not in code:
            result.suggestions.append("Add ingestion_timestamp metadata column")
        if "source_file" not in code:
            result.suggestions.append("Add source_file metadata column")
    
    def _validate_silver(self, code: str, result: ValidationResult):
        """Silver layer suggestions."""
        if "dropna" not in code and "fillna" not in code:
            result.suggestions.append("Consider null handling for data quality")
        if "dropDuplicates" not in code:
            result.suggestions.append("Consider deduplication for data quality")
    
    def _validate_gold(self, code: str, result: ValidationResult):
        """Gold layer - supports Parquet."""
        # Check for aggregations
        if "groupBy" not in code and "agg" not in code:
            result.warnings.append("No aggregations found in Gold layer")
        
        has_parquet = ".parquet(" in code
        
    
    def _check_anti_patterns(self, code: str, result: ValidationResult):
        for pattern, message in self.anti_patterns:
            if re.search(pattern, code):
                result.warnings.append(message)
    
    def auto_fix(self, code: str, validation_result: ValidationResult) -> str:
        """Attempt auto-fix for common issues."""
        fixed_code = code
        
        # Add SparkSession if missing
        if "SparkSession" not in fixed_code:
            spark_init = """from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("ETL").getOrCreate()

"""
            fixed_code = spark_init + fixed_code
        
        return fixed_code


# Example usage
if __name__ == "__main__":
    validator = ValidatorAgent()
    
    # Test with Gold code
    sample_gold = """
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("data/silver/sales_clean/")
result = df.groupBy("product_id").agg(sum("amount").alias("revenue"))
result.write.mode("overwrite").parquet("data/gold/revenue_by_product/")
"""
    
    result = validator.validate(sample_gold, layer="gold")
    print(result.summary())
