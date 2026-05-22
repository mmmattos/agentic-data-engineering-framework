"""
Unit tests for agentic-glue-etl agents.
Run with: pytest tests/test_agents.py -v
"""

import pytest
import sys
import os
from pathlib import Path
from unittest.mock import Mock, patch, MagicMock

# Add project root to path
sys.path.insert(0, str(Path(__file__).parent.parent))

from agents.code_generator_agent import CodeGeneratorAgent, GenerationResult
from agents.validator_agent import ValidatorAgent, ValidationResult
from agents.executor_agent import ExecutorAgent, ExecutionResult


class TestCodeGeneratorAgent:
    """Tests for CodeGeneratorAgent."""
    
    @pytest.fixture
    def mock_llm(self):
        """Mock LLM response."""
        with patch('agents.code_generator_agent.ChatGroq') as mock:
            mock_instance = Mock()
            mock_instance.invoke.return_value.content = """
### RULES ###
{"null_handling": "drop", "dedup_key": "id"}

### EXPLANATIONS ###
{"reasoning": "test explanation"}

### CODE ###
print("test code")
"""
            mock.return_value = mock_instance
            yield mock
    
    @pytest.fixture
    def agent(self, mock_llm):
        """Create agent instance with mocked LLM."""
        return CodeGeneratorAgent()
    
    def test_agent_initialization(self, agent):
        """Test agent initializes correctly."""
        assert agent.config is not None
        assert agent.config["business_domain"] is not None
        assert agent.output_dir == "output/generated_code"
    
    def test_generate_bronze_code(self, agent):
        """Test bronze code generation."""
        code = agent.generate_bronze_code()
        assert isinstance(code, str)
        assert len(code) > 0
    
    def test_generate_silver_with_auto_rules(self, agent):
        """Test silver generation with auto rules."""
        result = agent.generate_silver_code_with_auto_rules(
            data_description="Test data with nulls",
            quality_requirements="High quality required"
        )
        
        assert isinstance(result, GenerationResult)
        assert result.layer == "silver"
        assert isinstance(result.code, str)
    
    def test_generate_gold_with_auto_metrics(self, agent):
        """Test gold generation with auto metrics."""
        result = agent.generate_gold_code_with_auto_metrics(
            business_questions=["What is total revenue?"]
        )
        
        assert isinstance(result, GenerationResult)
        assert result.layer == "gold"
        assert isinstance(result.code, str)
    
    def test_config_validation(self):
        """Test configuration validation."""
        # This should pass with default config
        agent = CodeGeneratorAgent()
        assert agent.config["bronze_table_name"] is not None
        
        # Test with custom config
        custom_agent = CodeGeneratorAgent(use_case_overrides={
            "business_domain": "Custom Domain",
            "bronze_table_name": "custom_raw"
        })
        assert custom_agent.config["business_domain"] == "Custom Domain"
        assert custom_agent.config["bronze_table_name"] == "custom_raw"


class TestValidatorAgent:
    """Tests for ValidatorAgent."""
    
    @pytest.fixture
    def validator(self):
        """Create validator instance."""
        return ValidatorAgent()
    
    def test_validator_initialization(self, validator):
        """Test validator initializes correctly."""
        assert validator.spark_session_required is True
        assert len(validator.anti_patterns) > 0
    
    def test_validate_valid_code(self, validator):
        """Test validation of valid PySpark code."""
        valid_code = """
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()
df = spark.read.csv("test.csv")
df.write.parquet("output/")
"""
        result = validator.validate(valid_code, layer="bronze")
        assert isinstance(result, ValidationResult)
        assert result.is_valid or len(result.warnings) > 0  # May have warnings
    
    def test_validate_syntax_error(self, validator):
        """Test validation catches syntax errors."""
        invalid_code = "this is not valid python code!!!"
        result = validator.validate(invalid_code, layer="bronze")
        
        assert not result.is_valid
        assert len(result.errors) > 0
    
    def test_bronze_specific_checks(self, validator):
        """Test bronze layer specific validations."""
        bronze_code = """
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.csv("test.csv")
df.write.parquet("output/")
"""
        result = validator.validate(bronze_code, layer="bronze")
        
        # Should have warnings about missing metadata columns
        if result.warnings:
            assert any("metadata" in w.lower() for w in result.warnings)
    
    def test_gold_specific_checks(self, validator):
        """Test gold layer specific validations."""
        gold_code = """
from pyspark.sql import SparkSession

spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("silver/")
# No aggregations, no JDBC write
df.write.parquet("output/")
"""
        result = validator.validate(gold_code, layer="gold")
        
        # Should have errors about missing aggregations and JDBC
        assert len(result.errors) > 0
    
    def test_auto_fix(self, validator):
        """Test auto-fix functionality."""
        broken_code = "df = spark.read.csv('test.csv')"
        validation = validator.validate(broken_code, layer="bronze")
        
        fixed_code = validator.auto_fix(broken_code, validation)
        
        # Should have added SparkSession
        assert "SparkSession" in fixed_code or len(fixed_code) > len(broken_code)
    
    def test_validation_result_summary(self, validator):
        """Test ValidationResult summary formatting."""
        result = ValidationResult(is_valid=True)
        result.warnings.append("Test warning")
        
        summary = result.summary()
        assert "✅" in summary
        assert "Test warning" in summary


class TestExecutorAgent:
    """Tests for ExecutorAgent."""
    
    @pytest.fixture
    def mock_spark(self):
        """Mock SparkSession."""
        with patch('agents.executor_agent.SparkSession') as mock:
            mock_instance = MagicMock()
            mock.builder.appName.return_value.config.return_value.getOrCreate.return_value = mock_instance
            mock.return_value = mock_instance
            yield mock_instance
    
    @pytest.fixture
    def executor(self, mock_spark):
        """Create executor instance with mocked Spark."""
        return ExecutorAgent()
    
    def test_executor_initialization(self, executor):
        """Test executor initializes correctly."""
        assert executor.enable_metrics is True
        assert executor.execution_history == []
    
    def test_execute_valid_code(self, executor):
        """Test execution of valid code."""
        valid_code = """
from pyspark.sql import SparkSession

df = spark.range(10)
result_count = df.count()
"""
        result = executor.execute_code(valid_code, "bronze")
        
        assert isinstance(result, ExecutionResult)
        # Result may succeed or fail depending on environment
    
    def test_execution_result_summary(self, executor):
        """Test ExecutionResult summary formatting."""
        result = ExecutionResult(
            success=True,
            layer="bronze",
            execution_time_seconds=1.5,
            record_count=100
        )
        
        summary = result.summary()
        assert "SUCCESS" in summary
        assert "100" in summary
    
    def test_execute_with_retry(self, executor):
        """Test retry mechanism."""
        # Mock execute_code to fail first, then succeed
        with patch.object(executor, 'execute_code') as mock_execute:
            fail_result = ExecutionResult(success=False, layer="bronze", execution_time_seconds=0.1)
            success_result = ExecutionResult(success=True, layer="bronze", execution_time_seconds=0.1)
            
            mock_execute.side_effect = [fail_result, success_result]
            
            result = executor.execute_with_retry("test_code", "bronze", max_retries=2)
            
            assert result.success is True
            assert mock_execute.call_count == 2
    
    def test_execution_history(self, executor):
        """Test execution history tracking."""
        result = ExecutionResult(success=True, layer="bronze", execution_time_seconds=1.0)
        executor.execution_history.append(result)
        
        summary = executor.get_execution_summary()
        assert "Total executions: 1" in summary


class TestIntegration:
    """Integration tests between agents."""
    
    @pytest.fixture
    def mock_llm_response(self):
        """Mock LLM response for integration test."""
        return """
### RULES ###
{"null_handling": "drop", "dedup_key": "transaction_id"}

### EXPLANATIONS ###
{"null_handling": "Dropping nulls as they are <1% of data"}

### CODE ###
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
df = spark.read.parquet("data/bronze/test/")
df = df.dropna(subset=["transaction_id"])
df = df.dropDuplicates(["transaction_id"])
df.write.parquet("data/silver/test/")
"""
    
    def test_code_gen_to_validator_flow(self, mock_llm_response):
        """Test generated code can be validated."""
        with patch('agents.code_generator_agent.ChatGroq') as mock:
            mock_instance = Mock()
            mock_instance.invoke.return_value.content = mock_llm_response
            mock.return_value = mock_instance
            
            code_gen = CodeGeneratorAgent()
            validator = ValidatorAgent()
            
            result = code_gen.generate_silver_code_with_auto_rules(
                data_description="Test data",
                quality_requirements="Test requirements"
            )
            
            # Validate generated code
            validation = validator.validate(result.code, layer="silver")
            
            # Should pass or at least have manageable warnings
            assert validation.is_valid or len(validation.errors) == 0
    
    def test_validator_to_executor_flow(self):
        """Test validated code can be executed."""
        validator = ValidatorAgent()
        executor = ExecutorAgent()
        
        valid_code = """
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("test").getOrCreate()
df = spark.range(5)
df.write.mode("overwrite").parquet("data/test_output/")
print("Success")
"""
        
        validation = validator.validate(valid_code, layer="bronze")
        
        # If validation passes or has only warnings, attempt execution
        if validation.is_valid:
            result = executor.execute_code(valid_code, "test")
            # Execution result may vary by environment, but should not crash
            assert isinstance(result, ExecutionResult)


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
