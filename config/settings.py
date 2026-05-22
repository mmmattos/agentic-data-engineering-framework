"""Configuration management for Agentic Glue ETL."""
import os
from dataclasses import dataclass
from dotenv import load_dotenv

load_dotenv()

@dataclass
class PipelineConfig:
    """Pipeline configuration settings."""
    
    # Groq settings
    groq_api_key: str = os.getenv("GROQ_API_KEY", "")
    groq_model: str = "llama-3.3-70b-versatile"
    groq_temperature: float = 0.1
    
    # Data paths
    raw_path: str = os.getenv("RAW_PATH", "data/raw/")
    bronze_path: str = os.getenv("BRONZE_PATH", "data/bronze/")
    silver_path: str = os.getenv("SILVER_PATH", "data/silver/")
    gold_path: str = os.getenv("GOLD_PATH", "data/gold/")
    
    # Spark settings
    spark_master: str = os.getenv("SPARK_MASTER", "local[*]")
    spark_app_name: str = "AgenticGlueETL"
    
    # Glue settings
    glue_version: str = os.getenv("GLUE_VERSION", "4.0")
    
    # Logging
    log_level: str = os.getenv("LOG_LEVEL", "INFO")
    
    def validate(self):
        """Validate required configurations."""
        if not self.groq_api_key:
            raise ValueError("GROQ_API_KEY not set in environment")
        return True

# Global config instance
config = PipelineConfig()
