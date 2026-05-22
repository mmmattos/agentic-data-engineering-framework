# General Instructions

## Project Overview

This is a config-driven ETL pipeline that uses an LLM (Groq) to generate PySpark code for medallion architecture data processing.

## Prerequisites

### System Requirements
- Python 3.10 or higher
- Java 8 or 11 (for Spark)
- 8GB RAM minimum (16GB recommended for Spark)

### Python Packages

pip install -r requirements.txt

Key packages:
- \`groq\`, \`langchain-groq\` - LLM integration
- \`pyspark\` - Data processing
- \`watchdog\` - File monitoring (event simulation)
- \`jupyter\` - Development notebooks

## Getting Started


## 1. Setup Virtual Environment

python -m venv .venv
### Linux/Mac
source .venv/bin/activate
### Windows
.venv\\Scripts\\activate

Then:

pip install -r requirements.txt

### 2. Configure Environment

cp .env.example .env
 - Edit .env with your GROQ_API_KEY

### 3. Configure Pipeline

 - Edit \`agents/code_generator_agent.py\`
 - modify \`USE_CASE_CONFIG\` for your domain.
 - Edit other variables. USE_CASE_CONFIG.md for details.

### 4. Run Jupyter (Development)

$ jupyter notebook notebooks/

Run notebook \`01_complete_pipeline.ipynb\` from top to bottom.

### 5. Run Pipeline (Production)

python scripts/run_pipeline.py

## Development Workflow

### 1. Modify Configuration
- Update \`USE_CASE_CONFIG\` in \`code_generator_agent.py\`
- Test with small dataset in Jupyter

### 2. Generate Code
- Code is auto-generated from config
- Layers generation in individual notebook cells (Bronze, Silver and Gold).
- Saved to \`output/generated_code/\` for audit

### 3. Validate
- Code syntax validation
- Best practices checking
- Layer-specific validations

### 4. Execute
- Bronze → Silver → Gold sequentially
- Stops on first failure
- Metrics logged to console

### 5. Review Outputs
- Bronze: \`data/bronze/\` (Parquet)
- Silver: \`data/silver/\` (Parquet)
- Gold: \`data/gold/\` (Parquet) 

## Event Simulation (Offline)

### Terminal 1: Start file monitor
python scripts/file_monitor.py

### Terminal 2: Simulate file arrival
 - Put your csv file in data/raw/file_$(date +%s).csv

## Switching Domains

Create new config and initialize agent:

```python
from agents.code_generator_agent import CodeGeneratorAgent

IOT_CONFIG = {
    "business_domain": "IoT Sensors",
    # ... rest of config
}

agent = CodeGeneratorAgent(use_case_overrides=IOT_CONFIG)
```

## Common Tasks

### Reset Pipeline

```bash
rm -rf data/bronze/* data/silver/* data/gold/*
```
 - Look in notebooks/ too.

### Clear Generated Code

```bash
rm -rf output/generated_code/*.py
```
 - Look in notebooks/ too.

### View Spark UI

http://localhost:4040 (when running locally)


### Debug Generated Code

```python:
print(agent.generate_bronze_code())
print(agent.generate_silver_code())
print(agent.generate_gold_code())
```


## Testing

### Run unit tests
```bash
pytest tests/test_agents.py -v
```

### Run specific test
```basah
pytest tests/test_agents.py::TestCodeGeneratorAgent -v
```

# Deployment to AWS Glue

## Package and deploy
```bash
python scripts/deploy_to_glue.py --bucket my-scripts-bucket --job-name my-pipeline
```

## Update existing job
```bash
python scripts/deploy_to_glue.py --bucket my-scripts-bucket --job-name my-pipeline --update
```

## Troubleshooting

### PySpark Not Found
```bash
pip install pyspark
```

### Groq API Key Not Set

#### Linux/Mac

```bash
export GROQ_API_KEY="your-key" 
```

#### Windows
```bash
set GROQ_API_KEY=your-key
```

### Spark Session Errors

#### Add to code
```python 
spark = SparkSession.builder.config("spark.sql.adaptive.enabled", "true").getOrCreate()
```

### Out of Memory

 - Reduce data size or increase Spark memory:

```python
spark = SparkSession.builder \
    .config("spark.executor.memory", "4g") \
    .config("spark.driver.memory", "4g") \
    .getOrCreate()
```

## Best Practices

1. **Version Control**: Commit config, not data
2. **Testing**: Test with small datasets first
3. **Validation**: Always validate generated code before execution
4. **Audit**: Review \`output/generated_code/\` for generated logic
5. **Incremental**: Start with Bronze only, add Silver, then Gold

## Support

- Check \`output/logs/\` for execution logs
- Review metadata JSON files for agent decisions
- Run \`python scripts/validate_config.py\` for config validation

## Next Steps

1. Customize \`USE_CASE_CONFIG\` for your domain
2. Add your data to \`data/raw/\`
3. Run pipeline
4. Review outputs
5. Deploy to AWS Glue for production


