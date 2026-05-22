# Agentic Glue ETL Pipeline

Config-driven ETL pipeline using Groq LLM and PySpark for medallion architecture (Bronze → Silver → Gold).


## Description

This repository delivers a config-driven ETL pipeline that leverages Groq's LLM to auto-generate PySpark code for medallion architecture (Bronze → Silver → Gold). 

It supports offline development with local Spark and production deployment on AWS Glue with built-in job queuing and S3 event triggers. 

The pipeline handles three input sources (batch files, event-triggered files, and database queries) and outputs to Parquet configurable for both local and AWS environments via a single USE_CASE_CONFIG dictionary. 

Switch domains (IoT, Fintech, Web Logs) or ingestion patterns without changing pipeline logic, making it ideal for LLM-powered data engineering with zero infrastructure lock-in.


# Quick Start
## Setup
```python
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

# Configure
cp .env.example .env
#### Edit .env with your GROQ_API_KEY

 - Run
```python
python scripts/run_pipeline.py
```
# Architecture
```mermaid
flowchart TB
    subgraph INPUT["Input Sources"]
        A1[("📁 File<br/>CSV/JSON/Parquet")]
        A2[("⚡ Event<br/>S3 + Glue Trigger")]
        A3[("🗄️ Database<br/>JDBC (PostgreSQL/MySQL)")]
    end

    subgraph AGENTS["Agentic Layer (Groq LLM)"]
        B1["🤖 Code Generator<br/>• Converts config → PySpark<br/>• Uses llama-3.3-70b<br/>• 280 tokens/sec"]
        B2["🔍 Validator<br/>• AST syntax check<br/>• Spark best practices<br/>• Auto-fix capabilities"]
        B3["⚙️ Executor<br/>• Orchestrates layers<br/>• Metrics collection<br/>• Retry logic"]
    end

    subgraph LAYERS["Medallion Architecture"]
        direction LR
        C1["🥉 Bronze Layer<br/>Raw ingestion<br/>+ metadata columns<br/>Write: append/overwrite"]
        C2["🥈 Silver Layer<br/>Cleaning & validation<br/>• Null handling<br/>• Deduplication<br/>• Type casting"]
        C3["🥇 Gold Layer<br/>Aggregations<br/>• Business metrics<br/>• Window functions<br/>• Sorting"]
    end

    subgraph OUTPUT["Output Targets"]
        D1[("📊 Parquet<br/>Columnar storage<br/>5-10x compression")]
        D2[("☁️ AWS Glue<br/>Production deployment<br/>Job queuing enabled")]
    end

    subgraph OFFLINE["Offline Development"]
        E1["🐍 Local Spark<br/>master('local[*]')"]
        E2["🐳 Docker<br/>Glue container"]
        E3["👀 Watchdog<br/>File event simulation"]
    end

    A1 --> B1
    A2 --> B1
    A3 --> B1
    B1 --> B2
    B2 --> B3
    B3 --> C1
    C1 --> C2
    C2 --> C3
    C3 --> D1
    D1 --> D2
    
    B3 -.-> E1
    B3 -.-> E2
    B3 -.-> E3

    style AGENTS fill:#e1f5fe,stroke:#01579b,stroke-width:2px
    style LAYERS fill:#f3e5f5,stroke:#4a148c,stroke-width:2px
    style C1 fill:#fff3e0,stroke:#e65100
    style C2 fill:#e0f2f1,stroke:#004d40
    style C3 fill:#fff8e1,stroke:#f57f17
    style OUTPUT fill:#c8e6c9,stroke:#2e7d32
```

Config (USE_CASE_CONFIG)\
then\
 → Code Generator → PySpark Code → Validator → Executor → Bronze/Silver/Gold

# Directory Structure
```bash
├── agents/               # CodeGenerator, Validator, Executor
├── config/               # Settings and configuration
├── data/                 # Data lake (raw/bronze/silver/gold)
├── notebooks/            # Jupyter notebooks for development
├── scripts/              # run_pipeline.py, file_monitor.py
├── output/               # Generated code and logs
└── tests/                # Unit tests
```

## Configuration

Modify \`USE_CASE_CONFIG\` in \`agents/code_generator_agent.py\`:

```json
USE_CASE_CONFIG = {
    "business_domain": "Your Domain",
    "source": {"type": "file", "environment": "offline"},
    "cleaning_rules": {...},
    "aggregations": {...},
}
``` 
# Usage

## Run complete pipeline
```bash
python scripts/run_pipeline.py
```
# Run specific layer
```bash
python scripts/run_pipeline.py --bronze
```

# With input file (event source)
```bash
python scripts/run_pipeline.py --input data/raw/file.csv
```

# File monitoring (event simulation)
```bash
python scripts/file_monitor.py
```
 - Then copy the files one at a time and they will get processed sequentially (as in a queue)
# Layers

- **Bronze**: Raw ingestion with metadata columns
- **Silver**: Data cleaning, validation, deduplication
- **Gold**: Aggregations and business metrics

## Output Formats

- **Parquet** (default) - Local development

## Switching Domains

Create new config (examples) for IoT, Fintech, Web Logs, Heathcare, etc.:
```python
agent = CodeGeneratorAgent(use_case_overrides=USE_CASE_CONFIG_IOT)
```

## License

MIT
