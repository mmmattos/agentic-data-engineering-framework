#!/bin/bash
# Setup local Spark for development

echo "Setting up local Spark environment..."

# Create temp directories for Spark
mkdir -p /tmp/spark-events
mkdir -p /tmp/spark-warehouse

# Set Spark environment variables
export SPARK_LOCAL_IP=127.0.0.1
export PYSPARK_PYTHON=python3

echo "✓ Spark environment ready"
echo "You can now run: pyspark --master local[*]"
