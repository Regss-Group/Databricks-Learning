# Databricks notebook source
# MAGIC %md
# MAGIC # Bronze Layer Ingestion
# MAGIC Ingest raw CSV data from S3 into Delta tables in UC bronze schema.

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, input_file_name
import datetime

spark = SparkSession.builder.getOrCreate()

# ---------- CONFIG ----------
BUCKET_NAME = "learning-databricks-0001"
BRONZE_SCHEMA = "bronze"
TABLES = ["customers", "orders", "sales"]

today = datetime.datetime.today().strftime("%Y-%m-%d")

# COMMAND ----------
# Function to ingest a table
def ingest_bronze(table_name):
    path = f"s3a://{BUCKET_NAME}/raw/{table_name}/{today}/*.csv"
    
    print(f"📥 Reading {table_name} from {path}")
    
    df = spark.read.option("header", True).csv(path)
    
    # Add metadata columns
    df = df.withColumn("load_date", lit(today)) \
           .withColumn("source_file", input_file_name())
    
    # Write to Delta in UC bronze schema
    delta_table_path = f"dbfs:/mnt/uc/{BRONZE_SCHEMA}/{table_name}"
    
    print(f"💾 Writing {table_name} to Delta at {delta_table_path}")
    
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{BRONZE_SCHEMA}.{table_name}")
    
    print(f"✅ {table_name} ingested into {BRONZE_SCHEMA}.{table_name}")

# COMMAND ----------
# Ingest all tables
for table in TABLES:
    ingest_bronze(table)
