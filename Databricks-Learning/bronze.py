# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col
import datetime

spark = SparkSession.builder.getOrCreate()

# ---------- CONFIG ----------
BUCKET_NAME = "learning-databricks-0001"
TABLES = ["customers", "orders", "sales"]

today = datetime.datetime.today().strftime("%Y-%m-%d")

# COMMAND ----------

def ingest_bronze(table_name):
    path = f"s3a://{BUCKET_NAME}/raw/{table_name}/{today}/*.csv"
    print(f"Reading {table_name} from {path}")

    df = spark.read.option("header", True).csv(path)

    df = (
        df.withColumn("load_date", lit(today))
          .withColumn("source_file", col("_metadata.file_path"))
    )

    table_full_name = f"commercial.bronze.{table_name}"
    print(f"Writing {table_name} to {table_full_name}")

    (
        df.write.format("delta")
          .mode("overwrite")
          .option("overwriteSchema", "true")
          .saveAsTable(table_full_name)
    )

    print(f"{table_name} ingested successfully")

# COMMAND ----------

for table in TABLES:
    ingest_bronze(table)
