# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Ingestion (UC)
# MAGIC Transform bronze tables into silver (cleaned & joined) in UC silver schema.

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
import datetime

spark = SparkSession.builder.getOrCreate()

# ---------- CONFIG ----------
UC_CATALOG = "sales"          # Replace with your UC catalog
SILVER_SCHEMA = f"{UC_CATALOG}.silver"
BRONZE_SCHEMA = f"{UC_CATALOG}.bronze"
TABLES = ["customers", "orders", "sales"]

today = datetime.datetime.today().strftime("%Y-%m-%d")

# COMMAND ----------
# Read bronze tables
customers_df = spark.table(f"{BRONZE_SCHEMA}.customers")
orders_df = spark.table(f"{BRONZE_SCHEMA}.orders")
sales_df = spark.table(f"{BRONZE_SCHEMA}.sales")

# Example Silver transformations:
# - Remove duplicates
# - Correct data types
# - Join orders with customers
orders_clean = orders_df.dropDuplicates(["order_id"]) \
                        .withColumn("quantity", col("quantity").cast("int")) \
                        .withColumn("price", col("price").cast("double"))

customers_clean = customers_df.dropDuplicates(["customer_id"])

orders_customers = orders_clean.join(customers_clean, on="customer_id", how="left")

sales_clean = sales_df.dropDuplicates(["sale_id"]) \
                      .withColumn("total_amount", col("total_amount").cast("double")) \
                      .withColumn("discount", col("discount").cast("double")) \
                      .withColumn("tax", col("tax").cast("double")) \
                      .withColumn("final_amount", col("final_amount").cast("double"))

# COMMAND ----------
# Write Silver tables
orders_customers.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_SCHEMA}.orders_customers")

sales_clean.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{SILVER_SCHEMA}.sales_clean")

print("✅ Silver tables created in UC silver schema")
