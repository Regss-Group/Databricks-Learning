# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer
# MAGIC Clean, join, and enrich Bronze tables into Silver tables.

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

spark = SparkSession.builder.getOrCreate()

# ---------- CONFIG ----------
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# COMMAND ----------
# Read Bronze tables
df_customers = spark.table(f"{BRONZE_SCHEMA}.customers")
df_orders = spark.table(f"{BRONZE_SCHEMA}.orders")
df_sales = spark.table(f"{BRONZE_SCHEMA}.sales")

# COMMAND ----------
# Cleaning example: remove null customer_id and normalize names
df_customers_clean = df_customers.filter(col("customer_id").isNotNull()) \
                                 .withColumn("name", upper(col("name")))

# COMMAND ----------
# Join orders with customers
df_orders_enriched = df_orders.join(df_customers_clean, "customer_id", "left") \
                              .withColumnRenamed("name", "customer_name") \
                              .withColumnRenamed("email", "customer_email")

# Join with sales
df_sales_enriched = df_sales.join(df_orders_enriched, "order_id", "left")

# COMMAND ----------
# Write Silver tables to Delta in UC
df_customers_clean.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{SILVER_SCHEMA}.customers")

df_orders_enriched.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{SILVER_SCHEMA}.orders")

df_sales_enriched.write.format("delta") \
    .mode("overwrite") \
    .saveAsTable(f"{SILVER_SCHEMA}.sales")

print("✅ Silver tables written to UC schema 'silver'")
