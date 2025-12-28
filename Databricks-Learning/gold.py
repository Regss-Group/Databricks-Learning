# Databricks notebook source
# MAGIC %md
# MAGIC # Gold Layer Aggregation (UC)
# MAGIC Aggregate silver tables to create business-level metrics.

# COMMAND ----------
from pyspark.sql import SparkSession
from pyspark.sql.functions import sum, avg, count, col
import datetime

spark = SparkSession.builder.getOrCreate()

# ---------- CONFIG ----------
UC_CATALOG = "sales"         # Replace with your UC catalog
GOLD_SCHEMA = f"{UC_CATALOG}.gold"
SILVER_SCHEMA = f"{UC_CATALOG}.silver"

# COMMAND ----------
# Read silver tables
orders_customers = spark.table(f"{SILVER_SCHEMA}.orders_customers")
sales_clean = spark.table(f"{SILVER_SCHEMA}.sales_clean")

# Example Gold transformations:
# - Total sales per customer
# - Total orders per product
# - Average discount per customer

gold_sales_per_customer = sales_clean.groupBy("order_id") \
                                     .agg(sum("final_amount").alias("total_spent"))

gold_orders_per_product = orders_customers.groupBy("product") \
                                          .agg(count("order_id").alias("total_orders"),
                                               sum("quantity").alias("total_quantity"))

gold_avg_discount = sales_clean.groupBy("order_id") \
                               .agg(avg("discount").alias("avg_discount"))

# COMMAND ----------
# Write Gold tables
gold_sales_per_customer.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_SCHEMA}.sales_per_customer")

gold_orders_per_product.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_SCHEMA}.orders_per_product")

gold_avg_discount.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{GOLD_SCHEMA}.avg_discount_per_order")

print("✅ Gold tables created in UC gold schema")
