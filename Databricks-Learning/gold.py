# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg, max as spark_max, min as spark_min, when, lit, countDistinct
from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# ---------- CONFIG ----------
SILVER_SCHEMA = "silver"
GOLD_SCHEMA = "gold"

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS commercial.gold;").collect()

# COMMAND ----------

# Read Silver tables
CATALOG = 'commercial'
df_customers = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.customers")
df_orders = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.orders")
df_sales = spark.table(f"{CATALOG}.{SILVER_SCHEMA}.sales")

# COMMAND ----------

# 1. Customer Lifetime Value (CLV) - using 'price' column from your sales data
customer_metrics = df_sales \
    .groupBy("customer_id", "customer_name") \
    .agg(
        spark_sum("price").alias("total_revenue"),      # Changed from 'amount' to 'price'
        avg("price").alias("avg_order_value"),
        count("order_id").alias("total_orders"),
        spark_max("order_date").alias("last_order_date"),
        spark_min("order_date").alias("first_order_date"),
        countDistinct("product_id").alias("unique_products_bought")
    ) \
    .withColumn("days_customer_active", 
                (spark_max("order_date") - spark_min("order_date")).days) \
    .withColumn("clv", 
                col("total_revenue") / (col("total_orders") + 1))  # Simple CLV

display(customer_metrics)

# COMMAND ----------

# 2. Product Performance - using 'price' and available columns
product_performance = df_sales \
    .groupBy("product_id", "product") \
    .agg(
        spark_sum("price").alias("total_revenue"),
        count("order_id").alias("order_count"),
        avg("price").alias("avg_sale_price"),
        sum(when(col("discount").isNotNull(), col("discount")).otherwise(0)).alias("total_discounts")
    ) \
    .withColumn("popularity_rank", 
                row_number().over(Window.orderBy(col("order_count").desc())))

display(product_performance)

# COMMAND ----------

# 3. Daily Sales Summary (KPI dashboard ready)
daily_sales = df_sales \
    .groupBy("order_date") \
    .agg(
        spark_sum("price").alias("daily_revenue"),
        count("order_id").alias("daily_orders"),
        countDistinct("customer_id").alias("unique_customers"),
        avg("price").alias("avg_order_price"),
        sum(when(col("discount").isNotNull(), col("discount")).otherwise(0)).alias("total_discount_amount")
    ) \
    .orderBy(col("order_date").desc())

display(daily_sales)

# COMMAND ----------

# 4. High Value Customers (Top customers by revenue)
high_value_customers = customer_metrics \
    .orderBy(col("total_revenue").desc()) \
    .limit(20) \
    .select("customer_id", "customer_name", "total_revenue", "total_orders", "clv", "unique_products_bought")

display(high_value_customers)

# COMMAND ----------

# Write Gold tables to Delta (business-ready analytics)
GOLD_TABLES = {
    "customer_ltv": customer_metrics,
    "product_performance": product_performance, 
    "daily_sales_summary": daily_sales,
    "high_value_customers": high_value_customers
}

for table_name, df in GOLD_TABLES.items():
    df.write.format("delta") \
        .mode("overwrite") \
        .option("overwriteSchema", "true") \
        .saveAsTable(f"{CATALOG}.{GOLD_SCHEMA}.{table_name}")
    
    print(f"✅ Gold table '{table_name}' written successfully")

print("🏆 Gold layer complete! Business-ready tables: customer_ltv, product_performance, daily_sales_summary, high_value_customers")

# COMMAND ----------
