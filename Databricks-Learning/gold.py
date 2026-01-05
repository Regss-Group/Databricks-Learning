# Databricks notebook source
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum as spark_sum, count, avg, max as spark_max, min as spark_min, when, lit
from pyspark.sql.window import Window

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

# 1. Customer Lifetime Value (CLV) & Order Metrics
customer_metrics = df_sales \
    .groupBy("customer_id", "customer_name") \
    .agg(
        spark_sum("amount").alias("total_revenue"),
        avg("amount").alias("avg_order_value"),
        count("order_id").alias("total_orders"),
        spark_max("order_date").alias("last_order_date"),
        spark_min("order_date").alias("first_order_date")
    ) \
    .withColumn("days_since_first_order", 
                (spark_max("order_date") - spark_min("order_date")).alias("days_since_first_order")) \
    .withColumn("clv", col("total_revenue") / (count("order_id") + 1))  # Simple CLV

display(customer_metrics)

# COMMAND ----------

# 2. Product Performance
product_performance = df_sales \
    .groupBy("product_id", "product_name") \
    .agg(
        spark_sum("amount").alias("total_revenue"),
        count("order_id").alias("order_count"),
        avg("amount").alias("avg_sale_price")
    ) \
    .withColumn("popularity_rank", 
                row_number().over(Window.orderBy(col("order_count").desc())))

display(product_performance)

# COMMAND ----------

# 3. Daily Sales Summary (Business KPI dashboard ready)
daily_sales = df_sales \
    .groupBy("order_date") \
    .agg(
        spark_sum("amount").alias("daily_revenue"),
        count("order_id").alias("daily_orders"),
        countDistinct("customer_id").alias("unique_customers")
    ) \
    .orderBy(col("order_date").desc())

display(daily_sales)

# COMMAND ----------

# 4. High Value Customers (Top 20% by revenue)
high_value_customers = customer_metrics \
    .filter(col("total_revenue") > col("total_revenue").approxQuantile(0.8, 0.5)) \
    .select("customer_id", "customer_name", "total_revenue", "total_orders", "clv")

display(high_value_customers)

# COMMAND ----------

# Write Gold tables to Delta (business-ready, aggregated)
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
    
    print(f"Gold table '{table_name}' written successfully")

print("🏆 Gold layer complete: Business-ready analytics tables created!")
print("Tables: customer_ltv, product_performance, daily_sales_summary, high_value_customers")

# COMMAND ----------
