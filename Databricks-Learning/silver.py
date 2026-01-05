# COMMAND ----------

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper

spark = SparkSession.builder.getOrCreate()

# COMMAND ----------

# ---------- CONFIG ----------
BRONZE_SCHEMA = "bronze"
SILVER_SCHEMA = "silver"

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS commercial.silver;").collect()

# COMMAND ----------

# Read Bronze tables
CATALOG = 'commercial'
df_customers = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.customers")
df_orders = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.orders")
df_sales = spark.table(f"{CATALOG}.{BRONZE_SCHEMA}.sales")

# COMMAND ----------

# Cleaning example: remove null customer_id and normalize names
df_customers_clean = df_customers.filter(col("customer_id").isNotNull()) \
                                .withColumn("name", upper(col("name")))

# COMMAND ----------

# Join orders with customers
df_orders_enriched = df_orders.join(df_customers_clean, "customer_id", "left") \
                              .withColumnRenamed("name", "customer_name") \
                              .withColumnRenamed("email", "customer_email")

# COMMAND ----------

# Join with sales
df_sales_enriched = df_sales.join(df_orders_enriched, "order_id", "left")

# COMMAND ----------

# Remove duplicate 'load_date' columns
df_orders_enriched = df_orders_enriched.drop("load_date")
df_orders_enriched = df_orders_enriched.drop("source_file")

display(df_orders_enriched)

# COMMAND ----------

# Remove duplicate 'load_date' columns
df_sales_enriched = df_sales_enriched.drop("load_date")
df_sales_enriched = df_sales_enriched.drop("source_file")

display(df_orders_enriched)

# COMMAND ----------

# Write Silver tables to Delta in UC, overwriting schema to avoid column conflicts
df_customers_clean.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.customers")

df_orders_enriched.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.orders")

df_sales_enriched.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(f"{CATALOG}.{SILVER_SCHEMA}.sales")

print(f"Silver tables written to Unity Catalog schema '{CATALOG}.{SILVER_SCHEMA}'")

# COMMAND ----------
