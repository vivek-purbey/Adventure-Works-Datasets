# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Access Using App

# COMMAND ----------

spark.conf.set("fs.azure.account.auth.type.adestoragedatalakev2.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.adestoragedatalakev2.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.adestoragedatalakev2.dfs.core.windows.net", "c06e15d5-f7a3-4ca3-977b-1b6d94fe6c43")
spark.conf.set("fs.azure.account.oauth2.client.secret.adestoragedatalakev2.dfs.core.windows.net", "ffZ8Q~LnHS3eXZpi-ItVSjqrO4GbeuNhCZwSRcDW")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.adestoragedatalakev2.dfs.core.windows.net", "https://login.microsoftonline.com/141eea1d-34de-4a04-a0f1-8f5a3a990bcb/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ###Data Loading

# COMMAND ----------

df_calendar=spark.read.format('csv').option("header", True).option("inferSchema", True).load('abfss://bronze-raw@adestoragedatalakev2.dfs.core.windows.net/AdventureWorks_Calendar')

# COMMAND ----------

# MAGIC %md
# MAGIC ####alternate to read

# COMMAND ----------

# spark.read.format('csv').options(header='true', inferSchema='true').load('abfss://bronze-raw@adestoragedatalakev2.dfs.core.windows.net/AdventureWorks_Calendar/AdventureWorks_Calendar.csv').display()

# COMMAND ----------

df_customer=spark.read.format('csv').option("header", True).option("inferSchema", True).load('abfss://bronze-raw@adestoragedatalakev2.dfs.core.windows.net/AdventureWorks_Customers')

# COMMAND ----------

df_product_category=spark.read.format('csv').options(header=True, inferSchema=True).load('abfss://bronze-raw@adestoragedatalakev2.dfs.core.windows.net/AdventureWorks_Product_Categories')

# COMMAND ----------

df_product=spark.read.format('csv').options(header=True, inferSchema=True).load('abfss://bronze-raw@adestoragedatalakev2.dfs.core.windows.net/AdventureWorks_Products')

# COMMAND ----------

df_return=spark.read.format('csv').options(header=True, inferSchema=True).load('abfss://bronze-raw@adestoragedatalakev2.dfs.core.windows.net/AdventureWorks_Returns')

# COMMAND ----------

# MAGIC %md
# MAGIC ### how to read recursively files

# COMMAND ----------

df_sales=spark.read.format('csv').options(header=True, inferSchema=True).load('abfss://bronze-raw@adestoragedatalakev2.dfs.core.windows.net/AdventureWorks_Sales*')

# COMMAND ----------

df_territory=spark.read.format('csv').options(header=True, inferSchema=True).load('abfss://bronze-raw@adestoragedatalakev2.dfs.core.windows.net/AdventureWorks_Territories')

# COMMAND ----------

df_product_subcategory=spark.read.format('csv').options(header=True, inferSchema=True).load('abfss://bronze-raw@adestoragedatalakev2.dfs.core.windows.net/Product_Subcategories')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

df_calendar=df_calendar.withColumn('Month', month(col('Date')))\
    .withColumn('Year', year(col('Date')))

# COMMAND ----------

df_calendar.write.format('parquet')\
    .mode('overwrite')\
    .save('abfss://silver-transformed@adestoragedatalakev2.dfs.core.windows.net/AdventureWorks_Calendar')

# COMMAND ----------

df_customer=df_customer.withColumn('FullName', concat_ws(' ', col('Prefix'), col('FirstName'), col('LastName')))

# COMMAND ----------

df_customer.write.format('parquet')\
    .mode('overwrite')\
    .save('abfss://silver-transformed@adestoragedatalakev2.dfs.core.windows.net/AdventureWorks_Customers')

# COMMAND ----------

df_product_category.write.format('parquet')\
    .mode('overwrite')\
    .save('abfss://silver-transformed@adestoragedatalakev2.dfs.core.windows.net/AdventureWorks_Product_Categories')

# COMMAND ----------

df_product_subcategory.write.format('parquet')\
    .mode('overwrite')\
    .save('abfss://silver-transformed@adestoragedatalakev2.dfs.core.windows.net/AdventureWorks_Product_Subcategories')

# COMMAND ----------

df_product=df_product.withColumn('ProductSKU', split(col('ProductSKU'), '-')[0])\
    .withColumn('ProductName', split(col('ProductName'), ' ').getItem(0))

# COMMAND ----------

df_product.write.format('parquet')\
    .mode('overwrite')\
    .save('abfss://silver-transformed@adestoragedatalakev2.dfs.core.windows.net/AdventureWorks_Products')

# COMMAND ----------

df_return.write.format('parquet')\
    .mode('overwrite')\
    .save('abfss://silver-transformed@adestoragedatalakev2.dfs.core.windows.net/AdventureWorks_Returns')

# COMMAND ----------

df_territory.write.format('parquet')\
    .mode('overwrite')\
    .save('abfss://silver-transformed@adestoragedatalakev2.dfs.core.windows.net/AdventureWorks_Territories')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Sales Table

# COMMAND ----------

df_sales=df_sales.withColumn('StockDate', to_timestamp('StockDate'))

# COMMAND ----------

df_sales=df_sales.withColumn('OrderNumber', regexp_replace(col('OrderNumber'), 'S', 'T'))

# COMMAND ----------

df_sales=df_sales.withColumn('Multiply', col('OrderLineItem')*col('OrderQuantity'))

# COMMAND ----------

df_sales.write.format('parquet')\
    .mode('overwrite')\
    .save('abfss://silver-transformed@adestoragedatalakev2.dfs.core.windows.net/AdventureWorks_Sales')

# COMMAND ----------

df_sales.groupBy('OrderDate').agg(count("OrderNumber").alias('TotalOrder')).display()

# COMMAND ----------

df_product_category.display()

# COMMAND ----------

df_territory.display()