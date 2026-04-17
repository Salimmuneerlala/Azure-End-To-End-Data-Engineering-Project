# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

df = spark.read \
        .format("parquet") \
        .option("inferSchema", True) \
        .load("abfss://bronze@carsdataadls.dfs.core.windows.net/rawdata")

# COMMAND ----------

display(df)

# COMMAND ----------

splitted_df = df.withColumn("model_category", split(col("Model_ID"),'-')[0])

# COMMAND ----------

display(splitted_df)

# COMMAND ----------

new_df = splitted_df.withColumn("RevPerUnit", col("Revenue")/col("Units_Sold"))
new_df.display()

# COMMAND ----------

display(new_df.groupBy("Year", "BranchName").agg(sum("Units_Sold").alias("TotalUnitsSold")).sort("Year","TotalUnitsSold",ascending=[True, False]))

# COMMAND ----------

new_df.write \
    .format("parquet") \
    .mode("overwrite") \
    .option("path", "abfss://silver@carsdataadls.dfs.core.windows.net/carsales") \
    .save()

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from parquet.`abfss://silver@carsdataadls.dfs.core.windows.net/carsales`

# COMMAND ----------

