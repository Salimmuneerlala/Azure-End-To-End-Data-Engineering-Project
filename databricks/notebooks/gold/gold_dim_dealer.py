# Databricks notebook source
from pyspark.sql.types import *
from pyspark.sql.functions import *

# COMMAND ----------



# COMMAND ----------

dbutils.widgets.text("incremental_flag", "0")

# COMMAND ----------

incremental_flag = dbutils.widgets.get("incremental_flag")
print(incremental_flag)

# COMMAND ----------



# COMMAND ----------

spark.sql("""
    select * 
    from parquet.`abfss://silver@carsdataadls.dfs.core.windows.net/carsales`
    """).display()

# COMMAND ----------

df_src = spark.sql("""
    select distinct(Dealer_ID), DealerName 
    from parquet.`abfss://silver@carsdataadls.dfs.core.windows.net/carsales`
    """)

# COMMAND ----------



# COMMAND ----------

if spark.catalog.tableExists("car_catalog.gold.dim_dealer"):
    df_sink = spark.sql(
        """
        select dim_dealer_key, Dealer_ID, DealerName
        from car_catalog.gold.dim_dealer
        """)

else:
    df_sink = spark.sql(
        """
        select 1 as dim_dealer_key, Dealer_ID, DealerName
        from parquet.`abfss://silver@carsdataadls.dfs.core.windows.net/carsales`
        where 1=0
        """)

# COMMAND ----------

display(df_sink)

# COMMAND ----------



# COMMAND ----------

df_filter = df_src.join(df_sink, df_src.Dealer_ID == df_sink.Dealer_ID, "left") \
        .select(df_src.Dealer_ID, df_src.DealerName, df_sink.dim_dealer_key)

# COMMAND ----------

df_filter.display()

# COMMAND ----------



# COMMAND ----------

df_filter_old = df_filter.filter(df_filter.dim_dealer_key.isNotNull())
df_filter_new = df_filter.filter(df_filter.dim_dealer_key.isNull())

# COMMAND ----------



# COMMAND ----------

if incremental_flag == '0':
    max_value = 1
else:
    max_df_value = spark.sql("select max(dim_dealer_key) from car_catalog.gold.dim_dealer")
    max_value = max_df_value.collect()[0][0]+1 

# COMMAND ----------



# COMMAND ----------

df_filter_new = df_filter_new.withColumn("dim_dealer_key", max_value + monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------



# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------



# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("car_catalog.gold.dim_dealer"):
    delta_tbl = DeltaTable.forPath(spark, "abfss://gold@carsdataadls.dfs.core.windows.net/dim_dealer")

    delta_tbl.alias("trg").merge(df_final.alias("src"), "trg.dim_dealer_key = src.dim_dealer_key") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
else:
    df_final.write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", "abfss://gold@carsdataadls.dfs.core.windows.net/dim_dealer") \
        .saveAsTable("car_catalog.gold.dim_dealer")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from car_catalog.gold.dim_dealer

# COMMAND ----------

