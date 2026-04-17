# Databricks notebook source
# MAGIC %md
# MAGIC ### In this we'll Create date Dimension model

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# MAGIC %md
# MAGIC ### 1. Create variable for trancking, whether this is the first run or not

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



# COMMAND ----------

# MAGIC %md
# MAGIC ### 2. Select columns related to Date from big table

# COMMAND ----------

df_src = spark.sql("""
    select distinct(Date_ID) as Date_ID 
    from parquet.`abfss://silver@carsdataadls.dfs.core.windows.net/carsales`
    """)

# COMMAND ----------

df_src.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 3. Check if dimension date table exists or not in sink (i.e. already some data exists or not)
# MAGIC - If exists then get all the data of that table
# MAGIC - If not exists, simply get the table column names, which will be helpful in performing join later

# COMMAND ----------

if spark.catalog.tableExists("car_catalog.gold.dim_date"):
    df_sink = spark.sql(
        """
        select dim_date_key, Date_ID
        from car_catalog.gold.dim_date
        """
    )

else:
    df_sink = spark.sql(
        """
        select 1 as dim_date_key, Date_ID
        from parquet.`abfss://silver@carsdataadls.dfs.core.windows.net/carsales`
        where 1=0
        """
    )

# COMMAND ----------

display(df_sink)

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 4. Perform left join of incoming data (source) and existing data

# COMMAND ----------

df_filter = df_src.join(df_sink, df_src.Date_ID == df_sink.Date_ID, "left") \
        .select(df_src.Date_ID, df_sink.dim_date_key)

# COMMAND ----------

df_filter.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ### 5. Filter out data, to check which data is new and which data has been updated in source (this can be done by checking if sarogate key is null that means it's new data and if it's not null that means it's new data)

# COMMAND ----------

df_filter_old = df_filter.filter(col("dim_date_key").isNotNull())

# COMMAND ----------

df_filter_old.display()

# COMMAND ----------

df_filter_new = df_filter.filter(col("dim_date_key").isNull()).select(df_filter.Date_ID)

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC # 6. Surrogate logic
# MAGIC ### In this we'll check 
# MAGIC - if incremtal_flag is 0, then that means it's first run and we can start surrogate key from 0
# MAGIC - if incremtal_flag is not 0 then that means it's incremental run and we can start surrogate key from max of surrogate key column from existing branch table  
# MAGIC

# COMMAND ----------

if incremental_flag == '0':
    max_value = 1
else:
    max_df_value = spark.sql("select max(dim_date_key) from car_catalog.gold.dim_date")
    max_value = max_df_value.collect()[0][0]+1 

# COMMAND ----------



# COMMAND ----------

# MAGIC %md 
# MAGIC ### 7. After this we'll start assigning surrogate keys to new records

# COMMAND ----------

df_filter_new = df_filter_new.withColumn("dim_date_key", max_value + monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 8. Now start merging existing data and newly data

# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------

df_final.display()

# COMMAND ----------



# COMMAND ----------

# MAGIC %md
# MAGIC ### 9. Now import delta table, and 
# MAGIC - create delta table for existing branch data in sink and merge with new data
# MAGIC - perfrom scd type 1 i.e upsert

# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("car_catalog.gold.dim_date"):
    delta_tbl = DeltaTable.forPath(spark, "abfss://gold@carsdataadls.dfs.core.windows.net/dim_date")

    delta_tbl.alias("trg").merge(df_final.alias("src"), "trg.dim_date_key = src.dim_date_key") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
else:
    df_final.write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", "abfss://gold@carsdataadls.dfs.core.windows.net/dim_date") \
        .saveAsTable("car_catalog.gold.dim_date")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from car_catalog.gold.dim_date

# COMMAND ----------

