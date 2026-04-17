# Databricks notebook source
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

dbutils.widgets.text("incremental_flag", "0")

# COMMAND ----------

incremental_flag = dbutils.widgets.get("incremental_flag")
print(incremental_flag)

# COMMAND ----------



# COMMAND ----------

df_src = spark.sql("""
    select distinct(Model_ID) as Model_ID, model_category 
    from parquet.`abfss://silver@carsdataadls.dfs.core.windows.net/carsales`
    """)

# COMMAND ----------

df_src.display()

# COMMAND ----------



# COMMAND ----------

if spark.catalog.tableExists("car_catalog.gold.dim_model"):
    df_sink = spark.sql(
        """
        select dim_model_key, Model_ID, model_category
        from car_catalog.gold.dim_model
        """
    )

else:
    df_sink = spark.sql(
        """
        select 1 as dim_model_key, Model_ID, model_category
        from parquet.`abfss://silver@carsdataadls.dfs.core.windows.net/carsales`
        where 1=0
        """
    )

# COMMAND ----------

display(df_sink)

# COMMAND ----------



# COMMAND ----------

df_filter = df_src.join(df_sink, df_src.Model_ID == df_sink.Model_ID, "left") \
        .select(df_src.Model_ID, df_src.model_category, df_sink.dim_model_key)

# COMMAND ----------

df_filter.show()

# COMMAND ----------

df_filter_old = df_filter.filter(col("dim_model_key").isNotNull())

# COMMAND ----------

df_filter_old.display()

# COMMAND ----------

df_filter_new = df_filter.filter(col("dim_model_key").isNull()).select(df_filter.Model_ID, df_filter.model_category)

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------



# COMMAND ----------

if incremental_flag == '0':
    max_value = 1
else:
    max_df_value = spark.sql("select max(dim_model_key) from car_catalog.gold.dim_model")
    max_value = max_df_value.collect()[0][0]+1

# COMMAND ----------



# COMMAND ----------

df_filter_new = df_filter_new.withColumn("dim_model_key", max_value + monotonically_increasing_id())

# COMMAND ----------

df_filter_new.display()

# COMMAND ----------



# COMMAND ----------

df_final = df_filter_new.union(df_filter_old)

# COMMAND ----------



# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("car_catalog.gold.dim_model"):
    delta_tbl = DeltaTable.forPath(spark, "abfss://gold@carsdataadls.dfs.core.windows.net/dim_model")

    delta_tbl.alias("trg").merge(df_final.alias("src"), "trg.dim_model_key = src.dim_model_key") \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
else:
    df_final.write \
        .format("delta") \
        .mode("overwrite") \
        .option("path", "abfss://gold@carsdataadls.dfs.core.windows.net/dim_model") \
        .saveAsTable("car_catalog.gold.dim_model")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from car_catalog.gold.dim_model

# COMMAND ----------

