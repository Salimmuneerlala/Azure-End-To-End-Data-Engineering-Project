# Databricks notebook source


# COMMAND ----------

df_silver = spark.sql("""
    select * 
    from parquet.`abfss://silver@carsdataadls.dfs.core.windows.net/carsales`
    """)


# COMMAND ----------

df_silver.display()

# COMMAND ----------



# COMMAND ----------

df_branch = spark.sql("select * from car_catalog.gold.dim_branch")

df_dealer = spark.sql("select * from car_catalog.gold.dim_dealer")

df_model = spark.sql("select * from car_catalog.gold.dim_model")

df_date = spark.sql("select * from car_catalog.gold.dim_date")

# COMMAND ----------



# COMMAND ----------

df_fact = df_silver.join(df_branch, df_silver.Branch_ID == df_branch.Branch_ID, 'left') \
        .join(df_dealer, df_silver.Dealer_ID == df_dealer.Dealer_ID, 'left') \
        .join(df_model, df_silver.Model_ID == df_model.Model_ID, 'left') \
        .join(df_date, df_silver.Date_ID == df_date.Date_ID, 'left') \
        .select(df_silver.Revenue, df_silver.Units_Sold, df_silver.RevPerUnit,df_branch.dim_branch_key, df_dealer.dim_dealer_key, df_model.dim_model_key, df_date.dim_date_key)

# COMMAND ----------

df_fact.display()

# COMMAND ----------



# COMMAND ----------

from delta.tables import DeltaTable

# COMMAND ----------

if spark.catalog.tableExists("car_catalog.gold.factsales"):
    deltatbl = DeltaTable.forName(spark, "car_catalog.gold.factsales")

    deltatbl.alias('trg').merge(df_fact.alias("src"), 'trg.dim_date_key == src.dim_date_key and trg.dim_branch_key == src.dim_branch_key and trg.dim_dealer_key == src.dim_dealer_key and trg.dim_model_key == src.dim_model_key') \
            .whenMatchedUpdateAll() \
            .whenNotMatchedInsertAll() \
            .execute()
else:
    df_fact.write \
        .format("delta") \
        .mode("Overwrite") \
        .option("path", "abfss://gold@carsdataadls.dfs.core.windows.net/factsales") \
        .saveAsTable("car_catalog.gold.factsales")

# COMMAND ----------

# MAGIC %sql
# MAGIC select * from car_catalog.gold.factsales

# COMMAND ----------

