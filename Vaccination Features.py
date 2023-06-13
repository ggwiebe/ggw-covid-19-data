# Databricks notebook source
# MAGIC %md
# MAGIC ## Initial Environment Setup  
# MAGIC   

# COMMAND ----------

dbutils.widgets.removeAll()
dbutils.widgets.text("db_name","ggw_covid")

# COMMAND ----------

db_name = dbutils.widgets.get("db_name")
spark.sql(f'USE CATALOG hive_metastore')
spark.sql(f'USE {db_name}')

print("Setting up and using database: {}".format(db_name))

# COMMAND ----------

# MAGIC %md
# MAGIC ## Service Feature Table Setup
# MAGIC
# MAGIC This sets up the feature store table `service_features`, which again is presumed to have been created earlier by data engineers or other teams.

# COMMAND ----------

# DBTITLE 1,Basic get dataframe - Read the Silver Vaccinations table
import pyspark.sql.functions as F

vaccinations_df = spark.table('vaccinations_silver')

# COMMAND ----------

# DBTITLE 1,Use FeatureStore API to create a special FeatureTable
from databricks.feature_store import FeatureStoreClient

fs = FeatureStoreClient()

vaccinations_features_table = fs.create_table(
  name='{}.vaccinations_features'.format(db_name),
  primary_keys=['country','date'],
  schema=vaccinations_df.schema,
  description='GGW Covid-19 Country Vaccinations Features'
)

# COMMAND ----------

# MAGIC %md
# MAGIC Note: If you need to re-create the `vaccinations_features` table for any reason, the feature table has to be deleted manually from the Feature Store tab UI before `create_table` can run again.

# COMMAND ----------

fs.write_table("{}.vaccinations_features".format(db_name), vaccinations_df)

# COMMAND ----------

# MAGIC %md
# MAGIC At this point you should have:
# MAGIC
# MAGIC - a database table at `ggw_covid.vaccinations_features`, visible in the Data tab, which contains country vaccination data
# MAGIC - a feature table in the Feature Store tab called `ggw_covid.vaccinatinos_features` with country vaccination-related info -- try it!
# MAGIC
