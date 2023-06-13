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
# MAGIC ## Hospitalizations
# MAGIC
# MAGIC Basic Exploratory Data Analysis 

# COMMAND ----------

# DBTITLE 1,Basic get dataframe - Read the Silver Vaccinations table
import pyspark.sql.functions as F

hosp_df = spark.table('hospitalizations')

display(hosp_df)

# COMMAND ----------

# DBTITLE 1,Get pandas & bamboo
import pandas as pd
import bamboolib as bam

hosp_pdf = hosp_df.toPandas()

hosp_pdf

# COMMAND ----------

import pandas as pd; import numpy as np

# Step: Manipulate strings of 'indicator' via Find ' ' and Replace with '_'
hosp_pdf["indicator"] = hosp_pdf["indicator"].str.replace(' ', '_', regex=False)

# Step: Pivot dataframe from long to wide format using the variable column 'indicator' and the value column 'value'
hosp_wide_pdf = hosp_pdf.set_index(['entity', 'iso_code', 'date', 'indicator'])['value'].unstack(-1).reset_index()
hosp_wide_pdf.columns.name = ''



# COMMAND ----------

# DBTITLE 1,Code copied from bamboolib above
import pandas as pd; import numpy as np

# Step: Manipulate strings of 'indicator' via Find ' ' and Replace with '_'
hosp_pdf["indicator"] = hosp_pdf["indicator"].str.replace(' ', '_', regex=False)

# Step: Pivot dataframe from long to wide format using the variable column 'indicator' and the value column 'value'
hosp_wide_pdf = hosp_pdf.set_index(['entity', 'iso_code', 'date', 'indicator'])['value'].unstack(-1).reset_index()
hosp_wide_pdf.columns.name = ''

# Step: Rename column
hosp_wide_pdf = hosp_wide_pdf.rename(columns={'entity': 'country'})


# COMMAND ----------

display(hosp_wide_pdf)

# COMMAND ----------

hosp_wide_df = spark.createDataFrame(hosp_wide_pdf)
hosp_wide_df.write.saveAsTable(f'{db_name}.hospitalizations_wide')
