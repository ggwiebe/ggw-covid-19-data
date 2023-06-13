-- Databricks notebook source
-- MAGIC %md ## Delta Live Table - Customer Change Data Capture Demo. 
-- MAGIC   
-- MAGIC ![DLT Process Flow]()
-- MAGIC

-- COMMAND ----------

-- MAGIC %md ### 0. Raw - Access Stream  
-- MAGIC   
-- MAGIC **Common Storage Format:** CloudFiles, Kafka, (non-DLT) Delta tables, etc.  
-- MAGIC
-- MAGIC Here is an example against a Delta table:
-- MAGIC ```
-- MAGIC -- RAW STREAM - View for new customers Delta Table example
-- MAGIC CREATE INCREMENTAL LIVE VIEW customer_v
-- MAGIC COMMENT "View built against raw, streaming Customer data source."
-- MAGIC AS SELECT * FROM STREAM(retail.customer)
-- MAGIC ```  
-- MAGIC Below we use the streaming Autoloader CloudFiles utility.

-- COMMAND ----------

-- MAGIC %md ### i. REFERENCE - Define Views to outside reference data
-- MAGIC   
-- MAGIC

-- COMMAND ----------

-- REFERENCE - View for Vaccinations Locations reference table 
CREATE LIVE VIEW vaccinations_locations_v
COMMENT "View built against Vaccinations_Locations reference data."
AS SELECT *
     FROM ggw_covid_new.vaccinations_locations
;

-- COMMAND ----------

-- REFERENCE - View for Hospitalizations Locations reference table 
CREATE LIVE VIEW hospitalizations_locations_v
COMMENT "View built against Hospitalizations_Locations reference data."
AS SELECT *
     FROM ggw_covid_new.hospitalizations_locations
;

-- COMMAND ----------

-- MAGIC %md ### 1. BRONZE - Land Raw Data and standardize types
-- MAGIC   
-- MAGIC **Common Storage Format:** Delta  
-- MAGIC **Data Types:** Loose schema enforcement (avoid losing ingested data)

-- COMMAND ----------

-- BRONZE - CloudFiles AutoLoader reads raw streaming files for "new" Hosptization Location records
CREATE OR REFRESH STREAMING LIVE TABLE vaccinations_bronze
TBLPROPERTIES ("quality" = "bronze")
COMMENT "Land Raw Data: Vaccinations"
AS 
SELECT *,
       current_timestamp() AS input_timestamp,
       input_file_name() AS input_file_name,
       "DLT:IngestCovid" AS input_routine
  FROM cloud_files('abfss://ggwstdlrscont1@ggwstdlrs.dfs.core.windows.net/ggw_covid/data/in/vaccination*', 
                   'csv', 
                   map("header", "true", "cloudFiles.inferColumnTypes", "true")
                  ) v
;

-- COMMAND ----------

-- MAGIC %md ### 2. SILVER - Enrich, Clean and Standardize
-- MAGIC   
-- MAGIC **Common Storage Format:** Delta  
-- MAGIC **Data Types:** Business Fidelity & check Nulls

-- COMMAND ----------

-- SILVER - CloudFiles AutoLoader reads raw streaming files for "new" Hosptization Location records
CREATE OR REFRESH STREAMING LIVE TABLE vaccinations_silver (
  CONSTRAINT valid_country      EXPECT (country_code IS NOT NULL) ON VIOLATION DROP ROW,
  CONSTRAINT select_vaccines    EXPECT (
       array_size(array_except(array( if(instr(l.vaccines,'Johnson')>0,'Johnson',null),
                                      if(instr(l.vaccines,'Pfizer') >0,'Pfizer' ,null),
                                      if(instr(l.vaccines,'Moderna')>0,'Moderna',null),
                                      if(instr(l.vaccines,'Oxford') >0,'Oxford' ,null) ), array(null))) > 0 ) ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "silver")
COMMENT "Clean Data: Vaccinations"
AS 
SELECT v.location as country,
       v.* EXCEPT (iso_code, location),
       l.iso_code as country_code,
       l.last_observation_date,
       l.vaccines
  FROM STREAM(LIVE.vaccinations_bronze) v
  LEFT JOIN LIVE.vaccinations_locations_v l
       ON v.iso_code = l.iso_code
;

-- COMMAND ----------

-- MAGIC %md ### 2. b) SILVER - Quarantine!!!
-- MAGIC   
-- MAGIC **USE CONSTRAINTS** reverse above constraints  
-- MAGIC **Data Types:** Business Fidelity & check Nulls

-- COMMAND ----------

-- SILVER - CloudFiles AutoLoader reads raw streaming files for "new" Hosptization Location records
CREATE OR REFRESH STREAMING LIVE TABLE vaccinations_quarantine (
  CONSTRAINT invalid_country_or_vacines  EXPECT (
       (country_code IS NULL) OR
       ( array_size(array_except(array( if(instr(l.vaccines,'Johnson')>0,'Johnson',null),
                                        if(instr(l.vaccines,'Pfizer') >0,'Pfizer' ,null),
                                        if(instr(l.vaccines,'Moderna')>0,'Moderna',null),
                                        if(instr(l.vaccines,'Oxford') >0,'Oxford' ,null) ), array(null))) = 0 )
  ) ON VIOLATION DROP ROW
)
TBLPROPERTIES ("quality" = "quarantine")
COMMENT "Store Constraint Violations"
AS 
SELECT v.location as country,
       v.* EXCEPT (iso_code, location),
       l.iso_code as country_code,
       l.last_observation_date,
       l.vaccines
  FROM STREAM(LIVE.vaccinations_bronze) v
  LEFT JOIN LIVE.vaccinations_locations_v l
       ON v.iso_code = l.iso_code
;
