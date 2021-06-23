-- Databricks notebook source
-- MAGIC %md # CCU002_02-D05-inclusion_exclusion
-- MAGIC  
-- MAGIC **Description** This notebook runs through the inclusion/exclusion criteria for the skinny cohort after QA.
-- MAGIC 
-- MAGIC **Author(s)** Jenny Cooper, Samantha Ip

-- COMMAND ----------

-- MAGIC %md
-- MAGIC 
-- MAGIC ## Set up

-- COMMAND ----------

--Run this only if updating tables

--DROP VIEW IF EXISTS global_temp.patientinclusion;
--DROP VIEW IF EXISTS global_temp.practicesover1;
--DROP VIEW IF EXISTS global_temp.patients_died;
--DROP VIEW IF EXISTS global_temp.positive_test;
--DROP VIEW IF EXISTS global_temp.nhs_icd_exclude;
--DROP VIEW IF EXISTS global_temp.nhs_snomed_exclude;


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Vaccine parameters

-- COMMAND ----------

-- MAGIC %py
-- MAGIC #Dataset parameters
-- MAGIC gdppr_data = 'dars_nic_391419_j3w9t_collab.ccu002_vac_gdppr_dars_nic_391419_j3w9t'
-- MAGIC skinny_data = 'dars_nic_391419_j3w9t_collab.ccu002_vac_skinny_patient'
-- MAGIC death_data = 'dars_nic_391419_j3w9t_collab.ccu002_vac_deaths_dars_nic_391419_j3w9t'
-- MAGIC sgss_data = 'dars_nic_391419_j3w9t_collab.ccu002_vac_sgss_dars_nic_391419_j3w9t'
-- MAGIC 
-- MAGIC #Other data inputs
-- MAGIC conflicting_quality_assurance = 'dars_nic_391419_j3w9t_collab.ccu002_vac_conflictingpatients'
-- MAGIC 
-- MAGIC #Date parameters
-- MAGIC index_date = '2020-12-08'
-- MAGIC 
-- MAGIC #Final table name
-- MAGIC final_table_1 = 'ccu002_vac_included_patients'

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ##  QA on skinny table
-- MAGIC make skinny_withQA

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(F"""REFRESH TABLE {skinny_data}""")

-- COMMAND ----------

-- MAGIC %py
-- MAGIC display(spark.sql(F"""select MAX(DATE_OF_DEATH)
-- MAGIC from {skinny_data}"""))

-- COMMAND ----------

-- MAGIC %py
-- MAGIC from pyspark.sql.functions import countDistinct, year, dayofmonth, from_unixtime, month, unix_timestamp, to_timestamp, date_format, col, datediff, to_date, lit, months_between
-- MAGIC import pandas as pd
-- MAGIC import numpy as np
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC import datetime
-- MAGIC from pyspark.sql.functions import *
-- MAGIC import pyspark.sql.functions as f
-- MAGIC from pyspark.sql import Window
-- MAGIC import io
-- MAGIC from functools import reduce
-- MAGIC from pyspark.sql.types import StringType

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC spark.sql(f"""create or replace global temp view skinny as
-- MAGIC SELECT *
-- MAGIC FROM {skinny_data}""")

-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Run QA nb

-- COMMAND ----------

---%md %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU002_vacc/Data_Curation/in_progress/CCU002-D03-quality_assurance_parameters_new"


-- COMMAND ----------

-- MAGIC %md
-- MAGIC ### Anti-join QA-excluded IDs to skinny table -- gives skinny_withQA

-- COMMAND ----------

-- antijoin QA table to skinny table to remove conflicting patients identified from QA

create or replace global temp view skinny_withQA as

SELECT t1.*
FROM global_temp.skinny t1
LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_vac_conflictingpatients t2 ---those who didnt meet QA criteria, from the previous notebook
ON t1.nhs_number_deid = t2.nhs_number_deid
WHERE t2.nhs_number_deid IS NULL


-- COMMAND ----------

-- MAGIC %md 
-- MAGIC ### Eligibility criteria

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # inspect: individuals with null GP practice entries (GDPPR) 
-- MAGIC display(spark.sql(F"""select NHS_NUMBER_DEID, PRACTICE, record_date from {gdppr_data}
-- MAGIC where practice is null"""))

-- COMMAND ----------

-- MAGIC %py
-- MAGIC #DATE_OF_DEATH >= '2020-12-08' -- selects everyone who has not died before 2020-12-08
-- MAGIC spark.sql(F"""create or replace global temp view skinny_withQA as 
-- MAGIC SELECT *
-- MAGIC FROM global_temp.skinny_withQA
-- MAGIC where (DATE_OF_DEATH >= '{index_date}') or (DATE_OF_DEATH is null)""") 

-- COMMAND ----------

-- DBTITLE 1,Creating Temporary Tables for the inclusion and exclusion criteria
-- MAGIC %py
-- MAGIC #People to include:
-- MAGIC #Known sex and age 18 and over -- people who have not died before 8th Dec 2020 and who have SEX==1/2 and who are over 18
-- MAGIC spark.sql(F"""create or replace global temp view patientinclusion AS 
-- MAGIC 
-- MAGIC SELECT *
-- MAGIC FROM global_temp.skinny_withQA 
-- MAGIC WHERE nhs_number_deid is not null 
-- MAGIC AND AGE_AT_COHORT_START >=18 
-- MAGIC AND (SEX =1 OR SEX=2)""")

-- COMMAND ----------

-- MAGIC %py
-- MAGIC # Excluding those with a positive test prior to 1st Jan 2020 for vaccine work
-- MAGIC spark.sql(F"""create or replace global temp view positive_test AS   
-- MAGIC 
-- MAGIC SELECT distinct(PERSON_ID_DEID) as nhs_number_deid --same nhs identifer name needed to union
-- MAGIC FROM {sgss_data}
-- MAGIC WHERE Lab_Report_date <'2020-01-01' AND PERSON_ID_DEID is not null""")

-- COMMAND ----------

---anti-join inclusion population with exclusion NHS numbers
create or replace global temp view penultimate AS   

select 
NHS_NUMBER_DEID, SEX, CATEGORISED_ETHNICITY, ETHNIC, DATE_OF_BIRTH, DATE_OF_DEATH, AGE_AT_COHORT_START  --keep here if want to specify specific variables, otherwise not needed.
FROM
(
SELECT t1.*
FROM global_temp.patientinclusion t1
LEFT JOIN  

(
SELECT * FROM global_temp.positive_test --positive COVID test
) t2 ---snomed for cardiovascular conditions
ON t1.nhs_number_deid = t2.nhs_number_deid

WHERE t2.nhs_number_deid IS NULL)

--Gives a list of nhs numbers to include in the study

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC spark.sql(F"""DROP TABLE IF EXISTS dars_nic_391419_j3w9t_collab.{final_table_1}""")

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(F"""CREATE TABLE dars_nic_391419_j3w9t_collab.{final_table_1} AS 
-- MAGIC select *
-- MAGIC from global_temp.penultimate age
-- MAGIC """) 
