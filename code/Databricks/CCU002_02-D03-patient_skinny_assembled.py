# Databricks notebook source
# MAGIC %md # CCU002_02-D03-patient_skinny_assembled
# MAGIC 
# MAGIC **Description** Making a single record for each patient in primary and secondary care. this uses the output from **CCU002_02_D02_patient_skinny_unassembled*
# MAGIC  
# MAGIC **Author(s)** Sam Hollings, Jenny Cooper

# COMMAND ----------

# MAGIC %md ## Making a single record for each patient in primary and secondary care

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook will make a single record for each patient with the core facts about that patient, reconciled across the main datasets (primary and secondary care)
# MAGIC 
# MAGIC |Column | Content|
# MAGIC |----------------|--------------------|
# MAGIC |NHS_NUMBER_DEID | Patient NHS Number |
# MAGIC |ETHNIC | Patient Ethnicity |
# MAGIC |SEX | Patient Sex |
# MAGIC |DATE_OF_BIRTH | Patient Date of Birth (month level) |
# MAGIC |DATE_OF_DEATH | Patient Date of Death (month level) |
# MAGIC |record_id | The id of the record from which the data was drawn |
# MAGIC |dataset | The dataset from which the record comes from |
# MAGIC |primary | Whether the record refers to primary of secondary care |

# COMMAND ----------

# MAGIC %run Workspaces/dars_nic_391419_j3w9t_collab/CCU002_vacc/Data_Curation/in_progress/CCU002_vacc_functions/wrang000_functions

# COMMAND ----------

# MAGIC %md
# MAGIC ### Vaccine Parameters

# COMMAND ----------

#Dataset Parameters (needed for the GDPPR presence lookup)
gdppr_data = 'dars_nic_391419_j3w9t_collab.ccu002_vac_gdppr_dars_nic_391419_j3w9t'

#Date Parameters
index_date = '2020-12-08'

#Data output name
skinny_table_name = 'ccu002_vac_skinny_patient' 
#ccu002_inf_skinny_patient #For infection study

# COMMAND ----------

# MAGIC %md
# MAGIC ### Infection Parameters

# COMMAND ----------

#Dataset Parameters (needed for the GDPPR presence lookup)
gdppr_data = 'dars_nic_391419_j3w9t_collab.ccu002_vac_gdppr_dars_nic_391419_j3w9t'

#Date Parameters
index_date = '2020-01-01' #For infection study

#Data output name
skinny_table_name = 'ccu002_inf_skinny_patient' 
#ccu002_inf_skinny_patient #For infection study

# COMMAND ----------

# MAGIC %md ### Run dependency if not exists

# COMMAND ----------

dbutils.notebook.run("/Workspaces/dars_nic_391419_j3w9t_collab/CCU002_vacc/Data_Curation/in_progress/CCU002_vacc-D02-curr301_patient_skinny_unassembled",3000)

# COMMAND ----------

#Mark records before or after index date

spark.sql(
f"""CREATE OR REPLACE GLOBAL TEMP VIEW patient_skinny_unassembled_after_index as
SELECT *, 
CASE WHEN RECORD_DATE >= '{index_date}' THEN True ELSE False END as after_index
FROM global_temp.curr301_patient_skinny_unassembled""")

# COMMAND ----------

# MAGIC %md ## Handle the multiple versions of the truth

# COMMAND ----------

# MAGIC %md Choose the appropriate values so that we have one version of the truth for each Patient.
# MAGIC 
# MAGIC Currently, that simply involves picking the **most recent record**:
# MAGIC - choosing a populated field first
# MAGIC - choosing from primary care first if possible 
# MAGIC -  and then only choosing from secondary or null values if no other available).

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW patient_fields_ranked_pre_index AS
# MAGIC SELECT * --NHS_NUMBER_DEID, DATE_OF_DEATH
# MAGIC FROM (
# MAGIC       SELECT *, 
# MAGIC                 row_number() OVER (PARTITION BY NHS_NUMBER_DEID 
# MAGIC                                     ORDER BY date_of_birth_null asc, primary desc, RECORD_DATE DESC) as birth_recency_rank,
# MAGIC                 row_number() OVER (PARTITION BY NHS_NUMBER_DEID 
# MAGIC                                     ORDER BY sex_null asc, primary desc, RECORD_DATE DESC) as sex_recency_rank,
# MAGIC                 row_number() OVER (PARTITION BY NHS_NUMBER_DEID 
# MAGIC                                     ORDER BY ethnic_null asc, primary desc, RECORD_DATE DESC) as ethnic_recency_rank
# MAGIC                               
# MAGIC       FROM global_temp.patient_skinny_unassembled_after_index
# MAGIC       WHERE (after_index = False)-- or death_table = 1) --JC added this we want only records before index date but all deaths in case recorded after index date
# MAGIC       and dataset  <> "primary_SNOMED" -- <- this has the GDPPR SNOMED Ethnicity - we will just use the normal ones, which are in dataset = primary
# MAGIC       ) 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Get deaths after index as well as before
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW patient_fields_ranked_death AS
# MAGIC SELECT *
# MAGIC FROM (
# MAGIC       SELECT *, 
# MAGIC                 row_number() OVER (PARTITION BY NHS_NUMBER_DEID 
# MAGIC                                     ORDER BY death_table desc,  RECORD_DATE DESC) as death_recency_rank
# MAGIC                               
# MAGIC       FROM global_temp.patient_skinny_unassembled_after_index
# MAGIC       ) 

# COMMAND ----------

# MAGIC %md You can check the Ranking of the records below - it will keep only those with `recency_rank = 1` later

# COMMAND ----------

# MAGIC %md ### Assemble the columns together to make skinny record

# COMMAND ----------

# MAGIC %md GDPPR Presence Lookup to only keep those in GDPPR

# COMMAND ----------

# MAGIC %python
# MAGIC #Add a presence lookup
# MAGIC #(Could alternatively use presence code in previous notebook)
# MAGIC 
# MAGIC spark.sql(f"""CREATE OR REPLACE GLOBAL TEMP VIEW gdppr_presence_lookup AS
# MAGIC 
# MAGIC SELECT distinct(NHS_NUMBER_DEID)
# MAGIC FROM {gdppr_data}""")

# COMMAND ----------

# MAGIC %md Ethnicity lookup

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ethnicity_lookup AS
# MAGIC SELECT *, 
# MAGIC       CASE WHEN ETHNICITY_CODE IN ('1','2','3','N','M','P') THEN "Black or Black British"
# MAGIC            WHEN ETHNICITY_CODE IN ('0','A','B','C') THEN "White"
# MAGIC            WHEN ETHNICITY_CODE IN ('4','5','6','L','K','J','H') THEN "Asian or Asian British"
# MAGIC            WHEN ETHNICITY_CODE IN ('7','8','W','T','S','R') THEN "Other Ethnic Groups"
# MAGIC            WHEN ETHNICITY_CODE IN ('D','E','F','G') THEN "Mixed"
# MAGIC            WHEN ETHNICITY_CODE IN ('9','Z','X') THEN "Unknown"
# MAGIC            ELSE 'Unknown' END as ETHNIC_GROUP  
# MAGIC FROM (
# MAGIC   SELECT ETHNICITY_CODE, ETHNICITY_DESCRIPTION FROM dss_corporate.hesf_ethnicity
# MAGIC   UNION ALL
# MAGIC   SELECT Value as ETHNICITY_CODE, Label as ETHNICITY_DESCRIPTION FROM dss_corporate.gdppr_ethnicity WHERE Value not in (SELECT ETHNICITY_CODE FROM FROM dss_corporate.hesf_ethnicity))

# COMMAND ----------

# MAGIC %md assemble it all together:

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW curr302_patient_skinny_record AS
# MAGIC SELECT pat.NHS_NUMBER_DEID,
# MAGIC       eth.ETHNIC,
# MAGIC       eth_group.ETHNIC_GROUP as CATEGORISED_ETHNICITY,
# MAGIC       sex.SEX,
# MAGIC       dob.DATE_OF_BIRTH,
# MAGIC       dod.DATE_OF_DEATH
# MAGIC FROM (SELECT DISTINCT NHS_NUMBER_DEID FROM global_temp.patient_skinny_unassembled_after_index) pat 
# MAGIC         INNER JOIN (SELECT NHS_NUMBER_DEID, ETHNIC FROM global_temp.patient_fields_ranked_pre_index WHERE ethnic_recency_rank = 1) eth ON pat.NHS_NUMBER_DEID = eth.NHS_NUMBER_DEID
# MAGIC         INNER JOIN (SELECT NHS_NUMBER_DEID, SEX FROM global_temp.patient_fields_ranked_pre_index WHERE sex_recency_rank = 1) sex ON pat.NHS_NUMBER_DEID = sex.NHS_NUMBER_DEID
# MAGIC         INNER JOIN (SELECT NHS_NUMBER_DEID, DATE_OF_BIRTH FROM global_temp.patient_fields_ranked_pre_index WHERE birth_recency_rank = 1) dob ON pat.NHS_NUMBER_DEID = dob.NHS_NUMBER_DEID
# MAGIC         LEFT JOIN (SELECT NHS_NUMBER_DEID, DATE_OF_DEATH FROM global_temp.patient_fields_ranked_death WHERE death_recency_rank = 1 and death_table = 1) dod ON pat.NHS_NUMBER_DEID = dod.NHS_NUMBER_DEID --deaths come just from death_table in this case
# MAGIC         INNER JOIN global_temp.gdppr_presence_lookup pres ON pat.NHS_NUMBER_DEID = pres.NHS_NUMBER_DEID --added to ensure in GDPPR only as inner join, move this to the top for more efficiency
# MAGIC         LEFT JOIN global_temp.ethnicity_lookup eth_group ON eth.ETHNIC = eth_group.ETHNICITY_CODE
# MAGIC 
# MAGIC         

# COMMAND ----------

# Add age at cohort start

import pyspark.sql.functions as f
(spark.table("global_temp.curr302_patient_skinny_record")
      .selectExpr("*", 
                  f"floor(float(months_between('{index_date}', DATE_OF_BIRTH))/12.0) as AGE_AT_COHORT_START")
      .createOrReplaceGlobalTempView("curr302_patient_skinny_record_age"))

# COMMAND ----------

spark.sql(F"""DROP TABLE IF EXISTS dars_nic_391419_j3w9t_collab.{skinny_table_name}""")

# COMMAND ----------

create_table(skinny_table_name, select_sql_script=f"SELECT * FROM global_temp.curr302_patient_skinny_record_age") 

#skinny_table_name is defined at the top for both vaccine/infection
#takes 1.5 hours so be patient
