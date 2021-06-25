# Databricks notebook source
# MAGIC %md # CCU002_02-D20-table_S2
# MAGIC 
# MAGIC **Description** This notebook calculates event numbers for supplementary table 2.
# MAGIC 
# MAGIC **Author(s)** Venexia Walker

# COMMAND ----------

# MAGIC %md ## Define functions

# COMMAND ----------

# Define create table function by Sam Hollings
# Source: Workspaces/dars_nic_391419_j3w9t_collab/DATA_CURATION_wrang000_functions

def create_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', select_sql_script:str=None) -> None:
  """Will save to table from a global_temp view of the same name as the supplied table name (if no SQL script is supplied)
  Otherwise, can supply a SQL script and this will be used to make the table with the specificed name, in the specifcied database."""
  
  spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
  
  if select_sql_script is None:
    select_sql_script = f"SELECT * FROM global_temp.{table_name}"
  
  spark.sql(f"""CREATE TABLE {database_name}.{table_name} AS
                {select_sql_script}
             """)
  spark.sql(f"ALTER TABLE {database_name}.{table_name} OWNER TO {database_name}")
  
def drop_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', if_exists=True):
  if if_exists:
    IF_EXISTS = 'IF EXISTS'
  else: 
    IF_EXISTS = ''
  spark.sql(f"DROP TABLE {IF_EXISTS} {database_name}.{table_name}")

# COMMAND ----------

# MAGIC %md ## Make populations

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW venous_astrazeneca AS
# MAGIC SELECT *
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_vacc_cohort
# MAGIC WHERE diag1_Venous_event_date IS NOT NULL
# MAGIC AND diag1_Venous_event_date >= '2020-12-08'
# MAGIC AND diag1_Venous_event_date < '2021-03-19'
# MAGIC AND VACCINATION_DATE_FIRST IS NOT NULL
# MAGIC AND diag1_Venous_event_date >= VACCINATION_DATE_FIRST 
# MAGIC AND ((diag1_Venous_event_date <= death_date) OR (death_date IS NULL))
# MAGIC AND VACCINE_PRODUCT_FIRST='AstraZeneca'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW venous_pfizer AS
# MAGIC SELECT *
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_vacc_cohort
# MAGIC WHERE diag1_Venous_event_date IS NOT NULL
# MAGIC AND diag1_Venous_event_date >= '2020-12-08'
# MAGIC AND diag1_Venous_event_date < '2021-03-19'
# MAGIC AND VACCINATION_DATE_FIRST IS NOT NULL
# MAGIC AND diag1_Venous_event_date >= VACCINATION_DATE_FIRST 
# MAGIC AND ((diag1_Venous_event_date <= death_date) OR (death_date IS NULL))
# MAGIC AND VACCINE_PRODUCT_FIRST='Pfizer'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW arterial_astrazeneca AS
# MAGIC SELECT *
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_vacc_cohort
# MAGIC WHERE diag1_Arterial_event_date IS NOT NULL
# MAGIC AND diag1_Arterial_event_date >= '2020-12-08'
# MAGIC AND diag1_Arterial_event_date < '2021-03-19'
# MAGIC AND VACCINATION_DATE_FIRST IS NOT NULL
# MAGIC AND diag1_Arterial_event_date >= VACCINATION_DATE_FIRST 
# MAGIC AND ((diag1_Arterial_event_date <= death_date) OR (death_date IS NULL))
# MAGIC AND VACCINE_PRODUCT_FIRST='AstraZeneca'

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW arterial_pfizer AS
# MAGIC SELECT *
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_vacc_cohort
# MAGIC WHERE diag1_Arterial_event_date IS NOT NULL
# MAGIC AND diag1_Arterial_event_date >= '2020-12-08'
# MAGIC AND diag1_Arterial_event_date < '2021-03-19'
# MAGIC AND VACCINATION_DATE_FIRST IS NOT NULL
# MAGIC AND diag1_Arterial_event_date >= VACCINATION_DATE_FIRST 
# MAGIC AND ((diag1_Arterial_event_date <= death_date) OR (death_date IS NULL))
# MAGIC AND VACCINE_PRODUCT_FIRST='Pfizer'

# COMMAND ----------

# MAGIC %md ## Generate output for all populations

# COMMAND ----------

table = " AS category, COUNT(NHS_NUMBER_DEID) AS N, SUM(CASE WHEN AGE_AT_COHORT_START>=18 AND AGE_AT_COHORT_START<30 THEN 1 ELSE 0 END) AS Age_18_29, SUM(CASE WHEN AGE_AT_COHORT_START>=30 AND AGE_AT_COHORT_START<40 THEN 1 ELSE 0 END) AS Age_30_39, SUM(CASE WHEN AGE_AT_COHORT_START>=40 AND AGE_AT_COHORT_START<50 THEN 1 ELSE 0 END) AS Age_40_49, SUM(CASE WHEN AGE_AT_COHORT_START>=50 AND AGE_AT_COHORT_START<60 THEN 1 ELSE 0 END) AS Age_50_59, SUM(CASE WHEN AGE_AT_COHORT_START>=60 AND AGE_AT_COHORT_START<70 THEN 1 ELSE 0 END) AS Age_60_69, SUM(CASE WHEN AGE_AT_COHORT_START>=70 AND AGE_AT_COHORT_START<80 THEN 1 ELSE 0 END) AS Age_70_79, SUM(CASE WHEN AGE_AT_COHORT_START>=80 AND AGE_AT_COHORT_START<90 THEN 1 ELSE 0 END) AS Age_80_89, SUM(CASE WHEN AGE_AT_COHORT_START>=90 THEN 1 ELSE 0 END) AS Age_90plus, SUM(CASE WHEN SEX=1 THEN 1 ELSE 0 END) AS Sex_Male, SUM(CASE WHEN SEX=2 THEN 1 ELSE 0 END) AS Sex_Female, SUM(CASE WHEN CATEGORISED_ETHNICITY='Asian or Asian British' THEN 1 ELSE 0 END) AS Ethnicity_Asian, SUM(CASE WHEN CATEGORISED_ETHNICITY='Black or Black British' THEN 1 ELSE 0 END) AS Ethnicity_Black, SUM(CASE WHEN CATEGORISED_ETHNICITY='Mixed' THEN 1 ELSE 0 END) AS Ethnicity_Mixed, SUM(CASE WHEN CATEGORISED_ETHNICITY='Other Ethnic Groups' THEN 1 ELSE 0 END) AS Ethnicity_Other, SUM(CASE WHEN CATEGORISED_ETHNICITY='White' THEN 1 ELSE 0 END) AS Ethnicity_White, SUM(CASE WHEN CATEGORISED_ETHNICITY='Unknown' THEN 1 ELSE 0 END) AS Ethnicity_Unknown, SUM(CASE WHEN CATEGORISED_ETHNICITY='missing' THEN 1 ELSE 0 END) AS Ethnicity_Missing, SUM(CASE WHEN IMD='Deciles_1_2' THEN 1 ELSE 0 END) AS Deprivation_1_2, SUM(CASE WHEN IMD='Deciles_3_4' THEN 1 ELSE 0 END) AS Deprivation_3_4, SUM(CASE WHEN IMD='Deciles_5_6' THEN 1 ELSE 0 END) AS Deprivation_5_6, SUM(CASE WHEN IMD='Deciles_7_8' THEN 1 ELSE 0 END) AS Deprivation_7_8, SUM(CASE WHEN IMD='Deciles_9_10' THEN 1 ELSE 0 END) AS Deprivation_9_10, SUM(CASE WHEN (diag1_thrombocytopenia_date < VACCINATION_DATE_FIRST) AND (VACCINATION_DATE_FIRST IS NOT NULL) AND (diag1_thrombocytopenia_date IS NOT NULL) THEN 1 ELSE 0 END) AS Thrombocytopenia_Yes, COUNT(NHS_NUMBER_DEID) - SUM(CASE WHEN (diag1_thrombocytopenia_date < VACCINATION_DATE_FIRST) AND (VACCINATION_DATE_FIRST IS NOT NULL) AND (diag1_thrombocytopenia_date IS NOT NULL) THEN 1 ELSE 0 END) AS Thrombocytopenia_No, SUM(CASE WHEN EVER_THROMBOPHILIA=1 THEN 1 ELSE 0 END) AS Thrombophilia_Yes, COUNT(NHS_NUMBER_DEID) - SUM(CASE WHEN EVER_THROMBOPHILIA=1 THEN 1 ELSE 0 END) AS Thrombophilia_No, SUM(CASE WHEN (diag1_Venous_event_date < VACCINATION_DATE_FIRST) AND (diag1_Venous_event_date IS NOT NULL) AND (VACCINATION_DATE_FIRST IS NOT NULL) THEN 1 ELSE 0 END) AS Venous_event_Yes, COUNT(NHS_NUMBER_DEID) - SUM(CASE WHEN (diag1_Venous_event_date < VACCINATION_DATE_FIRST) AND (diag1_Venous_event_date IS NOT NULL) AND (VACCINATION_DATE_FIRST IS NOT NULL) THEN 1 ELSE 0 END) AS Venous_event_No, SUM(CASE WHEN COVID_infection=1 THEN 1 ELSE 0 END) AS COVID_infection_Yes, COUNT(NHS_NUMBER_DEID) - SUM(CASE WHEN COVID_infection=1 THEN 1 ELSE 0 END) AS COVID_infection_No, SUM(CASE WHEN COCP_MEDS=1 THEN 1 ELSE 0 END) AS OCP_Yes, COUNT(NHS_NUMBER_DEID) - SUM(CASE WHEN COCP_MEDS=1 THEN 1 ELSE 0 END) AS OCP_No, SUM(CASE WHEN HRT_MEDS=1 THEN 1 ELSE 0 END) AS HRT_Yes, COUNT(NHS_NUMBER_DEID) - SUM(CASE WHEN HRT_MEDS=1 THEN 1 ELSE 0 END) AS HRT_No, SUM(CASE WHEN ANTICOAG_MEDS=1 THEN 1 ELSE 0 END) AS Anticoagulant_Yes, COUNT(NHS_NUMBER_DEID) - SUM(CASE WHEN ANTICOAG_MEDS=1 THEN 1 ELSE 0 END) AS Anticoagulant_No, SUM(CASE WHEN ANTIPLATLET_MEDS=1 THEN 1 ELSE 0 END) AS Antiplatelet_Yes, COUNT(NHS_NUMBER_DEID) - SUM(CASE WHEN ANTIPLATLET_MEDS=1 THEN 1 ELSE 0 END) AS Antiplatelet_No, SUM(CASE WHEN EVER_ALL_STROKE=1 OR EVER_AMI=1 THEN 1 ELSE 0 END) AS MI_Stroke_Yes, COUNT(NHS_NUMBER_DEID) - SUM(CASE WHEN EVER_ALL_STROKE=1 OR EVER_AMI=1 THEN 1 ELSE 0 END) AS MI_Stroke_No, SUM(CASE WHEN EVER_DIAB_DIAG_OR_MEDS=1 THEN 1 ELSE 0 END) AS Diabetes_Yes, COUNT(NHS_NUMBER_DEID) - SUM(CASE WHEN EVER_DIAB_DIAG_OR_MEDS=1 THEN 1 ELSE 0 END) AS Diabetes_No FROM global_temp."

for data in ["venous_astrazeneca","venous_pfizer","arterial_astrazeneca","arterial_pfizer"]:
  sql("CREATE OR REPLACE GLOBAL TEMP VIEW st2_" + data + " AS SELECT '" + data + "'" + table + data)

# COMMAND ----------

# MAGIC %md ## Join output for all populations together

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_vacc_st2 AS
# MAGIC SELECT *
# MAGIC FROM global_temp.st2_venous_astrazeneca
# MAGIC UNION ALL
# MAGIC SELECT *
# MAGIC FROM global_temp.st2_venous_pfizer
# MAGIC UNION ALL
# MAGIC SELECT *
# MAGIC FROM global_temp.st2_arterial_astrazeneca
# MAGIC UNION ALL
# MAGIC SELECT *
# MAGIC FROM global_temp.st2_arterial_pfizer

# COMMAND ----------

# MAGIC %md ## Save

# COMMAND ----------

drop_table('ccu002_vacc_st2')
create_table('ccu002_vacc_st2')

# COMMAND ----------

# MAGIC %md ## Export

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu002_vacc_st2
