# Databricks notebook source
# MAGIC %md # CCU002_02-D19-table_1
# MAGIC 
# MAGIC **Description** This notebook extracts the information needed for table 1.
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
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW whole AS
# MAGIC SELECT *
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_vacc_cohort

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW venous AS
# MAGIC SELECT *
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_vacc_cohort
# MAGIC WHERE diag1_Venous_event_date IS NOT NULL
# MAGIC AND diag1_Venous_event_date >= '2020-12-08'
# MAGIC AND diag1_Venous_event_date < '2021-03-19'
# MAGIC AND ((diag1_Venous_event_date < VACCINATION_DATE_FIRST) 
# MAGIC   OR (VACCINE_PRODUCT_FIRST IS NULL)
# MAGIC   OR (diag1_Venous_event_date >= VACCINATION_DATE_FIRST AND VACCINE_PRODUCT_FIRST = 'AstraZeneca' AND VACCINATION_DATE_FIRST IS NOT NULL)
# MAGIC   OR (diag1_Venous_event_date >= VACCINATION_DATE_FIRST AND VACCINE_PRODUCT_FIRST = 'Pfizer' AND VACCINATION_DATE_FIRST IS NOT NULL))
# MAGIC AND ((diag1_Venous_event_date <= death_date) OR (death_date IS NULL))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW arterial AS
# MAGIC SELECT *
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_vacc_cohort
# MAGIC WHERE diag1_Arterial_event_date IS NOT NULL
# MAGIC AND diag1_Arterial_event_date >= '2020-12-08'
# MAGIC AND diag1_Arterial_event_date < '2021-03-19'
# MAGIC AND ((diag1_Arterial_event_date < VACCINATION_DATE_FIRST) 
# MAGIC   OR (VACCINE_PRODUCT_FIRST IS NULL)
# MAGIC   OR (diag1_Arterial_event_date >= VACCINATION_DATE_FIRST AND VACCINE_PRODUCT_FIRST = 'AstraZeneca' AND VACCINATION_DATE_FIRST IS NOT NULL)
# MAGIC   OR (diag1_Arterial_event_date >= VACCINATION_DATE_FIRST AND VACCINE_PRODUCT_FIRST = 'Pfizer' AND VACCINATION_DATE_FIRST IS NOT NULL))
# MAGIC AND ((diag1_Arterial_event_date <= death_date) OR (death_date IS NULL))

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW thrombocytopenia AS
# MAGIC SELECT *
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_vacc_cohort
# MAGIC WHERE diag1_thrombocytopenia_date IS NOT NULL
# MAGIC AND diag1_thrombocytopenia_date >= '2020-12-08'
# MAGIC AND diag1_thrombocytopenia_date < '2021-03-19'
# MAGIC AND ((diag1_thrombocytopenia_date < VACCINATION_DATE_FIRST) 
# MAGIC   OR (VACCINE_PRODUCT_FIRST IS NULL)
# MAGIC   OR (diag1_thrombocytopenia_date >= VACCINATION_DATE_FIRST AND VACCINE_PRODUCT_FIRST = 'AstraZeneca' AND VACCINATION_DATE_FIRST IS NOT NULL)
# MAGIC   OR (diag1_thrombocytopenia_date >= VACCINATION_DATE_FIRST AND VACCINE_PRODUCT_FIRST = 'Pfizer' AND VACCINATION_DATE_FIRST IS NOT NULL))
# MAGIC AND ((diag1_thrombocytopenia_date <= death_date) OR (death_date IS NULL))

# COMMAND ----------

# MAGIC %md ## Generate output for each population

# COMMAND ----------

table = " AS category, COUNT(NHS_NUMBER_DEID) AS N, SUM(CASE WHEN SEX=1 THEN 1 ELSE 0 END) AS Sex_Male, SUM(CASE WHEN SEX=2 THEN 1 ELSE 0 END) AS Sex_Female, SUM(CASE WHEN AGE_AT_COHORT_START>=18 AND AGE_AT_COHORT_START<30 THEN 1 ELSE 0 END) AS Age_18_29, SUM(CASE WHEN AGE_AT_COHORT_START>=30 AND AGE_AT_COHORT_START<50 THEN 1 ELSE 0 END) AS Age_30_49, SUM(CASE WHEN AGE_AT_COHORT_START>=50 AND AGE_AT_COHORT_START<70 THEN 1 ELSE 0 END) AS Age_50_69, SUM(CASE WHEN AGE_AT_COHORT_START>=70 AND AGE_AT_COHORT_START<80 THEN 1 ELSE 0 END) AS Age_70_79, SUM(CASE WHEN AGE_AT_COHORT_START>=80 THEN 1 ELSE 0 END) AS Age_80plus, SUM(CASE WHEN CATEGORISED_ETHNICITY='Asian or Asian British' THEN 1 ELSE 0 END) AS Ethnicity_Asian, SUM(CASE WHEN CATEGORISED_ETHNICITY='Black or Black British' THEN 1 ELSE 0 END) AS Ethnicity_Black, SUM(CASE WHEN CATEGORISED_ETHNICITY='Mixed' THEN 1 ELSE 0 END) AS Ethnicity_Mixed, SUM(CASE WHEN CATEGORISED_ETHNICITY='Other Ethnic Groups' THEN 1 ELSE 0 END) AS Ethnicity_Other, SUM(CASE WHEN CATEGORISED_ETHNICITY='White' THEN 1 ELSE 0 END) AS Ethnicity_White, SUM(CASE WHEN CATEGORISED_ETHNICITY='Unknown' OR CATEGORISED_ETHNICITY='missing' THEN 1 ELSE 0 END) AS Ethnicity_UnknownOrMissing, SUM(CASE WHEN IMD='Deciles_1_2' THEN 1 ELSE 0 END) AS Deprivation_1_2, SUM(CASE WHEN IMD='Deciles_3_4' THEN 1 ELSE 0 END) AS Deprivation_3_4, SUM(CASE WHEN IMD='Deciles_5_6' THEN 1 ELSE 0 END) AS Deprivation_5_6, SUM(CASE WHEN IMD='Deciles_7_8' THEN 1 ELSE 0 END) AS Deprivation_7_8, SUM(CASE WHEN IMD='Deciles_9_10' THEN 1 ELSE 0 END) AS Deprivation_9_10, SUM(CASE WHEN smoking_status='Current-smoker' THEN 1 ELSE 0 END) AS SmokingStatus_Current, SUM(CASE WHEN smoking_status='Ex-smoker' THEN 1 ELSE 0 END) AS SmokingStatus_Former, SUM(CASE WHEN smoking_status='Never-Smoker' THEN 1 ELSE 0 END) AS SmokingStatus_Never, SUM(CASE WHEN EVER_ALL_STROKE=1 THEN 1 ELSE 0 END) AS MedicalHistory_Stroke, SUM(CASE WHEN EVER_AMI=1 THEN 1 ELSE 0 END) AS MedicalHistory_MyocardialInfarction,  SUM(CASE WHEN EVER_PE_VT=1 THEN 1 ELSE 0 END) AS MedicalHistory_DVT_PE, SUM(CASE WHEN EVER_THROMBOPHILIA=1 THEN 1 ELSE 0 END) AS MedicalHistory_Thrombophilia, SUM(CASE WHEN COVID_infection=1 THEN 1 ELSE 0 END) AS MedicalHistory_CoronavirusInfection, SUM(CASE WHEN EVER_DIAB_DIAG_OR_MEDS=1 THEN 1 ELSE 0 END) AS MedicalHistory_Diabetes, SUM(CASE WHEN EVER_DEPR=1 THEN 1 ELSE 0 END) AS MedicalHistory_Depression, SUM(CASE WHEN EVER_OBESITY=1 THEN 1 ELSE 0 END) AS MedicalHistory_Obesity, SUM(CASE WHEN EVER_CANCER=1 THEN 1 ELSE 0 END) AS MedicalHistory_Cancer, SUM(CASE WHEN EVER_COPD=1 THEN 1 ELSE 0 END) AS MedicalHistory_COPD, SUM(CASE WHEN EVER_LIVER=1 THEN 1 ELSE 0 END) AS MedicalHistory_LiverDisease, SUM(CASE WHEN EVER_CKD=1 THEN 1 ELSE 0 END) AS MedicalHistory_ChronicKidneyDisease, SUM(CASE WHEN SURGERY_LASTYR=1 THEN 1 ELSE 0 END) AS MedicalHistory_MajorSurgery, SUM(CASE WHEN EVER_DEMENTIA=1 THEN 1 ELSE 0 END) AS  MedicalHistory_Dementia, SUM(CASE WHEN ANTIPLATLET_MEDS=1 THEN 1 ELSE 0 END) AS Medication_Antiplatelet, SUM(CASE WHEN BP_LOWER_MEDS=1 THEN 1 ELSE 0 END) AS Medication_BPLowering, SUM(CASE WHEN LIPID_LOWER_MEDS=1 THEN 1 ELSE 0 END) AS Medication_LipidLowering, SUM(CASE WHEN ANTICOAG_MEDS=1 THEN 1 ELSE 0 END) AS Medication_Anticoagulant, SUM(CASE WHEN COCP_MEDS=1 THEN 1 ELSE 0 END) AS Medication_OralContraceptive, SUM(CASE WHEN HRT_MEDS=1 THEN 1 ELSE 0 END) AS Medication_HRT, SUM(CASE WHEN N_DISORDER=0 THEN 1 ELSE 0 END) AS NumberOfDiagnoses_0, SUM(CASE WHEN N_DISORDER>0 AND N_DISORDER<6 THEN 1 ELSE 0 END) AS NumberOfDiagnoses_1_5, SUM(CASE WHEN N_DISORDER>=6 THEN 1 ELSE 0 END) AS NumberOfDiagnoses_6plus, SUM(CASE WHEN UNIQUE_BNF_CHAPS=0 THEN 1 ELSE 0 END) AS NumberOfMedications_0, SUM(CASE WHEN UNIQUE_BNF_CHAPS>0 AND UNIQUE_BNF_CHAPS<6  THEN 1 ELSE 0 END) AS NumberOfMedications_1_5, SUM(CASE WHEN UNIQUE_BNF_CHAPS>=6 THEN 1 ELSE 0 END) AS NumberOfMedications_6plus FROM global_temp."

for data in ["whole","venous","arterial","thrombocytopenia"]:
  sql("CREATE OR REPLACE GLOBAL TEMP VIEW t1_" + data + " AS SELECT '" + data + "_all'" + table + data + " UNION ALL SELECT '" + data + "_under70'" + table + data + " WHERE AGE_AT_COHORT_START<70 UNION ALL SELECT '" + data + "_70plus'" + table + data + " WHERE AGE_AT_COHORT_START>=70")

# COMMAND ----------

# MAGIC %md ## Join output for all populations together

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_vacc_t1 AS
# MAGIC SELECT *
# MAGIC FROM global_temp.t1_whole
# MAGIC UNION ALL
# MAGIC SELECT *
# MAGIC FROM global_temp.t1_venous
# MAGIC UNION ALL
# MAGIC SELECT *
# MAGIC FROM global_temp.t1_arterial
# MAGIC UNION ALL
# MAGIC SELECT *
# MAGIC FROM global_temp.t1_thrombocytopenia

# COMMAND ----------

# MAGIC %md ## Save

# COMMAND ----------

drop_table('ccu002_vacc_t1')
create_table('ccu002_vacc_t1')

# COMMAND ----------

# MAGIC %md ## Export

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * 
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_vacc_t1
