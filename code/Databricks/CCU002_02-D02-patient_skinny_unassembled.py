# Databricks notebook source
# MAGIC %md # CCU002_02-D02-patient_skinny_unassembled
# MAGIC 
# MAGIC **Description** Gather together the records for each patient in primary and secondary care before they are assembled into a skinny record for CCU002. The output of this is a global temp View which is then used by **CCU002_02-D03-patient_skinny_record**
# MAGIC  
# MAGIC **Author(s)** Sam Hollings, Jenny Cooper

# COMMAND ----------

# MAGIC %md
# MAGIC This notebook will make a single record for each patient with the core facts about that patient, reconciled across the main datasets (primary and secondary care):
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

# MAGIC %md ### Set the values for the widgets and import common functions

# COMMAND ----------

#Datasets
hes_apc_data = 'ccu002_vac_hes_apc_all_years'
hes_op_data = 'ccu002_vac_hes_op_all_years'
hes_ae_data = 'ccu002_vac_hes_ae_all_years'
gdppr_data = 'ccu002_vac_gdppr_dars_nic_391419_j3w9t'
deaths_data = 'ccu002_vac_deaths_dars_nic_391419_j3w9t'

# COMMAND ----------

# MAGIC %run "/Workspaces/dars_nic_391419_j3w9t_collab/CCU002_vacc/Data_Curation/in_progress/CCU002_vacc_functions/wrang000_functions

# COMMAND ----------

dbutils.widgets.removeAll();

# COMMAND ----------

database_name, collab_database_name, datawrang_database_name = database_widgets(database_name = 'dars_nic_391419_j3w9t', collab_database_name='dars_nic_391419_j3w9t_collab');

# COMMAND ----------

# MAGIC %md ### Get the secondary care data for each patient
# MAGIC First pull all the patient facts from HES

# COMMAND ----------

spark.sql(f"""
 CREATE OR REPLACE GLOBAL TEMP VIEW curr301_patient_skinny_all_hes_apc AS
 SELECT DISTINCT PERSON_ID_DEID as NHS_NUMBER_DEID, 
      ETHNOS as ETHNIC, 
      SEX, 
      to_date(MYDOB,'MMyyyy') as DATE_OF_BIRTH , 
      NULL as DATE_OF_DEATH, 
      EPISTART as RECORD_DATE, 
      epikey as record_id,
      "hes_apc" as dataset,
      0 as primary,
      FYEAR
  FROM {datawrang_database_name}.{hes_apc_data}"""
)

# COMMAND ----------

spark.sql(f"""
 CREATE OR REPLACE GLOBAL TEMP VIEW curr301_patient_skinny_all_hes_ae AS
  SELECT DISTINCT PERSON_ID_DEID as NHS_NUMBER_DEID, 
      ETHNOS as ETHNIC, 
      SEX, 
      date_format(date_trunc("MM", date_add(ARRIVALDATE, -ARRIVALAGE_CALC*365)),"yyyy-MM-dd") as DATE_OF_BIRTH,
      NULL as DATE_OF_DEATH, 
      ARRIVALDATE as RECORD_DATE, 
      COALESCE(epikey, aekey) as record_id,
      "hes_ae" as dataset,
      0 as primary,
      FYEAR
  FROM {datawrang_database_name}.{hes_ae_data}""")

# COMMAND ----------

spark.sql(f"""
 CREATE OR REPLACE GLOBAL TEMP VIEW curr301_patient_skinny_all_hes_op AS
  SELECT DISTINCT PERSON_ID_DEID as NHS_NUMBER_DEID, 
      ETHNOS as ETHNIC, 
      SEX,
      date_format(date_trunc("MM", date_add(APPTDATE, -APPTAGE_CALC*365)),"yyyy-MM-dd") as DATE_OF_BIRTH,
      NULL as DATE_OF_DEATH,
      APPTDATE  as RECORD_DATE,
      ATTENDKEY as record_id,
      'hes_op' as dataset,
      0 as primary,
      FYEAR
  FROM {datawrang_database_name}.{hes_op_data}""")

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW curr301_patient_skinny_all_hes as
# MAGIC SELECT NHS_NUMBER_DEID, ETHNIC, SEX, DATE_OF_BIRTH, DATE_OF_DEATH, RECORD_DATe, record_id, dataset, primary FROM global_temp.curr301_patient_skinny_all_hes_apc
# MAGIC UNION ALL
# MAGIC SELECT NHS_NUMBER_DEID, ETHNIC, SEX, DATE_OF_BIRTH, DATE_OF_DEATH, RECORD_DATe, record_id, dataset, primary FROM global_temp.curr301_patient_skinny_all_hes_ae
# MAGIC UNION ALL
# MAGIC SELECT NHS_NUMBER_DEID, ETHNIC, SEX, DATE_OF_BIRTH, DATE_OF_DEATH, RECORD_DATe, record_id, dataset, primary FROM global_temp.curr301_patient_skinny_all_hes_op

# COMMAND ----------

# MAGIC %md ## Primary care for each patient
# MAGIC Get the patients in the standard template from GDPPR
# MAGIC 
# MAGIC These values are standard for a patient across the system, so its hard to assign a date, so Natasha from primary care told me they use `REPORTING_PERIOD_END_DATE` as the date for these patient features

# COMMAND ----------

spark.sql(f"""
CREATE OR REPLACE GLOBAL TEMP VIEW curr301_patient_skinny_gdppr_patients AS
                SELECT NHS_NUMBER_DEID, 
                      gdppr.ETHNIC, 
                      gdppr.SEX,
                      to_date(string(YEAR_OF_BIRTH),"yyyy") as DATE_OF_BIRTH,
                      to_date(string(YEAR_OF_DEATH),"yyyy") as DATE_OF_DEATH,
                      REPORTING_PERIOD_END_DATE as RECORD_DATE, -- I got this off Natasha from Primary Care
                      NULL as record_id,
                      'GDPPR' as dataset,
                      1 as primary
                FROM {datawrang_database_name}.{gdppr_data} as gdppr""")

# COMMAND ----------

# MAGIC %md GDPPR can also store the patient ethnicity in the `CODE` column as a SNOMED code, hence we need to bring this in as another record for the patient (but with null for the other features as they come from the generic record above)

# COMMAND ----------

spark.sql(f"""CREATE OR REPLACE GLOBAL TEMP VIEW curr301_patient_skinny_gdppr_patients_SNOMED AS
                SELECT NHS_NUMBER_DEID, 
                      eth.PrimaryCode as ETHNIC, 
                      gdppr.SEX,
                      to_date(string(YEAR_OF_BIRTH),"yyyy") as DATE_OF_BIRTH,
                      to_date(string(YEAR_OF_DEATH),"yyyy") as DATE_OF_DEATH,
                      DATE as RECORD_DATE,
                      NULL as record_id,
                      'GDPPR_snomed' as dataset,
                      1 as primary
                FROM {datawrang_database_name}.{gdppr_data} as gdppr
                      INNER JOIN dss_corporate.gdppr_ethnicity_mappings eth on gdppr.CODE = eth.ConceptId""")

# COMMAND ----------

# MAGIC %md ### Single death per patient
# MAGIC In the deaths table (Civil registration deaths), some unfortunate people are down as dying twice. Let's take the most recent death date. 

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_single_patient_death AS
# MAGIC 
# MAGIC SELECT * 
# MAGIC FROM 
# MAGIC   (SELECT * , row_number() OVER (PARTITION BY DEC_CONF_NHS_NUMBER_CLEAN_DEID 
# MAGIC                                       ORDER BY REG_DATE desc, REG_DATE_OF_DEATH desc) as death_rank
# MAGIC     FROM dars_nic_391419_j3w9t_collab.ccu002_deaths_dars_nic_391419_j3w9t
# MAGIC     ) cte
# MAGIC WHERE death_rank = 1
# MAGIC AND DEC_CONF_NHS_NUMBER_CLEAN_DEID IS NOT NULL
# MAGIC and REG_DATE_OF_DEATH_formatted > '1900-01-01'
# MAGIC AND REG_DATE_OF_DEATH_formatted <= current_date()

# COMMAND ----------

spark.sql(F"""CREATE OR REPLACE GLOBAL TEMP VIEW curr301_patient_skinny_single_patient_death AS

SELECT *
FROM
  (SELECT *,
          row_number() OVER (PARTITION BY DEC_CONF_NHS_NUMBER_CLEAN_DEID 
                             ORDER BY REG_DATE desc, REG_DATE_OF_DEATH desc,
                             S_UNDERLYING_COD_ICD10 desc 
                             ) as death_rank
    FROM {datawrang_database_name}.{deaths_data}
    ) cte
WHERE death_rank = 1
AND DEC_CONF_NHS_NUMBER_CLEAN_DEID IS NOT NULL
AND REG_DATE_OF_DEATH_FORMATTED > '1900-01-01'
AND REG_DATE_OF_DEATH_FORMATTED <= current_date()
""")


# COMMAND ----------

# MAGIC %md ## Combine Primary and Secondary Care along with Deaths data
# MAGIC Flag some values as NULLs:
# MAGIC - DATE_OF_DEATH flag the following as like NULL: 'NULL', "" empty strings (or just spaces),  < 1900-01-01, after the current_date(), after the record_date (the person shouldn't be set to die in the future!)
# MAGIC - DATE_OF_BIRTH flag the following as like NULL: 'NULL', "" empty strings (or just spaces),  < 1900-01-01, after the current_date(), after the record_date (the person shouldn't be set to die in the future!)
# MAGIC - SEX flag the following as NULL: 'NULL', empty string, "9", "0" (9 and 0 are coded nulls, like unknown or not specified)
# MAGIC - ETHNIC flag the following as MULL: 'NULL', empty string, "9", "99", "X", "Z" - various types of coded nulls (unknown etc.)

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW curr301_patient_skinny_unassembled AS
# MAGIC SELECT *,      
# MAGIC       CASE WHEN ETHNIC IS NULL or TRIM(ETHNIC) IN ("","9", "99", "X" , "Z") THEN 1 ELSE 0 END as ethnic_null,
# MAGIC       CASE WHEN SEX IS NULL or TRIM(SEX) IN ("", "9", "0" ) THEN 1 ELSE 0 END as sex_null,
# MAGIC       CASE WHEN DATE_OF_BIRTH IS NULL OR TRIM(DATE_OF_BIRTH) = "" OR DATE_OF_BIRTH < '1900-01-01' or DATE_OF_BIRTH > current_date() OR DATE_OF_BIRTH > RECORD_DATE THEN 1 ELSE 0 END as date_of_birth_null,
# MAGIC       CASE WHEN DATE_OF_DEATH IS NULL OR TRIM(DATE_OF_DEATH) = "" OR DATE_OF_DEATH < '1900-01-01' OR DATE_OF_DEATH > current_date() OR DATE_OF_DEATH > RECORD_DATE THEN 1 ELSE 0 END as date_of_death_null,
# MAGIC       CASE WHEN dataset = 'death' THEN 1 ELSE 0 END as death_table
# MAGIC FROM (
# MAGIC       SELECT  NHS_NUMBER_DEID,
# MAGIC               ETHNIC,
# MAGIC               SEX,
# MAGIC               DATE_OF_BIRTH,
# MAGIC               DATE_OF_DEATH,
# MAGIC               RECORD_DATE,
# MAGIC               record_id,
# MAGIC               dataset,
# MAGIC               primary,
# MAGIC               care_domain        
# MAGIC       FROM (
# MAGIC             SELECT NHS_NUMBER_DEID,
# MAGIC                 ETHNIC,
# MAGIC                 SEX,
# MAGIC                 DATE_OF_BIRTH,
# MAGIC                 DATE_OF_DEATH,
# MAGIC                 RECORD_DATE,
# MAGIC                 record_id,
# MAGIC                 dataset,
# MAGIC                 primary, 'primary' as care_domain
# MAGIC               FROM global_temp.curr301_patient_skinny_gdppr_patients 
# MAGIC             UNION ALL
# MAGIC             SELECT NHS_NUMBER_DEID,
# MAGIC                 ETHNIC,
# MAGIC                 SEX,
# MAGIC                 DATE_OF_BIRTH,
# MAGIC                 DATE_OF_DEATH,
# MAGIC                 RECORD_DATE,
# MAGIC                 record_id,
# MAGIC                 dataset,
# MAGIC                 primary, 'primary_SNOMED' as care_domain
# MAGIC               FROM global_temp.curr301_patient_skinny_gdppr_patients_SNOMED
# MAGIC             UNION ALL
# MAGIC             SELECT NHS_NUMBER_DEID,
# MAGIC                 ETHNIC,
# MAGIC                 SEX,
# MAGIC                 DATE_OF_BIRTH,
# MAGIC                 DATE_OF_DEATH,
# MAGIC                 RECORD_DATE,
# MAGIC                 record_id,
# MAGIC                 dataset,
# MAGIC                 primary, 'secondary' as care_domain
# MAGIC               FROM global_temp.curr301_patient_skinny_all_hes
# MAGIC             UNION ALL
# MAGIC             SELECT DEC_CONF_NHS_NUMBER_CLEAN_DEID as NHS_NUMBER_DEID,
# MAGIC                 Null as ETHNIC,
# MAGIC                 Null as SEX,
# MAGIC                 Null as DATE_OF_BIRTH,
# MAGIC                 REG_DATE_OF_DEATH_formatted as DATE_OF_DEATH,
# MAGIC                 REG_DATE_formatted as RECORD_DATE,
# MAGIC                 Null as record_id,
# MAGIC                 'death' as dataset,
# MAGIC                 0 as primary, 'death' as care_domain
# MAGIC               FROM global_temp.curr301_patient_skinny_single_patient_death
# MAGIC           ) all_patients 
# MAGIC           --LEFT JOIN dars_nic_391419_j3w9t.deaths_dars_nic_391419_j3w9t death on all_patients.NHS_NUMBER_DEID = death.DEC_CONF_NHS_NUMBER_CLEAN_DEID
# MAGIC     )
