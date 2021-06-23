# Databricks notebook source
# MAGIC %md # CCU002_02-D01-event_counts
# MAGIC 
# MAGIC **Description** This notebook calculates the number of primary events from HES and death records for events.
# MAGIC 
# MAGIC **Author(s)** Venexia Walker, Tom Bolton

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

# MAGIC %md ## Refresh tables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Remove cached HES table
# MAGIC REFRESH TABLE dars_nic_391419_j3w9t_collab.ccu002_vac_hes_apc_all_years

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Remove cached deaths table
# MAGIC REFRESH TABLE dars_nic_391419_j3w9t_collab.ccu002_vac_deaths_dars_nic_391419_j3w9t

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Remove cached skinny table
# MAGIC REFRESH TABLE dars_nic_391419_j3w9t_collab.curr302_patient_skinny_record

# COMMAND ----------

# MAGIC %md ## Define event codes

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Record the codes that define each event of interest
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW event_codes AS
# MAGIC SELECT
# MAGIC   *
# MAGIC FROM VALUES 
# MAGIC   ('I21','incident myocardial infarction',0),
# MAGIC   ('I22','incident myocardial infarction',0),
# MAGIC   ('I23','incident myocardial infarction',0),
# MAGIC   ('H34','retinal infarction',0),
# MAGIC   ('I630','ischaemic stroke',0),
# MAGIC   ('I631','ischaemic stroke',0),
# MAGIC   ('I632','ischaemic stroke',0),
# MAGIC   ('I633','ischaemic stroke',0),
# MAGIC   ('I634','ischaemic stroke',0),
# MAGIC   ('I635','ischaemic stroke',0),
# MAGIC   ('I637','ischaemic stroke',0),
# MAGIC   ('I638','ischaemic stroke',0),
# MAGIC   ('I639','ischaemic stroke',0),
# MAGIC   ('I63X','ischaemic stroke',0),
# MAGIC   ('I64','stroke of unknown type',0),
# MAGIC   ('I60','stroke, subarachnoid haemorrhage',0),
# MAGIC   ('I74','other arterial embolism',0),
# MAGIC   ('I260','pulmonary embolism',0),
# MAGIC   ('I269','pulmonary embolism',0),
# MAGIC   ('I80','deep vein thrombosis',0),
# MAGIC   ('I81','portal vein thrombosis',0),
# MAGIC   ('I820','other deep vein thrombosis',0),
# MAGIC   ('I822','other deep vein thrombosis',0),
# MAGIC   ('I823','other deep vein thrombosis',0),
# MAGIC   ('I828','other deep vein thrombosis',0),
# MAGIC   ('I829','other deep vein thrombosis',0),
# MAGIC   ('O223','thrombosis during pregnancy and puerperium',1),
# MAGIC   ('O871','thrombosis during pregnancy and puerperium',1),
# MAGIC   ('O879','thrombosis during pregnancy and puerperium',1),
# MAGIC   ('O882','thrombosis during pregnancy and puerperium',1),
# MAGIC   ('O225','cerebral venous thrombosis',1),
# MAGIC   ('O873','cerebral venous thrombosis',1),
# MAGIC   ('G08','cerebral venous thrombosis',0),
# MAGIC   ('I676','cerebral venous thrombosis',0),
# MAGIC   ('I636','cerebral venous thrombosis',0),
# MAGIC   ('D693','thrombocytopenia',0),
# MAGIC   ('D694','thrombocytopenia',0),
# MAGIC   ('D695','thrombocytopenia',0),
# MAGIC   ('D696','thrombocytopenia',0),
# MAGIC   ('D65','disseminated intravascular coagulation',0),
# MAGIC   ('M311','thrombotic thrombocytopenic purpura',0),
# MAGIC   ('K559','mesenteric thrombus',0),
# MAGIC   ('K550','mesenteric thrombus',0),
# MAGIC   ('I61','intracerebral haemorrhage',0),
# MAGIC   ('G951','spinal stroke',0)
# MAGIC AS tab(CODE, EVENT, WOMEN_ONLY)

# COMMAND ----------

# MAGIC %md ## Restrict skinny table

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Create a reduced skinny table with just sex, date_of_birth and date_of_death
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW skinny_distinct AS
# MAGIC SELECT DISTINCT NHS_NUMBER_DEID,
# MAGIC                 SEX,
# MAGIC                 DATE_OF_BIRTH,
# MAGIC                 DATE_OF_DEATH
# MAGIC FROM dars_nic_391419_j3w9t_collab.curr302_patient_skinny_record
# MAGIC WHERE NHS_NUMBER_DEID IS NOT NULL
# MAGIC   AND SEX IS NOT NULL
# MAGIC   AND DATE_OF_BIRTH IS NOT NULL

# COMMAND ----------

# MAGIC %md ## Extract all events

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Extract all records that contain a relevant event
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW all_events AS
# MAGIC --- from HES APC (4 digit codes)
# MAGIC SELECT DISTINCT PERSON_ID_DEID,
# MAGIC   YEAR(ADMIDATE) AS YEAR,
# MAGIC   ADMIDATE,
# MAGIC   DIAG_4_01 AS CODE,
# MAGIC   "HES_APC" AS SOURCE
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_vac_hes_apc_all_years
# MAGIC WHERE PERSON_ID_DEID IS NOT NULL
# MAGIC AND ADMIDATE >= '2018-01-01'
# MAGIC AND ADMIDATE <= '2020-12-31'
# MAGIC AND DIAG_4_01 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=4)
# MAGIC --- from HES APC (3 digit codes)
# MAGIC UNION ALL
# MAGIC SELECT DISTINCT PERSON_ID_DEID,
# MAGIC   YEAR(ADMIDATE) AS YEAR,
# MAGIC   ADMIDATE,
# MAGIC   DIAG_3_01 AS CODE,
# MAGIC   "HES_APC" AS SOURCE
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_vac_hes_apc_all_years
# MAGIC WHERE PERSON_ID_DEID IS NOT NULL
# MAGIC AND ADMIDATE >= '2018-01-01'
# MAGIC AND ADMIDATE <= '2020-12-31'
# MAGIC AND DIAG_3_01 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=3)
# MAGIC --- from DEATHS (4 digit codes)
# MAGIC UNION ALL
# MAGIC SELECT DISTINCT DEC_CONF_NHS_NUMBER_CLEAN_DEID AS PERSON_ID_DEID,
# MAGIC   left(REG_DATE_OF_DEATH,4) AS YEAR,
# MAGIC   to_date(REG_DATE_OF_DEATH, 'yyyyMMdd') AS ADMIDATE,
# MAGIC   S_UNDERLYING_COD_ICD10 AS CODE,
# MAGIC   "DEATHS" AS SOURCE
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_vac_deaths_dars_nic_391419_j3w9t
# MAGIC WHERE DEC_CONF_NHS_NUMBER_CLEAN_DEID IS NOT NULL
# MAGIC AND left(REG_DATE_OF_DEATH,4) >= 2018
# MAGIC AND left(REG_DATE_OF_DEATH,4) <= 2020
# MAGIC AND S_UNDERLYING_COD_ICD10 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=4)
# MAGIC --- from DEATHS (3 digit codes)
# MAGIC UNION ALL
# MAGIC SELECT DISTINCT DEC_CONF_NHS_NUMBER_CLEAN_DEID AS PERSON_ID_DEID,
# MAGIC   left(REG_DATE_OF_DEATH,4) AS YEAR,
# MAGIC   to_date(REG_DATE_OF_DEATH, 'yyyyMMdd') AS ADMIDATE,
# MAGIC   left(S_UNDERLYING_COD_ICD10,3) AS CODE,
# MAGIC   "DEATHS" AS SOURCE
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_vac_deaths_dars_nic_391419_j3w9t
# MAGIC WHERE DEC_CONF_NHS_NUMBER_CLEAN_DEID IS NOT NULL
# MAGIC AND left(REG_DATE_OF_DEATH,4) >= 2018
# MAGIC AND left(REG_DATE_OF_DEATH,4) <= 2020
# MAGIC AND left(S_UNDERLYING_COD_ICD10,3) IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=3)

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add event names to codes
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW all_events_annotated AS
# MAGIC SELECT x.*,
# MAGIC        y.EVENT,
# MAGIC        y.WOMEN_ONLY
# MAGIC FROM global_temp.all_events AS x
# MAGIC LEFT JOIN global_temp.event_codes AS y ON x.CODE = y.CODE

# COMMAND ----------

# MAGIC %md ## Add skinny table information to all events

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Add skinny table information to events
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW all_events_annotated_skinny AS
# MAGIC SELECT PERSON_ID_DEID,
# MAGIC        YEAR,
# MAGIC        ADMIDATE,
# MAGIC        CODE,
# MAGIC        SOURCE,
# MAGIC        EVENT,
# MAGIC        ind_death28days,
# MAGIC        SEX,
# MAGIC        DATE_OF_BIRTH,
# MAGIC        DATE_OF_DEATH
# MAGIC FROM(SELECT x.*,
# MAGIC             y.SEX,
# MAGIC             y.DATE_OF_BIRTH,
# MAGIC             y.DATE_OF_DEATH,
# MAGIC             CASE WHEN (DATEDIFF(y.DATE_OF_DEATH,x.ADMIDATE)<29) THEN 1 ELSE 0 END AS ind_death28days
# MAGIC      FROM global_temp.all_events_annotated AS x
# MAGIC      LEFT JOIN global_temp.skinny_distinct AS y on x.PERSON_ID_DEID = y.NHS_NUMBER_DEID)
# MAGIC WHERE ((WOMEN_ONLY=1 AND SEX=2) OR (WOMEN_ONLY=0))
# MAGIC   AND SEX IS NOT NULL
# MAGIC   AND DATE_OF_BIRTH IS NOT NULL

# COMMAND ----------

# MAGIC %md ## Extract thrombocytopenia events 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create table containing unique non-primary thrombocytopenia events from HES APC
# MAGIC CREATE OR REPLACE GLOBAL TEMPORARY VIEW thrombocytopenia AS
# MAGIC SELECT DISTINCT PERSON_ID_DEID, 
# MAGIC                 ADMIDATE,
# MAGIC                 "HES_APC" AS SOURCE,
# MAGIC                 1 as thrombocytopenia
# MAGIC FROM (SELECT *
# MAGIC       FROM dars_nic_391419_j3w9t_collab.ccu002_vac_hes_apc_all_years
# MAGIC       WHERE ADMIDATE >= '2018-01-01'
# MAGIC       AND ADMIDATE <= '2020-12-31'
# MAGIC       AND ((DIAG_4_01 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=4 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_4_02 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=4 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_4_03 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=4 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_4_04 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=4 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_4_05 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=4 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_4_06 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=4 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_4_07 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=4 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_4_08 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=4 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_4_09 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=4 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_4_10 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=4 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_4_11 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=4 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_4_12 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=4 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_4_13 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=4 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_4_14 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=4 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_4_15 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=4 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_4_16 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=4 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_4_17 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=4 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_4_18 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=4 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_4_19 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=4 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_4_20 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=4 AND EVENT='thrombocytopenia')))
# MAGIC       UNION ALL
# MAGIC       SELECT *
# MAGIC       FROM dars_nic_391419_j3w9t_collab.ccu002_vac_hes_apc_all_years
# MAGIC       WHERE ADMIDATE >= '2018-01-01'
# MAGIC       AND ADMIDATE <= '2020-12-31'
# MAGIC       AND ((DIAG_3_01 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=3 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_3_02 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=3 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_3_03 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=3 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_3_04 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=3 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_3_05 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=3 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_3_06 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=3 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_3_07 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=3 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_3_08 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=3 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_3_09 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=3 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_3_10 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=3 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_3_11 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=3 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_3_12 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=3 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_3_13 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=3 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_3_14 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=3 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_3_15 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=3 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_3_16 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=3 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_3_17 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=3 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_3_18 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=3 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_3_19 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=3 AND EVENT='thrombocytopenia'))
# MAGIC         OR (DIAG_3_20 IN (SELECT CODE FROM global_temp.event_codes WHERE length(code)=3 AND EVENT='thrombocytopenia'))))
# MAGIC WHERE PERSON_ID_DEID IS NOT NULL
# MAGIC   AND ADMIDATE IS NOT NULL

# COMMAND ----------

# MAGIC %md ## Add thrombocytopenia flag 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Using the skinny table as master: inner join all events, annotate events and define indicators
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW all_events_annotated_skinny_thrombocytopenia AS
# MAGIC SELECT x.*,
# MAGIC        CASE WHEN (y.thrombocytopenia=1 AND x.EVENT!='thrombocytopenia' AND x.SOURCE='HES_APC') THEN 1 ELSE 0 END AS ind_thrombocytopenia
# MAGIC FROM global_temp.all_events_annotated_skinny AS x
# MAGIC LEFT JOIN global_temp.thrombocytopenia AS y on x.PERSON_ID_DEID = y.PERSON_ID_DEID AND x.ADMIDATE = y.ADMIDATE AND x.SOURCE = y.SOURCE

# COMMAND ----------

# MAGIC %md ## Determine source record 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Determine for each year which patients have events in HES_APC only, DEATHS only, or both
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW source_record AS
# MAGIC SELECT PERSON_ID_DEID,
# MAGIC        YEAR,
# MAGIC        EVENT,
# MAGIC        CASE WHEN SUM(HES_APC)>0 AND SUM(DEATHS)>0 THEN 'HES_APC + DEATHS'
# MAGIC             WHEN SUM(HES_APC)>0 AND SUM(DEATHS)=0 THEN 'HES_APC only'
# MAGIC             WHEN SUM(HES_APC)=0 AND SUM(DEATHS)>0 THEN 'DEATHS only' END RECORD
# MAGIC FROM (SELECT PERSON_ID_DEID,
# MAGIC              YEAR,
# MAGIC              SOURCE,
# MAGIC              EVENT,
# MAGIC              CASE WHEN SOURCE="HES_APC" THEN 1 ELSE 0 END AS HES_APC,
# MAGIC              CASE WHEN SOURCE="DEATHS" THEN 1 ELSE 0 END AS DEATHS
# MAGIC        FROM global_temp.all_events_annotated_skinny_thrombocytopenia
# MAGIC        WHERE PERSON_ID_DEID IS NOT null)
# MAGIC GROUP BY PERSON_ID_DEID, YEAR, EVENT

# COMMAND ----------

# MAGIC %md ## Determine first events 

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Select first event from all events
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW first_events AS
# MAGIC SELECT PERSON_ID_DEID,
# MAGIC       YEAR,
# MAGIC       EVENT,
# MAGIC       "event with thrombocytopenia" AS EVENT_WITH,
# MAGIC       min(ADMIDATE) as ADMIDATE,
# MAGIC       SEX,
# MAGIC       DATE_OF_BIRTH,
# MAGIC       DATE_OF_DEATH
# MAGIC FROM global_temp.all_events_annotated_skinny_thrombocytopenia
# MAGIC WHERE ind_thrombocytopenia=1
# MAGIC GROUP BY PERSON_ID_DEID, YEAR, EVENT, EVENT_WITH, SEX, DATE_OF_BIRTH, DATE_OF_DEATH
# MAGIC UNION ALL
# MAGIC SELECT PERSON_ID_DEID,
# MAGIC       YEAR,
# MAGIC       EVENT,
# MAGIC       "event with death in 28 days" AS EVENT_WITH,
# MAGIC       min(ADMIDATE) as ADMIDATE,
# MAGIC       SEX,
# MAGIC       DATE_OF_BIRTH,
# MAGIC       DATE_OF_DEATH
# MAGIC FROM global_temp.all_events_annotated_skinny_thrombocytopenia
# MAGIC WHERE ind_death28days=1
# MAGIC GROUP BY PERSON_ID_DEID, YEAR, EVENT, EVENT_WITH, SEX, DATE_OF_BIRTH, DATE_OF_DEATH
# MAGIC UNION ALL
# MAGIC SELECT PERSON_ID_DEID,
# MAGIC        YEAR,
# MAGIC        EVENT,
# MAGIC        "event only" AS EVENT_WITH,
# MAGIC        min(ADMIDATE) as ADMIDATE,
# MAGIC        SEX,
# MAGIC        DATE_OF_BIRTH,
# MAGIC        DATE_OF_DEATH
# MAGIC FROM global_temp.all_events_annotated_skinny_thrombocytopenia
# MAGIC GROUP BY PERSON_ID_DEID, YEAR, EVENT, EVENT_WITH, SEX, DATE_OF_BIRTH, DATE_OF_DEATH

# COMMAND ----------

# MAGIC %md ## Add record information to first events

# COMMAND ----------

# MAGIC %sql 
# MAGIC -- Add record information to first events table and determine death within 28 days
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW first_events_source AS
# MAGIC SELECT DISTINCT x.*,
# MAGIC                 z.RECORD
# MAGIC FROM global_temp.first_events AS x
# MAGIC INNER JOIN global_temp.source_record AS z ON z.PERSON_ID_DEID = x.PERSON_ID_DEID AND z.YEAR = x.YEAR AND z.EVENT = x.EVENT

# COMMAND ----------

# MAGIC %md ## Add age bands to first events

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Determine age and assign age bands
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW first_events_source_ageband AS
# MAGIC SELECT *,
# MAGIC        CASE WHEN AGE>=0 AND AGE<19 THEN '0-18'
# MAGIC             WHEN AGE>=19 AND AGE<31 THEN '19-30'
# MAGIC             WHEN AGE>=31 AND AGE<40 THEN '31-39'
# MAGIC             WHEN AGE>=40 AND AGE<50 THEN '40-49'
# MAGIC             WHEN AGE>=50 AND AGE<60 THEN '50-59'
# MAGIC             WHEN AGE>=60 AND AGE<70 THEN '60-69'
# MAGIC             WHEN AGE>=70 AND AGE<80 THEN '70-79'
# MAGIC             WHEN AGE>=80 AND AGE<90 THEN '80-89'
# MAGIC             WHEN AGE>=90 AND AGE<116 THEN '90+' 
# MAGIC             WHEN AGE>=116 THEN null END AGE_BAND
# MAGIC        FROM (SELECT PERSON_ID_DEID,
# MAGIC                     SEX,
# MAGIC                     YEAR,
# MAGIC                     RECORD,
# MAGIC                     EVENT,
# MAGIC                     EVENT_WITH,
# MAGIC                     (DATEDIFF(ADMIDATE, DATE_OF_BIRTH)/365.25) AS AGE
# MAGIC              FROM global_temp.first_events_source)

# COMMAND ----------

# MAGIC %md ## Count number of first events

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Count number of patients in each sex/year/event/age_band/record combination
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_vacc_event_counts AS
# MAGIC SELECT YEAR,
# MAGIC        CASE WHEN SEX=1 THEN 'Men'
# MAGIC             WHEN SEX=2 THEN 'Women' END SEX,
# MAGIC        RECORD,
# MAGIC        EVENT,
# MAGIC        EVENT_WITH,
# MAGIC        AGE_BAND,
# MAGIC        COUNT(PERSON_ID_DEID) AS N
# MAGIC FROM global_temp.first_events_source_ageband
# MAGIC GROUP BY YEAR, SEX, RECORD, EVENT, EVENT_WITH, AGE_BAND

# COMMAND ----------

# MAGIC %md ## Export table

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Export cell
# MAGIC SELECT *
# MAGIC FROM global_temp.ccu002_vacc_event_counts

# COMMAND ----------

# MAGIC %md ## Save table

# COMMAND ----------

# Save table
drop_table('ccu002_vacc_event_counts')
create_table('ccu002_vacc_event_counts')
