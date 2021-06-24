-- Databricks notebook source
-- MAGIC %md # CCU002_02-D11-sus_outcomes
-- MAGIC 
-- MAGIC **Description** This notebook generates the outcomes listed in the CCU002 vaccination protocol using the SUS dataset.
-- MAGIC 
-- MAGIC **Author(s)** Spencer Keene, Rachel Denholm, Jenny Cooper

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Vaccine parameters

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC #Dataset parameters
-- MAGIC sus_data = 'dars_nic_391419_j3w9t_collab.ccu002_vac_sus_dars_nic_391419_j3w9t'
-- MAGIC 
-- MAGIC #Date parameters
-- MAGIC from datetime import date
-- MAGIC 
-- MAGIC index_date = '2020-12-08'
-- MAGIC 
-- MAGIC today = date.today()
-- MAGIC end_date = today.strftime('%Y-%m-%d')
-- MAGIC end_date2 = today.strftime('%Y-%m-%d')
-- MAGIC 
-- MAGIC #Final table name
-- MAGIC final_table_1 = 'ccu002_vac_outcomes_sus_final' 
-- MAGIC final_table_2 = 'ccu002_vac_outcomes_sus_final_anydiag'

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Infection parameters

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC #Dataset parameters
-- MAGIC sus_data = 'dars_nic_391419_j3w9t_collab.ccu002_vac_sus_dars_nic_391419_j3w9t'
-- MAGIC 
-- MAGIC #Date parameters
-- MAGIC from datetime import date
-- MAGIC 
-- MAGIC index_date = '2020-01-01'
-- MAGIC end_date = '2020-12-07'
-- MAGIC end_date2 = today.strftime('%Y-%m-%d')
-- MAGIC 
-- MAGIC 
-- MAGIC #Final table name
-- MAGIC final_table_1 = 'ccu002_inf_outcomes_sus_final' 
-- MAGIC final_table_2 = 'ccu002_inf_outcomes_sus_final_anydiag'

-- COMMAND ----------

-- MAGIC %md ## create functions

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Define create table function by Sam H
-- MAGIC # Source: Workspaces/dars_nic_391419_j3w9t_collab/DATA_CURATION_wrang000_functions
-- MAGIC 
-- MAGIC def create_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', select_sql_script:str=None) -> None:
-- MAGIC   """Will save to table from a global_temp view of the same name as the supplied table name (if no SQL script is supplied)
-- MAGIC   Otherwise, can supply a SQL script and this will be used to make the table with the specificed name, in the specifcied database."""
-- MAGIC   
-- MAGIC   spark.conf.set("spark.sql.legacy.allowCreatingManagedTableUsingNonemptyLocation","true")
-- MAGIC   
-- MAGIC   if select_sql_script is None:
-- MAGIC     select_sql_script = f"SELECT * FROM global_temp.{table_name}"
-- MAGIC   
-- MAGIC   spark.sql(f"""CREATE TABLE {database_name}.{table_name} AS
-- MAGIC                 {select_sql_script}
-- MAGIC              """)
-- MAGIC   spark.sql(f"ALTER TABLE {database_name}.{table_name} OWNER TO {database_name}")
-- MAGIC   
-- MAGIC def drop_table(table_name:str, database_name:str='dars_nic_391419_j3w9t_collab', if_exists=True):
-- MAGIC   if if_exists:
-- MAGIC     IF_EXISTS = 'IF EXISTS'
-- MAGIC   else: 
-- MAGIC     IF_EXISTS = ''
-- MAGIC   spark.sql(f"DROP TABLE {IF_EXISTS} {database_name}.{table_name}")

-- COMMAND ----------

-- MAGIC %md ## SUS
-- MAGIC  adapted from HES notebooks by Rachel Denholm

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC spark.sql(F"""
-- MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_vaccine_sus AS
-- MAGIC SELECT * FROM {sus_data}""")

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE TABLE dars_nic_391419_j3w9t_collab.ccu002_vac_sus_diag_stack_vacc AS
-- MAGIC WITH CTE AS (
-- MAGIC     SELECT AS_AT_DATE, GENERATED_RECORD_ID, START_DATE_HOSPITAL_PROVIDER_SPELL, END_DATE_HOSPITAL_PROVIDER_SPELL, EPISODE_START_DATE, EPISODE_END_DATE, EPISODE_DURATION, EPISODE_NUMBER, NHS_NUMBER_DEID, STACK(25,
-- MAGIC     'PRIMARY_DIAGNOSIS_CODE', PRIMARY_DIAGNOSIS_CODE,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_1', SECONDARY_DIAGNOSIS_CODE_1,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_2', SECONDARY_DIAGNOSIS_CODE_2,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_3', SECONDARY_DIAGNOSIS_CODE_3,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_4', SECONDARY_DIAGNOSIS_CODE_4,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_5', SECONDARY_DIAGNOSIS_CODE_5,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_6', SECONDARY_DIAGNOSIS_CODE_6,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_7', SECONDARY_DIAGNOSIS_CODE_7,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_8', SECONDARY_DIAGNOSIS_CODE_8,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_9', SECONDARY_DIAGNOSIS_CODE_9,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_10', SECONDARY_DIAGNOSIS_CODE_10,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_11', SECONDARY_DIAGNOSIS_CODE_11,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_12', SECONDARY_DIAGNOSIS_CODE_12,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_13', SECONDARY_DIAGNOSIS_CODE_13,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_14', SECONDARY_DIAGNOSIS_CODE_14,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_15', SECONDARY_DIAGNOSIS_CODE_15,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_16', SECONDARY_DIAGNOSIS_CODE_16,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_17', SECONDARY_DIAGNOSIS_CODE_17,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_18', SECONDARY_DIAGNOSIS_CODE_18,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_19', SECONDARY_DIAGNOSIS_CODE_19,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_20', SECONDARY_DIAGNOSIS_CODE_20,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_21', SECONDARY_DIAGNOSIS_CODE_21,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_22', SECONDARY_DIAGNOSIS_CODE_22,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_23', SECONDARY_DIAGNOSIS_CODE_23,
-- MAGIC     'SECONDARY_DIAGNOSIS_CODE_24', SECONDARY_DIAGNOSIS_CODE_24
-- MAGIC     ) as (DIAGNOSIS_POSITION, DIAGNOSIS_CODE)
-- MAGIC     FROM global_temp.ccu002_vaccine_sus),
-- MAGIC mapping as (SELECT * FROM VALUES
-- MAGIC     ('PRIMARY_DIAGNOSIS_CODE', 1),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_1', 2),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_2', 3),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_3', 4),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_4', 5),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_5', 6),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_6', 7),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_7', 8),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_8', 9),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_9', 10),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_10', 11),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_11', 12),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_12', 13),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_13', 14),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_14', 15),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_15', 16),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_16', 17),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_17', 18),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_18', 19),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_19', 20),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_20', 21),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_21', 22),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_22', 23),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_23', 24),
-- MAGIC     ('SECONDARY_DIAGNOSIS_CODE_24', 25)
-- MAGIC     as mapping(column_name, position))
-- MAGIC SELECT CTE.*, 
-- MAGIC       LEFT ( REGEXP_REPLACE(DIAGNOSIS_CODE,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4 ) AS DIAGNOSIS_CODE_CLEAN, position as DIAGNOSIS_POSITION_INT 
-- MAGIC FROM CTE INNER JOIN mapping ON CTE.DIAGNOSIS_POSITION = mapping.column_name

-- COMMAND ----------

--select diagnosis_code, diagnosis_code_clean, ICD10code, ICD10code_clean, ICD10codeDescr
--from global_temp.icdchecks
--group by diagnosis_code, diagnosis_code_clean, ICD10code, ICD10code_clean, ICD10codeDescr
--order by ICD10code

---diagnosis_code is the original ICD10 code from primary_diagnosis_code to secondary_diagnosis_codex
---diagnosis_code_clean is the resulting ICD10 code from the reg expression transformation
---ICD10code is the original ICD 10 code listed in the calibre codes
---ICD10code_clean is the calibre codes cleaned from punctuation
---ICD10codeDescr is the calibre icd10 description of the code

--There may be old format codes which have not been translated into new ICD-10 style - this would require a lookup for each of the codes we are interested in so could potentially miss some here.

--Regular Expressions used to clean SUS (works for CVD codes)
---LEFT ( REGEXP_REPLACE(primary_diagnosis_code,'[-]|[\.]|[ ]|[\*]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4 ) AS clean_diag_code

---Regular Expressions used to clean ICD-10 codes aka remove punctuation (only dots used really but for completeness)
---LEFT Statement not needed as 4 maximum
---LEFT ( REGEXP_REPLACE(ICD10code,'[.,-,' ']','') , 4 ) as ICD10code_clean
--This is equivalent to REGEXP_REPLACE(ICD10code,'[\.]|[-]|[ ]','') 

--Regular Expression for HES (left statement not required as 4 maximum and cleaned already)
---REGEXP_REPLACE(diag_4_01, '[X]$','') as trunc

-------------------------------------------------------------------------------------------------------------------------------------------

---[A-Za-z\-]*$ Removes any capital letter or lowercase letter at the end of a code OR removes any capital letter or lowercase letter with a - at the end of a code
---[X][A-Z]$ Removes X plus another capital letter at the end of the code [XA-Z]$ probably the same
---[X\-]*$  Removes X- if at the end of a string OR X at the end of a string
---[\*] Removes a * anywhere in the string
---[ ] Removes a space anywhere in the string
---[\.] Removes . anywhere in the string
---[X\] Removes X at the end of the string
---[-]  Removes dashes anywhere in the string
---[X\+]*$ Removes X+ if at the end of a string OR X at the end of a string

-------------------------------------------------------------------------------------------------------------------------------------------
---Need a lookup for ICD codes which have 5 characters to convert to latest ICD-10
--Check Trud

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC 
-- MAGIC spark.sql(F"""create or replace global temp view CCU002_sus_vacc as
-- MAGIC select * from dars_nic_391419_j3w9t_collab.ccu002_vac_sus_diag_stack_vacc
-- MAGIC where DIAGNOSIS_CODE_CLEAN is not NULL 
-- MAGIC and (EPISODE_START_DATE >= '{index_date}'
-- MAGIC and EPISODE_START_DATE <= '{end_date}' and START_DATE_HOSPITAL_PROVIDER_SPELL <= '{end_date}' 
-- MAGIC and AS_AT_DATE <= '{end_date2}'
-- MAGIC )""")

-- COMMAND ----------

create or replace global temp view susoutcomes  AS
SELECT *,
 LEFT ( REGEXP_REPLACE(DIAGNOSIS_CODE_CLEAN,'[X]$','') , 3 ) AS DIAGNOSIS_CODE_CLEAN_trunc
 FROM global_temp.CCU002_sus_vacc
WHERE NHS_NUMBER_DEID is not null 

-- COMMAND ----------

-- MAGIC %md ## icd10

-- COMMAND ----------

create or replace global temp view susoutcomes2 AS 
SELECT *
FROM global_temp.susoutcomes
WHERE (DIAGNOSIS_CODE_CLEAN_trunc = 'I60' OR DIAGNOSIS_CODE_CLEAN_trunc ='I21' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I22' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I23' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I61' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I26' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I81' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I63' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I64' OR DIAGNOSIS_CODE_CLEAN_trunc = 'D65' OR  DIAGNOSIS_CODE_CLEAN_trunc = 'I50' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I65' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I66' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I43' OR DIAGNOSIS_CODE_CLEAN_trunc = 'K55' OR DIAGNOSIS_CODE_CLEAN_trunc = 'G08' OR DIAGNOSIS_CODE_CLEAN_trunc = 'H34' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I74' OR DIAGNOSIS_CODE_CLEAN_trunc = 'S72' OR DIAGNOSIS_CODE_CLEAN_trunc = 'S82' OR DIAGNOSIS_CODE_CLEAN_trunc = 'S92' OR DIAGNOSIS_CODE_CLEAN_trunc = 'T12' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I80'

OR DIAGNOSIS_CODE_CLEAN = 'I808' OR DIAGNOSIS_CODE_CLEAN = 'I809' OR DIAGNOSIS_CODE_CLEAN = 'I710' OR DIAGNOSIS_CODE_CLEAN = 'I670' OR DIAGNOSIS_CODE_CLEAN = 'I241' OR DIAGNOSIS_CODE_CLEAN = 'I801' OR DIAGNOSIS_CODE_CLEAN = 'I803' OR DIAGNOSIS_CODE_CLEAN = 'I828' OR DIAGNOSIS_CODE_CLEAN = 'I820' OR DIAGNOSIS_CODE_CLEAN = 'I829' OR DIAGNOSIS_CODE_CLEAN = 'I822' OR DIAGNOSIS_CODE_CLEAN = 'I802' OR DIAGNOSIS_CODE_CLEAN = 'I823' OR DIAGNOSIS_CODE_CLEAN = 'I676' OR DIAGNOSIS_CODE_CLEAN = 'O225' OR DIAGNOSIS_CODE_CLEAN = 'O873' OR DIAGNOSIS_CODE_CLEAN = 'G951' OR DIAGNOSIS_CODE_CLEAN = 'D693' OR DIAGNOSIS_CODE_CLEAN = 'D694' OR DIAGNOSIS_CODE_CLEAN = 'D695' OR DIAGNOSIS_CODE_CLEAN = 'D696' OR DIAGNOSIS_CODE_CLEAN = 'M311' OR DIAGNOSIS_CODE_CLEAN = 'I472' OR DIAGNOSIS_CODE_CLEAN = 'I490' OR DIAGNOSIS_CODE_CLEAN = 'I460' OR DIAGNOSIS_CODE_CLEAN = 'I469' OR DIAGNOSIS_CODE_CLEAN = 'I470' OR DIAGNOSIS_CODE_CLEAN = 'I461' OR DIAGNOSIS_CODE_CLEAN = 'I110' OR DIAGNOSIS_CODE_CLEAN = 'I130' OR DIAGNOSIS_CODE_CLEAN = 'I132' OR DIAGNOSIS_CODE_CLEAN = 'I420' OR DIAGNOSIS_CODE_CLEAN = 'I426' OR DIAGNOSIS_CODE_CLEAN = 'I255' OR DIAGNOSIS_CODE_CLEAN = 'I423' OR DIAGNOSIS_CODE_CLEAN = 'I425' OR DIAGNOSIS_CODE_CLEAN = 'I427' OR DIAGNOSIS_CODE_CLEAN = 'I428' OR DIAGNOSIS_CODE_CLEAN = 'I429' OR DIAGNOSIS_CODE_CLEAN = 'G450' OR DIAGNOSIS_CODE_CLEAN = 'G451' OR DIAGNOSIS_CODE_CLEAN = 'G452' OR DIAGNOSIS_CODE_CLEAN = 'G453' OR DIAGNOSIS_CODE_CLEAN = 'G454' OR DIAGNOSIS_CODE_CLEAN = 'G458' OR DIAGNOSIS_CODE_CLEAN = 'G459' OR DIAGNOSIS_CODE_CLEAN = 'G460' OR DIAGNOSIS_CODE_CLEAN = 'G461' OR DIAGNOSIS_CODE_CLEAN = 'O882' OR DIAGNOSIS_CODE_CLEAN = 'G462' OR DIAGNOSIS_CODE_CLEAN = 'O871' OR DIAGNOSIS_CODE_CLEAN = 'O223' OR DIAGNOSIS_CODE_CLEAN = 'O879' OR DIAGNOSIS_CODE_CLEAN = 'T025' OR DIAGNOSIS_CODE_CLEAN = 'T023') 



-- COMMAND ----------

-- MAGIC %md ## phenotypes

-- COMMAND ----------

create or replace global temp view susoutcomes3  AS 
SELECT *, 
CASE 
  --WHEN DIAGNOSIS_CODE_CLEAN IN('I241') THEN 'AMI'
  WHEN DIAGNOSIS_CODE_CLEAN like 'I21%' THEN 'AMI'
  WHEN DIAGNOSIS_CODE_CLEAN like 'I22%' THEN 'AMI'
  WHEN DIAGNOSIS_CODE_CLEAN like 'I23%' THEN 'AMI'
  WHEN DIAGNOSIS_CODE_CLEAN IN('O223', 'O871', 'O879', 'O882') THEN 'DVT_pregnancy'
  WHEN DIAGNOSIS_CODE_CLEAN like 'I80%' THEN 'DVT_DVT'
  WHEN DIAGNOSIS_CODE_CLEAN IN('I820','I829', 'I822', 'I823', 'I828') THEN 'other_DVT'
  --WHEN DIAGNOSIS_CODE_CLEAN IN('I828') THEN 'splenic vein thrombosis'
  WHEN DIAGNOSIS_CODE_CLEAN like 'I81%' THEN 'portal_vein_thrombosis'
  WHEN DIAGNOSIS_CODE_CLEAN IN('I260', 'I269') THEN 'PE'
  --WHEN DIAGNOSIS_CODE_CLEAN IN('I269') THEN 'PE_with_acp'
  --WHEN DIAGNOSIS_CODE_CLEAN = 'O882' THEN 'PE_pregnancy'
  --WHEN DIAGNOSIS_CODE_CLEAN IN('I461') THEN 'SCD'
  --WHEN DIAGNOSIS_CODE_CLEAN IN('I710', 'I670') THEN 'artery_dissect'
  --WHEN DIAGNOSIS_CODE_CLEAN IN('I420', 'I426', 'I255', 'I423', 'I425', 'I427', 'I428', 'I429') THEN 'cardiomyopathy'
  --WHEN DIAGNOSIS_CODE_CLEAN like 'I43%' THEN 'cardiomyopathy'
  --WHEN DIAGNOSIS_CODE_CLEAN IN('I472', 'I490', 'I460', 'I470', 'I469') THEN 'life_arrhythmias'
  --WHEN DIAGNOSIS_CODE_CLEAN IN('I110', 'I130', 'I132') THEN 'HF'
  --WHEN DIAGNOSIS_CODE_CLEAN like 'I50%' THEN 'HF'
  WHEN DIAGNOSIS_CODE_CLEAN like 'I60%' THEN 'stroke_SAH_HS'
  WHEN DIAGNOSIS_CODE_CLEAN like 'I61%' THEN 'stroke_SAH_HS'
  WHEN DIAGNOSIS_CODE_CLEAN IN ('I630','I631','I632', 'I633', 'I634', 'I635', 'I638', 'I639') THEN 'stroke_isch'
  WHEN DIAGNOSIS_CODE_CLEAN like 'I64%' THEN 'stroke_isch'
  --WHEN DIAGNOSIS_CODE_CLEAN IN('G450', 'G451', 'G452' 'G453', 'G454', 'G458', 'G459', 'G460', 'G461', 'G462') THEN 'stroke_TIA'
  --WHEN DIAGNOSIS_CODE_CLEAN like 'I65%' THEN 'stroke_TIA'
  --WHEN DIAGNOSIS_CODE_CLEAN like 'I66%' THEN 'stroke_TIA'
  WHEN DIAGNOSIS_CODE_CLEAN IN('G951') THEN 'stroke_isch'
  WHEN DIAGNOSIS_CODE_CLEAN like 'G08%' THEN 'DVT_ICVT'
  WHEN DIAGNOSIS_CODE_CLEAN IN('I676', 'I636') THEN 'DVT_ICVT'
  WHEN DIAGNOSIS_CODE_CLEAN IN('O225', 'O873') THEN 'ICVT_pregnancy'
  WHEN DIAGNOSIS_CODE_CLEAN IN('K550','K559') THEN 'mesenteric_thrombus'
  WHEN DIAGNOSIS_CODE_CLEAN IN('M311') THEN 'TTP'
  WHEN DIAGNOSIS_CODE_CLEAN IN('D693', 'D694', 'D695', 'D696') THEN 'thrombocytopenia'
  --WHEN DIAGNOSIS_CODE_CLEAN IN('D685', 'D686') THEN 'thrombophilia'
  WHEN DIAGNOSIS_CODE_CLEAN like 'D65%' THEN 'DIC'
  WHEN DIAGNOSIS_CODE_CLEAN like 'H34%' THEN 'stroke_isch'
  WHEN DIAGNOSIS_CODE_CLEAN like 'I74%' THEN 'other_arterial_embolism'
  WHEN DIAGNOSIS_CODE_CLEAN like 'S72%' THEN 'fracture'
  WHEN DIAGNOSIS_CODE_CLEAN like 'S82%' THEN 'fracture'
  WHEN DIAGNOSIS_CODE_CLEAN IN('S920', 'S921', 'S922', 'S923', 'S927', 'S929') THEN 'fracture'
  WHEN DIAGNOSIS_CODE_CLEAN like('T12%') THEN 'fracture'
  WHEN DIAGNOSIS_CODE_CLEAN IN('T025', 'T023') THEN 'fracture'
  ELSE NULL
END AS SUSNAME
from global_temp.susoutcomes2

  

-- COMMAND ----------

create or replace global temp view susoutcomes4  AS 
SELECT *, 
CASE 
  --WHEN DIAGNOSIS_CODE_CLEAN IN('I241') THEN 'myocardial infarction'
  WHEN DIAGNOSIS_CODE_CLEAN like 'I21%' THEN 'myocardial infarction'
  WHEN DIAGNOSIS_CODE_CLEAN like 'I22%' THEN 'myocardial infarction'
  WHEN DIAGNOSIS_CODE_CLEAN like 'I23%' THEN 'myocardial infarction'
  WHEN DIAGNOSIS_CODE_CLEAN IN('O223', 'O871', 'O879', 'O882') THEN 'thrombosis during pregnancy and puerperium'
  WHEN DIAGNOSIS_CODE_CLEAN like 'I80%' THEN 'Phlebitis and thrombophlebitis'
  WHEN DIAGNOSIS_CODE_CLEAN IN('I820','I829', 'I822', 'I823', 'I828') THEN 'other vein thrombosis'
  --WHEN DIAGNOSIS_CODE_CLEAN IN('I828') THEN 'splenic vein thrombosis'
  WHEN DIAGNOSIS_CODE_CLEAN like 'I81%' THEN 'portal vein thrombosis'
  WHEN DIAGNOSIS_CODE_CLEAN IN('I260', 'I269') THEN 'pulmonary embolism'
  --WHEN DIAGNOSIS_CODE_CLEAN IN('I269') THEN 'pulmonary embolism with acute cor pulmonale'
  --WHEN DIAGNOSIS_CODE_CLEAN IN('O882') THEN 'Obstetric blood-clot embolism'
  --WHEN DIAGNOSIS_CODE_CLEAN IN('I461') THEN 'SCD'
  --WHEN DIAGNOSIS_CODE_CLEAN IN('I710', 'I670') THEN 'artery_dissect'
  --WHEN DIAGNOSIS_CODE_CLEAN IN('I420', 'I426', 'I255', 'I423', 'I425', 'I427', 'I428', 'I429') THEN 'cardiomyopathy'
  --WHEN DIAGNOSIS_CODE_CLEAN like 'I43%' THEN 'cardiomyopathy'
  --WHEN DIAGNOSIS_CODE_CLEAN IN('I472', 'I490', 'I460', 'I470', 'I469') THEN 'life_arrhythmias'
  --WHEN DIAGNOSIS_CODE_CLEAN IN('I110', 'I130', 'I132') THEN 'HF'
  --WHEN DIAGNOSIS_CODE_CLEAN like 'I50%' THEN 'HF'
  WHEN DIAGNOSIS_CODE_CLEAN like 'I60%' THEN 'Nontraumatic subarachnoid hemorrhage'
  WHEN DIAGNOSIS_CODE_CLEAN like 'I61%' THEN 'Nontraumatic intracerebral hemorrhage'
  WHEN DIAGNOSIS_CODE_CLEAN IN ('I630','I631','I632', 'I633', 'I634', 'I635', 'I638', 'I639') THEN 'cerebral infarction'
  WHEN DIAGNOSIS_CODE_CLEAN like 'I64%' THEN 'stroke, not specified as haemorrhage or infarction'
  ---WHEN DIAGNOSIS_CODE_CLEAN IN('G450', 'G451', 'G452' 'G453', 'G454', 'G458', 'G459', 'G460', 'G461', 'G462') THEN 'stroke_TIA'
  ---WHEN DIAGNOSIS_CODE_CLEAN like 'I65%' THEN 'stroke_TIA'
  ---WHEN DIAGNOSIS_CODE_CLEAN like 'I66%' THEN 'stroke_TIA'
  WHEN DIAGNOSIS_CODE_CLEAN IN('G951') THEN 'vascular myelopathies (arterial or venous)'
  WHEN DIAGNOSIS_CODE_CLEAN like 'G08%' THEN 'intracranial venous thrombosis'
  WHEN DIAGNOSIS_CODE_CLEAN IN('I676', 'I636') THEN 'intracranial venous thrombosis'
  WHEN DIAGNOSIS_CODE_CLEAN IN('O225', 'O873') THEN 'intracranial venous thrombosis in pregnancy and puerperium'
  WHEN DIAGNOSIS_CODE_CLEAN IN('K550', 'K559') THEN 'acute vascular disorders of intestine'
  WHEN DIAGNOSIS_CODE_CLEAN IN('M311') THEN 'thrombotic microangiopathy'
  WHEN DIAGNOSIS_CODE_CLEAN IN('D693', 'D694', 'D695', 'D696') THEN 'thrombocytopenia'
  --WHEN DIAGNOSIS_CODE_CLEAN IN('D685', 'D686') THEN 'thrombophilia'
  WHEN DIAGNOSIS_CODE_CLEAN like 'D65%' THEN 'disseminated intravascular coagulation'
  WHEN DIAGNOSIS_CODE_CLEAN like 'H34%' THEN 'retinal infarction'
  WHEN DIAGNOSIS_CODE_CLEAN like 'I74%' THEN 'arterial embolism and thrombosis'
  WHEN DIAGNOSIS_CODE_CLEAN like 'S72%' THEN 'upper leg fracture'
  WHEN DIAGNOSIS_CODE_CLEAN like 'S82%' THEN 'lower leg fracture'
  WHEN DIAGNOSIS_CODE_CLEAN IN('S920', 'S921', 'S922', 'S923', 'S927', 'S929') THEN 'foot fracture'
  WHEN DIAGNOSIS_CODE_CLEAN like('T12%') THEN 'fracture of lower limb'
  WHEN DIAGNOSIS_CODE_CLEAN IN('T025', 'T023') THEN 'fractures involving multiple regions of both lower limbs'
  ELSE NULL
END AS term
from global_temp.susoutcomes3

  

-- COMMAND ----------

create or replace global temp view ccu002_susoutcome5 AS 
SELECT * 
from global_temp.susoutcomes4 
where SUSNAME is not NULL

-- COMMAND ----------

-- MAGIC %md ## FIRST DIAGNOSIS POSITION

-- COMMAND ----------

create or replace global temp view ccu002_susoutcome5_first_diag as
from global_temp.ccu002_susoutcome5
where DIAGNOSIS_POSITION_INT=1

-- COMMAND ----------

--first of everything: First hospital spell, first episode start date with the first spell, first episode number (a patient may have multiple episode with the same number within the same date if they move hospitals) and the first generate_record_id.
create or replace global temp view ccu002_susoutcomes5_first as 

with cte as (
    SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY NHS_NUMBER_DEID, SUSNAME ORDER BY START_DATE_HOSPITAL_PROVIDER_SPELL ASC, 
                                                                                      EPISODE_START_DATE ASC,
                                                                                          EPISODE_NUMBER ASC,
                                                                                     GENERATED_RECORD_ID ASC) AS seq
FROM global_temp.ccu002_susoutcome5_first_diag
)

SELECT *
FROM cte 
WHERE seq = 1

-- COMMAND ----------

create or replace global temp view ccu002_vaccine_susoutcomes_final AS 
select NHS_NUMBER_DEID, EPISODE_START_DATE as record_date, START_DATE_HOSPITAL_PROVIDER_SPELL as spell_start, END_DATE_HOSPITAL_PROVIDER_SPELL as spell_end, SUSNAME as name, term, 'sus' as SOURCE, 'ICD10' as terminology, DIAGNOSIS_CODE_CLEAN as code, DIAGNOSIS_POSITION_INT as diagnosis_position 
from global_temp.ccu002_susoutcomes5_first




-- COMMAND ----------

-- MAGIC %python
-- MAGIC create_table(table_name=final_table_1, database_name='dars_nic_391419_j3w9t_collab', select_sql_script=f"SELECT * FROM global_temp.ccu002_vaccine_susoutcomes_final")
-- MAGIC 
-- MAGIC #final_table is a parameter name defined at the start so can be used for infection and vaccine

-- COMMAND ----------

-- MAGIC %md ## ANY DIAGNOSIS POSITION

-- COMMAND ----------

--first of everything: First hospital spell, first episode start date with the first spell, first episode number (a patient may have multiple episode with the same number within the same date if they move hospitals) and the first generate_record_id.
create or replace global temp view ccu002_susoutcomes5_anydiag as

with cte as (
    SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY NHS_NUMBER_DEID, SUSNAME ORDER BY START_DATE_HOSPITAL_PROVIDER_SPELL ASC, 
                                                                                      EPISODE_START_DATE ASC,
                                                                                          EPISODE_NUMBER ASC,
                                                                                     GENERATED_RECORD_ID ASC) AS seq
FROM global_temp.ccu002_susoutcome5
)

SELECT *
FROM cte 
WHERE seq = 1

-- COMMAND ----------

create or replace global temp view ccu002_vaccine_susoutcomes_final_anydiag AS 
select NHS_NUMBER_DEID, EPISODE_START_DATE as record_date, START_DATE_HOSPITAL_PROVIDER_SPELL as spell_start, END_DATE_HOSPITAL_PROVIDER_SPELL as spell_end, SUSNAME as name, term, 'sus' as SOURCE, 'ICD10' as terminology, DIAGNOSIS_CODE_CLEAN as code, DIAGNOSIS_POSITION_INT as diagnosis_position from global_temp.ccu002_susoutcomes5_anydiag

-- COMMAND ----------

-- MAGIC %python
-- MAGIC create_table(table_name=final_table_2, database_name='dars_nic_391419_j3w9t_collab', select_sql_script=f"SELECT * FROM global_temp.ccu002_vaccine_susoutcomes_final_anydiag")
