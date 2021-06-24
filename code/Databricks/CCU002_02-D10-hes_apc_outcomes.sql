-- Databricks notebook source
-- MAGIC %md # CCU002_02-D10-hes_apc_outcomes
-- MAGIC 
-- MAGIC **Description** This notebook generates the outcomes listed in the CCU002 vaccination protocol using the HES APC dataset.
-- MAGIC 
-- MAGIC **Author(s)** Spencer Keene, Jenny Cooper

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Vaccine parameters

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC #Dataset parameters
-- MAGIC hes_data = 'dars_nic_391419_j3w9t_collab.ccu002_vac_hes_apc_all_years'
-- MAGIC 
-- MAGIC #Date parameters
-- MAGIC from datetime import date
-- MAGIC 
-- MAGIC index_date = '2020-12-08'
-- MAGIC 
-- MAGIC today = date.today()
-- MAGIC end_date = today.strftime('%Y-%m-%d')
-- MAGIC 
-- MAGIC #Other data inputs
-- MAGIC skinny_QA_inclusion_table = 'dars_nic_391419_j3w9t_collab.ccu002_vac_included_patients' 
-- MAGIC #This should be the skinny patient table that has undergone QA and inclusion/exclusion
-- MAGIC 
-- MAGIC 
-- MAGIC #Final table name
-- MAGIC final_table = 'ccu002_vac_outcomes_hes_final' 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #####Infection parameters

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC #Dataset parameters
-- MAGIC hes_data = 'dars_nic_391419_j3w9t_collab.ccu002_vac_hes_apc_all_years'
-- MAGIC 
-- MAGIC #Date parameters
-- MAGIC index_date = '2020-01-01'
-- MAGIC end_date = '2020-12-07'
-- MAGIC 
-- MAGIC #Other data inputs
-- MAGIC skinny_QA_inclusion_table = 'dars_nic_391419_j3w9t_collab.ccu002_inf_included_patients' 
-- MAGIC #This should be the skinny patient table that has undergone QA and inclusion/exclusion
-- MAGIC 
-- MAGIC #Final table name
-- MAGIC final_table = 'ccu002_inf_outcomes_hes_final' 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC # Define create table function by Sam Hollings
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
-- MAGIC   

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC spark.sql(F"""
-- MAGIC create or replace global temp view ccu002_vacc_hes_apc as
-- MAGIC select * 
-- MAGIC from {hes_data}""")

-- COMMAND ----------


CREATE OR REPLACE GLOBAL TEMP VIEW hes_diag_stack_vacc AS
WITH CTE AS (
    SELECT PERSON_ID_DEID, SPELBGIN, EPISTART, EPIKEY, ADMIDATE, DISDATE, ADMIMETH, ADMINCAT, STACK(20,
    'DIAG_4_01', DIAG_4_01,
    'DIAG_4_02', DIAG_4_02,
    'DIAG_4_03', DIAG_4_03,
    'DIAG_4_04', DIAG_4_04,
    'DIAG_4_05', DIAG_4_05,
    'DIAG_4_06', DIAG_4_06,
    'DIAG_4_07', DIAG_4_07,
    'DIAG_4_08', DIAG_4_08,
    'DIAG_4_09', DIAG_4_09,
    'DIAG_4_10', DIAG_4_10,
    'DIAG_4_11', DIAG_4_11,
    'DIAG_4_12', DIAG_4_12,
    'DIAG_4_13', DIAG_4_13,
    'DIAG_4_14', DIAG_4_14,
    'DIAG_4_15', DIAG_4_15,
    'DIAG_4_16', DIAG_4_16,
    'DIAG_4_17', DIAG_4_17,
    'DIAG_4_18', DIAG_4_18,
    'DIAG_4_19', DIAG_4_19,
    'DIAG_4_20', DIAG_4_20
        ) as (DIAGNOSIS_POSITION, DIAGNOSIS_CODE)
    FROM global_temp.ccu002_vacc_hes_apc),
mapping as (SELECT * FROM VALUES
    ('DIAG_4_01', 1),
    ('DIAG_4_02', 2),
    ('DIAG_4_03', 3),
    ('DIAG_4_04', 4),
    ('DIAG_4_05', 5),
    ('DIAG_4_06', 6),
    ('DIAG_4_07', 7),
    ('DIAG_4_08', 8),
    ('DIAG_4_09', 9),
    ('DIAG_4_10', 10),
    ('DIAG_4_11', 11),
    ('DIAG_4_12', 12),
    ('DIAG_4_13', 13),
    ('DIAG_4_14', 14),
    ('DIAG_4_15', 15),
    ('DIAG_4_16', 16),
    ('DIAG_4_17', 17),
    ('DIAG_4_18', 18),
    ('DIAG_4_19', 19),
    ('DIAG_4_20', 20)
    as mapping(column_name, position))
SELECT CTE.*, 
      LEFT ( REGEXP_REPLACE(DIAGNOSIS_CODE,'[-]|[\.]|[X\+]*$|[ ]|[\*]|[X\]|[X\-]*$|[X][A-Z]$|[A-Za-z\-]*$',''), 4 ) AS DIAGNOSIS_CODE_CLEAN, position as DIAGNOSIS_POSITION_INT
FROM CTE INNER JOIN mapping ON CTE.DIAGNOSIS_POSITION = mapping.column_name




-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC spark.sql(F"""create or replace global temp view CCU002_hes_vacc as
-- MAGIC select * from global_temp.hes_diag_stack_vacc
-- MAGIC where DIAGNOSIS_CODE_CLEAN is not NULL 
-- MAGIC and (EPISTART >= '{index_date}' and EPISTART <='{end_date}'
-- MAGIC and ADMIDATE <= '{end_date}')""")

-- COMMAND ----------

create or replace global temp view ccu002_hes_outcomes  AS
SELECT *,
 LEFT ( REGEXP_REPLACE(DIAGNOSIS_CODE_CLEAN,'[X]$','') , 3 ) AS DIAGNOSIS_CODE_CLEAN_trunc
 FROM global_temp.CCU002_hes_vacc
WHERE PERSON_ID_DEID is not null


-- COMMAND ----------

create or replace global temp view ccu002_hes_outcomes2 AS 
SELECT *
FROM global_temp.ccu002_hes_outcomes
WHERE (DIAGNOSIS_CODE_CLEAN_trunc = 'I60' OR DIAGNOSIS_CODE_CLEAN_trunc ='I21' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I22' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I23' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I61' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I26' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I81' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I63' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I64' OR DIAGNOSIS_CODE_CLEAN_trunc = 'D65' OR  DIAGNOSIS_CODE_CLEAN_trunc = 'I50' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I65' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I66' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I43' OR DIAGNOSIS_CODE_CLEAN_trunc = 'K55' OR DIAGNOSIS_CODE_CLEAN_trunc = 'G08' OR DIAGNOSIS_CODE_CLEAN_trunc = 'H34' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I74' OR DIAGNOSIS_CODE_CLEAN_trunc = 'S72' OR DIAGNOSIS_CODE_CLEAN_trunc = 'S82' OR DIAGNOSIS_CODE_CLEAN_trunc = 'S92' OR DIAGNOSIS_CODE_CLEAN_trunc = 'T12' OR DIAGNOSIS_CODE_CLEAN_trunc = 'I80'

OR DIAGNOSIS_CODE_CLEAN = 'I808' OR DIAGNOSIS_CODE_CLEAN = 'I809' OR DIAGNOSIS_CODE_CLEAN = 'I710' OR DIAGNOSIS_CODE_CLEAN = 'I670' OR DIAGNOSIS_CODE_CLEAN = 'I241' OR DIAGNOSIS_CODE_CLEAN = 'I801' OR DIAGNOSIS_CODE_CLEAN = 'I803' OR DIAGNOSIS_CODE_CLEAN = 'I828' OR DIAGNOSIS_CODE_CLEAN = 'I820' OR DIAGNOSIS_CODE_CLEAN = 'I829' OR DIAGNOSIS_CODE_CLEAN = 'I822' OR DIAGNOSIS_CODE_CLEAN = 'I802' OR DIAGNOSIS_CODE_CLEAN = 'I823' OR DIAGNOSIS_CODE_CLEAN = 'I676' OR DIAGNOSIS_CODE_CLEAN = 'O225' OR DIAGNOSIS_CODE_CLEAN = 'O873' OR DIAGNOSIS_CODE_CLEAN = 'G951' OR DIAGNOSIS_CODE_CLEAN = 'D693' OR DIAGNOSIS_CODE_CLEAN = 'D694' OR DIAGNOSIS_CODE_CLEAN = 'D695' OR DIAGNOSIS_CODE_CLEAN = 'D696' OR DIAGNOSIS_CODE_CLEAN = 'M311' OR DIAGNOSIS_CODE_CLEAN = 'I472' OR DIAGNOSIS_CODE_CLEAN = 'I490' OR DIAGNOSIS_CODE_CLEAN = 'I460' OR DIAGNOSIS_CODE_CLEAN = 'I469' OR DIAGNOSIS_CODE_CLEAN = 'I470' OR DIAGNOSIS_CODE_CLEAN = 'I461' OR DIAGNOSIS_CODE_CLEAN = 'I110' OR DIAGNOSIS_CODE_CLEAN = 'I130' OR DIAGNOSIS_CODE_CLEAN = 'I132' OR DIAGNOSIS_CODE_CLEAN = 'I420' OR DIAGNOSIS_CODE_CLEAN = 'I426' OR DIAGNOSIS_CODE_CLEAN = 'I255' OR DIAGNOSIS_CODE_CLEAN = 'I423' OR DIAGNOSIS_CODE_CLEAN = 'I425' OR DIAGNOSIS_CODE_CLEAN = 'I427' OR DIAGNOSIS_CODE_CLEAN = 'I428' OR DIAGNOSIS_CODE_CLEAN = 'I429' OR DIAGNOSIS_CODE_CLEAN = 'G450' OR DIAGNOSIS_CODE_CLEAN = 'G451' OR DIAGNOSIS_CODE_CLEAN = 'G452' OR DIAGNOSIS_CODE_CLEAN = 'G453' OR DIAGNOSIS_CODE_CLEAN = 'G454' OR DIAGNOSIS_CODE_CLEAN = 'G458' OR DIAGNOSIS_CODE_CLEAN = 'G459' OR DIAGNOSIS_CODE_CLEAN = 'G460' OR DIAGNOSIS_CODE_CLEAN = 'G461' OR DIAGNOSIS_CODE_CLEAN = 'O882' OR DIAGNOSIS_CODE_CLEAN = 'G462' OR DIAGNOSIS_CODE_CLEAN = 'O871' OR DIAGNOSIS_CODE_CLEAN = 'O223' OR DIAGNOSIS_CODE_CLEAN = 'O879' OR DIAGNOSIS_CODE_CLEAN = 'T025' OR DIAGNOSIS_CODE_CLEAN = 'T023') 



-- COMMAND ----------

create or replace global temp view ccu002_hes_outcomes3  AS 
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
END AS HESNAME
from global_temp.ccu002_hes_outcomes2

  

-- COMMAND ----------

create or replace global temp view ccu002_hes_outcomes4  AS 
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
from global_temp.ccu002_hes_outcomes3


  

-- COMMAND ----------

create or replace global temp view hes_outcomes5 AS 
SELECT * 
from global_temp.ccu002_hes_outcomes4
where HESNAME is not NULL



-- COMMAND ----------

--IGNORE THIS IF LOOKING AT ALL DIAGNOSIS POSITIONS need to change the global_temp of the below FROM statemnt to global_temp.susoutcomes5
create or replace global temp view ccu002_hesoutcome5_first_diag as
from global_temp.hes_outcomes5
where DIAGNOSIS_POSITION_INT=1

-- COMMAND ----------

create or replace global temp view ccu002_vaccine_hes_outcomes5_first AS 

with cte as (
    SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY PERSON_ID_DEID, HESNAME ORDER BY ADMIDATE ASC, 
                                                                     EPISTART ASC) AS seq
                                                                                           
FROM global_temp.ccu002_hesoutcome5_first_diag
)

SELECT *
FROM cte



-- COMMAND ----------

create or replace global temp view ccu002_vaccine_hesoutcomes_final AS 
select PERSON_ID_DEID as NHS_NUMBER_DEID, /*SPELBGIN,*/ EPISTART as record_date, ADMIDATE as spell_start, DISDATE as spell_end, HESNAME as name, term, 'hes' as SOURCE, 'ICD10' as terminology, DIAGNOSIS_CODE_CLEAN as code, DIAGNOSIS_POSITION_INT as diagnosis_position from global_temp.ccu002_vaccine_hes_outcomes5_first 
where seq=1 


-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC create_table(table_name=final_table, database_name='dars_nic_391419_j3w9t_collab', select_sql_script=f"SELECT * FROM global_temp.ccu002_vaccine_hesoutcomes_final")
-- MAGIC 
-- MAGIC #final_table is a parameter name defined at the start so can be used for infection and vaccine
