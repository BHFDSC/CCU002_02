-- Databricks notebook source
-- MAGIC %md # CCU002_02-D14-thrombo_plus_outcomes
-- MAGIC  
-- MAGIC **Description** This notebook produces a table for combination of thrombocytopenia plus each individual outcome within the same spell.
-- MAGIC 
-- MAGIC **Author(s)** Spencer Keene

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

-- MAGIC %py
-- MAGIC 
-- MAGIC #Dataset parameters
-- MAGIC #These tables were dervied from the separate notebooks for each of these outcomes
-- MAGIC gdppr_table = 'dars_nic_391419_j3w9t_collab.ccu002_vac_outcomes_gdppr_final'
-- MAGIC hes_table = 'dars_nic_391419_j3w9t_collab.ccu002_vac_outcomes_hes_final'
-- MAGIC sus_table = 'dars_nic_391419_j3w9t_collab.ccu002_vac_outcomes_sus_final' #TWO FOR SUS TABLES 
-- MAGIC sus_table2 = 'dars_nic_391419_j3w9t_collab.ccu002_vac_outcomes_sus_final_anydiag'
-- MAGIC deaths_table = 'dars_nic_391419_j3w9t_collab.ccu002_vac_outcomes_deaths_final'
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
-- MAGIC #skinny patient table that has undergone QA and inclusion/exclusion
-- MAGIC 
-- MAGIC 
-- MAGIC #Final table names
-- MAGIC final_table_1 = 'ccu002_vac_outcomes_allsources_firstdiag_final' 
-- MAGIC #final_table_2 = 'ccu002_vac_outcomes_allsources_firstdiag_final'
-- MAGIC final_table_2 = 'ccu002_vac_combined_outcomes_firstdiag_thrombo'
-- MAGIC final_table_3 = 'ccu002_vac_outcomes_allsources_firstdiag_final_wohes' 
-- MAGIC final_table_4 = 'ccu002_vac_combined_outcomes_firstdiag_thrombo_wohes'

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC 
-- MAGIC spark.sql(F"""create or replace global temp view ccu002_vaccine_allsources_outcomes_wohes_individual_events  AS
-- MAGIC 
-- MAGIC with cte_alloutcomes as (
-- MAGIC SELECT *
-- MAGIC FROM {deaths_table}
-- MAGIC UNION
-- MAGIC SELECT *
-- MAGIC FROM {sus_table}
-- MAGIC UNION
-- MAGIC SELECT *
-- MAGIC FROM {gdppr_table}
-- MAGIC --UNION
-- MAGIC --SELECT *
-- MAGIC --FROM {hes_table}
-- MAGIC where name is not null 
-- MAGIC )
-- MAGIC 
-- MAGIC SELECT NHS_NUMBER_DEID, record_date, spell_start, name, term, SOURCE, terminology, code, diagnosis_position
-- MAGIC from cte_alloutcomes""")

-- COMMAND ----------

create or replace global temp view 
--ccu002_vaccine_allsources_outcomes_final_firstdiag_wohes as
ccu002_vaccine_allsources_outcomes_firstdiag_wohes_individual_events  AS
select NHS_NUMBER_DEID, record_date, name, spell_start, term, SOURCE, terminology, code, diagnosis_position
FROM global_temp.ccu002_vaccine_allsources_outcomes_wohes_individual_events  
where diagnosis_position = 1 or diagnosis_position = 'NA'


-- COMMAND ----------

---- First event of each phenotype

create or replace global temp view 
--ccu002_vaccine_allsources_outcomes_first_wohes AS
ccu002_vaccine_allsources_outcomes_final_wohes_individual_events  as

with cte_names as (
    SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY NHS_NUMBER_DEID, name ORDER BY record_date ASC) AS seq
FROM global_temp.ccu002_vaccine_allsources_outcomes_firstdiag_wohes_individual_events 
--FROM global_temp.ccu002_vaccine_allsources_outcomes_firstdiag_wohes
)

SELECT NHS_NUMBER_DEID, record_date as individ_record_date, spell_start as individ_spell_start, name as individ_name, term as individ_term, SOURCE as individ_source, terminology as individ_terminology, code as individ_code, diagnosis_position as individ_diagnosis_position 
FROM cte_names WHERE seq = 1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(F"""create or replace global temp view ccu002_vaccine_combined_thrombo_individual_events  as
-- MAGIC select *
-- MAGIC from {sus_table2}
-- MAGIC where name='thrombocytopenia' 
-- MAGIC --and diagnosis_position>1
-- MAGIC """)

-- COMMAND ----------

create or replace global temp view ccu002_vacc_combined_outcomes_thrombo_wohes_individual_events  AS

with cte_combined as (
select individ.NHS_NUMBER_DEID, 
individ.individ_record_date, individ.individ_spell_start, individ.individ_name, individ.individ_source, 


thrombo.record_date, thrombo.spell_start, thrombo.name, thrombo.term, thrombo.diagnosis_position


from global_temp.ccu002_vaccine_allsources_outcomes_final_wohes_individual_events individ
LEFT JOIN global_temp.ccu002_vaccine_combined_thrombo_individual_events thrombo
ON individ.NHS_NUMBER_DEID = thrombo.NHS_NUMBER_DEID
)

select NHS_NUMBER_DEID, individ_record_date, individ_spell_start, individ_name, individ_source, 
record_date, spell_start, name, term, diagnosis_position,

(case when individ_spell_start=spell_start AND individ_name='mesenteric_thrombus' THEN 1 ELSE 0 END) AS THROMBO_plus_MESENT, 
(case when individ_spell_start=spell_start AND individ_name='DIC' THEN 1 ELSE 0 END) AS THROMBO_plus_DIC,
(case when individ_spell_start=spell_start AND individ_name='stroke_isch' THEN 1 ELSE 0 END) AS THROMBO_plus_stroke_isch, 
(case when individ_spell_start=spell_start AND individ_name='other_arterial_embolism' THEN 1 ELSE 0 END) AS THROMBO_plus_other_art,
(case when individ_spell_start=spell_start AND individ_name='AMI' THEN 1 ELSE 0 END) AS THROMBO_plus_AMI, 
(case when individ_spell_start=spell_start AND individ_name='PE' THEN 1 ELSE 0 END) AS THROMBO_plus_PE,
(case when individ_spell_start=spell_start AND individ_name='portal_vein_thrombosis' THEN 1 ELSE 0 END) AS THROMBO_plus_portal, 
(case when individ_spell_start=spell_start AND individ_name='stroke_SAH_HS' THEN 1 ELSE 0 END) AS THROMBO_plus_stroke_SAH_HS,
(case when individ_spell_start=spell_start AND individ_name='DVT_DVT' THEN 1 ELSE 0 END) AS THROMBO_plus_DVT, 
(case when individ_spell_start=spell_start AND individ_name='ICVT_pregnancy' THEN 1 ELSE 0 END) AS THROMBO_plus_ICVT_preg,
(case when individ_spell_start=spell_start AND individ_name='other_DVT' THEN 1 ELSE 0 END) AS THROMBO_plus_other_DVT, 
(case when individ_spell_start=spell_start AND individ_name='DVT_ICVT' THEN 1 ELSE 0 END) AS THROMBO_plus_DVT_ICVT,
(case when individ_spell_start=spell_start AND individ_name='DVT_pregnancy' THEN 1 ELSE 0 END) AS THROMBO_plus_DVT_preg, 
(case when individ_spell_start=spell_start AND individ_name='fracture' THEN 1 ELSE 0 END) AS THROMBO_plus_fracture,
(case when individ_spell_start=spell_start AND individ_name='TTP' THEN 1 ELSE 0 END) AS THROMBO_plus_TTP

from cte_combined


-- COMMAND ----------


/*
mesenteric_thrombus

DIC

stroke_isch

other_arterial_embolism

PE

AMI

portal_vein_thrombosis

stroke_SAH_HS

ICVT_pregnancy

other_DVT

DVT_ICVT

DVT_pregnancy

DVT_DVT

fracture

thrombocytopenia

TTP
*/

-- COMMAND ----------

--CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_vac_combined_outcomes_longformat_individual_events AS
CREATE TABLE dars_nic_391419_j3w9t_collab.ccu002_vac_thrombo_outcomes_longformat_individual_events AS
WITH CTE AS (
    SELECT NHS_NUMBER_DEID,  
    STACK(15,
    individ_record_date, 'THROMBO_plus_MESENT', THROMBO_plus_MESENT,
    individ_record_date, 'THROMBO_plus_DIC', THROMBO_plus_DIC,
    individ_record_date, 'THROMBO_plus_stroke_isch', THROMBO_plus_stroke_isch,
    individ_record_date,'THROMBO_plus_other_art', THROMBO_plus_other_art,
    individ_record_date,'THROMBO_plus_AMI', THROMBO_plus_AMI,
    individ_record_date, 'THROMBO_plus_PE', THROMBO_plus_PE,
    individ_record_date, 'THROMBO_plus_portal', THROMBO_plus_portal,
    individ_record_date, 'THROMBO_plus_stroke_SAH_HS', THROMBO_plus_stroke_SAH_HS,
    individ_record_date,'THROMBO_plus_DVT', THROMBO_plus_DVT,
    individ_record_date,'THROMBO_plus_ICVT_preg', THROMBO_plus_ICVT_preg,
    individ_record_date, 'THROMBO_plus_other_DVT', THROMBO_plus_other_DVT,
    individ_record_date, 'THROMBO_plus_DVT_ICVT', THROMBO_plus_DVT_ICVT,
    individ_record_date, 'THROMBO_plus_DVT_preg', THROMBO_plus_DVT_preg,
    individ_record_date,'THROMBO_plus_fracture', THROMBO_plus_fracture,
    individ_record_date,'THROMBO_plus_TTP', THROMBO_plus_TTP
    
        
    ) as (record_date, composite_event_type, composite_event)
    FROM global_temp.ccu002_vacc_combined_outcomes_thrombo_wohes_individual_events)
    
    
SELECT NHS_NUMBER_DEID, record_date, composite_event_type
FROM CTE --INNER JOIN mapping ON CTE.DIAGNOSIS_POSITION = mapping.column_name
where composite_event =1

-- COMMAND ----------

select * 

from dars_nic_391419_j3w9t_collab.ccu002_vac_thrombo_outcomes_longformat_individual_events

-- COMMAND ----------

select count(distinct NHS_NUMBER_DEID) as count, composite_event_type from dars_nic_391419_j3w9t_collab.ccu002_vac_thrombo_outcomes_longformat_individual_events group by composite_event_type
