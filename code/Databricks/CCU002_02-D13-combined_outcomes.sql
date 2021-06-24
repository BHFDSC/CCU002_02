-- Databricks notebook source
-- MAGIC %md # CCU002_02-D13-combined_outcomes
-- MAGIC 
-- MAGIC **Description** This notebook combines all the outcomes listed in the CCU002 vaccination protocol from deaths, sus, gdppr and HES.
-- MAGIC 
-- MAGIC **Author(s)** Spencer Keene, Rachel Denholm, Jenny Cooper

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Vaccine parameters

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC #Dataset parameters
-- MAGIC #These tables were dervied from the separate notebooks for each of these outcomes
-- MAGIC gdppr_table = 'dars_nic_391419_j3w9t_collab.ccu002_vac_outcomes_gdppr_final'
-- MAGIC hes_table = 'dars_nic_391419_j3w9t_collab.ccu002_vac_outcomes_hes_final'
-- MAGIC sus_table = 'dars_nic_391419_j3w9t_collab.ccu002_vac_outcomes_sus_final' 
-- MAGIC sus_table2 = 'dars_nic_391419_j3w9t_collab.ccu002_vac_outcomes_sus_final_anydiag' #TWO TABLES FOR SUS 
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
-- MAGIC final_table_5 = 'ccu002_vac_outcomes_allsources_anydiag_final_wohes' 
-- MAGIC                 

-- COMMAND ----------

-- MAGIC %md 
-- MAGIC #####Infection parameters

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC #Dataset parameters
-- MAGIC #These tables were dervied from the separate notebooks for each of these outcomes
-- MAGIC gdppr_table = 'dars_nic_391419_j3w9t_collab.ccu002_inf_outcomes_gdppr_final'
-- MAGIC hes_table = 'dars_nic_391419_j3w9t_collab.ccu002_inf_outcomes_hes_final'
-- MAGIC sus_table = 'dars_nic_391419_j3w9t_collab.ccu002_inf_outcomes_sus_final'
-- MAGIC deaths_table = 'dars_nic_391419_j3w9t_collab.ccu002_inf_outcomes_deaths_final'
-- MAGIC 
-- MAGIC #Date parameters
-- MAGIC index_date = '2020-01-01'
-- MAGIC end_date = '2020-12-07'
-- MAGIC 
-- MAGIC #Other data inputs
-- MAGIC skinny_QA_inclusion_table = 'dars_nic_391419_j3w9t_collab.ccu002_inf_included_patients' 
-- MAGIC #skinny patient table that has undergone QA and inclusion/exclusion
-- MAGIC 
-- MAGIC #Final table names
-- MAGIC final_table_1 = 'ccu002_inf_outcomes_allsources_firstdiag_final' 
-- MAGIC #final_table_2 = 'ccu002_inf_outcomes_allsources_firstdiag_final'
-- MAGIC final_table_2 = 'ccu002_inf_combined_outcomes_firstdiag_thrombo'
-- MAGIC final_table_3 = 'ccu002_inf_outcomes_allsources_firstdiag_final_wohes' 
-- MAGIC                 #ccu002_vac_outcomes_allsources_firstdiag_final_wohes
-- MAGIC final_table_4 = 'ccu002_inf_combined_outcomes_thrombo_wohes'
-- MAGIC final_table_5 = 'ccu002_inf_outcomes_allsources_firstdiag_wohes_final'

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

-- COMMAND ----------

-- MAGIC %md ## source files

-- COMMAND ----------

-- MAGIC %py
-- MAGIC display(spark.sql(F"""select * from {sus_table}"""))

-- COMMAND ----------

-- MAGIC %py
-- MAGIC display(spark.sql(F"""select * from {deaths_table}"""))

-- COMMAND ----------

-- MAGIC %py
-- MAGIC display(spark.sql(F"""select * from {gdppr_table}"""))

-- COMMAND ----------

-- MAGIC %py
-- MAGIC display(spark.sql(F"""select * from {hes_table}"""))

-- COMMAND ----------

-- MAGIC %md ## Combine sources together

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC #pay attention to sus_table2 vs sus_table (any diag vs first)
-- MAGIC #with and without HES. For HES comment it in.
-- MAGIC 
-- MAGIC 
-- MAGIC spark.sql(F"""create or replace global temp view ccu002_vaccine_allsources_outcomes_wohes AS
-- MAGIC 
-- MAGIC with cte_alloutcomes as (
-- MAGIC SELECT *
-- MAGIC FROM {deaths_table}
-- MAGIC UNION
-- MAGIC SELECT *
-- MAGIC FROM {sus_table2}
-- MAGIC --FROM dars_nic_391419_j3w9t_collab.ccu002_vac_outcomes_sus_final_anydiag
-- MAGIC UNION
-- MAGIC SELECT *
-- MAGIC FROM {gdppr_table}
-- MAGIC --UNION
-- MAGIC --SELECT *
-- MAGIC --FROM {hes_table}
-- MAGIC where name is not null 
-- MAGIC )
-- MAGIC 
-- MAGIC SELECT NHS_NUMBER_DEID, record_date, spell_start, name, term, SOURCE, terminology, code, diagnosis_position,
-- MAGIC (case when name = 'AMI' OR name = 'stroke_isch' /*OR name = 'stroke_SAH_HS' OR name = 'stroke_NOS' OR name = 'stroke_SAH' OR name = 'retinal_infarction' */ OR name = 'other_arterial_embolism' THEN 1 Else 0 End) as Arterial_event,
-- MAGIC (case when name like 'PE%' OR name like '%DVT%' OR name like '%ICVT%' OR name like 'portal%' THEN 1 Else 0 End) as Venous_event,
-- MAGIC (case when name like 'DIC' OR name = 'TTP' OR name = 'thrombocytopenia' THEN 1 Else 0 End) as Haematological_event
-- MAGIC from cte_alloutcomes""")

-- COMMAND ----------

--pay attention to table names for below for any diagnosis and first diagnosis. This cell is skipped for any diagnosis position

create or replace global temp view 
ccu002_vaccine_allsources_outcomes_firstdiag_wohes AS
select NHS_NUMBER_DEID, record_date, name, spell_start, term, SOURCE, terminology, code, diagnosis_position, Arterial_event, Venous_event, Haematological_event
FROM global_temp.ccu002_vaccine_allsources_outcomes_wohes 
where diagnosis_position = 1 or diagnosis_position = 'NA'


-- COMMAND ----------

---- First event of each phenotype

create or replace global temp view 
--ccu002_vaccine_allsources_outcomes_first_wohes AS
ccu002_vaccine_allsources_outcomes_final_wohes as

with cte_names as (
    SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY NHS_NUMBER_DEID, name ORDER BY record_date ASC) AS seq
FROM global_temp.ccu002_vaccine_allsources_outcomes_wohes 
--FROM global_temp.ccu002_vaccine_allsources_outcomes_firstdiag_wohes
)

SELECT *
FROM cte_names WHERE seq = 1

-- COMMAND ----------

create or replace global temp view ccu002_vaccine_allsources_outcomes_final_wohes2 as
select NHS_NUMBER_DEID, record_date, name, spell_start, term, SOURCE, terminology, code, diagnosis_position, Arterial_event, Venous_event, Haematological_event 
from global_temp.ccu002_vaccine_allsources_outcomes_final_wohes

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC create_table(table_name=final_table_3, database_name='dars_nic_391419_j3w9t_collab', select_sql_script=f"SELECT * FROM global_temp.ccu002_vaccine_allsources_outcomes_final_wohes2")
-- MAGIC 
-- MAGIC #final_table is a parameter name defined at the start so can be used for infection and vaccine

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC #below is for anydiag
-- MAGIC 
-- MAGIC create_table(table_name=final_table_5, database_name='dars_nic_391419_j3w9t_collab', select_sql_script=f"SELECT * FROM global_temp.ccu002_vaccine_allsources_outcomes_final_wohes2")

-- COMMAND ----------

-- MAGIC %md ## Summary outcomes table

-- COMMAND ----------

--below uses all global temps so parameter not required.
--pay attention to table names for below for any diagnosis and first diagnosis.

create or replace global temp view  ccu002_vaccine_allsources_outcomes_final_wohes_summary AS --table title is used for anydiag and firstdiag 

with cte_arterial as (
SELECT NHS_NUMBER_DEID, record_date as arterial_date, spell_start as art_spell, Arterial_event, SOURCE as art_source, ROW_NUMBER()
OVER (PARTITION BY NHS_NUMBER_DEID, Arterial_event ORDER BY record_date) AS seq
FROM global_temp.ccu002_vaccine_allsources_outcomes_wohes 
--from global_temp.ccu002_vaccine_allsources_outcomes_firstdiag_wohess
),

cte_firstarterial as 
(SELECT *
FROM cte_arterial WHERE seq = 1
),

cte_venous as (
SELECT NHS_NUMBER_DEID as ID, record_date as venous_date, spell_start as ven_spell, Venous_event, SOURCE as ven_source, ROW_NUMBER()
OVER(PARTITION BY NHS_NUMBER_DEID, Venous_event ORDER BY record_date) AS seq
FROM global_temp.ccu002_vaccine_allsources_outcomes_wohes 
--from global_temp.ccu002_vaccine_allsources_outcomes_firstdiag_wohes
),

cte_firstvenous as 
(SELECT *
FROM cte_venous WHERE seq = 1
),

cte_hemato as (
SELECT NHS_NUMBER_DEID as ID2, record_date as Haematological_date, spell_start as hemato_spell, Haematological_event, SOURCE as hemato_source, ROW_NUMBER()
OVER(PARTITION BY NHS_NUMBER_DEID, Haematological_event ORDER BY record_date) AS seq
FROM global_temp.ccu002_vaccine_allsources_outcomes_wohes  
--from global_temp.ccu002_vaccine_allsources_outcomes_firstdiag_wohes
),

cte_firsthematological as
(SELECT *
FROM cte_hemato WHERE seq = 1
)


SELECT 
tab1.NHS_NUMBER_DEID, tab1.arterial_date, tab1.art_spell, tab1.Arterial_event, tab1.art_source, 
tab2.ID, tab2.venous_date, tab2.ven_spell, tab2.Venous_event, tab2.ven_source, 
tab3.ID2, tab3.Haematological_date, tab3.hemato_spell, tab3.Haematological_event, tab3.hemato_source

FROM cte_firstarterial tab1
FULL OUTER JOIN cte_firstvenous tab2
ON tab1.NHS_NUMBER_DEID = tab2.ID
FULL OUTER JOIN cte_firsthematological tab3
ON tab1.NHS_NUMBER_DEID = tab3.ID2



-- COMMAND ----------

--pay attention to table names for below for any diagnosis and first diagnosis.

create or replace global temp view 
--ccu002_vaccine_allsources_outcomes_first_wohes AS
ccu002_vaccine_allsources_outcomes_combined_order_anydiag as

with cte_order as (
    SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY NHS_NUMBER_DEID ORDER BY Arterial_event DESC, Venous_event DESC, Haematological_event DESC) AS seq
FROM global_temp.ccu002_vaccine_allsources_outcomes_final_wohes_summary
)

SELECT *
FROM cte_order WHERE seq = 1

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC 
-- MAGIC 
-- MAGIC CREATE TABLE dars_nic_391419_j3w9t_collab.ccu002_vac_combined_outcomes_longformat_anydiag AS
-- MAGIC WITH CTE AS (
-- MAGIC     SELECT NHS_NUMBER_DEID,  
-- MAGIC     STACK(3,
-- MAGIC     arterial_date, 'Arterial_event', Arterial_event,
-- MAGIC     venous_date, 'Venous_event', Venous_event,
-- MAGIC     Haematological_date, 'Haematological_event', Haematological_event
-- MAGIC     ) as (record_date, composite_event_type, composite_event)
-- MAGIC     FROM global_temp.ccu002_vaccine_allsources_outcomes_combined_order_anydiag)
-- MAGIC     
-- MAGIC     
-- MAGIC SELECT NHS_NUMBER_DEID, record_date, composite_event_type
-- MAGIC FROM CTE 
-- MAGIC where composite_event =1

-- COMMAND ----------

-- MAGIC %md ## Plus thrombocytopenia for first diagnosis position

-- COMMAND ----------

-- MAGIC %python
-- MAGIC spark.sql(F"""create or replace global temp view ccu002_vaccine_combined_thrombo as
-- MAGIC select *
-- MAGIC from {sus_table2}
-- MAGIC where name='thrombocytopenia' 
-- MAGIC --and diagnosis_position>1
-- MAGIC """)

-- COMMAND ----------

create or replace global temp view ccu002_vacc_combined_outcomes_thrombo_wohes  AS

with cte_combined as (
select summ.NHS_NUMBER_DEID, 
summ.arterial_date, summ.art_spell, summ.Arterial_event, summ.art_source, 
summ.venous_date, summ.ven_spell, summ.Venous_event, summ.ven_source,
summ.Haematological_date, summ.hemato_spell, summ.Haematological_event, summ.hemato_source, 

thrombo.record_date, thrombo.spell_start, thrombo.spell_end, thrombo.name, thrombo.term, thrombo.diagnosis_position

from global_temp.ccu002_vaccine_allsources_outcomes_final_wohes_summary summ
LEFT JOIN global_temp.ccu002_vaccine_combined_thrombo thrombo
ON summ.NHS_NUMBER_DEID = thrombo.NHS_NUMBER_DEID
)

select NHS_NUMBER_DEID, arterial_date, art_spell, Arterial_event, art_source, venous_date, ven_spell, Venous_event, ven_source, Haematological_date, hemato_spell, Haematological_event, hemato_source, 

(case when ven_spell=spell_start AND Venous_event=1 THEN 1 ELSE 0 END) AS THROMBO_plus_VEN, 
(case when art_spell=spell_start AND Arterial_event=1 THEN 1 ELSE 0 END) AS THROMBO_plus_ART
from cte_combined


-- COMMAND ----------



create or replace global temp view 
--ccu002_vaccine_allsources_outcomes_first_wohes AS
ccu002_vaccine_allsources_outcomes_combined_order as

with cte_order as (
    SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY NHS_NUMBER_DEID ORDER BY Arterial_event DESC, Venous_event DESC, Haematological_event DESC) AS seq 
FROM global_temp.ccu002_vacc_combined_outcomes_thrombo_wohes
)

SELECT *
FROM cte_order WHERE seq = 1

-- COMMAND ----------

create or replace global temp view ccu002_vaccine_allsources_outcomes_combined_order_select as
select NHS_NUMBER_DEID, arterial_date, Arterial_event, venous_date, Venous_event, Haematological_date, Haematological_event, THROMBO_plus_VEN, THROMBO_plus_ART from 
--global_temp.ccu002_vacc_combined_outcomes_thrombo_wohes
global_temp.ccu002_vaccine_allsources_outcomes_combined_order
order by NHS_NUMBER_DEID
                                 

-- COMMAND ----------

-- MAGIC %python
-- MAGIC create_table(table_name=final_table_4, database_name='dars_nic_391419_j3w9t_collab', select_sql_script=f"SELECT * FROM global_temp.ccu002_vaccine_allsources_outcomes_combined_order_select")
-- MAGIC 
-- MAGIC #final_table is a parameter name defined at the start so can be used for infection and vaccine
-- MAGIC #need to drop ccu002_vacc_combined_outcomes_thrombo

-- COMMAND ----------

--drop table if exists dars_nic_391419_j3w9t_collab.ccu002_vac_combined_outcomes_firstdiag_thrombo_wohes

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_vac_combined_outcomes_longformat AS
-- MAGIC WITH CTE AS (
-- MAGIC     SELECT NHS_NUMBER_DEID,  
-- MAGIC     STACK(5,
-- MAGIC     arterial_date, 'Arterial_event', Arterial_event,
-- MAGIC     venous_date, 'Venous_event', Venous_event,
-- MAGIC     Haematological_date, 'Haematological_event', Haematological_event,
-- MAGIC     arterial_date,'THROMBO_plus_ART', THROMBO_plus_ART,
-- MAGIC     venous_date,'THROMBO_plus_VEN', THROMBO_plus_VEN
-- MAGIC     ) as (record_date, composite_event_type, composite_event)
-- MAGIC     FROM dars_nic_391419_j3w9t_collab.ccu002_vac_combined_outcomes_firstdiag_thrombo_wohes)
-- MAGIC     
-- MAGIC     
-- MAGIC SELECT NHS_NUMBER_DEID, record_date, composite_event_type
-- MAGIC FROM CTE --INNER JOIN mapping ON CTE.DIAGNOSIS_POSITION = mapping.column_name
-- MAGIC where composite_event =1

-- COMMAND ----------

-- MAGIC %python
-- MAGIC create_table('ccu002_vac_combined_outcomes_longformat')

-- COMMAND ----------

-- MAGIC %md ## Additional outcomes

-- COMMAND ----------

--Pay attention to table names for any diagnosis versus first diagnosis below

--create or replace global temp view ccu002_vaccine_dvt_icvt_summ as
create or replace global temp view ccu002_vaccine_dvt_icvt_summ_anydiag as
SELECT *,
(case when name = 'DVT_DVT' OR name = 'DVT_pregnancy' THEN 1 Else 0 End) as DVT_summ_event,
(case when name like 'DVT_ICVT' OR name like 'ICVT_pregnancy' THEN 1 Else 0 End) as ICVT_summ_event 
--from dars_nic_391419_j3w9t_collab.ccu002_vac_outcomes_allsources_firstdiag_final_wohes
from dars_nic_391419_j3w9t_collab.ccu002_vac_outcomes_allsources_anydiag_final_wohes

-- COMMAND ----------

--below uses all global temps so parameter not required.

--create or replace global temp view  ccu002_vacc_ICVT_DVT_summary AS
create or replace global temp view  ccu002_vacc_ICVT_DVT_summary_anydiag AS

with cte_dvt as (
SELECT NHS_NUMBER_DEID, record_date as dvt_date, spell_start as dvt_spell, DVT_summ_event, SOURCE as dvt_source, ROW_NUMBER()
OVER (PARTITION BY NHS_NUMBER_DEID, DVT_summ_event ORDER BY record_date) AS seq
--FROM global_temp.ccu002_vaccine_allsources_outcomes_wohes 
--from global_temp.ccu002_vaccine_dvt_icvt_summ
from global_temp.ccu002_vaccine_dvt_icvt_summ_anydiag
),

cte_firstdvt as 
(SELECT *
FROM cte_dvt WHERE seq = 1
),

cte_icvt as (
SELECT NHS_NUMBER_DEID as ID, record_date as icvt_date, spell_start as icvt_spell, ICVT_summ_event, SOURCE as icvt_source, ROW_NUMBER()
OVER(PARTITION BY NHS_NUMBER_DEID, ICVT_summ_event ORDER BY record_date) AS seq
--FROM global_temp.ccu002_vaccine_allsources_outcomes_wohes 
--from global_temp.ccu002_vaccine_dvt_icvt_summ
FROM global_temp.ccu002_vaccine_dvt_icvt_summ_anydiag
),

cte_firsticvt as 
(SELECT *
FROM cte_icvt WHERE seq = 1
)


SELECT 
tab1.NHS_NUMBER_DEID, tab1.dvt_date, tab1.dvt_spell, tab1.DVT_summ_event, tab1.dvt_source, 
tab2.ID, tab2.icvt_date, tab2.icvt_spell, tab2.ICVT_summ_event, tab2.icvt_source 

FROM cte_firstdvt tab1
FULL OUTER JOIN cte_firsticvt tab2
ON tab1.NHS_NUMBER_DEID = tab2.ID



-- COMMAND ----------

----getting rid of duplicates

--create or replace global temp view ccu002_vacc_DVT_ICVT_order as
create or replace global temp view ccu002_vacc_DVT_ICVT_order_anydiag as

with cte_order as (
    SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY NHS_NUMBER_DEID ORDER BY DVT_summ_event DESC, ICVT_summ_event DESC) AS seq
--FROM global_temp.ccu002_vaccine_allsources_outcomes_wohes 
--FROM global_temp.ccu002_vacc_ICVT_DVT_summary
from global_temp.ccu002_vacc_ICVT_DVT_summary_anydiag
)

SELECT NHS_NUMBER_DEID, dvt_date, DVT_summ_event, icvt_date, ICVT_summ_event 
FROM cte_order WHERE seq = 1

-- COMMAND ----------

-- MAGIC %sql
-- MAGIC --CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_vac_DVT_ICVT_longformat AS
-- MAGIC --create table dars_nic_391419_j3w9t_collab.ccu002_vac_DVT_ICVT_longformat_sjk
-- MAGIC --create table dars_nic_391419_j3w9t_collab.ccu002_vac_DVT_ICVT_longformat_sjk as
-- MAGIC create table dars_nic_391419_j3w9t_collab.ccu002_vac_DVT_ICVT_longformat_anydiag_sjk as
-- MAGIC WITH CTE AS (
-- MAGIC     SELECT NHS_NUMBER_DEID,  
-- MAGIC     STACK(2,
-- MAGIC     dvt_date, 'DVT_summ_event', DVT_summ_event,
-- MAGIC     icvt_date, 'ICVT_summ_event', ICVT_summ_event
-- MAGIC     ) as (record_date, composite_event_type, composite_event)
-- MAGIC     --FROM global_temp.ccu002_vacc_DVT_ICVT_order
-- MAGIC     FROM global_temp.ccu002_vacc_DVT_ICVT_order_anydiag
-- MAGIC     )
-- MAGIC     
-- MAGIC SELECT NHS_NUMBER_DEID, record_date, composite_event_type
-- MAGIC FROM CTE --INNER JOIN mapping ON CTE.DIAGNOSIS_POSITION = mapping.column_name
-- MAGIC where composite_event =1

-- COMMAND ----------

select count (distinct NHS_NUMBER_DEID) as count, composite_event_type
from dars_nic_391419_j3w9t_collab.ccu002_vac_DVT_ICVT_longformat_anydiag_sjk
group by composite_event_type
