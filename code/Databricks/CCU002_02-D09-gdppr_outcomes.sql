-- Databricks notebook source
-- MAGIC %md # CCU002_02-D09-gdppr_outcomes
-- MAGIC 
-- MAGIC **Description** This notebook generates the outcomes listed in the CCU002 vaccination protocol using the GDPPR dataset
-- MAGIC 
-- MAGIC **Author(s)** Spencer Keene, Rachel Denholm, Jenny Cooper

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Vaccine parameters

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC #Dataset parameters
-- MAGIC gdppr_data = 'dars_nic_391419_j3w9t_collab.ccu002_vac_gdppr_dars_nic_391419_j3w9t'
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
-- MAGIC #Final table name
-- MAGIC final_table = 'ccu002_vac_outcomes_gdppr_final' 

-- COMMAND ----------

-- MAGIC %md
-- MAGIC #####Infection parameters

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC #Dataset parameters
-- MAGIC gdppr_data = 'dars_nic_391419_j3w9t_collab.ccu002_vac_gdppr_dars_nic_391419_j3w9t'
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
-- MAGIC final_table ='ccu002_inf_outcomes_gdppr_final'   

-- COMMAND ----------

-- MAGIC %py
-- MAGIC display(spark.sql(F"""select *
-- MAGIC from {gdppr_data}"""))

-- COMMAND ----------

-- MAGIC %py
-- MAGIC spark.sql(F"""
-- MAGIC create or replace global temp view CCU002_vaccine_gdppr as
-- MAGIC select *
-- MAGIC from {gdppr_data}
-- MAGIC where RECORD_DATE <= current_date and NHS_NUMBER_DEID is not NULL""") 

-- COMMAND ----------

-- MAGIC %py
-- MAGIC #Relevant outcomes codes
-- MAGIC 
-- MAGIC spark.sql(F"""
-- MAGIC create or replace global temp view CCU002_vaccine_gdpproutcomes as
-- MAGIC 
-- MAGIC SELECT name, terminology, code, term, code_type, RecordDate
-- MAGIC FROM bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127
-- MAGIC WHERE ((name = 'AMI' OR name = 'stroke_IS'  
-- MAGIC --OR name = 'stroke_SAH' 
-- MAGIC OR name = 'stroke_NOS') 
-- MAGIC and (term != 'Stroke in the puerperium (disorder)')
-- MAGIC and (term != 'Cerebrovascular accident (disorder)')
-- MAGIC and (term != 'Right sided cerebral hemisphere cerebrovascular accident (disorder)')
-- MAGIC and (term != 'Left sided cerebral hemisphere cerebrovascular accident (disorder)')
-- MAGIC and (term != 'Brainstem stroke syndrome (disorder)')
-- MAGIC AND (terminology ='SNOMED') 
-- MAGIC AND (code_type=1) 
-- MAGIC AND (RecordDate=20210127))""")
-- MAGIC       
-- MAGIC       

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC spark.sql(F"""
-- MAGIC create or replace global temp view CCU002_vaccine_outcomes_gdppr AS
-- MAGIC 
-- MAGIC with cte_gdppr as (
-- MAGIC SELECT NHS_NUMBER_DEID, DATE, RECORD_DATE, CODE as snomed
-- MAGIC FROM global_temp.CCU002_vaccine_gdppr
-- MAGIC WHERE DATE >= '{index_date}' and DATE <= '{end_date}'
-- MAGIC )
-- MAGIC  
-- MAGIC select *
-- MAGIC from cte_gdppr t1
-- MAGIC inner join global_temp.CCU002_vaccine_gdpproutcomes t2 on t1.snomed = t2.code""")

-- COMMAND ----------


create or replace global temp view CCU002_vaccine_outcomes_gdppr2 as
select *, 
case 
when name = 'stroke_IS' OR name = 'stroke_NOS' THEN 'stroke_isch'
when name = 'AMI' THEN 'AMI' END as name2
from global_temp.CCU002_vaccine_outcomes_gdppr


-- COMMAND ----------

--earliest date

create or replace global temp view CCU002_vaccine_outcomes_gdppr_first AS 

with cte as (
    SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY NHS_NUMBER_DEID, name2 ORDER BY DATE ASC, 
                                                                     RECORD_DATE ASC) AS seq
                                                                   
                                                                                           
FROM global_temp.CCU002_vaccine_outcomes_gdppr2
)

SELECT *
FROM cte



-- COMMAND ----------

select * from global_temp.CCU002_vaccine_outcomes_gdppr_first

-- COMMAND ----------

create or replace global temp view ccu002_vaccine_outcomes_gdppr_final AS
select NHS_NUMBER_DEID, DATE as record_date, 'NA' as spell_start, 'NA' as spell_end, name2 as name, term, 'gdppr' as SOURCE, terminology, code, 'NA' as diagnosis_position
from global_temp.CCU002_vaccine_outcomes_gdppr_first
where seq=1



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
-- MAGIC   

-- COMMAND ----------

select * from global_temp.ccu002_vaccine_outcomes_gdppr_final

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC create_table(table_name=final_table, database_name='dars_nic_391419_j3w9t_collab', select_sql_script=f"SELECT * FROM global_temp.ccu002_vaccine_outcomes_gdppr_final")
-- MAGIC 
-- MAGIC #final_table is a parameter name defined at the start so can be used for infection and vaccine
