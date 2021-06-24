-- Databricks notebook source
-- MAGIC %md # CCU002_02-D15-vaccine_status
-- MAGIC  
-- MAGIC **Description** This notebook generates a vaccine status table.
-- MAGIC 
-- MAGIC **Author(s)** Spencer Keene, Jenny Cooper

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC #Dataset parameters
-- MAGIC vaccine_data = 'dars_nic_391419_j3w9t_collab.ccu002_vac_vaccine_status_dars_nic_391419_j3w9t'
-- MAGIC 
-- MAGIC #Other data inputs
-- MAGIC skinny_QA_inclusion_table = 'dars_nic_391419_j3w9t_collab.ccu002_vac_included_patients' 
-- MAGIC #skinny patient table that has undergone QA and inclusion/exclusion
-- MAGIC 
-- MAGIC #Final table
-- MAGIC 
-- MAGIC final_table = 'ccu002_vac_vaccine_status'

-- COMMAND ----------

select * from dars_nic_391419_j3w9t_collab.ccu002_vac_vaccine_status_dars_nic_391419_j3w9t

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC from pyspark.sql.functions import countDistinct, year, dayofmonth, from_unixtime, month, unix_timestamp, to_timestamp, date_format, col, datediff, to_date, lit, months_between
-- MAGIC import pandas as pd
-- MAGIC import numpy as np
-- MAGIC import seaborn as sns
-- MAGIC import matplotlib.pyplot as plt
-- MAGIC import datetime
-- MAGIC from pyspark.sql.functions import *
-- MAGIC import pyspark.sql.functions as f
-- MAGIC from pyspark.sql import Window
-- MAGIC import io
-- MAGIC from functools import reduce
-- MAGIC from pyspark.sql.types import StringType

-- COMMAND ----------

-- MAGIC %py
-- MAGIC 
-- MAGIC #PROBABLY THE MOST IMPORTANT COLUMNS FROM VACCINE STATUS TABLE
-- MAGIC spark.sql(F"""create or replace global temp view vaccine_status_table as
-- MAGIC select * from {vaccine_data}""") 

-- COMMAND ----------

-- MAGIC %md ## Codes to values

-- COMMAND ----------

SELECT count(DISTINCT PERSON_ID_DEID) as count, VACCINATION_PROCEDURE_CODE --VACCINE_PRODUCT_CODE
--FROM global_temp.vaccine5
from global_temp.vaccine_status_table
group by VACCINATION_PROCEDURE_CODE --VACCINE_PRODUCT_CODE
order by count desc

-- COMMAND ----------

create or replace global temp view vaccine1 as
SELECT *,
    CASE 
        WHEN VACCINE_PRODUCT_CODE IN ('39114911000001105', '39115011000001105', '39115111000001106') THEN 'AstraZeneca'
        WHEN VACCINE_PRODUCT_CODE IN ('39115711000001107', '39115611000001103') THEN 'Pfizer'
        WHEN VACCINE_PRODUCT_CODE IN ('39326911000001101', '39375411000001104') THEN 'Moderna' END as vaccine_product
    
from global_temp.vaccine_status_table 
    

-- COMMAND ----------

create or replace global temp view vaccine2 as
SELECT *,
    CASE 
        WHEN VACCINATION_PROCEDURE_CODE IN ('1324691000000104') THEN 'second dose' 
        WHEN VACCINATION_PROCEDURE_CODE IN ('1324681000000101') THEN 'first dose' END AS vaccine_dose
from global_temp.vaccine1

-- COMMAND ----------

create or replace global temp view vaccine3 as
SELECT *,
    CASE        
        WHEN VACCINATION_SITUATION_CODE IN ('1324741000000101') THEN 'first dose declined' 
        WHEN VACCINATION_SITUATION_CODE IN ('1324751000000103') THEN 'second dose declined' END AS declined
from global_temp.vaccine2

-- COMMAND ----------

create or replace global temp view vaccine4 as
SELECT *,
    CASE 
        WHEN SITE_OF_VACCINATION_CODE IN ('61396006') THEN 'left thigh'
        WHEN SITE_OF_VACCINATION_CODE IN ('368209003') THEN 'right upper arm'
        WHEN SITE_OF_VACCINATION_CODE IN ('368208006') THEN 'left upper arm'
        WHEN SITE_OF_VACCINATION_CODE IN ('723980000') THEN 'right buttock'
        WHEN SITE_OF_VACCINATION_CODE IN ('723979003') THEN 'left buttock'
        WHEN SITE_OF_VACCINATION_CODE IN ('11207009') THEN 'right thigh' END AS site_of_vaccination
from global_temp.vaccine3

-- COMMAND ----------

create or replace global temp view vaccine5 as
SELECT *,
    CASE         
        WHEN CARE_SETTING_TYPE_CODE IN('413294000') THEN 'Community health services'
        WHEN CARE_SETTING_TYPE_CODE IN('310065000') THEN 'Open access service'
        WHEN CARE_SETTING_TYPE_CODE IN('788007007') THEN 'General practice service' END AS care_setting_vaccination
from global_temp.vaccine4

-- COMMAND ----------

create or replace global temp view ccu002_vacc_vaccine_status as
select PERSON_ID_DEID as NHS_NUMBER_DEID, /*MYDOB, AGE, POSTCODE_DISTRICT, LSOA, TRACE_VERIFIED,*/ to_date(cast(RECORDED_DATE as string), 'yyyyMMdd') as VACCINATION_DATE, vaccine_product as VACCINE_PRODUCT, vaccine_dose as VACCINE_DOSE, DOSE_AMOUNT, DOSE_SEQUENCE, NOT_GIVEN, declined as DECLINED, care_setting_vaccination as CARE_SETTING from global_temp.vaccine5
---antijoin

-- COMMAND ----------

-- MAGIC %md ## Getting rid of dupicates and multiple first doses

-- COMMAND ----------



create or replace global temp view ccu002_vacc_vaccine_status_final AS 

with cte as (
    SELECT *, 
    ROW_NUMBER() OVER (PARTITION BY NHS_NUMBER_DEID, VACCINE_DOSE ORDER BY VACCINATION_DATE ASC) AS seq
                                                                                             
                                                                   
                                                                                           
FROM global_temp.ccu002_vacc_vaccine_status
)

SELECT *
FROM cte

where seq=1 and VACCINATION_DATE>='2020-12-08' and VACCINATION_DATE<=current_date and DECLINED is NULL 

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

create or replace global temp view ccu002_vacc_vaccine_status_final_temp as
select NHS_NUMBER_DEID, VACCINATION_DATE, VACCINE_PRODUCT, VACCINE_DOSE
from global_temp.ccu002_vacc_vaccine_status_final

-- COMMAND ----------

-- MAGIC %python
-- MAGIC 
-- MAGIC create_table(table_name=final_table, database_name='dars_nic_391419_j3w9t_collab', select_sql_script=f"SELECT * FROM global_temp.ccu002_vacc_vaccine_status_final_temp")
-- MAGIC 
-- MAGIC #final_table is a parameter name defined at the start so can be used for infection and vaccine

-- COMMAND ----------

--drop table if exists dars_nic_391419_j3w9t_collab.ccu002_vac_vaccine_status

-- COMMAND ----------

-- MAGIC %md ## Vaccine Interval and mixed doses

-- COMMAND ----------

CREATE OR REPLACE GLOBAL TEMP VIEW vaccine_interval as
/*with cte_interval as (SELECT NHS_NUMBER_DEID, VACCINE_DOSE, VACCINATION_DATE, VACCINE_PRODUCT FROM dars_nic_391419_j3w9t_collab.ccu002_vacc_vaccine_status_final
)
*/
SELECT tbl1.NHS_NUMBER_DEID, tbl1.VACCINE_PRODUCT as vaccine_prod1, tbl1.VACCINE_DOSE as vaccine_1, tbl1.VACCINATION_DATE as vaccine_date_1, 
                             tbl2.VACCINE_PRODUCT as vaccine_prod2, tbl2.VACCINE_DOSE as vaccine_2, tbl2.VACCINATION_DATE as vaccine_date_2
from dars_nic_391419_j3w9t_collab.ccu002_vacc_vaccine_status_final as tbl1 
LEFT JOIN 
     dars_nic_391419_j3w9t_collab.ccu002_vacc_vaccine_status_final as tbl2
     on tbl1.NHS_NUMBER_DEID = tbl2.NHS_NUMBER_DEID
WHERE tbl1.VACCINE_DOSE != tbl2.VACCINE_DOSE and tbl1.VACCINE_DOSE='first dose' --and tbl1.VACCINE_DOSE='second_dose'

-- COMMAND ----------

SELECT Vaccine_Date_diff, count(*)
FROM
  (select /*NHS_NUMBER_DEID,*/ (DATEDIFF(vaccine_date_2, vaccine_date_1)) as vaccine_date_diff 
  from global_temp.vaccine_interval)
GROUP BY vaccine_Date_diff
ORDER BY VACCine_DATE_DIFF

-- COMMAND ----------

WITH CTE AS
(
select /*NHS_NUMBER_DEID,*/ (DATEDIFF(vaccine_date_2, vaccine_date_1)) as vaccine_date_diff 
from global_temp.vaccine_interval
where vaccine_date_1 < '2021-01-01'
)
SELECT Vaccine_Date_diff, count(*)
FROM
  CTE
GROUP BY vaccine_Date_diff
ORDER BY VACCine_DATE_DIFF

-- COMMAND ----------

WITH CTE AS
(
select /*NHS_NUMBER_DEID,*/ (DATEDIFF(vaccine_date_2, vaccine_date_1)) as vaccine_date_diff 
from global_temp.vaccine_interval
where vaccine_date_1 >= '2021-01-01'
)
SELECT Vaccine_Date_diff, count(*)
FROM
  CTE
GROUP BY vaccine_Date_diff
ORDER BY VACCine_DATE_DIFF

-- COMMAND ----------

Create or replace global temp view ccu002_vaccine_product as

with cte_product as (SELECT *,
(case when vaccine_prod2 like 'Pfizer%' then 'Pfizer'
 when vaccine_prod2 like 'Astra%' then 'Astra'
 when vaccine_prod2 like 'Moderna%' then 'Moderna' end) as Vaccine_Product2
FROM global_temp.vaccine_interval
),

cte_product2 as (select *, 
(case when vaccine_prod1 like 'Pfizer%' then 'Pfizer'
 when vaccine_prod1 like 'Astra%' then 'Astra'
 when vaccine_prod1 like 'Moderna%' then 'Moderna' end) as Vaccine_Product1
from cte_product)

--Create or replace global temp view ccu002_vaccine_product as
Select NHS_NUMBER_DEID, Vaccine_Product1, vaccine_1, vaccine_date_1, Vaccine_Product2, vaccine_2, vaccine_date_2 from cte_product2


-- COMMAND ----------

select * from global_temp.ccu002_vaccine_product

-- COMMAND ----------

Create or replace global temp view ccu002_vaccine_loyal as

with cte_loyal as (select *, 
(case when Vaccine_Product1= 'Pfizer' and Vaccine_Product2= 'Pfizer' then 'both Pfizer'
when Vaccine_Product1= 'Astra' and Vaccine_Product2= 'Astra' then 'both AstraZenecca'
when Vaccine_Product1= 'Astra' and Vaccine_Product2= 'Pfizer' then 'Astra then Pfizer'
when Vaccine_Product1= 'Pfizer' and Vaccine_Product2= 'Astra' then 'Pfizer then Astra' END) AS Vaccine_loyalty
from global_temp.ccu002_vaccine_product)

select NHS_NUMBER_DEID, Vaccine_loyalty from cte_loyal



-- COMMAND ----------

select * from global_temp.ccu002_vaccine_loyal

-- COMMAND ----------

WITH CTE AS
(
select /*NHS_NUMBER_DEID,*/ (DATEDIFF(vaccine_date_2, vaccine_date_1)) as vaccine_date_diff 
from global_temp.ccu002_vaccine_product
where Vaccine_Product2 like 'Pfizer%'
)
SELECT Vaccine_Date_diff, count(*)
FROM
  CTE
GROUP BY vaccine_Date_diff
ORDER BY VACCine_DATE_DIFF

-- COMMAND ----------

WITH CTE AS
(
select /*NHS_NUMBER_DEID,*/ (DATEDIFF(vaccine_date_2, vaccine_date_1)) as vaccine_date_diff 
from global_temp.ccu002_vaccine_product
where Vaccine_Product2 like 'Astra%'
)
SELECT Vaccine_Date_diff, count(*)
FROM
  CTE
GROUP BY vaccine_Date_diff
ORDER BY VACCine_DATE_DIFF

-- COMMAND ----------

select /*NHS_NUMBER_DEID,*/ (DATEDIFF(vaccine_date_2, vaccine_date_1)) as vaccine_date_diff 
from global_temp.ccu002_vaccine_product
where Vaccine_Product2 like 'Astra%'

-- COMMAND ----------

select count(distinct NHS_NUMBER_DEID) as count, Vaccine_loyalty
from global_temp.ccu002_vaccine_loyal 
group by Vaccine_loyalty
order by count desc

-- COMMAND ----------

--drop table if exists dars_nic_391419_j3w9t_collab.ccu002_vacc_vaccine_status_final

-- COMMAND ----------

-- MAGIC %md ## Other important figures

-- COMMAND ----------

SELECT count(DISTINCT PERSON_ID_DEID) as count, RECORDED_DATE
FROM global_temp.vaccine5
group by RECORDED_DATE
order by RECORDED_DATE asc

-- COMMAND ----------

SELECT count(DISTINCT PERSON_ID_DEID) as count, age
FROM global_temp.vaccine5
group by age
order by age asc



-- COMMAND ----------

SELECT count(DISTINCT NHS_NUMBER_DEID) as count, vaccine_product
--FROM global_temp.vaccine5
from global_temp.ccu002_vacc_vaccine_status_final
group by vaccine_product
order by count desc

-- COMMAND ----------

SELECT count(DISTINCT PERSON_ID_DEID) as count, declined
FROM global_temp.vaccine5
group by declined
order by count desc

-- COMMAND ----------

SELECT count(Distinct PERSON_ID_DEID) as count, care_setting_vaccination
FROM global_temp.vaccine5
group by care_setting_vaccination
order by count desc

-- COMMAND ----------

SELECT count(Distinct PERSON_ID_DEID) as count, site_of_vaccination
FROM global_temp.vaccine5
group by site_of_vaccination
order by count desc

-- COMMAND ----------

SELECT count(PERSON_ID_DEID) as count, vaccine_dose
FROM global_temp.vaccine5
group by vaccine_dose
order by count desc
