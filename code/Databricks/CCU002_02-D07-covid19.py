# Databricks notebook source
# MAGIC %md # CCU002_02-D07-covid19
# MAGIC  
# MAGIC **Description** This notebook determines the COVID19 infection and hospital outcomes.
# MAGIC 
# MAGIC **Author(s)** Venexia Walker

# COMMAND ----------

# MAGIC %md ## Define functions

# COMMAND ----------

# Define create table function by Sam H
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

# MAGIC %md ## Define COVID19 events

# COMMAND ----------

# MAGIC %sql -- Create global temporary view containing all confirmed COVID19 diagnoses
# MAGIC CREATE
# MAGIC OR REPLACE GLOBAL TEMPORARY VIEW ccu002_vacc_covid19 AS
# MAGIC -- Events from HES admitted patient care
# MAGIC SELECT PERSON_ID_DEID AS NHS_NUMBER_DEID,
# MAGIC        min(EPISTART) AS DATE,
# MAGIC        "HES_APC" AS SOURCE,
# MAGIC        "confirmed" AS STATUS
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu001_hes_apc_all_years
# MAGIC WHERE ((DIAG_4_CONCAT RLIKE 'U071') OR (DIAG_4_CONCAT RLIKE 'U07.1'))
# MAGIC GROUP BY NHS_NUMBER_DEID, SOURCE, STATUS 
# MAGIC -- Events from HES outpatient care
# MAGIC UNION ALL
# MAGIC SELECT PERSON_ID_DEID AS NHS_NUMBER_DEID,
# MAGIC        min(APPTDATE) AS DATE,
# MAGIC        "HES_OP" AS SOURCE,
# MAGIC        "confirmed" AS STATUS
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu001_hes_op_all_years
# MAGIC WHERE ((DIAG_4_CONCAT RLIKE 'U071') OR (DIAG_4_CONCAT RLIKE 'U07.1'))
# MAGIC GROUP BY NHS_NUMBER_DEID, SOURCE, STATUS 
# MAGIC -- Lab results from primary care
# MAGIC UNION ALL
# MAGIC SELECT NHS_NUMBER_DEID,
# MAGIC        min(DATE) AS DATE,
# MAGIC        "GDPPR" AS SOURCE,
# MAGIC        "confirmed_lab" AS STATUS
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu001_gdppr_dars_nic_391419_j3w9t
# MAGIC WHERE CODE IN (SELECT code
# MAGIC                FROM dars_nic_391419_j3w9t_collab.ccu001_codelists
# MAGIC                WHERE codelist = 'covid19_lab_confirmed_incidence')
# MAGIC GROUP BY NHS_NUMBER_DEID, SOURCE, STATUS 
# MAGIC -- Events from primary care
# MAGIC UNION ALL
# MAGIC SELECT NHS_NUMBER_DEID,
# MAGIC        min(DATE) AS DATE,
# MAGIC        "GDPPR" AS SOURCE,
# MAGIC        "confirmed" AS STATUS
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu001_gdppr_dars_nic_391419_j3w9t
# MAGIC WHERE CODE IN (SELECT code 
# MAGIC                FROM dars_nic_391419_j3w9t_collab.ccu001_codelists
# MAGIC                WHERE codelist = 'covid19_clinically_confirmed')
# MAGIC -- Test results from SGSS
# MAGIC UNION ALL
# MAGIC SELECT Person_ID_DEID AS NHS_NUMBER_DEID,
# MAGIC        min(specimen_date) AS DATE,
# MAGIC        "SGSS" AS SOURCE,
# MAGIC        (CASE WHEN REPORTING_LAB_ID = '840' THEN 'confirmed_pillar2' ELSE 'confirmed_pillar1' END) AS STATUS
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu001_sgss_dars_nic_391419_j3w9t
# MAGIC WHERE Person_ID_DEID IS NOT NULL
# MAGIC GROUP BY NHS_NUMBER_DEID, SOURCE, STATUS 

# COMMAND ----------

# MAGIC %md ## Save COVID19 events

# COMMAND ----------

# Replace global temp view of COVID19 cases with table

drop_table('ccu002_vacc_covid19')
create_table('ccu002_vacc_covid19')
