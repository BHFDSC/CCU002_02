# Databricks notebook source
# MAGIC %md # CCU002_02-D06a-collate_codelists
# MAGIC 
# MAGIC **Description** This notebook collates all codelists for the CCU002 vaccine project.
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

# MAGIC %md ## Make vaccine codelists

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_02_vaccine_codelists AS
# MAGIC SELECT * FROM VALUES
# MAGIC ('Administration of first dose of severe acute respiratory syndrome coronavirus 2 vaccine (procedure)','1324681000000101','COVID19 vaccine: dose'),
# MAGIC ('Administration of second dose of severe acute respiratory syndrome coronavirus 2 vaccine (procedure)','1324691000000104','COVID19 vaccine: dose'),
# MAGIC ('COVID-19 Vaccine AstraZeneca (ChAdOx1 S [recombinant]) 5x10,000,000,000 viral particles/0.5ml dose solution for injection multidose vials (AstraZeneca UK Ltd) (product)','39114911000001105','COVID19 vaccine: AstraZeneca'),
# MAGIC ('COVID-19 Vaccine AstraZeneca (ChAdOx1 S [recombinant]) 5x10,000,000,000 viral particles/0.5ml dose solution for injection multidose vials (AstraZeneca UK Ltd) 8 dose (product)','39114911000001105','COVID19 vaccine: AstraZeneca'),
# MAGIC ('COVID-19 Vaccine AstraZeneca (ChAdOx1 S [recombinant]) 5x10,000,000,000 viral particles/0.5ml dose solution for injection multidose vials (AstraZeneca UK Ltd) 10 dose (product)','39114911000001105','COVID19 vaccine: AstraZeneca'),
# MAGIC ('COVID-19 mRNA Vaccine Pfizer-BioNTech BNT162b2 30micrograms/0.3ml dose concentrate for suspension for injection multidose vials (Pfizer Ltd) 6 dose (product)','39114911000001105','COVID19vaccine: Pfizer'),
# MAGIC ('COVID-19 mRNA Vaccine Pfizer-BioNTech BNT162b2 30micrograms/0.3ml dose concentrate for suspension for injection multidose vials (Pfizer Ltd) (product)','39115611000001103','COVID19vaccine: Pfizer'),
# MAGIC ('COVID-19 mRNA (nucleoside modified) Vaccine Moderna 0.1mg/0.5ml dose dispersion for injection multidose vials (Moderna, Inc) (product)','39326911000001101','COVID19vaccine: Moderna'),
# MAGIC ('COVID-19 mRNA (nucleoside modified) Vaccine Moderna 0.1mg/0.5ml dose dispersion for injection multidose vials (Moderna, Inc) 10 dose (product)','39375411000001104','COVID19vaccine: Moderna')
# MAGIC AS tab(term, code, name)

# COMMAND ----------

# MAGIC %md ## Collate codelists

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_02_codelists AS
# MAGIC SELECT codelist AS name,
# MAGIC        system AS terminology,
# MAGIC        code,
# MAGIC        term,
# MAGIC        "" AS code_type,
# MAGIC        "" AS RecordDate
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_vacc_drug_codelists
# MAGIC UNION ALL
# MAGIC SELECT *     
# MAGIC FROM dars_nic_391419_j3w9t_collab.master_codelist_ccu002_vaccine_covariates_sjk
# MAGIC UNION ALL
# MAGIC SELECT smoking_status AS codelist,
# MAGIC        'SNOMED' AS terminology,
# MAGIC        conceptID AS code,
# MAGIC        description AS term,
# MAGIC        "" AS code_type,
# MAGIC        "" AS RecordDate       
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_vaccine_smokingstatus_SNOMED
# MAGIC UNION ALL
# MAGIC SELECT *
# MAGIC FROM bhf_cvd_covid_uk_byod.bhf_covid_uk_phenotypes_20210127 
# MAGIC UNION ALL
# MAGIC SELECT name,
# MAGIC        CONCAT(terminology, '_SNOMEDmapped') AS terminology,
# MAGIC        code,
# MAGIC        ctv3_term AS term,
# MAGIC        "" AS code_type,
# MAGIC        "" AS RecordDate
# MAGIC FROM dars_nic_391419_j3w9t_collab.samantha_wp25_covariates_20210127_liver_ctv3_to_snomed_codelist
# MAGIC UNION ALL
# MAGIC SELECT name,
# MAGIC        'SNOMED' AS terminolgy,
# MAGIC        code,
# MAGIC        term,
# MAGIC        "" AS code_type,
# MAGIC        "" AS RecordDate
# MAGIC FROM global_temp.ccu002_02_vaccine_codelists

# COMMAND ----------

# MAGIC %md ## Save codelists

# COMMAND ----------

drop_table('ccu002_02_codelists')
create_table('ccu002_02_codelists')

# COMMAND ----------

# MAGIC %md ## Export codelists

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM dars_nic_391419_j3w9t_collab.ccu002_02_codelists
