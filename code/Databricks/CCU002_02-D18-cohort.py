# Databricks notebook source
# MAGIC %md %md # CCU002_02-D18-cohort
# MAGIC 
# MAGIC **Description** This notebook makes the analysis dataset.
# MAGIC 
# MAGIC **Author(s)** Venexia Walker, Sam Ip, Spencer Keene

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

# MAGIC %md ## Create temporary views for each outcome

# COMMAND ----------

# Generate temporary views for each outcome
for outcome in ["ICVT_pregnancy","other_DVT","DVT_ICVT","DVT_pregnancy","DVT_DVT","fracture","thrombocytopenia","TTP","mesenteric_thrombus","DIC","stroke_isch","other_arterial_embolism","PE","AMI","portal_vein_thrombosis","stroke_SAH_HS"]:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW " + outcome + " AS SELECT NHS_NUMBER_DEID, record_date AS " + outcome + "_date FROM dars_nic_391419_j3w9t_collab.ccu002_vac_outcomes_allsources_firstdiag_final_wohes WHERE name = '" + outcome + "'")

# COMMAND ----------

# Generate temporary views for each outcome
for outcome in ["DVT_summ_event","ICVT_summ_event"]:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW " + outcome + " AS SELECT NHS_NUMBER_DEID, record_date AS " + outcome + "_date FROM dars_nic_391419_j3w9t_collab.ccu002_vac_dvt_icvt_longformat_sjk WHERE composite_event_type = '" + outcome + "'")

# COMMAND ----------

# Generate temporary views for each outcome
for outcome in ["DVT_summ_event","ICVT_summ_event"]:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW anydiag_" + outcome + " AS SELECT NHS_NUMBER_DEID, record_date AS anydiag_" + outcome + "_date FROM dars_nic_391419_j3w9t_collab.ccu002_vac_dvt_icvt_longformat_anydiag_sjk WHERE composite_event_type = '" + outcome + "'")

# COMMAND ----------

# Generate temporary views for each outcome
for outcome in ["THROMBO_plus_ART","Arterial_event","THROMBO_plus_VEN","Venous_event","Haematological_event"]:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW " + outcome + " AS SELECT NHS_NUMBER_DEID, record_date AS " + outcome + "_date FROM dars_nic_391419_j3w9t_collab.ccu002_vac_combined_outcomes_longformat WHERE composite_event_type = '" + outcome + "'")

# COMMAND ----------

# Generate temporary views for each outcome
for outcome in ["AMI","DIC","DVT_DVT","DVT_ICVT","DVT_pregnancy","ICVT_pregnancy","PE","TTP","fracture","mesenteric_thrombus","other_DVT","other_arterial_embolism","portal_vein_thrombosis","stroke_SAH_HS","stroke_isch","thrombocytopenia"]:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW anydiag_" + outcome + " AS SELECT NHS_NUMBER_DEID, record_date AS anydiag_" + outcome + "_date FROM dars_nic_391419_j3w9t_collab.ccu002_vac_outcomes_allsources_anydiag_final_wohes WHERE name = '" + outcome + "'")

# COMMAND ----------

# Generate temporary views for each outcome
for outcome in ["Arterial_event","Venous_event","Haematological_event"]:
   sql("CREATE OR REPLACE GLOBAL TEMP VIEW anydiag_" + outcome + " AS SELECT NHS_NUMBER_DEID, record_date AS anydiag_" + outcome + "_date FROM dars_nic_391419_j3w9t_collab.ccu002_vac_combined_outcomes_longformat_anydiag WHERE composite_event_type = '" + outcome + "'")

# COMMAND ----------

# Generate temporary views for each outcome
for outcome in ["THROMBO_plus_fracture","THROMBO_plus_other_art","THROMBO_plus_DVT_ICVT","THROMBO_plus_DVT","THROMBO_plus_PE","THROMBO_plus_TTP","THROMBO_plus_AMI","THROMBO_plus_other_DVT","THROMBO_plus_stroke_SAH_HS","THROMBO_plus_DIC","THROMBO_plus_portal","THROMBO_plus_MESENT","THROMBO_plus_stroke_isch"]:
  sql("CREATE OR REPLACE GLOBAL TEMP VIEW " + outcome + " AS SELECT NHS_NUMBER_DEID, record_date AS " + outcome + "_date FROM dars_nic_391419_j3w9t_collab.ccu002_vac_thrombo_outcomes_longformat_individual_events WHERE composite_event_type = '" + outcome + "'")

# COMMAND ----------

# MAGIC %md ## Combine cohort variables

# COMMAND ----------

# MAGIC %sql
# MAGIC -- Create cohort from skinny table + vaccination + covariates + outcomes
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_vacc_cohort_full AS
# MAGIC SELECT FLOOR(RAND()*7)+1 AS CHUNK, -- CHUNK divides the data into parts for import into R
# MAGIC        cohort.NHS_NUMBER_DEID,
# MAGIC        cohort.SEX,
# MAGIC        CASE WHEN cohort.CATEGORISED_ETHNICITY IS NULL THEN 'missing' ELSE cohort.CATEGORISED_ETHNICITY END AS CATEGORISED_ETHNICITY,
# MAGIC        cohort.DATE_OF_DEATH AS death_date,
# MAGIC        cohort.AGE_AT_COHORT_START,
# MAGIC        vaccination.VACCINATION_DATE AS VACCINATION_DATE_FIRST,
# MAGIC        vaccination.VACCINE_PRODUCT AS VACCINE_PRODUCT_FIRST,
# MAGIC        CASE WHEN unique_bnf_chaps.UNIQUE_BNF_CHAPS IS NULL THEN 0 ELSE unique_bnf_chaps.UNIQUE_BNF_CHAPS END AS UNIQUE_BNF_CHAPS,
# MAGIC        CASE WHEN meds.ANTICOAG_MEDS=1 THEN 1 ELSE 0 END AS ANTICOAG_MEDS,
# MAGIC        CASE WHEN meds.ANTIPLATELET_MEDS=1 THEN 1 ELSE 0 END AS ANTIPLATLET_MEDS,
# MAGIC        CASE WHEN meds.BP_LOWER_MEDS=1 THEN 1 ELSE 0 END AS BP_LOWER_MEDS,
# MAGIC        CASE WHEN meds.LIPID_LOWER_MEDS=1 THEN 1 ELSE 0 END AS LIPID_LOWER_MEDS,
# MAGIC        CASE WHEN meds.COCP_MEDS=1 AND cohort.AGE_AT_COHORT_START<70 THEN 1 ELSE 0 END AS COCP_MEDS,
# MAGIC        CASE WHEN meds.HRT_MEDS=1 THEN 1 ELSE 0 END AS HRT_MEDS,
# MAGIC        covariates.COVID_infection_ AS COVID_infection,
# MAGIC        CASE WHEN covariates.EVER_ALL_STROKE=1 THEN 1 ELSE 0 END AS  EVER_ALL_STROKE,
# MAGIC        CASE WHEN covariates.EVER_AMI_=1 THEN 1 ELSE 0 END AS EVER_AMI,
# MAGIC        CASE WHEN covariates.EVER_CANCER_=1 THEN 1 ELSE 0 END AS EVER_CANCER,
# MAGIC        CASE WHEN covariates.EVER_CKD_=1 THEN 1 ELSE 0 END AS EVER_CKD,
# MAGIC        CASE WHEN covariates.EVER_COPD_=1 THEN 1 ELSE 0 END AS EVER_COPD,
# MAGIC        CASE WHEN covariates.EVER_DEMENTIA_=1 THEN 1 ELSE 0 END AS EVER_DEMENTIA,
# MAGIC        CASE WHEN covariates.EVER_DEPR_=1 THEN 1 ELSE 0 END AS EVER_DEPR,
# MAGIC        CASE WHEN covariates.EVER_DIAB_DIAG_OR_MEDS=1 THEN 1 ELSE 0 END AS EVER_DIAB_DIAG_OR_MEDS,
# MAGIC        CASE WHEN covariates.EVER_ICVT_=1 THEN 1 ELSE 0 END AS EVER_ICVT,
# MAGIC        CASE WHEN covariates.EVER_LIVER_=1 THEN 1 ELSE 0 END AS EVER_LIVER,
# MAGIC        CASE WHEN covariates.EVER_OBESITY=1 THEN 1 ELSE 0 END AS EVER_OBESITY,
# MAGIC        CASE WHEN covariates.EVER_PE_VT=1 THEN 1 ELSE 0 END AS EVER_PE_VT,
# MAGIC        CASE WHEN covariates.EVER_TCP_=1 THEN 1 ELSE 0 END AS EVER_TCP,
# MAGIC        CASE WHEN covariates.EVER_THROMBOPHILIA_=1 THEN 1 ELSE 0 END AS EVER_THROMBOPHILIA,        
# MAGIC        CASE WHEN covariates.N_DISORDER IS NULL THEN 0 ELSE covariates.N_DISORDER END AS N_DISORDER,
# MAGIC        CASE WHEN covariates.SURGERY_LASTYR IS NULL THEN 0 ELSE covariates.SURGERY_LASTYR END AS SURGERY_LASTYR,
# MAGIC        CASE WHEN covariates.region_name IS NULL THEN 'missing' ELSE covariates.region_name END AS region_name,
# MAGIC        CASE WHEN covariates.smoking_status_ IS NULL THEN 'missing' ELSE covariates.smoking_status_ END AS smoking_status,
# MAGIC        CASE WHEN covariates.DECI_IMD IS NULL THEN 'missing' 
# MAGIC             WHEN (covariates.DECI_IMD=1 OR covariates.DECI_IMD=2) THEN 'Deciles_1_2'
# MAGIC             WHEN (covariates.DECI_IMD=3 OR covariates.DECI_IMD=4) THEN 'Deciles_3_4'
# MAGIC             WHEN (covariates.DECI_IMD=5 OR covariates.DECI_IMD=6) THEN 'Deciles_5_6'
# MAGIC             WHEN (covariates.DECI_IMD=7 OR covariates.DECI_IMD=8) THEN 'Deciles_7_8'
# MAGIC             WHEN (covariates.DECI_IMD=9 OR covariates.DECI_IMD=10) THEN 'Deciles_9_10' END AS IMD,
# MAGIC        ICVT_pregnancy.ICVT_pregnancy_date AS diag1_ICVT_pregnancy_date,
# MAGIC        other_DVT.other_DVT_date AS diag1_other_DVT_date,
# MAGIC        DVT_ICVT.DVT_ICVT_date AS diag1_DVT_ICVT_date,
# MAGIC        DVT_pregnancy.DVT_pregnancy_date AS diag1_DVT_pregnancy_date,
# MAGIC        DVT_DVT.DVT_DVT_date AS diag1_DVT_DVT_date,
# MAGIC        fracture.fracture_date AS diag1_fracture_date ,
# MAGIC        thrombocytopenia.thrombocytopenia_date AS diag1_thrombocytopenia_date,
# MAGIC        TTP.TTP_date AS diag1_TTP_date,
# MAGIC        mesenteric_thrombus.mesenteric_thrombus_date AS diag1_mesenteric_thrombus_date,
# MAGIC        DIC.DIC_date AS diag1_DIC_date,
# MAGIC        stroke_isch.stroke_isch_date AS diag1_stroke_isch_date,
# MAGIC        other_arterial_embolism.other_arterial_embolism_date AS diag1_other_arterial_embolism_date,
# MAGIC        PE.PE_date AS diag1_PE_date,
# MAGIC        AMI.AMI_date AS diag1_AMI_date,
# MAGIC        portal_vein_thrombosis.portal_vein_thrombosis_date AS diag1_portal_vein_thrombosis_date,
# MAGIC        stroke_SAH_HS.stroke_SAH_HS_date AS diag1_stroke_SAH_HS_date,
# MAGIC        DVT_summ_event.DVT_summ_event_date AS diag1_DVT_summ_event_date,
# MAGIC        ICVT_summ_event.ICVT_summ_event_date AS diag1_ICVT_summ_event_date,
# MAGIC        Arterial_event.Arterial_event_date AS diag1_Arterial_event_date,
# MAGIC        Venous_event.Venous_event_date AS diag1_Venous_event_date,
# MAGIC        Haematological_event.Haematological_event_date AS diag1_Haematological_event_date,
# MAGIC        THROMBO_plus_ART.THROMBO_plus_ART_date,
# MAGIC        THROMBO_plus_VEN.THROMBO_plus_VEN_date,
# MAGIC        THROMBO_plus_fracture.THROMBO_plus_fracture_date,
# MAGIC        THROMBO_plus_other_art.THROMBO_plus_other_art_date,
# MAGIC        THROMBO_plus_DVT_ICVT.THROMBO_plus_DVT_ICVT_date,
# MAGIC        THROMBO_plus_DVT.THROMBO_plus_DVT_date,
# MAGIC        THROMBO_plus_PE.THROMBO_plus_PE_date,
# MAGIC        THROMBO_plus_TTP.THROMBO_plus_TTP_date,
# MAGIC        THROMBO_plus_AMI.THROMBO_plus_AMI_date,
# MAGIC        THROMBO_plus_other_DVT.THROMBO_plus_other_DVT_date,
# MAGIC        THROMBO_plus_stroke_SAH_HS.THROMBO_plus_stroke_SAH_HS_date,
# MAGIC        THROMBO_plus_DIC.THROMBO_plus_DIC_date,
# MAGIC        THROMBO_plus_portal.THROMBO_plus_portal_date,
# MAGIC        THROMBO_plus_MESENT.THROMBO_plus_MESENT_date,
# MAGIC        THROMBO_plus_stroke_isch.THROMBO_plus_stroke_isch_date,
# MAGIC        anydiag_AMI.anydiag_AMI_date,
# MAGIC        anydiag_DIC.anydiag_DIC_date,
# MAGIC        anydiag_DVT_DVT.anydiag_DVT_DVT_date,
# MAGIC        anydiag_DVT_ICVT.anydiag_DVT_ICVT_date,
# MAGIC        anydiag_DVT_pregnancy.anydiag_DVT_pregnancy_date,
# MAGIC        anydiag_ICVT_pregnancy.anydiag_ICVT_pregnancy_date,
# MAGIC        anydiag_PE.anydiag_PE_date,
# MAGIC        anydiag_TTP.anydiag_TTP_date,
# MAGIC        anydiag_fracture.anydiag_fracture_date,
# MAGIC        anydiag_mesenteric_thrombus.anydiag_mesenteric_thrombus_date,
# MAGIC        anydiag_other_DVT.anydiag_other_DVT_date,
# MAGIC        anydiag_other_arterial_embolism.anydiag_other_arterial_embolism_date,
# MAGIC        anydiag_portal_vein_thrombosis.anydiag_portal_vein_thrombosis_date,
# MAGIC        anydiag_stroke_SAH_HS.anydiag_stroke_SAH_HS_date,
# MAGIC        anydiag_stroke_isch.anydiag_stroke_isch_date,
# MAGIC        anydiag_thrombocytopenia.anydiag_thrombocytopenia_date,
# MAGIC        anydiag_DVT_summ_event.anydiag_DVT_summ_event_date,
# MAGIC        anydiag_ICVT_summ_event.anydiag_ICVT_summ_event_date,
# MAGIC        anydiag_Arterial_event.anydiag_Arterial_event_date AS anydiag_Arterial_event_date,
# MAGIC        anydiag_Venous_event.anydiag_Venous_event_date AS anydiag_Venous_event_date,
# MAGIC        anydiag_Haematological_event.anydiag_Haematological_event_date AS anydiag_Haematological_event_date,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,ICVT_pregnancy.ICVT_pregnancy_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (ICVT_pregnancy.ICVT_pregnancy_date IS NOT NULL)) THEN 1 ELSE 0 END AS death28days_ICVT_pregnancy,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,other_DVT.other_DVT_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (other_DVT.other_DVT_date IS NOT NULL)) THEN 1 ELSE 0 END AS  death28days_other_DVT,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,DVT_ICVT.DVT_ICVT_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (DVT_ICVT.DVT_ICVT_date IS NOT NULL)) THEN 1 ELSE 0 END AS  death28days_DVT_ICVT,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,DVT_pregnancy.DVT_pregnancy_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (DVT_pregnancy.DVT_pregnancy_date IS NOT NULL)) THEN 1 ELSE 0 END AS  death28days_DVT_pregnancy,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,DVT_DVT.DVT_DVT_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (DVT_DVT.DVT_DVT_date IS NOT NULL)) THEN 1 ELSE 0 END AS  death28days_DVT_DVT,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,fracture.fracture_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (fracture.fracture_date IS NOT NULL)) THEN 1 ELSE 0 END AS  death28days_fracture,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,thrombocytopenia.thrombocytopenia_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (thrombocytopenia.thrombocytopenia_date IS NOT NULL)) THEN 1 ELSE 0 END AS  death28days_thrombocytopenia,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,TTP.TTP_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (TTP.TTP_date IS NOT NULL)) THEN 1 ELSE 0 END AS death28days_TTP,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,mesenteric_thrombus.mesenteric_thrombus_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (mesenteric_thrombus.mesenteric_thrombus_date IS NOT NULL)) THEN 1 ELSE 0 END AS  death28days_mesenteric_thrombus,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,DIC.DIC_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (DIC.DIC_date IS NOT NULL)) THEN 1 ELSE 0 END AS  death28days_DIC,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,stroke_isch.stroke_isch_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (stroke_isch.stroke_isch_date IS NOT NULL)) THEN 1 ELSE 0 END AS  death28days_stroke_isch,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,other_arterial_embolism.other_arterial_embolism_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (other_arterial_embolism.other_arterial_embolism_date IS NOT NULL)) THEN 1 ELSE 0 END AS  death28days_other_arterial_embolism,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,PE.PE_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (PE.PE_date IS NOT NULL)) THEN 1 ELSE 0 END AS  death28days_PE,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,AMI.AMI_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (AMI.AMI_date IS NOT NULL)) THEN 1 ELSE 0 END AS  death28days_AMI,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,portal_vein_thrombosis.portal_vein_thrombosis_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (portal_vein_thrombosis.portal_vein_thrombosis_date IS NOT NULL)) THEN 1 ELSE 0 END AS  death28days_portal_vein_thrombosis,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,stroke_SAH_HS.stroke_SAH_HS_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (stroke_SAH_HS.stroke_SAH_HS_date IS NOT NULL)) THEN 1 ELSE 0 END AS  death28days_stroke_SAH_HS,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,DVT_summ_event.DVT_summ_event_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (DVT_summ_event.DVT_summ_event_date IS NOT NULL)) THEN 1 ELSE 0 END AS  death28days_DVT_summ_event,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,ICVT_summ_event.ICVT_summ_event_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (ICVT_summ_event.ICVT_summ_event_date IS NOT NULL)) THEN 1 ELSE 0 END AS  death28days_ICVT_summ_event,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,THROMBO_plus_ART.THROMBO_plus_ART_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (THROMBO_plus_ART.THROMBO_plus_ART_date IS NOT NULL)) THEN 1 ELSE 0 END AS  death28days_THROMBO_plus_ART,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,Arterial_event.Arterial_event_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (Arterial_event.Arterial_event_date IS NOT NULL)) THEN 1 ELSE 0 END AS  death28days_Arterial_event,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,THROMBO_plus_VEN.THROMBO_plus_VEN_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (THROMBO_plus_VEN.THROMBO_plus_VEN_date IS NOT NULL)) THEN 1 ELSE 0 END AS  death28days_THROMBO_plus_VEN,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,Venous_event.Venous_event_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (Venous_event.Venous_event_date IS NOT NULL)) THEN 1 ELSE 0 END AS  death28days_Venous_event,
# MAGIC        CASE WHEN ((DATEDIFF(cohort.DATE_OF_DEATH,Haematological_event.Haematological_event_date)<29) AND (cohort.DATE_OF_DEATH IS NOT NULL) AND (Haematological_event.Haematological_event_date IS NOT NULL)) THEN 1 ELSE 0 END AS  death28days_Haematological_event
# MAGIC FROM dars_nic_391419_j3w9t_collab.ccu002_vac_included_patients AS cohort
# MAGIC LEFT JOIN (SELECT * FROM dars_nic_391419_j3w9t_collab.ccu002_vac_vaccine_status WHERE VACCINE_DOSE = "first dose") AS vaccination ON cohort.NHS_NUMBER_DEID = vaccination.NHS_NUMBER_DEID
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_meds_fix_sjk AS meds on cohort.NHS_NUMBER_DEID = meds.NHS_NUMBER_DEID
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_vac_included_patients_allcovariates_0524 AS covariates on cohort.NHS_NUMBER_DEID = covariates.NHS_NUMBER_DEID
# MAGIC LEFT JOIN dars_nic_391419_j3w9t_collab.ccu002_vaccine_covariates_BNFchap_corrected AS unique_bnf_chaps on cohort.NHS_NUMBER_DEID = unique_bnf_chaps.ID
# MAGIC LEFT JOIN global_temp.ICVT_pregnancy AS ICVT_pregnancy on cohort.NHS_NUMBER_DEID = ICVT_pregnancy.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.other_DVT AS other_DVT on cohort.NHS_NUMBER_DEID = other_DVT.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.DVT_ICVT AS DVT_ICVT on cohort.NHS_NUMBER_DEID = DVT_ICVT.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.DVT_pregnancy AS DVT_pregnancy on cohort.NHS_NUMBER_DEID = DVT_pregnancy.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.DVT_DVT AS DVT_DVT on cohort.NHS_NUMBER_DEID = DVT_DVT.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.fracture AS fracture on cohort.NHS_NUMBER_DEID = fracture.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.thrombocytopenia AS thrombocytopenia on cohort.NHS_NUMBER_DEID = thrombocytopenia.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.TTP AS TTP on cohort.NHS_NUMBER_DEID = TTP.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.mesenteric_thrombus AS mesenteric_thrombus on cohort.NHS_NUMBER_DEID = mesenteric_thrombus.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.DIC AS DIC on cohort.NHS_NUMBER_DEID = DIC.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.stroke_isch AS stroke_isch on cohort.NHS_NUMBER_DEID = stroke_isch.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.other_arterial_embolism AS other_arterial_embolism on cohort.NHS_NUMBER_DEID = other_arterial_embolism.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.PE AS PE on cohort.NHS_NUMBER_DEID = PE.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.AMI AS AMI on cohort.NHS_NUMBER_DEID = AMI.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.portal_vein_thrombosis AS portal_vein_thrombosis on cohort.NHS_NUMBER_DEID = portal_vein_thrombosis.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.stroke_SAH_HS AS stroke_SAH_HS on cohort.NHS_NUMBER_DEID = stroke_SAH_HS.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.DVT_summ_event AS DVT_summ_event on cohort.NHS_NUMBER_DEID = DVT_summ_event.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.ICVT_summ_event AS ICVT_summ_event on cohort.NHS_NUMBER_DEID = ICVT_summ_event.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.Arterial_event AS Arterial_event on cohort.NHS_NUMBER_DEID = Arterial_event.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.Venous_event AS Venous_event on cohort.NHS_NUMBER_DEID = Venous_event.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.Haematological_event AS Haematological_event on cohort.NHS_NUMBER_DEID = Haematological_event.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.THROMBO_plus_ART AS THROMBO_plus_ART on cohort.NHS_NUMBER_DEID = THROMBO_plus_ART.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.THROMBO_plus_VEN AS THROMBO_plus_VEN on cohort.NHS_NUMBER_DEID = THROMBO_plus_VEN.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.THROMBO_plus_fracture AS THROMBO_plus_fracture on cohort.NHS_NUMBER_DEID = THROMBO_plus_fracture.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.THROMBO_plus_other_art AS THROMBO_plus_other_art on cohort.NHS_NUMBER_DEID = THROMBO_plus_other_art.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.THROMBO_plus_DVT_ICVT AS THROMBO_plus_DVT_ICVT on cohort.NHS_NUMBER_DEID = THROMBO_plus_DVT_ICVT.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.THROMBO_plus_DVT AS THROMBO_plus_DVT on cohort.NHS_NUMBER_DEID = THROMBO_plus_DVT.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.THROMBO_plus_PE AS THROMBO_plus_PE on cohort.NHS_NUMBER_DEID = THROMBO_plus_PE.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.THROMBO_plus_TTP AS THROMBO_plus_TTP on cohort.NHS_NUMBER_DEID = THROMBO_plus_TTP.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.THROMBO_plus_AMI AS THROMBO_plus_AMI on cohort.NHS_NUMBER_DEID = THROMBO_plus_AMI.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.THROMBO_plus_other_DVT AS THROMBO_plus_other_DVT on cohort.NHS_NUMBER_DEID = THROMBO_plus_other_DVT.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.THROMBO_plus_stroke_SAH_HS AS THROMBO_plus_stroke_SAH_HS on cohort.NHS_NUMBER_DEID = THROMBO_plus_stroke_SAH_HS.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.THROMBO_plus_DIC AS THROMBO_plus_DIC on cohort.NHS_NUMBER_DEID = THROMBO_plus_DIC.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.THROMBO_plus_portal AS THROMBO_plus_portal on cohort.NHS_NUMBER_DEID = THROMBO_plus_portal.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.THROMBO_plus_MESENT AS THROMBO_plus_MESENT on cohort.NHS_NUMBER_DEID = THROMBO_plus_MESENT.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.THROMBO_plus_stroke_isch AS THROMBO_plus_stroke_isch on cohort.NHS_NUMBER_DEID = THROMBO_plus_stroke_isch.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.anydiag_AMI AS anydiag_AMI on cohort.NHS_NUMBER_DEID = anydiag_AMI.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.anydiag_DIC AS anydiag_DIC on cohort.NHS_NUMBER_DEID = anydiag_DIC.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.anydiag_DVT_DVT AS anydiag_DVT_DVT on cohort.NHS_NUMBER_DEID = anydiag_DVT_DVT.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.anydiag_DVT_ICVT AS anydiag_DVT_ICVT on cohort.NHS_NUMBER_DEID = anydiag_DVT_ICVT.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.anydiag_DVT_pregnancy AS anydiag_DVT_pregnancy on cohort.NHS_NUMBER_DEID = anydiag_DVT_pregnancy.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.anydiag_ICVT_pregnancy AS anydiag_ICVT_pregnancy on cohort.NHS_NUMBER_DEID = anydiag_ICVT_pregnancy.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.anydiag_PE AS anydiag_PE on cohort.NHS_NUMBER_DEID = anydiag_PE.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.anydiag_TTP AS anydiag_TTP on cohort.NHS_NUMBER_DEID = anydiag_TTP.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.anydiag_fracture AS anydiag_fracture on cohort.NHS_NUMBER_DEID = anydiag_fracture.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.anydiag_mesenteric_thrombus AS anydiag_mesenteric_thrombus on cohort.NHS_NUMBER_DEID = anydiag_mesenteric_thrombus.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.anydiag_other_DVT AS anydiag_other_DVT on cohort.NHS_NUMBER_DEID = anydiag_other_DVT.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.anydiag_other_arterial_embolism AS anydiag_other_arterial_embolism on cohort.NHS_NUMBER_DEID = anydiag_other_arterial_embolism.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.anydiag_portal_vein_thrombosis AS anydiag_portal_vein_thrombosis on cohort.NHS_NUMBER_DEID = anydiag_portal_vein_thrombosis.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.anydiag_stroke_SAH_HS AS anydiag_stroke_SAH_HS on cohort.NHS_NUMBER_DEID = anydiag_stroke_SAH_HS.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.anydiag_stroke_isch AS anydiag_stroke_isch on cohort.NHS_NUMBER_DEID = anydiag_stroke_isch.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.anydiag_thrombocytopenia AS anydiag_thrombocytopenia on cohort.NHS_NUMBER_DEID = anydiag_thrombocytopenia.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.anydiag_DVT_summ_event AS anydiag_DVT_summ_event on cohort.NHS_NUMBER_DEID = anydiag_DVT_summ_event.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.anydiag_ICVT_summ_event AS anydiag_ICVT_summ_event on cohort.NHS_NUMBER_DEID = anydiag_ICVT_summ_event.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.anydiag_Arterial_event AS anydiag_Arterial_event on cohort.NHS_NUMBER_DEID = anydiag_Arterial_event.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.anydiag_Venous_event AS anydiag_Venous_event on cohort.NHS_NUMBER_DEID = anydiag_Venous_event.NHS_NUMBER_DEID
# MAGIC LEFT JOIN global_temp.anydiag_Haematological_event AS anydiag_Haematological_event on cohort.NHS_NUMBER_DEID = anydiag_Haematological_event.NHS_NUMBER_DEID

# COMMAND ----------

# MAGIC %md ## Apply exclusions

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_vacc_cohort AS
# MAGIC SELECT *
# MAGIC FROM global_temp.ccu002_vacc_cohort_full
# MAGIC WHERE ((SEX=2) OR (SEX=1 AND COCP_MEDS=0 AND HRT_MEDS=0)) -- Remove men indicated for COCP or HRT as these drugs are primarily indicated for women
# MAGIC AND (AGE_AT_COHORT_START<111) -- Remove people older than 110
# MAGIC AND ((VACCINATION_DATE_FIRST IS NULL) OR (death_date IS NULL) OR (VACCINATION_DATE_FIRST < death_date)) -- Remove people who die prior to vaccination

# COMMAND ----------

# MAGIC %md ## Save cohort

# COMMAND ----------

drop_table('ccu002_vacc_cohort')
create_table('ccu002_vacc_cohort')
