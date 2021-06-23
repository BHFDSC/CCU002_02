# Databricks notebook source
# MAGIC %md # CCU002_02-D06-drug_codelists
# MAGIC 
# MAGIC **Description** This notebook generates the drug codelists needed for the CCU002 vaccine project.
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

# MAGIC %md ## Define drug codelists

# COMMAND ----------

# MAGIC %sql -- Create global temporary view containing all codelists
# MAGIC CREATE
# MAGIC OR REPLACE GLOBAL TEMPORARY VIEW ccu002_vacc_drug_codelists AS
# MAGIC SELECT
# MAGIC   DISTINCT PrescribedBNFCode AS code,
# MAGIC   PrescribedBNFName AS term,
# MAGIC   'BNF' AS system,
# MAGIC   'antiplatelet' AS codelist
# MAGIC FROM
# MAGIC   dars_nic_391419_j3w9t.primary_care_meds_dars_nic_391419_j3w9t
# MAGIC WHERE
# MAGIC   left(PrescribedBNFCode, 4) = '0209'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   DISTINCT PrescribedBNFCode AS code,
# MAGIC   PrescribedBNFName AS term,
# MAGIC   'BNF' AS system,
# MAGIC   'bp_lowering' AS codelist
# MAGIC FROM
# MAGIC   dars_nic_391419_j3w9t.primary_care_meds_dars_nic_391419_j3w9t
# MAGIC WHERE
# MAGIC   left(PrescribedBNFCode, 9) = '0205053A0' -- aliskiren
# MAGIC   OR left(PrescribedBNFCode, 6) = '020504' -- alpha blockers
# MAGIC   OR (
# MAGIC     left(PrescribedBNFCode, 4) = '0204' -- beta blockers
# MAGIC     AND NOT (
# MAGIC       left(PrescribedBNFCode, 9) = '0204000R0' -- exclude propranolol
# MAGIC       OR left(PrescribedBNFCode, 9) = '0204000Q0' -- exclude propranolol
# MAGIC     )
# MAGIC   )
# MAGIC   OR left(PrescribedBNFCode, 6) = '020602' -- calcium channel blockers
# MAGIC   OR (
# MAGIC     left(PrescribedBNFCode, 6) = '020502' -- centrally acting antihypertensives
# MAGIC     AND NOT (
# MAGIC       left(PrescribedBNFCode, 8) = '0205020G' -- guanfacine because it is only used for ADHD
# MAGIC       OR left(PrescribedBNFCode, 9) = '0205052AE' -- drugs for heart failure, not for hypertension
# MAGIC     )
# MAGIC   )
# MAGIC   OR left(PrescribedBNFCode, 6) = '020203' -- potassium sparing diuretics
# MAGIC   OR left(PrescribedBNFCode, 6) = '020201' -- thiazide diuretics
# MAGIC   OR left(PrescribedBNFCode, 6) = '020501' -- vasodilator antihypertensives
# MAGIC   OR left(PrescribedBNFCode, 7) = '0205051' -- angiotensin-converting enzyme inhibitors
# MAGIC   OR left(PrescribedBNFCode, 7) = '0205052' -- angiotensin-II receptor antagonists
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   DISTINCT PrescribedBNFCode AS code,
# MAGIC   PrescribedBNFName AS term,
# MAGIC   'BNF' AS system,
# MAGIC   'lipid_lowering' AS codelist
# MAGIC FROM
# MAGIC   dars_nic_391419_j3w9t.primary_care_meds_dars_nic_391419_j3w9t
# MAGIC WHERE
# MAGIC   left(PrescribedBNFCode, 4) = '0212'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   DISTINCT PrescribedBNFCode AS code,
# MAGIC   PrescribedBNFName AS term,
# MAGIC   'BNF' AS system,
# MAGIC   'anticoagulant' AS codelist
# MAGIC FROM
# MAGIC   dars_nic_391419_j3w9t.primary_care_meds_dars_nic_391419_j3w9t
# MAGIC WHERE
# MAGIC   (
# MAGIC     left(PrescribedBNFCode, 6) = '020802'
# MAGIC     AND NOT (
# MAGIC       left(PrescribedBNFCode, 8) = '0208020I'
# MAGIC       OR left(PrescribedBNFCode, 8) = '0208020W'
# MAGIC     )
# MAGIC   )
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   DISTINCT PrescribedBNFCode AS code,
# MAGIC   PrescribedBNFName AS term,
# MAGIC   'BNF' AS system,
# MAGIC   'cocp' AS codelist
# MAGIC FROM
# MAGIC   dars_nic_391419_j3w9t.primary_care_meds_dars_nic_391419_j3w9t
# MAGIC WHERE
# MAGIC   left(PrescribedBNFCode, 6) = '070301'
# MAGIC UNION ALL
# MAGIC SELECT
# MAGIC   DISTINCT PrescribedBNFCode AS code,
# MAGIC   PrescribedBNFName AS term,
# MAGIC   'BNF' AS system,
# MAGIC   'hrt' AS codelist
# MAGIC FROM
# MAGIC   dars_nic_391419_j3w9t.primary_care_meds_dars_nic_391419_j3w9t
# MAGIC WHERE
# MAGIC   left(PrescribedBNFCode, 7) = '0604011'

# COMMAND ----------

# MAGIC %md ## Define covariate codelists 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC create or replace global temp view master_codelist_ccu002_vaccine_covariates_sjk as
# MAGIC 
# MAGIC select * 
# MAGIC from values
# MAGIC ("stroke_HS","ICD10","I61","Nontraumatic intracerebral hemorrhage","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","71444005","Cerebral thrombosis","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","75543006","Cerebral embolism","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","78569004","Posterior inferior cerebellar artery syndrome","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","95457000","Brain stem infarction","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","95460007","Cerebellar infarction","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","195185009","Cerebral infarct due to thrombosis of precerebral arteries","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","195186005","Cerebral infarction due to embolism of precerebral arteries","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","195189003","Cerebral infarction due to thrombosis of cerebral arteries","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","195190007","Cerebral infarction due to embolism of cerebral arteries","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","195200006","Carotid artery syndrome hemispheric","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","195201005","Multiple and bilateral precerebral artery syndromes","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","195209007","Middle cerebral artery syndrome","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","195210002","Anterior cerebral artery syndrome","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","195211003","Posterior cerebral artery syndrome","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","230691006","CVA - cerebrovascular accident due to cerebral artery occlusion","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","230692004","Infarction - precerebral","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","230698000","Lacunar infarction","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","230699008","Pure motor lacunar syndrome","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","230700009","Pure sensory lacunar infarction","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","307766002","Left sided cerebral infarction","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","307767006","Right sided cerebral infarction","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","373606000","Occlusive stroke (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","413102000","Infarction of basal ganglia (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","432504007","Cerebral infarction (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","724424009","Cerebral ischemic stroke due to small artery occlusion (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","125081000119106","Cerebral infarction due to occlusion of precerebral artery","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","140921000119102","Ischaemic stroke without coma","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","1089421000000100","Cerebral infarction due to stenosis of cerebral artery (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","16000511000119100","Cerebrovascular accident due to occlusion of left middle cerebral artery (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","16002031000119100","Cerebrovascular accident due to thrombus of right middle cerebral artery (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","16002111000119100","Cerebrovascular accident due to thrombus of left middle cerebral artery (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","195230003","Cerebral infarction due to cerebral venous thrombosis, non-pyogenic (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","20059004","Occlusion of cerebral artery (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","230693009","Anterior cerebral circulation infarction (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","230696001","Posterior cerebral circulation infarction (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","230701008","Pure sensorimotor lacunar infarction (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","307363008","Multiple lacunar infarcts (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","329641000119104","Cerebrovascular accident due to thrombus of basilar artery (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","371041009","Embolic stroke (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","734383005","Thrombosis of left middle cerebral artery (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","87555007","Claude's syndrome (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","1089411000000100",	"Cerebral infarction due to occlusion of cerebral artery (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","195213000","Cerebellar stroke syndrome (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","230694003","Total anterior cerebral circulation infarction (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","230695002","Partial anterior cerebral circulation infarction (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","230702001","Lacunar ataxic hemiparesis (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","230703006","Dysarthria-clumsy hand syndrome (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","230704000","Multi-infarct state (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","24654003","Weber-Gubler syndrome (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","25133001","Completed stroke (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","276219001","Occipital cerebral infarction (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","276220007","Foville syndrome (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","276221006","Millard-Gubler syndrome (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","276222004","Top of basilar syndrome (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","413758000","Cardioembolic stroke (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","426107000","Acute lacunar infarction (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","734384004","Thrombosis of right middle cerebral artery (disorder)","1","20210127"),
# MAGIC ("stroke_IS","SNOMED","90099008","Subcortical leukoencephalopathy (disorder)","1","20210127"),
# MAGIC ("stroke_IS","ICD10","I63.0","Cerebral infarction due to thrombosis of precerebral arteries","1","20210127"),
# MAGIC ("stroke_IS","ICD10","I63.1","Cerebral infarction due to embolism of precerebral arteries","1","20210127"),
# MAGIC ("stroke_IS","ICD10","I63.2","Cerebral infarction due to unspecified occlusion or stenosis of precerebral arteries","1","20210127"),
# MAGIC ("stroke_IS","ICD10","I63.3","Cerebral infarction due to thrombosis of cerebral arteries","1","20210127"),
# MAGIC ("stroke_IS","ICD10","I63.4","Cerebral infarction due to embolism of cerebral arteries","1","20210127"),
# MAGIC ("stroke_IS","ICD10","I63.5","Cerebral infarction due to unspecified occlusion or stenosis of cerebral arteries","1","20210127"),
# MAGIC ("stroke_IS","ICD10","I63.8","Other cerebral infarction","1","20210127"),
# MAGIC ("stroke_IS","ICD10","I63.9","Cerebral infarction, unspecified","1","20210127"),
# MAGIC ("DVT_ICVT","ICD10","G08","Intracranial and intraspinal phlebitis and thrombophlebitis","1","20210127"),
# MAGIC ("DVT_ICVT","ICD10","I67.6","Nonpyogenic thrombosis of intracranial venous system","1","20210127"),
# MAGIC ("DVT_ICVT","ICD10","I63.6","Cerebral infarction due to cerebral venous thrombosis,nonpyogenic","1","20210127"),
# MAGIC ("DVT_ICVT","SNOMED","195230003","Cerebral infarction due to cerebral venous thrombosis,  nonpyogenic","1","20210127"),
# MAGIC ("TCP","ICD10","D69.3","Idiopathic thrombocytopenic purpura","1","20210127"),
# MAGIC ("TCP","ICD10","D69.4","Other primary thrombocytopenia","1","20210127"),
# MAGIC ("TCP","ICD10","D69.5","Secondary thrombocytopenia","1","20210127"),
# MAGIC ("TCP","ICD10","D69.6","Thrombocytopenia, unspecified","1","20210127"),
# MAGIC ("TCP","ICD10","M31.1","Thrombotic microangiopathy","1","20210127"),
# MAGIC ("thrombophilia","ICD10","D68.5","Primary thrombophilia","1","20210127"),
# MAGIC ("thrombophilia","ICD10","D68.6","Other thrombophilia","1","20210127"),
# MAGIC ("VT","ICD10","I80.1","Phlebitis and thrombophlebitis of femoral vein",	"1","20210127"),
# MAGIC ("VT","ICD10","I82.8","Embolism and thrombosis of other specified veins","1","20210127"),
# MAGIC ("VT","ICD10","I82.9","Embolism and thrombosis of unspecified vein","1","20210127"),
# MAGIC ("VT","ICD10","O22.3","Deep phlebothrombosis in pregnancy","1","20210127"),
# MAGIC ("VT","ICD10","I82.2","Embolism and thrombosis of vena cava","1","20210127"),
# MAGIC ("VT","ICD10","I82.0","Budd-Chiari syndrome","1","20210127"),
# MAGIC ("VT","ICD10","I80.2","Phlebitis and thrombophlebitis of other deep vessels of lower extremities","1","20210127"),
# MAGIC ("VT","ICD10","I81","Portal vein thrombosis","1","20210127"),
# MAGIC ("VT","ICD10","O08.2","Embolism following abortion and ectopic and molar pregnancy","1","20210127"),
# MAGIC ("VT","ICD10","I82.3","Embolism and thrombosis of renal vein","1","20210127"),
# MAGIC ("VT","ICD10","O87.1","Deep phlebothrombosis in the puerperium","1","20210127"),
# MAGIC ("VT","ICD10","O87.9","Venous complication in the puerperium, unspecified","1","20210127"),
# MAGIC ("VT","ICD10","O88.2","Obstetric blood-clot embolism","1","20210127"),
# MAGIC ("VT","ICD10","O22.5","Cerebral venous thrombosis in pregnancy","1","20210127"),
# MAGIC ("VT","ICD10","O87.3","Cerebral venous thrombosis in the puerperium","1","20210127"),
# MAGIC ("VT","ICD10","I80.3","Phlebitis and thrombophlebitis of lower extremities, unspecified","1","20210127"),
# MAGIC ("PE","ICD10","I26.0","Pulmonary embolism without mention of acute cor pulmonale","1","20210127"),
# MAGIC ("PE","ICD10","I26.9","Pulmonary embolism with mention of acute cor pulmonale","1","20210127"),
# MAGIC ("PE","SNOMED","438773007","Pulmonary embolism with mention of acute cor pulmonale","1","20210127"),
# MAGIC ("PE","SNOMED","133971000119108","Pulmonary embolism with mention of acute cor pulmonale","1","20210127"),
# MAGIC ("TCP","SNOMED","74576004","Acquired thrombocytopenia","1","20210127"),
# MAGIC ("TCP","SNOMED","439007008","Acquired thrombotic thrombocytopenic purpura","1","20210127"),
# MAGIC ("TCP","SNOMED","28505005","Acute idiopathic thrombocytopenic purpura","1","20210127"),
# MAGIC ("TCP","SNOMED","128091003","Autoimmune thrombocytopenia","1","20210127"),
# MAGIC ("TCP","SNOMED","13172003","Autoimmune thrombocytopenic purpura","1","20210127"),
# MAGIC ("TCP","SNOMED","438476003","Autoimmune thrombotic thrombocytopenic purpura","1","20210127"),
# MAGIC ("TCP","SNOMED","111588002","Heparin associated thrombotic thrombocytopenia","1","20210127"),
# MAGIC ("TCP","SNOMED","73397007","Heparin induced thrombocytopaenia","1","20210127"),
# MAGIC ("TCP","SNOMED","438492008","Hereditary thrombocytopenic disorder","1","20210127"),
# MAGIC ("TCP","SNOMED","441511006","History of immune thrombocytopenia","1","20210127"),
# MAGIC ("TCP","SNOMED","49341000119108","History of thrombocytopaenia",	"1","20210127"),
# MAGIC ("TCP","SNOMED","726769004","HIT (Heparin induced thrombocytopenia) antibody","1","20210127"),
# MAGIC ("TCP","SNOMED","371106008","Idiopathic maternal thrombocytopenia","1","20210127"),
# MAGIC ("TCP","SNOMED","32273002","Idiopathic thrombocytopenic purpura","1","20210127"),
# MAGIC ("TCP","SNOMED","2897005","Immune thrombocytopenia","1","20210127"),
# MAGIC ("TCP","SNOMED","32273002","Immune thrombocytopenic purpura","1","20210127"),
# MAGIC ("TCP","SNOMED","36070007","Immunodeficiency with thrombocytopenia AND eczema","1","20210127"),
# MAGIC ("TCP","SNOMED","33183004","Post infectious thrombocytopenic purpura","1","20210127"),
# MAGIC ("TCP","SNOMED","267534000","Primary thrombocytopenia","1","20210127"),
# MAGIC ("TCP","SNOMED","154826009","Secondary thrombocytopenia","1","20210127"),
# MAGIC ("TCP","SNOMED","866152006","Thrombocytopenia due to 2019 novel coronavirus","1","20210127"),
# MAGIC ("TCP","SNOMED","82190001","Thrombocytopenia due to defective platelet production","1","20210127"),
# MAGIC ("TCP","SNOMED","78345002","Thrombocytopenia due to diminished platelet production","1","20210127"),
# MAGIC ("TCP","SNOMED","191323001","Thrombocytopenia due to extracorporeal circulation of blood","1","20210127"),
# MAGIC ("TCP","SNOMED","87902006",	"Thrombocytopenia due to non-immune destruction","1","20210127"),
# MAGIC ("TCP","SNOMED","302873008","Thrombocytopenic purpura",	"1","20210127"),
# MAGIC ("TCP","SNOMED","417626001","Thrombocytopenic purpura associated with metabolic disorder","1","20210127"),
# MAGIC ("TCP","SNOMED","402653004","Thrombocytopenic purpura due to defective platelet production","1","20210127"),
# MAGIC ("TCP","SNOMED","402654005","Thrombocytopenic purpura due to platelet consumption","1","20210127"),
# MAGIC ("TCP","SNOMED","78129009","Thrombotic thrombocytopenic purpura","1","20210127"),
# MAGIC ("TCP","SNOMED","441322009","Drug induced thrombotic thrombocytopenic purpura","1","20210127"),
# MAGIC ("TCP","SNOMED","19307009","Drug-induced immune thrombocytopenia","1","20210127"),
# MAGIC ("TCP","SNOMED","783251006","Hereditary thrombocytopenia with normal platelets","1","20210127"),
# MAGIC ("TCP","SNOMED","191322006","Thrombocytopenia caused by drugs","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","439001009","Acquired thrombophilia","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","441882000","History of thrombophilia","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","439698008","Primary thrombophilia","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","234467004","Thrombophilia","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","441697004","Thrombophilia associated with pregnancy","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","442760001","Thrombophilia caused by antineoplastic agent therapy","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","442197003","Thrombophilia caused by drug therapy","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","442654007","Thrombophilia caused by hormone therapy","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","442363001","Thrombophilia caused by vascular device","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","439126002","Thrombophilia due to acquired antithrombin III deficiency","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","439002002","Thrombophilia due to acquired protein C deficiency","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","439125003","Thrombophilia due to acquired protein S deficiency","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","441079006","Thrombophilia due to antiphospholipid antibody","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","441762006","Thrombophilia due to immobilisation","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","442078001","Thrombophilia due to malignant neoplasm","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","441946009","Thrombophilia due to myeloproliferative disorder","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","441990004","Thrombophilia due to paroxysmal nocturnal haemoglobinuria","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","441945008","Thrombophilia due to trauma","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","442121006","Thrombophilia due to vascular anomaly","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","439698008","Hereditary thrombophilia","1","20210127"),
# MAGIC ("thrombophilia","SNOMED","783250007","Hereditary thrombophilia due to congenital histidine-rich (poly-L) glycoprotein deficiency","1",	"20210127")
# MAGIC 
# MAGIC 
# MAGIC 
# MAGIC as tab(name, terminology, code, term, code_type, RecordDate);

# COMMAND ----------

# MAGIC %md ## Define smoking status codelists 

# COMMAND ----------

# MAGIC %sql
# MAGIC 
# MAGIC CREATE OR REPLACE GLOBAL TEMP VIEW ccu002_vaccine_smokingstatus_SNOMED  AS
# MAGIC 
# MAGIC SELECT *
# MAGIC FROM VALUES
# MAGIC 
# MAGIC ("160603005","Light cigarette smoker (1-9 cigs/day) (finding)","Current-smoker","Light"),
# MAGIC ("160606002","Very heavy cigarette smoker (40+ cigs/day) (finding)","Current-smoker","Heavy"),
# MAGIC ("160613002","Admitted tobacco consumption possibly untrue (finding)","Current-smoker","Unknown"),
# MAGIC ("160619003","Rolls own cigarettes (finding)","Current-smoker","Unknown"),
# MAGIC ("230056004","Cigarette consumption (observable entity)","Current-smoker","Unknown"),
# MAGIC ("230057008","Cigar consumption (observable entity)","Current-smoker","Unknown"),
# MAGIC ("230058003","Pipe tobacco consumption (observable entity)","Current-smoker","Unknown"),
# MAGIC ("230060001","Light cigarette smoker (finding)","Current-smoker","Light"),
# MAGIC ("230062009","Moderate cigarette smoker (finding)","Current-smoker","Moderate"),
# MAGIC ("230065006","Chain smoker (finding)","Current-smoker","Heavy"),
# MAGIC ("266918002","Tobacco smoking consumption (observable entity)","Current-smoker","Unknown"),
# MAGIC ("446172000","Failed attempt to stop smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("449868002","Smokes tobacco daily (finding)","Current-smoker","Unknown"),
# MAGIC ("56578002","Moderate smoker (20 or less per day) (finding)","Current-smoker","Moderate"),
# MAGIC ("56771006","Heavy smoker (over 20 per day) (finding)","Current-smoker","Heavy"),
# MAGIC ("59978006","Cigar smoker (finding)","Current-smoker","Unknown"),
# MAGIC ("65568007","Cigarette smoker (finding)","Current-smoker","Unknown"),
# MAGIC ("134406006","Smoking reduced (finding)","Current-smoker","Unknown"),
# MAGIC ("160604004","Moderate cigarette smoker (10-19 cigs/day) (finding)","Current-smoker","Moderate"),
# MAGIC ("160605003","Heavy cigarette smoker (20-39 cigs/day) (finding)","Current-smoker","Heavy"),
# MAGIC ("160612007","Keeps trying to stop smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("160616005","Trying to give up smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("203191000000107","Wants to stop smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("225934006","Smokes in bed (finding)","Current-smoker","Unknown"),
# MAGIC ("230059006","Occasional cigarette smoker (finding)","Current-smoker","Light"),
# MAGIC ("230063004","Heavy cigarette smoker (finding)","Current-smoker","Heavy"),
# MAGIC ("230064005","Very heavy cigarette smoker (finding)","Current-smoker","Heavy"),
# MAGIC ("266920004","Trivial cigarette smoker (less than one cigarette/day) (finding)","Current-smoker","Light"),
# MAGIC ("266929003","Smoking started (finding)","Current-smoker","Unknown"),
# MAGIC ("308438006","Smoking restarted (finding)","Current-smoker","Unknown"),
# MAGIC ("394871007","Thinking about stopping smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("394872000","Ready to stop smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("394873005","Not interested in stopping smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("401159003","Reason for restarting smoking (observable entity)","Current-smoker","Unknown"),
# MAGIC ("413173009","Minutes from waking to first tobacco consumption (observable entity)","Current-smoker","Unknown"),
# MAGIC ("428041000124106","Occasional tobacco smoker (finding)","Current-smoker","Light"),
# MAGIC ("77176002","Smoker (finding)","Current-smoker","Unknown"),
# MAGIC ("82302008","Pipe smoker (finding)","Current-smoker","Unknown"),
# MAGIC ("836001000000109","Waterpipe tobacco consumption (observable entity)","Current-smoker","Unknown"),
# MAGIC ("160603005","Light cigarette smoker (1-9 cigs/day) (finding)","Current-smoker","Light"),
# MAGIC ("160612007","Keeps trying to stop smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("160613002","Admitted tobacco consumption possibly untrue (finding)","Current-smoker","Unknown"),
# MAGIC ("160616005","Trying to give up smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("160619003","Rolls own cigarettes (finding)","Current-smoker","Unknown"),
# MAGIC ("160625004","Date ceased smoking (observable entity)","Current-smoker","Unknown"),
# MAGIC ("225934006","Smokes in bed (finding)","Current-smoker","Unknown"),
# MAGIC ("230056004","Cigarette consumption (observable entity)","Current-smoker","Unknown"),
# MAGIC ("230057008","Cigar consumption (observable entity)","Current-smoker","Unknown"),
# MAGIC ("230059006","Occasional cigarette smoker (finding)","Current-smoker","Light"),
# MAGIC ("230060001","Light cigarette smoker (finding)","Current-smoker","Light"),
# MAGIC ("230062009","Moderate cigarette smoker (finding)","Current-smoker","Moderate"),
# MAGIC ("230063004","Heavy cigarette smoker (finding)","Current-smoker","Heavy"),
# MAGIC ("230064005","Very heavy cigarette smoker (finding)","Current-smoker","Heavy"),
# MAGIC ("266920004","Trivial cigarette smoker (less than one cigarette/day) (finding)","Current-smoker","Light"),
# MAGIC ("266929003","Smoking started (finding)","Current-smoker","Unknown"),
# MAGIC ("394872000","Ready to stop smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("401159003","Reason for restarting smoking (observable entity)","Current-smoker","Unknown"),
# MAGIC ("449868002","Smokes tobacco daily (finding)","Current-smoker","Unknown"),
# MAGIC ("65568007","Cigarette smoker (finding)","Current-smoker","Unknown"),
# MAGIC ("134406006","Smoking reduced (finding)","Current-smoker","Unknown"),
# MAGIC ("160604004","Moderate cigarette smoker (10-19 cigs/day) (finding)","Current-smoker","Moderate"),
# MAGIC ("160605003","Heavy cigarette smoker (20-39 cigs/day) (finding)","Current-smoker","Heavy"),
# MAGIC ("160606002","Very heavy cigarette smoker (40+ cigs/day) (finding)","Current-smoker","Heavy"),
# MAGIC ("203191000000107","Wants to stop smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("308438006","Smoking restarted (finding)","Current-smoker","Unknown"),
# MAGIC ("394871007","Thinking about stopping smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("394873005","Not interested in stopping smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("401201003","Cigarette pack-years (observable entity)","Current-smoker","Unknown"),
# MAGIC ("413173009","Minutes from waking to first tobacco consumption (observable entity)","Current-smoker","Unknown"),
# MAGIC ("428041000124106","Occasional tobacco smoker (finding)","Current-smoker","Light"),
# MAGIC ("446172000","Failed attempt to stop smoking (finding)","Current-smoker","Unknown"),
# MAGIC ("56578002","Moderate smoker (20 or less per day) (finding)","Current-smoker","Moderate"),
# MAGIC ("56771006","Heavy smoker (over 20 per day) (finding)","Current-smoker","Heavy"),
# MAGIC ("59978006","Cigar smoker (finding)","Current-smoker","Unknown"),
# MAGIC ("77176002","Smoker (finding)","Current-smoker","Unknown"),
# MAGIC ("82302008","Pipe smoker (finding)","Current-smoker","Unknown"),
# MAGIC ("836001000000109","Waterpipe tobacco consumption (observable entity)","Current-smoker","Unknown"),
# MAGIC ("53896009","Tolerant ex-smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("1092041000000100","Ex-very heavy smoker (40+/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("1092091000000100","Ex-moderate smoker (10-19/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("160620009","Ex-pipe smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("160621008","Ex-cigar smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("228486009","Time since stopped smoking (observable entity)","Ex-smoker","Unknown"),
# MAGIC ("266921000","Ex-trivial cigarette smoker (<1/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("266922007","Ex-light cigarette smoker (1-9/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("266923002","Ex-moderate cigarette smoker (10-19/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("266928006","Ex-cigarette smoker amount unknown (finding)","Ex-smoker","Unknown"),
# MAGIC ("281018007","Ex-cigarette smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("735128000","Ex-smoker for less than 1 year (finding)","Ex-smoker","Unknown"),
# MAGIC ("8517006","Ex-smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("1092031000000100","Ex-smoker amount unknown (finding)","Ex-smoker","Unknown"),
# MAGIC ("1092071000000100","Ex-heavy smoker (20-39/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("1092111000000100","Ex-light smoker (1-9/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("1092131000000100","Ex-trivial smoker (<1/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("160617001","Stopped smoking (finding)","Ex-smoker","Unknown"),
# MAGIC ("160625004","Date ceased smoking (observable entity)","Ex-smoker","Unknown"),
# MAGIC ("266924008","Ex-heavy cigarette smoker (20-39/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("266925009","Ex-very heavy cigarette smoker (40+/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("360890004","Intolerant ex-smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("360900008","Aggressive ex-smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("48031000119106","Ex-smoker for more than 1 year (finding)","Ex-smoker","Unknown"),
# MAGIC ("492191000000103","Ex roll-up cigarette smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("53896009","Tolerant ex-smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("735112005","Date ceased using moist tobacco (observable entity)","Ex-smoker","Unknown"),
# MAGIC ("1092041000000100","Ex-very heavy smoker (40+/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("1092071000000100","Ex-heavy smoker (20-39/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("1092111000000100","Ex-light smoker (1-9/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("228486009","Time since stopped smoking (observable entity)","Ex-smoker","Unknown"),
# MAGIC ("266921000","Ex-trivial cigarette smoker (<1/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("266923002","Ex-moderate cigarette smoker (10-19/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("266928006","Ex-cigarette smoker amount unknown (finding)","Ex-smoker","Unknown"),
# MAGIC ("360900008","Aggressive ex-smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("492191000000103","Ex roll-up cigarette smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("735112005","Date ceased using moist tobacco (observable entity)","Ex-smoker","Unknown"),
# MAGIC ("735128000","Ex-smoker for less than 1 year (finding)","Ex-smoker","Unknown"),
# MAGIC ("1092031000000100","Ex-smoker amount unknown (finding)","Ex-smoker","Unknown"),
# MAGIC ("1092091000000100","Ex-moderate smoker (10-19/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("1092131000000100","Ex-trivial smoker (<1/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("160617001","Stopped smoking (finding)","Ex-smoker","Unknown"),
# MAGIC ("160620009","Ex-pipe smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("160621008","Ex-cigar smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("230058003","Pipe tobacco consumption (observable entity)","Ex-smoker","Unknown"),
# MAGIC ("230065006","Chain smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("266918002","Tobacco smoking consumption (observable entity)","Ex-smoker","Unknown"),
# MAGIC ("266922007","Ex-light cigarette smoker (1-9/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("266924008","Ex-heavy cigarette smoker (20-39/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("266925009","Ex-very heavy cigarette smoker (40+/day) (finding)","Ex-smoker","Unknown"),
# MAGIC ("281018007","Ex-cigarette smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("360890004","Intolerant ex-smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("48031000119106","Ex-smoker for more than 1 year (finding)","Ex-smoker","Unknown"),
# MAGIC ("8517006","Ex-smoker (finding)","Ex-smoker","Unknown"),
# MAGIC ("221000119102","Never smoked any substance (finding)","Never-Smoker","NA"),
# MAGIC ("266919005","Never smoked tobacco (finding)","Never-Smoker","NA"),
# MAGIC ("221000119102","Never smoked any substance (finding)","Never-Smoker","NA"),
# MAGIC ("266919005","Never smoked tobacco (finding)","Never-Smoker","NA")
# MAGIC 
# MAGIC AS tab(conceptID, description, smoking_status, severity);

# COMMAND ----------

# MAGIC %md ## Save codelists

# COMMAND ----------

drop_table('ccu002_vacc_drug_codelists')
create_table('ccu002_vacc_drug_codelists')

# COMMAND ----------

drop_table('master_codelist_ccu002_vaccine_covariates_sjk')
create_table('master_codelist_ccu002_vaccine_covariates_sjk')

# COMMAND ----------

drop_table('ccu002_vaccine_smokingstatus_SNOMED')
create_table('ccu002_vaccine_smokingstatus_SNOMED')
