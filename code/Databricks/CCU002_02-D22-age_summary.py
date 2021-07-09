# Databricks notebook source
# MAGIC %md # CCU002_02-D22-age_summary
# MAGIC 
# MAGIC **Description** This notebook breaks down summary outcomes to show the contribution of individual events.
# MAGIC 
# MAGIC **Author(s)** Venexia Walker

# COMMAND ----------

# MAGIC %md ## Arterial events

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AGE_BAND,
# MAGIC        COUNT(NHS_NUMBER_DEID) AS Arterial_total,
# MAGIC        CONCAT(SUM(AMI),' (',ROUND(100*(SUM(AMI)/COUNT(NHS_NUMBER_DEID)),1),'%)') AS Arterial_AMI,
# MAGIC        CONCAT(SUM(stroke_isch),' (',ROUND(100*(SUM(stroke_isch)/COUNT(NHS_NUMBER_DEID)),1),'%)') AS Arterial_stroke_isch,
# MAGIC        CONCAT(SUM(other_arterial_embolism),' (',ROUND(100*(SUM(other_arterial_embolism)/COUNT(NHS_NUMBER_DEID)),1),'%)') AS Arterial_other_arterial_embolism
# MAGIC FROM (SELECT NHS_NUMBER_DEID, 
# MAGIC              AGE_AT_COHORT_START,
# MAGIC              CASE WHEN AGE_AT_COHORT_START>=18 AND AGE_AT_COHORT_START<40 THEN '18-39'
# MAGIC                   WHEN AGE_AT_COHORT_START>=40 AND AGE_AT_COHORT_START<60 THEN '40-59'
# MAGIC                   WHEN AGE_AT_COHORT_START>=60 AND AGE_AT_COHORT_START<70 THEN '60-69'
# MAGIC                   WHEN AGE_AT_COHORT_START>=70 THEN '70+' END AGE_BAND,
# MAGIC              CASE WHEN diag1_AMI_date=diag1_Arterial_event_date THEN 1 ELSE 0 END AS AMI,
# MAGIC              CASE WHEN diag1_stroke_isch_date=diag1_Arterial_event_date THEN 1 ELSE 0 END AS stroke_isch,
# MAGIC              CASE WHEN diag1_other_arterial_embolism_date=diag1_Arterial_event_date THEN 1 ELSE 0 END AS other_arterial_embolism
# MAGIC       FROM dars_nic_391419_j3w9t_collab.ccu002_vacc_cohort
# MAGIC       WHERE diag1_Arterial_event_date IS NOT NULL
# MAGIC       AND VACCINATION_DATE_FIRST IS NOT NULL
# MAGIC       AND diag1_Arterial_event_date>VACCINATION_DATE_FIRST)
# MAGIC GROUP BY AGE_BAND

# COMMAND ----------

# MAGIC %md ## Haematological events

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AGE_BAND,
# MAGIC        COUNT(NHS_NUMBER_DEID) AS Haematological_total,
# MAGIC        CONCAT(SUM(thrombocytopenia),' (',ROUND(100*(SUM(thrombocytopenia)/COUNT(NHS_NUMBER_DEID)),1),'%)') AS Haematological_thrombocytopenia,
# MAGIC        CONCAT(SUM(other),' (',ROUND(100*(SUM(other)/COUNT(NHS_NUMBER_DEID)),1),'%)') AS Haematological_other
# MAGIC FROM (SELECT NHS_NUMBER_DEID, 
# MAGIC              AGE_AT_COHORT_START,
# MAGIC              CASE WHEN AGE_AT_COHORT_START>=18 AND AGE_AT_COHORT_START<40 THEN '18-39'
# MAGIC                   WHEN AGE_AT_COHORT_START>=40 AND AGE_AT_COHORT_START<60 THEN '40-59'
# MAGIC                   WHEN AGE_AT_COHORT_START>=60 AND AGE_AT_COHORT_START<70 THEN '60-69'
# MAGIC                   WHEN AGE_AT_COHORT_START>=70 THEN '70+' END AGE_BAND,
# MAGIC              CASE WHEN diag1_thrombocytopenia_date=diag1_Haematological_event_date THEN 1 ELSE 0 END AS thrombocytopenia,
# MAGIC              CASE WHEN diag1_TTP_date=diag1_Haematological_event_date OR diag1_DIC_date=diag1_Haematological_event_date THEN 1 ELSE 0 END AS other
# MAGIC       FROM dars_nic_391419_j3w9t_collab.ccu002_vacc_cohort
# MAGIC       WHERE diag1_Haematological_event_date IS NOT NULL
# MAGIC       AND VACCINATION_DATE_FIRST IS NOT NULL
# MAGIC       AND diag1_Haematological_event_date>VACCINATION_DATE_FIRST)
# MAGIC GROUP BY AGE_BAND

# COMMAND ----------

# MAGIC %md ## Venous events

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT AGE_BAND,
# MAGIC        COUNT(NHS_NUMBER_DEID) AS Venous_total,
# MAGIC        CONCAT(SUM(DVT),' (',ROUND(100*(SUM(DVT)/COUNT(NHS_NUMBER_DEID)),1),'%)') AS Venous_DVT,
# MAGIC        CONCAT(SUM(ICVT),' (',ROUND(100*(SUM(ICVT)/COUNT(NHS_NUMBER_DEID)),1),'%)') AS Venous_ICVT,
# MAGIC        CONCAT(SUM(PE),' (',ROUND(100*(SUM(PE)/COUNT(NHS_NUMBER_DEID)),1),'%)') AS Venous_PE,
# MAGIC        CONCAT(SUM(other),' (',ROUND(100*(SUM(other)/COUNT(NHS_NUMBER_DEID)),1),'%)') AS Venous_other
# MAGIC FROM (SELECT NHS_NUMBER_DEID, 
# MAGIC              AGE_AT_COHORT_START,
# MAGIC              CASE WHEN AGE_AT_COHORT_START>=18 AND AGE_AT_COHORT_START<40 THEN '18-39'
# MAGIC                   WHEN AGE_AT_COHORT_START>=40 AND AGE_AT_COHORT_START<60 THEN '40-59'
# MAGIC                   WHEN AGE_AT_COHORT_START>=60 AND AGE_AT_COHORT_START<70 THEN '60-69'
# MAGIC                   WHEN AGE_AT_COHORT_START>=70 THEN '70+' END AGE_BAND,
# MAGIC              CASE WHEN diag1_DVT_summ_event_date=diag1_Venous_event_date THEN 1 ELSE 0 END AS DVT,
# MAGIC              CASE WHEN diag1_ICVT_summ_event_date=diag1_Venous_event_date THEN 1 ELSE 0 END AS ICVT,
# MAGIC              CASE WHEN diag1_PE_date=diag1_Venous_event_date THEN 1 ELSE 0 END AS PE,
# MAGIC              CASE WHEN diag1_other_DVT_date=diag1_Venous_event_date OR diag1_portal_vein_thrombosis_date=diag1_Venous_event_date THEN 1 ELSE 0 END AS other
# MAGIC       FROM dars_nic_391419_j3w9t_collab.ccu002_vacc_cohort
# MAGIC       WHERE diag1_Venous_event_date IS NOT NULL
# MAGIC       AND VACCINATION_DATE_FIRST IS NOT NULL
# MAGIC       AND diag1_Venous_event_date>VACCINATION_DATE_FIRST)
# MAGIC GROUP BY AGE_BAND
