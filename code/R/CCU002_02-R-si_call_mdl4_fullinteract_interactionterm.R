## =============================================================================
## MODEL4: INTERACTION (SUPPLEMENTARY TABLES 2 a & b)
## (1) Prep outcome and analysis specific dataset
##
## Author: Samantha Ip
## Contact detail: hyi20@medschl.cam.ac.uk
## =============================================================================
source("/mnt/efs/hyi20/dars_nic_391419_j3w9t_collab/CCU002_vac/scripts/si_fit_model_reducedcovariates_interactions.R")


get_vacc_res <- function(ls_interacting_feats, event, vac_str, cohort_vac, covars, cuts_weeks_since_expo, 
                         master_df_fpath, cohort_start_date, cohort_end_date, noncase_frac=0.1){
  outcomes <- fread(master_df_fpath, 
                    select=c("NHS_NUMBER_DEID", 
                             paste0(event, "_date")
                    ))
  outcomes$name <- event
  setnames(outcomes, 
           old = c(paste0(event, "_date")), 
           new = c("record_date"))
  
  survival_data <- cohort_vac %>% left_join(outcomes)
  
  schema <- sapply(survival_data, is.Date)
  for (colname in names(schema)[schema==TRUE]){
    print(colname)
    survival_data <- set_dates_outofrange_na(survival_data, colname, cohort_start_date, cohort_end_date)
  }
  
  names(survival_data)[names(survival_data) == 'VACCINATION_DATE'] <- 'expo_date'
  
  cat("survival_data before vac specific... \n")
  print(head(survival_data))
  
  if (vac_str=="vac_az"){
    vac_of_interest <- c("AstraZeneca", "AstraZeneca_8_dose", "AstraZeneca_10_dose", "AstraZeneca_n/a_dose")
  } else if (vac_str=="vac_pf"){
    vac_of_interest <- c("Pfizer","Pfizer_6_dose", "Pfizer_n/a_dose")
  } else if (vac_str=="vac_all"){
    vac_of_interest <- unique(na.omit(survival_data$VACCINE_PRODUCT))
  }
  
  survival_data <- get_vac_specific_dataset(survival_data, vac_of_interest)
  rm(cohort_vac)
  gc()
  
  
  fit_model_interactions_caller(ls_interacting_feats, covars, vac_str, event, survival_data, cuts_weeks_since_expo, 
                                            cohort_start_date, cohort_end_date, expo="VACCINE",  noncase_frac=0.1)
  
}
