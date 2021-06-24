## =============================================================================
## MODEL3B: FULLY ADJUSTED -- USING PRE-DEFINED OUTSOME-SPECIFIC FIXED COVARIATES 
## and AMI BACKWARD-SELECTED COVARIATES (SUPPLEMENTARY TABLES 1 a & b)
## (1) Prep outcome and analysis specific dataset
##
## Author: Samantha Ip
## Contact detail: hyi20@medschl.cam.ac.uk
## =============================================================================
source("/mnt/efs/hyi20/dars_nic_391419_j3w9t_collab/CCU002_vac/scripts/si_fit_model_reducedcovariates.R")


get_vacc_res <- function(sex_as_interaction, event, vac_str, agegp, cohort_vac, agebreaks, agelabels, covars, cuts_weeks_since_expo, master_df_fpath, cohort_start_date, cohort_end_date, noncase_frac=0.1){
  
  if (data_version == "210607"){
    outcomes <- fread(master_df_fpath, 
                      select=c("NHS_NUMBER_DEID", 
                               paste0(event, "_date")
                      ))
    setnames(outcomes, 
             old = c(paste0(event, "_date")), 
             new = c("record_date"))
    
  } else if (data_version == "death28days"){
    outcomes <- fread(master_df_fpath, 
                      select=c("NHS_NUMBER_DEID", 
                               paste0(event, "_date"),
                               paste0("death28days_", event)
                      )
    )
    outcomes[[paste0(event, "_date")]][outcomes[[paste0("death28days_", event)]] == 0] <- NA
    outcomes <- outcomes %>% dplyr::select(!paste0("death28days_", event))
    setnames(outcomes, 
             old = c(paste0(event, "_date")), 
             new = c("record_date"))
  } else if (data_version == "anydiag"){
      outcomes <- fread(master_df_fpath, 
                        select=c("NHS_NUMBER_DEID", 
                                 paste0("anydiag_", event, "_date")
                        ))
      setnames(outcomes, 
               old = c(paste0("anydiag_", event, "_date")), 
               new = c("record_date"))
      
    }


  outcomes$name <- event

  
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
  
  res_vacc <- fit_model_reducedcovariates(sex_as_interaction, covars, vac_str, agebreaks, agelabels, agegp, 
                                          event, survival_data, cuts_weeks_since_expo, 
                                          cohort_start_date="2020-12-08", cohort_end_date="2021-03-18", expo="VACCINE", noncase_frac=0.1)
  
  return(res_vacc)
}
