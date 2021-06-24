## =============================================================================
## MODEL1: UNADJUSTED (SUPPLEMENTARY TABLES 1 a & b)
##
## Author: Samantha Ip
## Contact detail: hyi20@medschl.cam.ac.uk
## =============================================================================
source("/mnt/efs/hyi20/dars_nic_391419_j3w9t_collab/CCU002_vac/scripts/si_fit_get_data_surv.R")


fit_model_justweeks_NOageorsex <- function(covars, vac_str, agebreaks, agelabels, agegp, event, survival_data, cuts_weeks_since_expo, cohort_start_date="2020-12-08", cohort_end_date="2021-03-18", expo="VACCINE", noncase_frac=0.1){

  #===============================================================================
  # GET data_surv -- with age, sex, region
  #-------------------------------------------------------------------------------
  list_data_surv_noncase_ids_interval_names <- fit_get_data_surv(covars, vac_str, agebreaks, agelabels, agegp, event, survival_data, cuts_weeks_since_expo, cohort_start_date="2020-12-08", cohort_end_date="2021-03-18", expo="VACCINE", noncase_frac=0.1)
  data_surv <- list_data_surv_noncase_ids_interval_names[[1]]
  noncase_ids <- list_data_surv_noncase_ids_interval_names[[2]]
  interval_names <-list_data_surv_noncase_ids_interval_names[[3]]
  #===============================================================================
  # COXPH
  #-------------------------------------------------------------------------------
  data_surv$cox_weights <- ifelse(data_surv$NHS_NUMBER_DEID %in% noncase_ids, 1/noncase_frac, 1)
  
  print("data_surv")
  print(head(data_surv))
  
  
  coxfit <- function(data_surv, sex, interval_names){
    fml <- paste0(
      "Surv(tstart, tstop, event) ~ ", 
      paste(interval_names, collapse="+"),
      "+ cluster(NHS_NUMBER_DEID) + strata(region_name)")

    print(paste0("sex: ",sex))
    print(fml)
    
    fit <- coxph(
      as.formula(fml), 
      data= data_surv, weights=data_surv$cox_weights
    )
    
    fit_tidy <-tidy(fit, exponentiate = TRUE, conf.int=TRUE)
    fit_tidy$sex<-c(sex)
    print(fit_tidy)
    return(fit_tidy)
  }
  
  

  fit_sexall <- coxfit(data_surv, "all", interval_names)
  fit_sex1 <- coxfit(data_surv %>% filter(SEX==1), "1", interval_names)
  fit_sex2 <- coxfit(data_surv %>% filter(SEX==2), "2", interval_names)
  
  fit <- rbindlist(list(fit_sexall, fit_sex1, fit_sex2), fill=TRUE)


  fit$event <- event
  fit$agegp <- agegp
  fit$vac <- vac_str
  
  write.csv(fit, paste0("tbl_hr_" , expo, "_", event, "_", agegp, "_", vac_str, ".csv"), row.names = T)
  
  print(paste0("finished......", event, "......!!! :D"))
  

  gc()
  
  }



