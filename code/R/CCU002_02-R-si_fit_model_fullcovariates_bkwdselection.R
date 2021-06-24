## =============================================================================
## MODEL3A: FULLY ADJUSTED -- BACKWARD SELECTION using AMI
## (SUPPLEMENTARY TABLES 1 a & b)
##
## Author: Samantha Ip
## Thanks to Venexia Walker and Jennifer Cooper for backwards-selection reference code
## Contact detail: hyi20@medschl.cam.ac.uk
## =============================================================================
source("/mnt/efs/hyi20/dars_nic_391419_j3w9t_collab/CCU002_vac/scripts/si_fit_get_data_surv.R")


fit_model_fullcovariates <- function(covars, vac_str, agebreaks, agelabels, agegp, event, survival_data, cuts_weeks_since_expo, cohort_start_date, cohort_end_date, expo, noncase_frac=0.1){
  #===============================================================================
  # GET data_surv -- with age, age^2, sex, region
  #-------------------------------------------------------------------------------
  list_data_surv_noncase_ids_interval_names <- fit_get_data_surv(covars, vac_str, agebreaks, agelabels, agegp, event, survival_data, cuts_weeks_since_expo, cohort_start_date="2020-12-08", cohort_end_date="2021-03-18", expo="VACCINE", noncase_frac=0.1)
  data_surv <- list_data_surv_noncase_ids_interval_names[[1]]
  noncase_ids <- list_data_surv_noncase_ids_interval_names[[2]]
  interval_names <-list_data_surv_noncase_ids_interval_names[[3]]
  
  #===============================================================================
  # COXPH
  #-------------------------------------------------------------------------------
  data_surv <- data_surv %>% left_join(covars)
  data_surv <- data_surv %>% mutate(SEX = factor(SEX))

  
  data_surv$cox_weights <- ifelse(data_surv$NHS_NUMBER_DEID %in% noncase_ids, 1/noncase_frac, 1)
  gc()
  cat("... data_surv ... \n")
  print(data_surv)

  if (event %in% "AMI"){
    fixed_covars <- c("ANTICOAG_MEDS", "ANTIPLATLET_MEDS", "BP_LOWER_MEDS", "CATEGORISED_ETHNICITY", "EVER_ALL_STROKE",
                      "EVER_AMI", "EVER_DIAB_DIAG_OR_MEDS", "IMD", "LIPID_LOWER_MEDS", "smoking_status") 
  } # "ANTIPLATELET_MEDS" is spelled this way in the datatable...
  
  covars_excl_region <- names(covars %>%  select_if(!(names(.) %in% c('NHS_NUMBER_DEID'))))
  
  coxfit_bkwdselection <- function(data_surv, sex, interval_names, fixed_covars, covars_excl_region, vac_str, event, agegp){
    covars_excl_region <- names(data_surv %>% 
                                  select(all_of(covars_excl_region)) %>%
                                  select_if(~n_distinct(.) != 1) )
    
    
    fml <- paste0(
      "Surv(tstart, tstop, event) ~ age + age_sq + ", 
      paste(interval_names, collapse="+"), "+",  
      paste(covars_excl_region, collapse="+"), 
      "+ cluster(NHS_NUMBER_DEID) + rms::strat(region_name)")
    
    if (sex == "all"){
      fml<- paste(fml, "SEX", sep="+")
    }
    print(paste0("sex: ", sex))
    print(fml)
    # if fitter overflow make sure N_VISTS (orig integer) as.numeric
    fit_full <- rms::cph(formula = as.formula(fml),## Run full model with all covariates
                           data= data_surv, 
                           weights=data_surv$cox_weights,
                           x = TRUE,
                           y = TRUE)
    

    backwards <- rms::fastbw(fit_full, rule = "p", sls = 0.2)# Perform backwards selection with p-value rule and threshold of 0.2
    
    redcovariates_excl_region <- unique(c(backwards$names.kept, fixed_covars))
    saveRDS(backwards$names.kept, file=paste0("backwards_names_kept_", vac_str , "_", event, "_", agegp, "_sex", sex,".rds"))
    redcovariates_excl_region <- unique(c("age", "age_sq", interval_names, backwards$names.kept, fixed_covars))
    
    fml_red <- paste0(
      "Surv(tstart, tstop, event) ~ ", 
      paste(redcovariates_excl_region, collapse="+"), ## Identify covariates to be kept 
      "+ cluster(NHS_NUMBER_DEID) + strata(region_name)")
    if (sex == "all"){
      fml_red<- paste(fml_red, "SEX", sep="+")
    }
    print(fml_red)
    
    fit_red <- coxph(
      formula = as.formula(fml_red), 
      data = data_surv, weights=data_surv$cox_weights
    )
    fit_tidy <-tidy(fit_red, exponentiate = TRUE, conf.int=TRUE)
    fit_tidy$sex<-c(sex)

    print(fit_tidy)
    return(fit_tidy)
  }
  

  fit_sexall <- coxfit_bkwdselection(data_surv, "all", interval_names, fixed_covars, covars_excl_region, vac_str, event, agegp)
  fit_sex1 <- coxfit_bkwdselection(data_surv %>% filter(SEX==1), "1", interval_names, fixed_covars, covars_excl_region, vac_str, event, agegp)
  fit_sex2 <- coxfit_bkwdselection(data_surv %>% filter(SEX==2), "2", interval_names, fixed_covars, covars_excl_region, vac_str, event, agegp)
  
  fit <- rbindlist(list(fit_sexall, fit_sex1, fit_sex2))
  fit$event <- event
  fit$agegp <- agegp
  fit$vac <- vac_str

  cat("... fit ... \n")
  print(fit)
    
  write.csv(fit, paste0("tbl_hr_" , expo, "_", event, "_", agegp, "_", vac_str, ".csv"), row.names = T)

  print(paste0("finished......", event, "......!!! :D"))
  
  gc()
  
  }
