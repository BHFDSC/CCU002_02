## =============================================================================
## MODEL3B: FULLY ADJUSTED -- USING PRE-DEFINED OUTSOME-SPECIFIC FIXED COVARIATES 
## and AMI BACKWARD-SELECTED COVARIATES
## (SUPPLEMENTARY TABLES 1 a & b)
##
## Author: Samantha Ip
## Contact detail: hyi20@medschl.cam.ac.uk
## =============================================================================
source("/mnt/efs/hyi20/dars_nic_391419_j3w9t_collab/CCU002_vac/scripts/si_fit_get_data_surv.R")
library(multcomp)


coxfit_bkwdselection <- function(data_surv, sex, interval_names, fixed_covars, vac_str, event, agegp, sex_as_interaction){
  cat("...... sex ", sex, " ...... \n")
  AMI_bkwdselected_covars <- readRDS(file=paste0("backwards_names_kept_vac_all_AMI_", agegp, "_sex", sex,".rds"))
  interval_names_withpre <- c("week_pre", interval_names)
  
  if (sex_as_interaction){
    cat("...... sex as interaction ...... \n")
    # ---------------------  *sex as interaction term  -------------------------
    # levels(data_surv$IMD) <- c("Deciles_1_2", "Deciles_3_4", "Deciles_5_6", "Deciles_7_8", "Deciles_9_10", "Deciles_9_10")
    redcovariates_excl_region <- unique(c("age", "age_sq", AMI_bkwdselected_covars, fixed_covars))
    redcovariates_excl_region <- names(data_surv %>%
                                         dplyr::select(all_of(redcovariates_excl_region)) %>%
                                         summarise_all(list(~n_distinct(.))) %>%
                                         select_if(. != 1) )
    redcovariates_excl_region <- redcovariates_excl_region[!redcovariates_excl_region %in% c("SEX", "week1_4", "week5_39")]
    data_surv$week_pre <- 1*((data_surv$week1_4==0) & (data_surv$week5_39==0))
    data_surv <- one_hot_encode("SEX", data_surv, interval_names_withpre)
    print(data_surv)
    #---------------------------------  * --------------------------------------
    fml_red <- paste0(
      "Surv(tstart, tstop, event) ~ week*SEX",
      "+", paste(redcovariates_excl_region, collapse="+"), 
      "+ cluster(NHS_NUMBER_DEID) + strata(region_name)")

    # --------------------------------------------------------------------------
  } else {
    cat("...... sex as additive ...... \n")
    # ------------------------------  + sex  -----------------------------------
    redcovariates_excl_region <- unique(c("age", "age_sq", interval_names, AMI_bkwdselected_covars, fixed_covars))
    cat("...... redcovariates_excl_region ...... \n")
    print(unlist(redcovariates_excl_region))
    fml_red <- paste0(
      "Surv(tstart, tstop, event) ~ ",
      paste(redcovariates_excl_region, collapse="+"), 
      "+ cluster(NHS_NUMBER_DEID) + strata(region_name)")
    if ((sex == "all") & (!"SEX" %in% redcovariates_excl_region)){
      fml_red <- paste(fml_red, "SEX", sep="+")
    }
  }
  
  print(fml_red)
  
  
  system.time(fit_red <- coxph(
    formula = as.formula(fml_red), 
    data = data_surv, weights=data_surv$cox_weights
  ))
  if (sex_as_interaction){
    cat("save fit_red where sex_as_interaction ...... \n")
    saveRDS(fit_red, paste0("fit_", event, "_", agegp, "_", vac_str, ".rds"))
  }
  
  
  
  fit_tidy <-tidy(fit_red, exponentiate = TRUE, conf.int=TRUE)
  fit_tidy$sex<-c(sex)
  
  gc()
  print(fit_tidy)
  return(fit_tidy)
}


fit_model_reducedcovariates <- function(sex_as_interaction, covars, vac_str, agebreaks, agelabels, agegp, event, survival_data, cuts_weeks_since_expo, cohort_start_date, cohort_end_date, expo, noncase_frac=0.1){
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

  cat("... data_surv ... \n")
  print(data_surv)
  gc()
  
  
  fixed_covars <- c("ANTICOAG_MEDS", "ANTIPLATLET_MEDS", "BP_LOWER_MEDS", "CATEGORISED_ETHNICITY", "EVER_ALL_STROKE",
                      "EVER_AMI", "EVER_DIAB_DIAG_OR_MEDS", "IMD", "LIPID_LOWER_MEDS", "smoking_status") 
  # "ANTIPLATELET_MEDS"
  
  umbrella_venous <- c("DVT_summ_event","PE", 
                       "portal_vein_thrombosis",  "ICVT_summ_event", "other_DVT", "Venous_event")
  
  
  if (event %in% umbrella_venous){
    fixed_covars <- c("CATEGORISED_ETHNICITY", "IMD", "ANTICOAG_MEDS", "COCP_MEDS", "HRT_MEDS", "EVER_PE_VT",  "COVID_infection")
    cat("...... VENOUS EVENT FIXED_COVARS! ...... \n")
  }
  
  
  if (sex_as_interaction){
    # ---------------------  *sex as interaction term  -------------------------
    levels(data_surv$IMD) <- c("Deciles_1_2", "Deciles_3_4", "Deciles_5_6", "Deciles_7_8", "Deciles_9_10", "Deciles_9_10")
    fit <- coxfit_bkwdselection(data_surv, "all", interval_names, fixed_covars, vac_str, event, agegp, sex_as_interaction)
  } else {
    # ------------------------------  + sex  -----------------------------------
    fit_sexall <- coxfit_bkwdselection(data_surv, "all", interval_names, fixed_covars, vac_str, event, agegp, sex_as_interaction)
    fit_sex1 <- coxfit_bkwdselection(data_surv %>% filter(SEX==1), "1", interval_names, fixed_covars, vac_str, event, agegp, sex_as_interaction)
    fit_sex2 <- coxfit_bkwdselection(data_surv %>% filter(SEX==2), "2", interval_names, fixed_covars, vac_str, event, agegp, sex_as_interaction)
    fit <- rbindlist(list(fit_sexall, fit_sex1, fit_sex2))
  }


  fit$event <- event
  fit$agegp <- agegp
  fit$vac <- vac_str
  
  cat("... fit ... \n")
  print(fit)
  
  
  write.csv(fit, paste0("tbl_hr_" , expo, "_", event, "_", agegp, "_", vac_str, ".csv"), row.names = T)

  
  gc()
  
}



mk_factor_orderlevels <- function(df, colname)
{
  df <- df %>% dplyr::mutate(
    !!sym(colname) := factor(!!sym(colname), levels = str_sort(unique(df[[colname]]), numeric = TRUE)))
  return(df)
}



one_hot_encode <- function(interacting_feat, data_surv, interval_names_withpre){
  cat("...... one_hot_encode ...... \n")
  data_surv <- as.data.frame(data_surv)
  data_surv$week <- apply(data_surv[unlist(interval_names_withpre)], 1, function(x) names( x[x==1]) )
  data_surv$week <- relevel(as.factor( data_surv$week) , ref="week_pre")

  data_surv$tmp <- as.factor(paste(data_surv$week, data_surv[[interacting_feat]], sep="_"))
  df_tmp <- as.data.frame(model.matrix( ~ 0 + tmp, data = data_surv))
  names(df_tmp) <- substring(names(df_tmp), 4)

  for (colname in names(df_tmp)){
    print(colname)
    df_tmp <- mk_factor_orderlevels(df_tmp, colname)
  }

  data_surv <- cbind(data_surv, df_tmp)

  str(data_surv)
  return(data_surv)
}
