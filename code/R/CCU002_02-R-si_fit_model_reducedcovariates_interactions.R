## =============================================================================
## MODEL4: INTERACTION (SUPPLEMENTARY TABLES 2 a & b)
##
## Author: Samantha Ip
## Thanks to Angela Wood, Tom Bolton and Tom Palmer for suggestions
## Contact detail: hyi20@medschl.cam.ac.uk
## =============================================================================
source("/mnt/efs/hyi20/dars_nic_391419_j3w9t_collab/CCU002_vac/scripts/CCU002_02-R-si_fit_get_data_surv.R")
library(AER)
library(stringi)  
library(stringr) 
library(multcomp)

fit_model_interactions_caller <- function(ls_interacting_feats, covars, vac_str, event, survival_data, cuts_weeks_since_expo, 
                                          cohort_start_date, cohort_end_date, expo="VACCINE", noncase_frac=0.1){
  #===============================================================================
  # GET data_surv -- with age, age^2, sex, region
  #-------------------------------------------------------------------------------
  list_data_surv_noncase_ids_interval_names <- fit_get_data_surv(covars, vac_str, agebreaks, agelabels, agegp="all", 
                                                                 event, survival_data, cuts_weeks_since_expo, 
                                                                 cohort_start_date="2020-12-08", cohort_end_date="2021-03-18", expo="VACCINE", noncase_frac=0.1)

  data_surv <- list_data_surv_noncase_ids_interval_names[[1]]
  noncase_ids <- list_data_surv_noncase_ids_interval_names[[2]]
  interval_names <-list_data_surv_noncase_ids_interval_names[[3]]
  interval_names_withpre <- c("week_pre", interval_names)
  
  # mclapply(ls_interacting_feats, mc.cores = 2,
  lapply(ls_interacting_feats,
     function(interacting_feat) 
       fit_model_interactions(interacting_feat, data_surv, covars, noncase_ids, interval_names, interval_names_withpre, 
                              vac_str, event, survival_data, cuts_weeks_since_expo, 
                              cohort_start_date, cohort_end_date, expo, noncase_frac=0.1))
}


fit_model_interactions <- function(interacting_feat, data_surv, covars, noncase_ids, interval_names, interval_names_withpre, 
                                   vac_str, event, survival_data, cuts_weeks_since_expo, 
                                   cohort_start_date, cohort_end_date, expo, noncase_frac=0.1){
  cat(paste0("...... fit_model_interactions ...... ", interacting_feat, "...... \n"))
  if (interacting_feat=="COCP_MEDS"){
    data_surv <- data_surv %>% filter(age < 50)
  } else if (interacting_feat=="HRT_MEDS") {
    data_surv <- data_surv %>% filter(age >= 50)
  }
  #===============================================================================
  # COXPH
  #-------------------------------------------------------------------------------
  data_surv <- data_surv %>% left_join(covars)
  data_surv <- data_surv %>% mutate(SEX = factor(SEX))
  
  
  data_surv$cox_weights <- ifelse(data_surv$NHS_NUMBER_DEID %in% noncase_ids, 1/noncase_frac, 1)
  
  
  cat("... data_surv ... \n")
  print(data_surv)
  gc()
  
  covars_excl_region <- names(covars %>%  select_if(!(names(.) %in% c('NHS_NUMBER_DEID'))))
  
  cat("...... start coxfit ...... \n")
  coxfit(data_surv, interval_names, covars_excl_region, vac_str, event, interacting_feat)

  gc()
  
}



mk_factor_orderlevels <- function(df, colname){
  df <- df %>% dplyr::mutate(
    !!sym(colname) := factor(!!sym(colname), levels = str_sort(unique(df[[colname]]), numeric = TRUE)))
  return(df)
}

mk_week_col <- function(data_surv, interval_names_withpre){
  cat("...... mk_week_col ...... \n")
  data_surv <- as.data.frame(data_surv)
  data_surv$week <- apply(data_surv[unlist(interval_names_withpre)], 1, function(x) names( x[x==1]) )
  data_surv$week <- relevel(as.factor( data_surv$week) , ref="week_pre")

  str(data_surv)
  return(data_surv)
}


coxfit <- function(data_surv, interval_names, covars_excl_region, vac_str, event, interacting_feat){
  interval_names_withpre <- c("week_pre", interval_names)
  levels(data_surv$IMD) <- c("Deciles_1_2", "Deciles_3_4", "Deciles_5_6", "Deciles_7_8", "Deciles_9_10", "Deciles_9_10")
  # ---------------------  *sex as interaction term  -------------------------
  # levels(data_surv$IMD) <- c("Deciles_1_2", "Deciles_3_4", "Deciles_5_6", "Deciles_7_8", "Deciles_9_10", "Deciles_9_10")
  redcovariates_excl_region <- unique(c("age", "age_sq", covars_excl_region))
  redcovariates_excl_region <- names(data_surv %>%
                                       dplyr::select(all_of(redcovariates_excl_region)) %>%
                                       summarise_all(list(~n_distinct(.))) %>%
                                       select_if(. != 1) )
  data_surv$week_pre <- 1*((data_surv$week1_4==0) & (data_surv$week5_39==0))
  data_surv <- mk_week_col(data_surv, interval_names_withpre)
  #------------------------------  reparam -----------------------------------
  # fml_red_reparam <- paste0(
  #   "Surv(tstart, tstop, event) ~ SEX + ", 
  #   paste(apply(expand.grid(interval_names, levels(data_surv$SEX)), 1, paste, collapse="_"), collapse = "+"),
  #   # "+", paste(redcovariates_excl_region, collapse="+"), ## Identify covariates to be kept
  #   "+ cluster(NHS_NUMBER_DEID) + strata(region_name)")
  
  #---------------------------------  * --------------------------------------
  if (interacting_feat == "age_deci"){
    ls <- interact_agegps(data_surv, redcovariates_excl_region)
    data_surv <- ls[[1]]
    redcovariates_excl_region <- ls[[2]]
  } else if (interacting_feat == "prior_ami_stroke") {
    ls <- interact_prior_ami_stroke(data_surv, redcovariates_excl_region)
    data_surv <- ls[[1]]
    redcovariates_excl_region <- ls[[2]]
    cat("...... data_surv ......")
    print(str(data_surv))
    }
  
  # ============================================================================
  #  REFERENCE MODEL & LINEAR HYPOTHESES
  # ----------------------------------------------------------------------------
  cat("...... WALD ...... ref ...... \n")
  fml_ref_wald <- paste0(
    # "Surv(tstart, tstop, event) ~ week1_4*", interacting_feat, "+ week5_39*", interacting_feat,
    "Surv(tstart, tstop, event) ~ week1_4 + week5_39 + ", interacting_feat, "+ week1_4:", interacting_feat, "+ week5_39:", interacting_feat,
    "+", paste(redcovariates_excl_region, collapse="+"), ## Identify covariates to be kept
    "+ cluster(NHS_NUMBER_DEID) + strata(region_name) + SEX")
  print(fml_ref_wald)
  system.time(fit_ref_wald <- coxph(
    formula = as.formula(fml_ref_wald), 
    data = data_surv, weights=data_surv$cox_weights
  ))
  cat("...... fit_ref_wald ...... \n")
  print(fit_ref_wald)
  # saveRDS(fit_ref_wald, paste0("fit_ref_wald_", interacting_feat,  "_", event,  "_", vac_str, ".rds"))
  
  
  fit_ref_wald_tidy <- as.data.frame(broom::tidy(fit_ref_wald, exponentiate = TRUE, conf.int=TRUE))
  write.csv(fit_ref_wald_tidy, paste0("fit_ref_wald_tidy_",  interacting_feat,  "_", event,  "_", vac_str, ".csv"), row.names = FALSE)
  
  
  ls_linfct_strata_week1_4 <- lapply(levels(data_surv[[interacting_feat]])[2:length(levels(data_surv[[interacting_feat]]))], 
                                     function(stratum) 
                                       paste0("week1_4 + week1_4:", interacting_feat, stratum, "=0"))
  ls_linfct_strata_week5_39 <- lapply(levels(data_surv[[interacting_feat]])[2:length(levels(data_surv[[interacting_feat]]))], 
                                      function(stratum) 
                                        paste0("week5_39 + week5_39:", interacting_feat, stratum, "=0"))
  
  
  
  fit_ref_wald_glht <- as.data.frame(broom::tidy(glht(fit_ref_wald, linfct = c(
    "week1_4 = 0", unlist(ls_linfct_strata_week1_4), 
    "week5_39 = 0", unlist(ls_linfct_strata_week5_39)))))
  fit_ref_wald_glht$interacting_feat <- interacting_feat
  fit_ref_wald_glht$event <- event
  fit_ref_wald_glht$vac_str <- vac_str
  
  cat("...... fit_ref_wald_glht ...... \n")
  print(fit_ref_wald_glht)
  write.csv(fit_ref_wald_glht, paste0("fit_ref_wald_glht_",  interacting_feat,  "_", event,  "_", vac_str, ".csv"), row.names = FALSE)
  gc()
  # ============================================================================
  #  WALD TEST
  # ----------------------------------------------------------------------------
  # week 1-4 ...................................................................
  cat("...... WALD ...... week1_4 ...... \n")
  fml_week1_4_wald <- paste0(
    "Surv(tstart, tstop, event) ~ week1_4 + week5_39 + ", interacting_feat, "+ week5_39:", interacting_feat,
    "+", paste(redcovariates_excl_region, collapse="+"), ## Identify covariates to be kept
    "+ cluster(NHS_NUMBER_DEID) + strata(region_name) + SEX")
  print(fml_week1_4_wald)
  system.time(fit_week1_4_wald <- coxph(
    formula = as.formula(fml_week1_4_wald), 
    data = data_surv, weights=data_surv$cox_weights
  ))
  print(fit_week1_4_wald)
  gc()
  # week 5-39 ...................................................................
  cat("...... WALD ...... week5_39 ...... \n")
  fml_week5_39_wald <- paste0(
    "Surv(tstart, tstop, event) ~ week1_4 + week5_39 + ", interacting_feat, "+ week1_4:", interacting_feat, 
    "+", paste(redcovariates_excl_region, collapse="+"), ## Identify covariates to be kept
    "+ cluster(NHS_NUMBER_DEID) + strata(region_name) + SEX")
  print(fml_week5_39_wald)
  system.time(fit_week5_39_wald <- coxph(
    formula = as.formula(fml_week5_39_wald), 
    data = data_surv, weights=data_surv$cox_weights
  ))
  print(fit_week5_39_wald)
  gc()
  cat("...... wald test ...... \n")
  wald_week1_4 <- as.data.frame(broom::tidy(waldtest(fit_ref_wald, fit_week1_4_wald)))
  wald_week5_39 <- as.data.frame(broom::tidy(waldtest(fit_ref_wald, fit_week5_39_wald)))
  wald_week1_4$week <- "week1_4"
  wald_week5_39$week <- "week5_39"
  wald_res <- rbind(wald_week1_4, wald_week5_39)
  
  wald_res$interacting_feat <- interacting_feat
  wald_res$event <- event
  wald_res$vac_str <- vac_str
  print(wald_res)
  write.csv(wald_res, paste0("wald_", interacting_feat,  "_", event,  "_", vac_str, ".csv"), row.names = FALSE)

  gc()
}

interact_agegps <- function(data_surv, redcovariates_excl_region){
  cat("...... interact_agegps ...... \n")
  
  data_surv$age_deci <- cut(data_surv$age, breaks = c(18, seq(30, 90, by = 10), Inf), include.lowest=TRUE, right=FALSE)
  data_surv$age_deci <- stri_replace_all_fixed(data_surv$age_deci, pattern = c("[", "]", ")", ","), replacement = c("", "", "", ""), vectorize_all = FALSE)
  data_surv$age_deci <- relevel(as.factor(data_surv$age_deci), ref="1830")
  redcovariates_excl_region <- redcovariates_excl_region[! redcovariates_excl_region %in% c("age", "age_sq")]
  
  print(str(data_surv))
  return(list(data_surv, redcovariates_excl_region))
}

interact_prior_ami_stroke <- function(data_surv, redcovariates_excl_region){
  data_surv$prior_ami_stroke <- 1*((data_surv$EVER_AMI==1) | (data_surv$EVER_ALL_STROKE==1))
  data_surv <- mk_factor_orderlevels(data_surv, "prior_ami_stroke")
  
  redcovariates_excl_region <- redcovariates_excl_region[! redcovariates_excl_region %in% c("EVER_AMI", "EVER_ALL_STROKE")]
  
  print(str(data_surv))
  return(list(data_surv, redcovariates_excl_region))
}
