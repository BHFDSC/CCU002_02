## =============================================================================
## Pipeline (1): Control center, calls relevant analysis scripts, sets working 
## and saving directories, parallelises processes
##
## Author: Samantha Ip
## Contact detail: hyi20@medschl.cam.ac.uk
## =============================================================================
library(data.table)
library(dplyr)
library(survival)
library(table1)
library(broom)
library(DBI)
library(ggplot2)
library(nlme)
library(tidyverse)
library(R.utils)
library(lubridate)
library(purrr)
library(parallel)
library(multcomp)

detectCores()


# con <- dbConnect(odbc::odbc(), "Databricks", timeout=60, PWD=rstudioapi::askForPassword("enter databricks personal access token:"))
rm(list=setdiff(ls(), c("con")))
gc()


mdl <- "mdl4_fullinteract_suppl34" # "mdl5_anydiag_death28days", "mdl4_fullinteract_suppl34"
res_dir_date <- "2021-06-15"

if (mdl %in% c("mdl1_unadj", "mdl2_agesex", "mdl3a_bkwdselect", "mdl3b_fullyadj", "mdl4_fullinteract_suppl34")){
  data_version <- "210607"
} else {cat("SPECIFY data_version!! \n") }

# data_version <-"death28days" #"210607", "death28days", "anydiag"

source("/mnt/efs/hyi20/dars_nic_391419_j3w9t_collab/CCU002_vac/scripts/CCU002_02-R-si_02_pipe.R")

gc()
# ==============================================================================
#  MAIN
# ------------------------------------------------------------------------------
if (mdl == "mdl1_unadj"){
  setwd(paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/unadj_nosexforcombined/"))
  res_dir <- paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/unadj_nosexforcombined/")
  source("/mnt/efs/hyi20/dars_nic_391419_j3w9t_collab/CCU002_vac/scripts/CCU002_02-R-si_call_mdl1_unadj.R")
  ls_events <- c(ls_events, "DIC", "TTP")
} else if (mdl == "mdl2_agesex"){
  setwd(paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/adj_age_sex_only/"))
  res_dir <- paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/adj_age_sex_only/")
  source("/mnt/efs/hyi20/dars_nic_391419_j3w9t_collab/CCU002_vac/scripts/CCU002_02-R-si_call_mdl2_agesex.R")
} else if (mdl == "mdl3a_bkwdselect"){
  setwd(paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/fully_adj_bkwdselect/"))
  res_dir <- paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/fully_adj_bkwdselect/")
  source("/mnt/efs/hyi20/dars_nic_391419_j3w9t_collab/CCU002_vac/scripts/CCU002_02-R-si_call_mdl3a_bkwdselect.R")
} else if (mdl == "mdl3b_fullyadj"){
  setwd(paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/fully_adj_bkwdselect/"))
  res_dir <- paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/fully_adj_bkwdselect/")
  source("/mnt/efs/hyi20/dars_nic_391419_j3w9t_collab/CCU002_vac/scripts/CCU002_02-R-si_call_mdl3b_fullyadj.R")
} else if (mdl == "mdl4_fullinteract_suppl34"){
  setwd(paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/interactionterm/"))
  res_dir <- paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/interactionterm/")
  source("/mnt/efs/hyi20/dars_nic_391419_j3w9t_collab/CCU002_vac/scripts/CCU002_02-R-si_call_mdl4_fullinteract_interactionterm.R")
} else if ((mdl == "mdl5_anydiag_death28days") & (data_version == "death28days")){
  setwd(paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/fully_adj_death28days/"))
  res_dir <- paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/fully_adj_death28days/")
  source("/mnt/efs/hyi20/dars_nic_391419_j3w9t_collab/CCU002_vac/scripts/CCU002_02-R-si_call_mdl3b_fullyadj.R")
} else if ((mdl == "mdl5_anydiag_death28days") & (data_version == "anydiag")){
  setwd(paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/fully_adj_anydiag/"))
  res_dir <- paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/fully_adj_anydiag/")
  source("/mnt/efs/hyi20/dars_nic_391419_j3w9t_collab/CCU002_vac/scripts/CCU002_02-R-si_call_mdl3b_fullyadj.R")
}





if (mdl == "mdl4_fullinteract_suppl34"){
  ls_interacting_feats <- c("age_deci", "SEX", "CATEGORISED_ETHNICITY", "IMD", 
                            "EVER_TCP", "EVER_THROMBOPHILIA", "EVER_PE_VT", "COVID_infection", 
                            "COCP_MEDS", "HRT_MEDS", "ANTICOAG_MEDS", "ANTIPLATLET_MEDS", 
                            "prior_ami_stroke", "EVER_DIAB_DIAG_OR_MEDS")
  
  interactingfeat_age_vac_combos <- expand.grid(ls_interacting_feats, c("Venous_event", "Arterial_event"), c("vac_az", "vac_pf"))
  names(interactingfeat_age_vac_combos) <- c("interacting_feat", "event", "vac")
  ls_should_have <- pmap(list(interactingfeat_age_vac_combos$interacting_feat, interactingfeat_age_vac_combos$event, interactingfeat_age_vac_combos$vac), 
                         function(interacting_feat, event, vac) 
                           paste0(res_dir,
                                  "wald_",
                                  interacting_feat, "_",
                                  event, "_",
                                  vac, ".csv"
                           ))
  
  ls_should_have <- unlist(ls_should_have)
  
  ls_events_missing <- data.frame()
  
  for (i in 1:nrow(interactingfeat_age_vac_combos)) {
    row <- interactingfeat_age_vac_combos[i,]
    fpath <- paste0(res_dir,
                    "wald_",
                    row$interacting_feat, "_",
                    row$event, "_",
                    row$vac, ".csv")
    
    if (!file.exists(fpath)) {
      ls_events_missing <- rbind(ls_events_missing, row)
    }
  }
} else {
  outcome_age_vac_combos <- expand.grid(ls_events, agelabels, c("vac_az", "vac_pf"))
  names(outcome_age_vac_combos) <- c("event", "agegp", "vac")
  ls_should_have <- pmap(list(outcome_age_vac_combos$event, outcome_age_vac_combos$agegp, outcome_age_vac_combos$vac), 
                         function(event, agegp, vac) 
                           paste0(res_dir,
                                  "tbl_hr_VACCINE_",
                                  event, "_",
                                  agegp, "_",
                                  vac, ".csv"
                           ))
  
  ls_should_have <- unlist(ls_should_have)
  
  ls_events_missing <- data.frame()
  
  for (i in 1:nrow(outcome_age_vac_combos)) {
    row <- outcome_age_vac_combos[i,]
    fpath <- paste0(res_dir,
                    "tbl_hr_VACCINE_",
                    row$event, "_",
                    row$agegp, "_",
                    row$vac, ".csv")
    
    if (!file.exists(fpath)) {
      ls_events_missing <- rbind(ls_events_missing, row)
    }
  }
  }



ls_events_missing %>% View()
# ls_events_missing <- ls_events_missing %>% filter(! event %in% c("death"))



if (mdl == "mdl4_fullinteract_suppl34"){
  ls_event_vac_missing <- unique(ls_events_missing %>% dplyr::select(event,vac))
  
  mclapply(split(ls_event_vac_missing,seq(nrow(ls_event_vac_missing))), mc.cores = 4,
  # lapply(split(ls_event_vac_missing,seq(nrow(ls_event_vac_missing))),
                 function(ls_event_vac_missing)
                   get_vacc_res(
                     ls_interacting_feats, 
                     event=ls_event_vac_missing$event, vac_str=ls_event_vac_missing$vac, 
                     cohort_vac, covars, cuts_weeks_since_expo, master_df_fpath, 
                     cohort_start_date, cohort_end_date, noncase_frac=0.1))
  
} else if (mdl %in% c("mdl3b_fullyadj", "mdl5_anydiag_death28days")) {
  mclapply(split(ls_events_missing,seq(nrow(ls_events_missing))), mc.cores = 4,
  #  lapply(split(ls_events_missing,seq(nrow(ls_events_missing))),
           function(ls_events_missing) 
             get_vacc_res(
               sex_as_interaction=TRUE,
               event=ls_events_missing$event, 
               vac_str=ls_events_missing$vac,
               agegp=ls_events_missing$agegp, 
               cohort_vac, agebreaks, agelabels, covars, cuts_weeks_since_expo, master_df_fpath, 
               cohort_start_date, cohort_end_date, noncase_frac=0.1)
           )
} else if(mdl %in% c("mdl1_unadj", "mdl2_agesex")){
  mclapply(split(ls_events_missing,seq(nrow(ls_events_missing))), mc.cores = 4,
  # lapply(split(ls_events_missing,seq(nrow(ls_events_missing))),
           function(ls_events_missing) 
             get_vacc_res(
               event=ls_events_missing$event, 
               vac_str=ls_events_missing$vac,
               agegp=ls_events_missing$agegp, 
               cohort_vac, agebreaks, agelabels, covars, cuts_weeks_since_expo, master_df_fpath, 
               cohort_start_date, cohort_end_date, noncase_frac=0.1)
    )
} else if (mdl == "mdl3a_bkwdselect"){
  print("...... lapply completed ......")
}
