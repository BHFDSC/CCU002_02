## =============================================================================
## FORMAT RESULTS for ICVT as INTERACTION
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
library(AER)
library(stringi)  
library(stringr) 
library(multcomp)

rm(list=ls())

res_dir_date <- "2021-06-15"
mdl <- "mdl5_anydiag_death28days"
data_version <- "death28days"

agebreaks <- c(0,70,500)
agelabels <- c("<70",">=70")

if (mdl == "mdl1_unadj"){
  setwd(paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/unadj_nosexforcombined/"))
  res_dir <- paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/unadj_nosexforcombined/")
} else if (mdl == "mdl2_agesex"){
  setwd(paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/adj_age_sex_only/"))
  res_dir <- paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/adj_age_sex_only/")
} else if (mdl == "mdl3b_fullyadj"){
  setwd(paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/fully_adj_bkwdselect/"))
  res_dir <- paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/fully_adj_bkwdselect/")
} else if (mdl == "mdl4_fullinteract_suppl34"){
  setwd(paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/interactionterm/"))
  res_dir <- paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/interactionterm/")
} else if ((mdl == "mdl5_anydiag_death28days") & (data_version == "death28days")){
  setwd(paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/fully_adj_death28days/"))
  res_dir <- paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/fully_adj_death28days/")
} else if ((mdl == "mdl5_anydiag_death28days") & (data_version == "anydiag")){
  setwd(paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/fully_adj_anydiag/"))
  res_dir <- paste0("/mnt/efs/hyi20/dvt_icvt_results/", res_dir_date, "/fully_adj_anydiag/")
}



ls_events <- c("ICVT_summ_event") # readRDS("/mnt/efs/hyi20/dvt_icvt_results/ls_events.rds")
print(ls_events)
outcome_age_vac_combos <- expand.grid(ls_events, agelabels, c("vac_az", "vac_pf"))
# outcome_age_vac_combos <- expand.grid(ls_events, agelabels, c("vac_az", "vac_pf", "vac_all"))
names(outcome_age_vac_combos) <- c("event", "agegp", "vac")


# if all 
ls_hrs <- pmap(list(outcome_age_vac_combos$event, outcome_age_vac_combos$agegp, outcome_age_vac_combos$vac), 
               function(event, agegp, vac) 
                 paste0(res_dir,
                        "fit_",
                        event, "_",
                        agegp, "_",
                        vac, ".rds"
                        )
               )

ls_hrs <- unlist(ls_hrs)
ls_hrs <- lapply(ls_hrs, readRDS)

print(ls_hrs[1])


ls_linfct <- lapply(ls_hrs, 
                    function(fit_star){
                      df <- as.data.frame(broom::tidy(glht(fit_star, linfct = c(
                        "weekweek1_4 = 0",
                         "weekweek1_4 + weekweek1_4:SEX2 = 0",
                         "weekweek5_39 = 0",
                         "weekweek5_39 + weekweek5_39:SEX2 = 0"
                      ))))
                      
                      }
                    )

ls_linfct <- pmap( list(ls_linfct, outcome_age_vac_combos$event, outcome_age_vac_combos$agegp, outcome_age_vac_combos$vac), 
                  function(df, event, agegp, vac) {
                    df$event <- event
                    df$agegp <- agegp
                    df$vac <- vac
                    return(df)
                  }
                  )


df_linfct <- rbindlist(ls_linfct)
print(df_linfct)
write.csv(df_linfct, paste0("df_linfct_SEXasinteraction_ICVT.csv"), row.names=FALSE)
