## =============================================================================
## READ IN & FORMAT DATABRICKS RESULTS 
##
## Author: Samantha Ip
## Contact detail: hyi20@medschl.cam.ac.uk
## =============================================================================

library(data.table)
library(dplyr)
library(DBI)
library(R.utils)
#===============================================================================
# Read in PY incidence rates & event counts
#...............................................................................
con <- dbConnect(odbc::odbc(), "Databricks", timeout=60, PWD=rstudioapi::askForPassword("enter databricks personal access token:"))
rm(list=setdiff(ls(), c("con")))

setwd("/mnt/efs/hyi20/dvt_icvt_results/2021-06-15/")
gc()
# ........................  LS_EVENTS ........................
# OUTCOMES -- singles
ls_events <- readRDS("/mnt/efs/hyi20/dvt_icvt_results/ls_events.rds")
print(ls_events)

# ..............................................................................
date_extension <- "_0614"
event_type_str <- "anydiag" #"anydiag", "death28days", "thrombo_plus", ""

outcome_vac_combos <- expand.grid(ls_events, c("vac_az", "vac_pf"))
names(outcome_vac_combos) <- c("outcome", "vac")

# if (event_type_str == "thrombo_plus"){
#   outcome_vac_combos <- outcome_vac_combos %>% filter(!outcome %in% c("Haematological_event", "thrombocytopenia"))
# } 

ls_ir_counts <- map2(outcome_vac_combos$outcome, outcome_vac_combos$vac, function(event, vac) dbGetQuery(
  con, paste0('SELECT * FROM dars_nic_391419_j3w9t_collab.ccu002vac_ircounts_', event_type_str, "_", tolower(event), "_", vac, date_extension)))

df <- rbindlist(ls_ir_counts)
df <- arrange(df, outcome, agegroup, factor(period, levels = c("unexposed", "28 days post-expo", ">28 days post-expo")))
col_order <- c("outcome", "vac", "agegroup", "period", "events_sexall", "events_sex1", "events_sex2", 
               "n_days_sexall", "n_days_sex1", "n_days_sex2", "IR_py_sexall", "IR_py_sex1", "IR_py_sex2")
df <- df %>% dplyr::select(col_order)
# df <- df %>% relocate(col_order)
df[, c("IR_py_sexall", "IR_py_sex1", "IR_py_sex2")] <-round(df[, c("IR_py_sexall", "IR_py_sex1", "IR_py_sex2")], 3)

names(df) <- c(
  "Outcome",
  "Vaccine",
  "Age group",
  "Period w.r.t. expo",
  "N events (Total)",
  "N events (Male)",
  "N events (Female)",
  "N years (Total)",
  "N years (Male)",
  "N years (Female)",
  "IR per 100,000 person-yrs (Total)",
  "IR per 100,000 person-yrs (Male)",
  "IR per 100,000 person-yrs (Female)"
)
df %>% View()
write.csv(df, file = paste0("ircounts_vac_", event_type_str, ".csv"), row.names=F)
