## =============================================================================
## Pipeline (2): Reads in analysis-specific data, loads parameters, 
## gets vaccine-specific dataset -- censoring at appropriate dates
##
## Author: Samantha Ip
## Contact detail: hyi20@medschl.cam.ac.uk
## =============================================================================

ls_events <- readRDS("~/dvt_icvt_results/ls_events.rds")
if (data_version == "210607"){
  master_df_fpath <- "/mnt/efs/hyi20/dars_nic_391419_j3w9t_collab/CCU002_vac/data/ccu002_vacc_cohort_backup/ccu002_vacc_cohort_210607.csv.gz" # cached version
} else if (data_version == "death28days"){ 
  master_df_fpath <- "/mnt/efs/hyi20/dars_nic_391419_j3w9t_collab/CCU002_vac/data/ccu002_vacc_cohort.csv.gz"
  ls_events <-  ls_events[!ls_events %in% c("death")]
} else if (data_version == "anydiag"){
  master_df_fpath <- "/mnt/efs/hyi20/dars_nic_391419_j3w9t_collab/CCU002_vac/data/ccu002_vacc_cohort_altoutcomes_210616.csv.gz"
  ls_events <-  ls_events[!ls_events %in% c("death")]
}

master_names <- fread(master_df_fpath, nrows=1)
# ls_events <- readRDS("~/dvt_icvt_results/ls_events.rds") # NO DIC or TTP ; sort( gsub("_date","",names(master_names)[grepl('_date', names(master_names))]))
print(ls_events)

agebreaks <- c(0,70,500)
agelabels <- c("<70",">=70")
noncase_frac <- 0.1
cuts_weeks_since_expo <- c(4, 39) 
expo <- "VACCINE"

cohort_start_date <- as.Date("2020-12-08")
cohort_end_date <- as.Date("2021-03-18")

#===============================================================================
#  READ IN DATA
#-------------------------------------------------------------------------------
cohort_vac <- fread(master_df_fpath, 
                    select=c("NHS_NUMBER_DEID", 
                             "SEX", 
                             "death_date", 
                             "AGE_AT_COHORT_START", 
                             "VACCINATION_DATE_FIRST", 
                             "VACCINE_PRODUCT_FIRST",
                             "region_name"
                    ))

setnames(cohort_vac, 
         old = c("death_date", "VACCINATION_DATE_FIRST", "VACCINE_PRODUCT_FIRST"), 
         new = c("DATE_OF_DEATH", "VACCINATION_DATE", "VACCINE_PRODUCT"))

gc()

if (! mdl %in% c("mdl1_unadj", "mdl2_agesex")){
  covar_names <- master_names %>% dplyr::select(!c(
    names(master_names)[grepl('_date$', names(master_names))], 
    names(master_names)[grepl('death28days', names(master_names))], 
    "SEX", 
    "death_date", 
    "AGE_AT_COHORT_START", 
    "VACCINATION_DATE_FIRST", 
    "VACCINE_PRODUCT_FIRST", 
    "CHUNK",
    "region_name"
  )) %>% names()
  
  covars <- fread(master_df_fpath, select = covar_names)
  gc()
  source("/mnt/efs/hyi20/dars_nic_391419_j3w9t_collab/CCU002_vac/scripts/CCU002_02-R-si_prep_covariates.R")

}


#-------------------------------------------------------------------------------
# SET DATES OUTSIDE RANGE AS NA 
#...............................................................................
set_dates_outofrange_na <- function(df, colname, cohort_start_date, cohort_end_date)
{
  df <- df %>% mutate(
    !!sym(colname) := as.Date(ifelse((!!sym(colname) > cohort_end_date) | (!!sym(colname) < cohort_start_date), NA, !!sym(colname) ), origin='1970-01-01')
  )
  return(df)
}


#===============================================================================
# GET VACCINE-SPECIFIC DATASET
#-------------------------------------------------------------------------------
get_vac_specific_dataset <- function(survival_data, vac_of_interest){
  survival_data$DATE_VAC_CENSOR <- as.Date(ifelse(!(survival_data$VACCINE_PRODUCT %in% vac_of_interest),
                                                  survival_data$expo_date, 
                                                  NA), origin='1970-01-01')
  
  
  survival_data$expo_date <- as.Date(ifelse((!is.na(survival_data$DATE_VAC_CENSOR)) & (survival_data$expo_date >= survival_data$DATE_VAC_CENSOR), NA, survival_data$expo_date), origin='1970-01-01')
  survival_data$record_date <- as.Date(ifelse((!is.na(survival_data$DATE_VAC_CENSOR)) & (survival_data$record_date >= survival_data$DATE_VAC_CENSOR), NA, survival_data$record_date), origin='1970-01-01')
  
  cat(paste("vac-sepcific df: should see vacs other than", paste(vac_of_interest, collapse = "|"), "as DATE_VAC_CENSOR ... \n", sep="..."))
  print(head(survival_data, 30 ))
  
  cat(paste("min-max expo_date: ", min(survival_data$expo_date, na.rm=TRUE), max(survival_data$expo_date, na.rm=TRUE), "\n", sep="   "))
  cat(paste("min-max record_date: ", min(survival_data$record_date, na.rm=TRUE), max(survival_data$record_date, na.rm=TRUE), "\n", sep="   "))

  return(survival_data)
}



