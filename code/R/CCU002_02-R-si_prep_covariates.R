## =============================================================================
## WRANGLE & FORMAT DATA TABLE -- sets data types, deals with missing entries, 
## defines reference levels
##
## Author: Samantha Ip
## Contact detail: hyi20@medschl.cam.ac.uk
## =============================================================================
library(stringr)

#-------------------------------------------------------------------------------
# Replace " " with "_" for glht's linfct
covars$CATEGORISED_ETHNICITY <- gsub(" ", "_", covars$CATEGORISED_ETHNICITY)
covars$smoking_status <- gsub("-", "_", covars$smoking_status)

print(unique(covars$CATEGORISED_ETHNICITY))

#-------------------------------------------------------------------------------
#  FACTOR()
factor_covars <- names(covars %>% dplyr::select(! c("NHS_NUMBER_DEID", "N_DISORDER", "UNIQUE_BNF_CHAPS")))

mk_factor_orderlevels <- function(covars, colname)
  {
  covars <- covars %>% mutate(
    !!sym(colname) := factor(!!sym(colname), levels = str_sort(unique(covars[[colname]]), numeric = TRUE)))
  return(covars)
}

for (colname in factor_covars){
  print(colname)
  covars <- mk_factor_orderlevels(covars, colname)
}

#-------------------------------------------------------------------------------
# replace "missing" with "0"
na_to_0_covars <- names(covars %>% select_if(~ nlevels(.) == 2) )
covars <- as.data.frame(covars)
covars[na_to_0_covars][is.na(covars[na_to_0_covars])] <- 0

##------------------------------------------------------------------------------
## replace "missing" with "0"
# mk_missing_to_0 <- function(covars, colname)
# {
#   covars <- covars %>% mutate(
#     !!sym(colname) := recode(!!sym(colname), "missing"="0"))
#   return(covars)
# }
# 
# for (colname in missing_to_0_covars){
#   print(colname)
#   covars <- mk_missing_to_0(covars, colname)
# }
# 
# 
# covars$smoking_status_ <- recode(covars$smoking_status_, "0"="missing")

##------------------------------------------------------------------------------
# any NAs left?
any(is.na(covars))
colnames(covars)[colSums(is.na(covars)) > 0]

##------------------------------------------------------------------------------
# # Get levels and add "missing' .......................................
# levels <- levels(covars$DECI_IMD)
# levels[length(levels) + 1] <- "missing"
# covars$DECI_IMD <- factor(covars$DECI_IMD, levels = levels)
# covars$DECI_IMD[is.na(covars$DECI_IMD)] <- "missing"
# levels(covars$DECI_IMD)

##------------------------------------------------------------------------------
# The few factors that have specific reference levels
covars$CATEGORISED_ETHNICITY <- relevel(covars$CATEGORISED_ETHNICITY, ref = "White")
covars$smoking_status <- relevel(covars$smoking_status, ref = "Never_Smoker")
cohort_vac$region_name <- relevel(factor(cohort_vac$region_name), ref = "London")

##------------------------------------------------------------------------------
# The few covariates that should be numeric
numeric_covars <- c("UNIQUE_BNF_CHAPS", "N_DISORDER")
mk_numeric <- function(covars, colname)
{
  covars <- covars %>% mutate(
    !!sym(colname) := as.numeric(!!sym(colname)))
  return(covars)
}
for (colname in numeric_covars){
  print(colname)
  covars <- mk_numeric(covars, colname)
}

str(covars)
