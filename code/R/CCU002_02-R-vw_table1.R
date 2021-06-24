# This script generates Table 1 for publication
# Author: Venexia Walker
# Date: 2021-03-02

rm(list = ls())

# Establish DataBricks connection ----------------------------------------------

con <- DBI::dbConnect(odbc::odbc(),
                      "Databricks",
                      timeout = 60,
                      PWD = rstudioapi::askForPassword("Password please:"))

# Load data --------------------------------------------------------------------

df <- DBI::dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu002_vacc_table1")

# Prepare denominators ---------------------------------------------------------

denom <- unique(df[,c("category","N")])
colnames(denom) <- c("category","value")
denom$variable1 <- "N"
denom$variable2 <- ""
denom <- denom[,c("category","variable1","variable2","value")]

# Prepare Table 1 covariates ---------------------------------------------------

df <- tidyr::pivot_longer(df, cols = setdiff(colnames(df),c("category","N")))
df$variable1 <- stringr::str_split_fixed(df$name, "_", 2)[,1]
df$variable2 <- stringr::str_split_fixed(df$name, "_", 2)[,2]
df <- df[,c("category","variable1","variable2","value")]

# Combine covariates and denominators ------------------------------------------

df <- rbind(denom,df)

# Pivot to wide table ----------------------------------------------------------

df <- tidyr::pivot_wider(df, id_cols = c("variable1","variable2"), names_from = "category", values_from = "value")

# Tidy variables ---------------------------------------------------------------

df$variable1 <- ifelse(df$variable1=="SmokingStatus", "Smoking status", df$variable1)
df$variable1 <- ifelse(df$variable1=="MedicalHistory", "Medical history", df$variable1)
df$variable1 <- ifelse(df$variable1=="NumberOfDiagnoses", "Number of diagnoses", df$variable1)
df$variable1 <- ifelse(df$variable1=="NumberOfMedications", "Number of medications", df$variable1)

df$variable2 <- gsub("_"," - ",df$variable2)
df$variable2 <- ifelse(df$variable2=="80plus", "80+", df$variable2)
df$variable2 <- ifelse(df$variable2=="AsianOrAsianBritish", "Asian", df$variable2)
df$variable2 <- ifelse(df$variable2=="BlackOrBlackBritish", "Black", df$variable2)
df$variable2 <- ifelse(df$variable2=="OtherEthnicGroups", "Other", df$variable2)
df$variable2 <- ifelse(df$variable2=="MyocardialInfarction", "MI", df$variable2)
df$variable2 <- ifelse(df$variable2=="DVT - PE", "DVT or PE", df$variable2)
df$variable2 <- ifelse(df$variable2=="CoronavirusInfection", "Coronavirus infection", df$variable2)
df$variable2 <- ifelse(df$variable2=="LiverDisease", "Liver disease", df$variable2)
df$variable2 <- ifelse(df$variable2=="ChronicKidneyDisease", "CKD", df$variable2)
df$variable2 <- ifelse(df$variable2=="MajorSurgery", "Major surgery", df$variable2)
df$variable2 <- ifelse(df$variable2=="BPLowering", "BP lowering", df$variable2)
df$variable2 <- ifelse(df$variable2=="LipidLowering", "Lipid lowering", df$variable2)
df$variable2 <- ifelse(df$variable2=="OralContraceptive", "Oral contraceptive", df$variable2)
df$variable2 <- ifelse(df$variable2=="6plus", "6+", df$variable2)
df$variable2 <- ifelse(df$variable2=="UnknownOrMissing","Unknown or missing", df$variable2)

# Order variables --------------------------------------------------------------

df <- df[,c("variable1","variable2",
            "whole_all","venous_all","arterial_all","thrombocytopenia_all",
            "whole_under70","venous_under70","arterial_under70", "thrombocytopenia_under70",
            "whole_70plus","venous_70plus","arterial_70plus","thrombocytopenia_70plus")]

df$whole_all <- as.numeric(df$whole_all)
df$venous_all <- as.numeric(df$venous_all)
df$arterial_all <- as.numeric(df$arterial_all)
df$thrombocytopenia_all <- as.numeric(df$thrombocytopenia_all)

df$whole_under70 <- as.numeric(df$whole_under70)
df$venous_under70 <- as.numeric(df$venous_under70)
df$arterial_under70 <- as.numeric(df$arterial_under70)
df$thrombocytopenia_under70 <- as.numeric(df$thrombocytopenia_under70)

df$whole_70plus <- as.numeric(df$whole_70plus)
df$venous_70plus <- as.numeric(df$venous_70plus)
df$arterial_70plus <- as.numeric(df$arterial_70plus)
df$thrombocytopenia_70plus <- as.numeric(df$thrombocytopenia_70plus)

# Add risk per 100000 for all events -------------------------------------------

df$venous_all <- ifelse(as.numeric(sprintf("%.0f",100000*(df$venous_all/df$whole_all)))<100,
                        paste0(df$venous_all," (",sprintf("%.1f",100000*(df$venous_all/df$whole_all)),")"),
                        paste0(df$venous_all," (",sprintf("%.0f",100000*(df$venous_all/df$whole_all)),")"))

df$arterial_all <- ifelse(as.numeric(sprintf("%.0f",100000*(df$arterial_all/df$whole_all)))<100,
                        paste0(df$arterial_all," (",sprintf("%.1f",100000*(df$arterial_all/df$whole_all)),")"),
                        paste0(df$arterial_all," (",sprintf("%.0f",100000*(df$arterial_all/df$whole_all)),")"))

df$thrombocytopenia_all <- ifelse(as.numeric(sprintf("%.0f",100000*(df$thrombocytopenia_all/df$whole_all)))<100,
                        paste0(df$thrombocytopenia_all," (",sprintf("%.1f",100000*(df$thrombocytopenia_all/df$whole_all)),")"),
                        paste0(df$thrombocytopenia_all," (",sprintf("%.0f",100000*(df$thrombocytopenia_all/df$whole_all)),")"))

# Add risk per 100000 for under 70 events --------------------------------------

df$venous_under70 <- ifelse(as.numeric(sprintf("%.0f",100000*(df$venous_under70/df$whole_under70)))<100,
                        paste0(df$venous_under70," (",sprintf("%.1f",100000*(df$venous_under70/df$whole_under70)),")"),
                        paste0(df$venous_under70," (",sprintf("%.0f",100000*(df$venous_under70/df$whole_under70)),")"))

df$arterial_under70 <- ifelse(as.numeric(sprintf("%.0f",100000*(df$arterial_under70/df$whole_under70)))<100,
                          paste0(df$arterial_under70," (",sprintf("%.1f",100000*(df$arterial_under70/df$whole_under70)),")"),
                          paste0(df$arterial_under70," (",sprintf("%.0f",100000*(df$arterial_under70/df$whole_under70)),")"))

df$thrombocytopenia_under70 <- ifelse(as.numeric(sprintf("%.0f",100000*(df$thrombocytopenia_under70/df$whole_under70)))<100,
                                  paste0(df$thrombocytopenia_under70," (",sprintf("%.1f",100000*(df$thrombocytopenia_under70/df$whole_under70)),")"),
                                  paste0(df$thrombocytopenia_under70," (",sprintf("%.0f",100000*(df$thrombocytopenia_under70/df$whole_under70)),")"))

# Add risk per 100000 for 70 plus events ---------------------------------------

df$venous_70plus <- ifelse(as.numeric(sprintf("%.0f",100000*(df$venous_70plus/df$whole_70plus)))<100,
                            paste0(df$venous_70plus," (",sprintf("%.1f",100000*(df$venous_70plus/df$whole_70plus)),")"),
                            paste0(df$venous_70plus," (",sprintf("%.0f",100000*(df$venous_70plus/df$whole_70plus)),")"))

df$arterial_70plus <- ifelse(as.numeric(sprintf("%.0f",100000*(df$arterial_70plus/df$whole_70plus)))<100,
                              paste0(df$arterial_70plus," (",sprintf("%.1f",100000*(df$arterial_70plus/df$whole_70plus)),")"),
                              paste0(df$arterial_70plus," (",sprintf("%.0f",100000*(df$arterial_70plus/df$whole_70plus)),")"))

df$thrombocytopenia_70plus <- ifelse(as.numeric(sprintf("%.0f",100000*(df$thrombocytopenia_70plus/df$whole_70plus)))<100,
                                      paste0(df$thrombocytopenia_70plus," (",sprintf("%.1f",100000*(df$thrombocytopenia_70plus/df$whole_70plus)),")"),
                                      paste0(df$thrombocytopenia_70plus," (",sprintf("%.0f",100000*(df$thrombocytopenia_70plus/df$whole_70plus)),")"))

# Make age brackets empty if nonsense ------------------------------------------

df$whole_under70 <- ifelse(df$variable1=="Age" & df$variable2 %in% c("70 - 79","80+"),"", df$whole_under70)
df$venous_under70 <- ifelse(df$variable1=="Age" & df$variable2 %in% c("70 - 79","80+"),"", df$venous_under70)
df$arterial_under70 <- ifelse(df$variable1=="Age" & df$variable2 %in% c("70 - 79","80+"),"", df$arterial_under70)
df$thrombocytopenia_under70 <- ifelse(df$variable1=="Age" & df$variable2 %in% c("70 - 79","80+"),"", df$thrombocytopenia_under70)

df$whole_70plus <- ifelse(df$variable1=="Age" & !(df$variable2 %in% c("70 - 79","80+")),"", df$whole_70plus)
df$venous_70plus <- ifelse(df$variable1=="Age" & !(df$variable2 %in% c("70 - 79","80+")),"", df$venous_70plus)
df$arterial_70plus <- ifelse(df$variable1=="Age" & !(df$variable2 %in% c("70 - 79","80+")),"", df$arterial_70plus)
df$thrombocytopenia_70plus <- ifelse(df$variable1=="Age" & !(df$variable2 %in% c("70 - 79","80+")),"", df$thrombocytopenia_70plus)

df$whole_70plus <- ifelse(df$variable2=="Oral contraceptive","", df$whole_70plus)
df$venous_70plus <- ifelse(df$variable2=="Oral contraceptive","", df$venous_70plus)
df$arterial_70plus <- ifelse(df$variable2=="Oral contraceptive","", df$arterial_70plus)
df$thrombocytopenia_70plus <- ifelse(df$variable2=="Oral contraceptive","", df$thrombocytopenia_70plus)

# Save table1 ------------------------------------------------------------------

data.table::fwrite(df,"tables/ccu002_vacc_table1.csv")
