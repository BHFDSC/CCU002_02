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

df <- DBI::dbGetQuery(con, "SELECT * FROM dars_nic_391419_j3w9t_collab.ccu002_vacc_icvt_comparison")

# Prepare denominators ---------------------------------------------------------

denom <- unique(df[,c("category","N")])
colnames(denom) <- c("category","value")
denom$name <- "N"
denom$N <- as.numeric(NA)
denom$pc <- ""
denom <- denom[,c("category","N","name","value","pc")]

# Pivot to long table ----------------------------------------------------------

df <- tidyr::pivot_longer(df, cols = setdiff(colnames(df),c("category","N")))

# Calculate column percentages -------------------------------------------------

df$N <- as.numeric(df$N)
df$pc <- ifelse(sprintf("%.0f",100*(df$value/df$N))=="0",ifelse(df$value==0,"0","<1"),sprintf("%.0f",100*(df$value/df$N)))

# Combine covariates and denominators ------------------------------------------

df <- rbind(denom,df)

# Pivot to wide table ----------------------------------------------------------

df <- tidyr::pivot_wider(df, id_cols = "name", names_from = "category", values_from = c("value","pc"))

# Specify groupings for Chi sq test --------------------------------------------

df$category <- gsub("_.*","",df$name)
df$category <- ifelse(df$category=="MedicalHistory",gsub("MedicalHistory_","",df$name),df$category)
df$category <- ifelse(df$category=="Medication",gsub("Medication_","",df$name),df$category)

# Chi sq test ------------------------------------------------------------------

df$value_icvt_before <- as.numeric(df$value_icvt_before)
df$value_icvt_after_az <- as.numeric(df$value_icvt_after_az)
df$value_icvt_after_pfizer <- as.numeric(df$value_icvt_after_pfizer)
df$chi_p <- as.numeric(NA)
df <- df[!(df$value_icvt_after_az==0 & df$value_icvt_after_pfizer==0 & df$value_icvt_before==0),]

for (i in unique(df$category)) {
  
  tmp_test <- as.matrix(df[df$category==i, c("value_icvt_before","value_icvt_after_az","value_icvt_after_pfizer")])
  tmp <- chisq.test(tmp_test)
  df[df$category==i,]$chi_p <- tmp$p.value
  
}

# Format for export ------------------------------------------------------------

df$icvt_after_az <- ifelse(df$name!="N",paste0(df$value_icvt_after_az," (",df$pc_icvt_after_az,")"),df$value_icvt_after_az)
df$icvt_after_pfizer <- ifelse(df$name!="N",paste0(df$value_icvt_after_pfizer," (",df$pc_icvt_after_pfizer,")"),df$value_icvt_after_pfizer)
df$icvt_before <- ifelse(df$name!="N",paste0(df$value_icvt_before," (",df$pc_icvt_before,")"),df$value_icvt_before)
df$chisq <- ifelse(df$chi_p>=0.01,round(df$chi_p,2),format(df$chi_p,digit = 2))

df <- df[,c("name","icvt_before","icvt_after_az","icvt_after_pfizer","chisq")]

# Save table -------------------------------------------------------------------

data.table::fwrite(df,"tables/ccu002_vacc_icvt_comparison.csv")
