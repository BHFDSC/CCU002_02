# This script checks the covariates prior to analysis
# Author: Venexia Walker
# Date: 2021-03-02

rm(list = ls())

library(magrittr)

# Load data --------------------------------------------------------------------

df <- data.table::fread("data/ccu002_vacc_cohort.csv.gz", data.table = FALSE)

# Create empty table 1 ---------------------------------------------------------

check <- data.frame(variable = character(),
                    statistic = character(),
                    value = character())

# Add sample size --------------------------------------------------------------

check[nrow(check)+1,] <- c("N","Count",nrow(df))

# Populate table 1 -------------------------------------------------------------

for (i in setdiff(colnames(df),c("NHS_NUMBER_DEID","CHUNK"))) {
  
  if (is.numeric(df[,i]) & length(unique(df[,i]))>2) {
    
    check[nrow(check)+1,] <- c(i,"Mean (SD)",paste0(sprintf(fmt = "%.1f",mean(na.omit(df[,i])))," (",sprintf(fmt = "%.1f",sd(na.omit(df[,i]))),")"))
    check[nrow(check)+1,] <- c(i,"Range",paste0(sprintf(fmt = "%.1f",min(na.omit(df[,i])))," to ",sprintf(fmt = "%.1f",max(na.omit(df[,i])))))
    check[nrow(check)+1,] <- c(i,"Missing (%)",paste0(sprintf(fmt = "%.1f",100*(nrow(df[is.na(df[,i]),])/nrow(df)))))
    
  } else if (is.numeric(df[,i]) & length(unique(df[,i]))<=2) {
    
    tmp <- data.frame(table(df[,i]))
    check[nrow(check)+1,] <- c(i,"Count (%)",paste0(tmp[tmp$Var1==1,]$Freq," (",sprintf("%.1f",100*(tmp[tmp$Var1==1,]$Freq/nrow(df))),")"))
    
  } else if (is.character(df[,i])) {
    
    tmp <- data.frame(table(df[,i]))
    tmp$Var1 <- ifelse(as.character(tmp$Var1)=="","Missing (%)",paste0(as.character(tmp$Var1)," (%)"))
    tmp$variable <- i
    tmp$value <- paste0(tmp$Freq," (",sprintf("%.1f",100*(tmp$Freq/nrow(df))),")")
    tmp <- tmp[,c("variable","Var1","value")]
    colnames(tmp) <- c("variable","statistic","value")
    check <- rbind(check, tmp)
    
  }
  
}

# Save -------------------------------------------------------------------------

data.table::fwrite(check,"tables/ccu002_vacc_covariate_check.csv")
