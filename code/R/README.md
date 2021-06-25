# R code

## *CCU002_02-R-vw_* scripts

*CCU002_02-R-vw_* scripts transfer data from Databricks to R ahead of analysis and generates various tables. Here is a brief outline of the functionalities of each script. The 'CCU002_02-R-vw_transfer_data' scripts should be run prior to any other R scripts (including those with prefix *CCU002_02-R-si_*).

CCU002_02-R-vw_transfer_data  (***2*** in total)
- transfers data from Databricks to R in chunks

CCU002_02-R-vw_ICVT_comparison.R
- compares demographic characteristics for individuals with ICVT events before and after vaccination

CCU002_02-R-vw_check_covariates.R
- checks the levels of categorical variables and reports missingness

CCU002_02-R-vw_table1.R
- formats the Databricks output for table 1

Please feel free to relay your suggestions or corrections regarding these scripts to Venexia Walker (venexia.walker@bristol.ac.uk).

##  *CCU002_02-R-si_* scripts 

*CCU002_02-R-si_* scripts form a pipeline that generates the results reported in Figure 1; Supplementary tables 1a, 1b, 2a, 2b in the paper *COVID-19 vaccines ChAdOx1 and BNT162b2 and the risks of major venous, arterial, and thrombocytopenic events in 46 million English adults*. Here is a brief outline of the functionalities of each script and the ordering.

CCU002_02-R-si_01_pipe.R 
- control center
- sets the analysis-specific working and saving directories
- sources CCU002_02-R-si_02_pipe.R and analysis-specific scripts
- gets analysis-specific combinations of outcomes, vaccine, age groups, interacting features of interest as required
- identifies which combinations have not been completed (eases resubmission)
- parallelises the outstanding combinations

CCU002_02-R-si_02_pipe.R 
- defines parameters
- defines relevant outcomes
- gets core cohort data
- reads in covariates, if applicable to the analysis
- defines function to set datees outside the date range of interest to NA
- defines function that makes vaccine-specific dataset, with careful censoring at the dates of vaccination not usign the vaccine of interest

CCU002_02-R-si_prep_covariates.R
- formats the core cohort data and the covariates data -- imposes data-types, recodes strings, deals with missing data and defines reference levels

CCU002_02-R-si_fit_get_data_surv.R 
- formats dataset for time-dependent coxph

CCU002_02-R-si_call_mdl scripts (***5*** in total)
- model and analysis-specific scripts
- creates and formats survival data with core cohort data and the outcome of interest
- sources its corresponding CCU002_02-R-si_fit_model script 

CCU002_02-R-si_fit_model scripts (***5*** in total)
- sources CCU002_02-R-si_fit_get_data_surv.R 
- defines analysis-specific covariates
- defines and fits the model of interest

CCU002_02-R-si_fmt scripts (***3*** in total)
- combines and formats export tables to be read into an R Markdown script to be exported as an HTML

Please feel free to relay your suggestions or corrections regarding these scripts to Samantha Ip (hyi20@medschl.cam.ac.uk).

## Acknowledgements

Thanks goes to these wonderful contributors: Samantha Ip, Jennifer Cooper, Thomas Bolton, Venexia Walker and Angela Wood.
