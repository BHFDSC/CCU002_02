# Databricks notebooks

This directory contains the following Databricks notebooks:

| Notebook | Author(s) | Description | 
| ----------- | -----------| -----------| 
| CCU002_02-D00-project_table_freeze | Sam Hollings, Jenny Cooper | This notebook extracts the data from specified time point (batchId) and then applies a specified common cutoff date (i.e. any records beyond this time are dropped).|
| CCU002_02-D01-event_counts | Venexia Walker, Tom Bolton | This notebook calculates the number of primary events from HES and death records for events. |
| CCU002_02-D02-patient_skinny_unassembled | Sam Hollings, Jenny Cooper |Gather together the records for each patient in primary and secondary care before they are assembled into a skinny record for CCU002. The output of this is a global temp View which is then used by CCU002_02-D03-patient_skinny_record.|
| CCU002_02-D03-patient_skinny_assembled  | Sam Hollings, Jenny Cooper | Making a single record for each patient in primary and secondary care. this uses the output from CCU002_02_D02_patient_skinny_unassembled. |
| CCU002_02-D04-quality_assurance | Jenny Cooper, Sam Ip | This notebook creates a register and applies a series of quality assurance steps to a cohort of data (Skinny Cohort table) of NHS Numbers to potentially remove from analyses due to conflicting data, with reference to previous work/coding by Spiros. |
| CCU002_02-D05-inclusion_exclusion | Jenny Cooper, Sam Ip | This notebook runs through the inclusion/exclusion criteria for the skinny cohort after QA. |
| CCU002_02-D06-codelists | Venexia Walker | This notebook generates the codelists needed for this project. |
| CCU002_02-D07-covid19 | Spencer Keene, Venexia Walker | This notebook determines the COVID19 infection and hospital outcomes. |
| CCU002_02-D08-covariate_flags | Sam Ip, Spencer Keene | Covariate flags across GDPPR, HES. |
| CCU002_02-D09-gdppr_outcomes | Spencer Keene, Jenny Cooper, Rachel Denholm | This notebook generates the outcomes listed in the CCU002 vaccination protocol using the GDPPR dataset. |
| CCU002_02-D10-hes_apc_outcomes | Spencer Keene, Jenny Cooper | This notebook generates the outcomes listed in the CCU002 vaccination protocol using the HES APC dataset. |
| CCU002_02-D11-sus_outcomes | Spencer Keene, Jenny Cooper, Rachel Denholm | This notebook generates the outcomes listed in the CCU002 vaccination protocol using the SUS dataset. |
| CCU002_02-D12-deaths_outcomes | Spencer Keene, Jenny Cooper | This notebook generates the outcomes listed in the CCU002 vaccination protocol using the Death data. |
| CCU002_02-D13-combined_outcomes | Spencer Keene, Jenny Cooper, Rachel Denholm | This notebook combines all the outcomes listed in the CCU002 vaccination protocol from deaths, sus, gdppr and HES. |
| CCU002_02-D14-thrombo_plus_outcomes | Spencer Keene | This notebook produces a table for combination of thrombocytopenia plus each individual outcome within the same spell. |
| CCU002_02-D15-vaccine_status | Spencer Keene, Jenny Cooper | This notebook generates a vaccine status table. |
| CCU002_02-D16-incidence_rates | Sam Ip | Generates Table 2. |
| CCU002_02-D17-call_incidence_rates | Sam Ip | Loops through (outcome, vaccine, outcome nature/data source) combos, calling D16. |
| CCU002_02-D18-cohort | Venexia Walker, Spencer Keene, Sam Ip | This notebook makes the analysis dataset. |
| CCU002_02-D19-table_1 | Venexia Walker | This notebook extracts the information needed for table 1. |
| CCU002_02-D20-table_S2 | Venexia Walker | This notebook calculates event numbers for supplementary table 2.|
| CCU002_02-D21-ICVT_comparison | Venexia Walker | This notebook compares the characteristics of patients who had ICVT before and after vaccination. |
