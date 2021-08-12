/*******************************************************************************
Description: This do-file reformats the data for supp table 1: Hazard ratios (95% CIs) for thrombotic and other outcomes 1-28 and >28 days post-vaccination, compared with pre-vaccination rates.
Author: Thomas Bolton
*******************************************************************************/


** -----------------------------------------------------------------------------
** filepaths
** -----------------------------------------------------------------------------
global infilepath "U:\BU\CVD-COVID-UK\CCU002\20210621\inputFiles"
global outfilepath  "U:\BU\CVD-COVID-UK\CCU002\20210621\outputFiles"


** -----------------------------------------------------------------------------
** parameters
** -----------------------------------------------------------------------------
global vaccine = "vac_pf" // "vac_az" | "vac_pf"


** -----------------------------------------------------------------------------
** import
** -----------------------------------------------------------------------------
** unadj
global infilename "Hazard Ratios Unadjusted, Agesex-adjusted, fully adjusted (using backwards selection)_UNADJ" 
import delimited "${infilepath}\\${infilename}.csv", varnames(1) clear 
gen adj = 0
tempfile tmpp0
save `tmpp0', replace

** age and sex adj
global infilename "Hazard Ratios Unadjusted, Agesex-adjusted, fully adjusted (using backwards selection)_AGESEXADJ"
import delimited "${infilepath}\\${infilename}.csv", varnames(1) clear 
gen adj = 1
tempfile tmpp1
save `tmpp1', replace

** fully adj
global infilename "Hazard Ratios Unadjusted, Agesex-adjusted, fully adjusted (using backwards selection)_FULLYADJ" 
import delimited "${infilepath}\\${infilename}.csv", varnames(1) clear 
drop if event=="Venous_event" // DROP VENOUS EVENT - IMPORTED SEPARATELY BELOW
gen adj = 2
tempfile tmpp2
save `tmpp2', replace

** fully adj - Venous_event
global infilename "Hazard Ratios venous event update – fully adjusted (using backwards selection)_FULLYADJ_VENOUS" 
import delimited "${infilepath}\\${infilename}.csv", varnames(1) clear 
gen adj = 2
tempfile tmpp3
save `tmpp3', replace

** append
use `tmpp0', clear
append using `tmpp1'
append using `tmpp2'
append using `tmpp3'


** -----------------------------------------------------------------------------
** tidy
** -----------------------------------------------------------------------------
** filter data
keep if vac=="${vaccine}"

** drop vars
drop v1 stderror robustse statistic pvalue
tab adj, mi

** term
set more off
tab term, mi
keep if term=="week1_4"|term=="week5_39"

** sex
tab sex, mi
drop if sex=="all_interact"

** age
tab agegp, mi
assert agegp=="<70"|agegp==">=70"
gen age = .
replace age = 1 if agegp=="<70"
replace age = 2 if agegp==">=70"
label define age 1 "<70" 2 "≥70"
label values age age
tab agegp age, mi
drop agegp

** vacc
rename vac vacc
tab vacc, mi 
replace vacc = "AZ" if vacc=="vac_az"
replace vacc = "Pfizer" if vacc=="vac_pf"
assert vacc=="AZ"|vacc=="Pfizer"
tab vacc, mi 

** time
tab term, mi
assert term=="week1_4"|term=="week5_39"
gen time = .
replace time = 1 if term=="week1_4"
replace time = 2 if term=="week5_39"
label define time 1 "<28" 2 "≥28"
label values time time
tab term time, mi
drop term

** hr
gen double hr = round(estimate*100000)/100000
gen double lb = round(conflow*100000)/100000
gen double ub = round(confhigh*100000)/100000
replace hr = round(hr,0.01)
replace lb = round(lb,0.01)
replace ub = round(ub,0.01)

drop estimate conflow confhigh 
gen hr95ci = string(hr,"%9.2f") + " (" + string(lb,"%9.2f") +  "-" + string(ub,"%9.2f")+ ")" 
replace hr95ci = string(hr,"%9.1f") + " (" + string(lb,"%9.1f") +  "-" + string(ub,"%9.1f")+ ")" if hr!=. & hr>10
replace hr95ci = "-" if hr95ci=="0.00 (.-.)"
replace hr95ci = "-" if hr95ci=="0.00 (0.00-0.00)"
replace hr95ci = "-" if hr95ci==". (.-.)"
local ii = ustrunescape("\u2013")
replace hr95ci = subinstr(hr95ci,"-","`ii'",.) if hr95ci!="-"
drop hr lb ub

** get all events for all adj (fully adj missing DIC and TTP, intentionally - few events)
reshape wide hr95ci, i(event vacc age sex time) j(adj)
reshape long
replace hr95ci = "-" if hr95ci==""


** -----------------------------------------------------------------------------
** outcome
** -----------------------------------------------------------------------------
rename event outcome 
tab outcome, mi
drop if outcome=="ICVT_pregnancy"
drop if outcome=="DVT_pregnancy"
drop if outcome=="Haematological_event"
drop if outcome=="THROMBO_plus_ART"
drop if outcome=="THROMBO_plus_VEN"
tab outcome, mi

gen outcome_old = outcome

** recode
replace outcome = "All venous" if outcome=="Venous_event"
replace outcome = "Intracranial venous thrombosis" if outcome=="ICVT_summ_event" 
replace outcome = "Intracranial venous thrombosis during pregnancy" if outcome=="ICVT_pregnancy"
replace outcome = "Portal vein thrombosis" if outcome=="portal_vein_thrombosis"
replace outcome = "Pulmonary embolism" if outcome=="PE"
replace outcome = "Deep vein thrombosis" if outcome=="DVT_summ_event" 
replace outcome = "Deep vein thrombosis during pregnancy" if outcome=="DVT_pregnancy"
replace outcome = "Other" if outcome=="other_DVT"

replace outcome = "All arterial" if outcome=="Arterial_event"
replace outcome = "Myocardial infarction" if outcome=="AMI"
replace outcome = "Ischaemic stroke" if outcome=="stroke_isch"
replace outcome = "Other arterial" if outcome=="other_arterial_embolism"

expand 2 if _n==1, gen(newobs)
replace outcome = "Haematological" if newobs==1
replace age = . if newobs==1 
replace sex = "" if newobs==1 
replace time = 1 if newobs==1
replace adj = 0 if newobs==1
replace hr95ci = "" if newobs==1
replace outcome_old = "" if newobs==1
drop newobs

replace outcome = "Disseminated intravascular coagulation" if outcome=="DIC"
replace outcome = "Thrombotic thrombocytopenic purpura" if outcome=="TTP"
replace outcome = "Any thrombocytopenia" if outcome=="thrombocytopenia"

expand 2 if _n==1, gen(newobs)
replace outcome = "Other" if newobs==1
replace age = . if newobs==1 
replace sex = "" if newobs==1 
replace time = 1 if newobs==1
replace adj = 0 if newobs==1
replace hr95ci = "" if newobs==1
replace outcome_old = "" if newobs==1
gen tmp = 1 if newobs==1
drop newobs

replace outcome = "Haemorrhagic stroke" if outcome=="stroke_SAH_HS"
replace outcome = "Mesenteric thrombosis" if outcome=="mesenteric_thrombus"
replace outcome = "Lower limb fracture" if outcome=="fracture"
replace outcome = "Death" if outcome=="death"

tab outcome, mi

** ordering
gen ord = 1  if outcome==""
replace ord = 2  if outcome=="All venous"
replace ord = 3  if outcome=="Intracranial venous thrombosis"
replace ord = 4  if outcome=="Intracranial venous thrombosis during pregnancy"
replace ord = 5  if outcome=="Portal vein thrombosis"
replace ord = 6  if outcome=="Pulmonary embolism"
replace ord = 7  if outcome=="Deep vein thrombosis"
replace ord = 8  if outcome=="Deep vein thrombosis during pregnancy"
replace ord = 9  if outcome=="Other"

replace ord = 10 if outcome=="All arterial"
replace ord = 11 if outcome=="Myocardial infarction"
replace ord = 12 if outcome=="Ischaemic stroke"
replace ord = 13 if outcome=="Other arterial"

replace ord = 14 if outcome=="Haematological" // HEADER
replace ord = 15 if outcome=="Disseminated intravascular coagulation"
replace ord = 16 if outcome=="Thrombotic thrombocytopenic purpura"
replace ord = 17 if outcome=="Any thrombocytopenia"

replace ord = 18 if outcome=="Other" & tmp==1 // HEADER
replace ord = 19 if outcome=="Haemorrhagic stroke"
replace ord = 20 if outcome=="Mesenteric thrombosis"
replace ord = 21 if outcome=="Lower limb fracture"
replace ord = 22 if outcome=="Death"

assert ord!=.

labmask ord, val(outcome)
drop outcome
rename ord outcome
order outcome, before(age)

** finally
order vacc time adj outcome_old outcome age sex hr95ci
sort vacc time adj outcome_old outcome age sex

** tidy
tab sex, mi
gen sexn = .
replace sexn = 0 if sex=="all"
replace sexn = 1 if sex=="1"
replace sexn = 2 if sex=="2"
tab sex sexn, mi
assert sexn!=. | (sexn==. & inlist(outcome,14,18))
drop sex 
rename sexn sex
label define sex 0 "Overall" 1 "Men" 2 "Women"
label values sex sex
tab sex, mi

** reshape
gen adjs = string(adj)
replace adjs = "_a" + adjs
tab adj adjs, mi
drop adj
reshape wide hr95ci, i(vacc time outcome_old outcome age sex) j(adjs) string
gen times = string(time)
replace times = "_t" + times
tab time times, mi
drop time
reshape wide hr95ci*, i(vacc outcome_old outcome age sex) j(times) string

** sort 
sort vacc outcome age sex
drop outcome_old

** format 
decode age, gen(ages)
order ages, after(age) 
drop age
replace ages = "" if ages==ages[_n-1] & outcome==outcome[_n-1] & vacc==vacc[_n-1]
replace ages = "" if ages==ages[_n-2] & outcome==outcome[_n-2] & vacc==vacc[_n-2]
decode outcome, gen(outcomes)
order outcomes, after(outcome) 
drop outcome
replace outcomes = "" if outcomes==outcomes[_n-1] & vacc==vacc[_n-1]
replace outcomes = "" if outcomes==outcomes[_n-2] & vacc==vacc[_n-2]
replace outcomes = "" if outcomes==outcomes[_n-3] & vacc==vacc[_n-3]
replace outcomes = "" if outcomes==outcomes[_n-4] & vacc==vacc[_n-4]
replace outcomes = "" if outcomes==outcomes[_n-5] & vacc==vacc[_n-5]


** -----------------------------------------------------------------------------
** output
** -----------------------------------------------------------------------------
local cdate: display %tdCCYYNNDD date(c(current_date), "DMY")	
export excel "${outfilepath}\\`cdate'_suppTable1n2_${vaccine}_v2.xlsx", replace firstrow(variables)
export delimited using "${outfilepath}\\`cdate'_suppTable1n2_${vaccine}_v2.csv", replace


