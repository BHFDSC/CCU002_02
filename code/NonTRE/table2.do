/*******************************************************************************
Description: This do-file reformats the data for table 2: Numbers of events and incidence rates pre and post first (ChAdOx1-S / BNT162b2) vaccination
Author: Thomas Bolton
*******************************************************************************/


** -----------------------------------------------------------------------------
** filepaths
** -----------------------------------------------------------------------------
global infilepath "U:\BU\CVD-COVID-UK\CCU002\20210610\inputFiles"
global outfilepath  "U:\BU\CVD-COVID-UK\CCU002\20210610\outputFiles"


** -----------------------------------------------------------------------------
** parameters
** -----------------------------------------------------------------------------
global vaccine = "vac_pf" // "vac_az" | "vac_pf"


** -----------------------------------------------------------------------------
** import
** -----------------------------------------------------------------------------
import delimited "${infilepath}\\Event Counts and Incidence Rates per 100,000 person-yrs.csv", varnames(1) clear 


** -----------------------------------------------------------------------------
** filter data
** -----------------------------------------------------------------------------
tab vaccine, mi
assert vaccine=="vac_az"|vaccine=="vac_pf"
keep if vaccine=="${vaccine}"


** -----------------------------------------------------------------------------
** tidy data
** -----------------------------------------------------------------------------
drop v1
drop *male *female
rename neventstotal n
rename ndaystotal days
rename irper100000personyrstotal ir


** -----------------------------------------------------------------------------
** recode agegrp
** -----------------------------------------------------------------------------
tab agegroup, mi
assert agegroup=="<70"|agegroup==">=70"|agegroup=="all"
gen age = .
replace age = 0 if agegroup=="all"
replace age = 1 if agegroup=="<70"
replace age = 2 if agegroup==">=70"
label define age 0 "All" 1 "<70" 2 "≥70"
label values age age
tab agegroup age, mi
drop agegroup


** -----------------------------------------------------------------------------
** recode time
** -----------------------------------------------------------------------------
tab periodwrtexpo, mi
assert periodwrtexpo=="28 days post-expo"|periodwrtexpo==">28 days post-expo"|periodwrtexpo=="unexposed"
gen time = .
replace time = 0 if periodwrtexpo=="unexposed"
replace time = 1 if periodwrtexpo=="28 days post-expo"
replace time = 2 if periodwrtexpo==">28 days post-expo"
tab periodwrtexpo time, mi
drop periodwrtexpo

order vaccine outcome age time
sort vaccine outcome age time


** -----------------------------------------------------------------------------
** recode outcome
** -----------------------------------------------------------------------------
tab outcome, mi

drop if outcome=="ICVT_pregnancy"
drop if outcome=="DVT_pregnancy"
drop if outcome=="Haematological_event"
drop if outcome=="THROMBO_plus_ART"
drop if outcome=="THROMBO_plus_VEN"

tab outcome, mi

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
replace time = 0 if newobs==1
replace n = . if newobs==1
replace days = . if newobs==1
replace ir = . if newobs==1
drop newobs
replace outcome = "Disseminated intravascular coagulation" if outcome=="DIC"
replace outcome = "Thrombotic thrombocytopenic purpura" if outcome=="TTP"
replace outcome = "Any thrombocytopenia" if outcome=="thrombocytopenia"

expand 2 if _n==1, gen(newobs)
replace outcome = "Other" if newobs==1
replace age = . if newobs==1 
replace time = 0 if newobs==1
replace n = . if newobs==1
replace days = . if newobs==1
replace ir = . if newobs==1
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

replace ord = 14 if outcome=="Haematological" 
replace ord = 15 if outcome=="Disseminated intravascular coagulation"
replace ord = 16 if outcome=="Thrombotic thrombocytopenic purpura"
replace ord = 17 if outcome=="Any thrombocytopenia"

replace ord = 18 if outcome=="Other" & tmp==1 
replace ord = 19 if outcome=="Haemorrhagic stroke"
replace ord = 20 if outcome=="Mesenteric thrombosis"
replace ord = 21 if outcome=="Lower limb fracture"
replace ord = 22 if outcome=="Death"

drop tmp
labmask ord, val(outcome)
drop outcome
rename ord outcome
order outcome, before(age)


** -----------------------------------------------------------------------------
** recode n
** -----------------------------------------------------------------------------
gen nx = string(n)
assert n==. | n>=0
replace nx = "<5" if n!=. & n>=0 & n<5
tab n if nx=="<5", mi
replace nx = "" if nx=="."
order nx, before(days)


** -----------------------------------------------------------------------------
** calculate 95% CI for IR
** -----------------------------------------------------------------------------
gen irx = n/(days*365.25)
gen iry = irx*365.25*100000
assert round(ir,2)==round(iry,2)
gen irz = iry
drop iry

gen se = sqrt((1 - irx)/n)
gen lbx = exp(ln(irx) - (invnorm(0.975)*se))
gen ubx = exp(ln(irx) + (invnorm(0.975)*se))
gen lb = lbx*365.25*100000 
gen ub = ubx*365.25*100000 
drop ir irx se lbx ubx

gen ir95ci = string(irz,"%9.2f") + " (" + string(lb,"%9.2f") +  "-" + string(ub,"%9.2f")+ ")" 
replace ir95ci = string(irz,"%9.1f") + " (" + string(lb,"%9.1f") +  "-" + string(ub,"%9.1f")+ ")" if irz!=. & irz>10
replace ir95ci = "-" if ir95ci=="0.00 (.-.)"
replace ir95ci = "" if ir95ci==". (.-.)"
local ii = ustrunescape("\u2013")
replace ir95ci = subinstr(ir95ci,"-","`ii'",.) if ir95ci!="-"

preserve
	keep if outcome=="Death":ord 	
	keep vacc age time days
	rename days pyears 
	reshape wide pyears, i(vacc age) j(time)
	local cdate: display %tdCCYYNNDD date(c(current_date), "DMY")	
	export excel "${outfilepath}\\`cdate'_pyears_${vaccine}_v1.xlsx", replace firstrow(variables)
restore

drop n days irz lb ub
rename nx n


** -----------------------------------------------------------------------------
** reshape
** -----------------------------------------------------------------------------
reshape wide n ir95ci, i(vaccine outcome age) j(time)


** -----------------------------------------------------------------------------
** manual edits
** -----------------------------------------------------------------------------
assert _N==0 
if("${vaccine}"=="vac_az") {
	replace n2 = "<10" if outcome=="Intracranial venous thrombosis":ord & age=="<70":age
	replace n2 = "<10" if outcome=="Portal vein thrombosis":ord & age=="≥70":age
}


** -----------------------------------------------------------------------------
** tidy
** -----------------------------------------------------------------------------
gen tmp = 1 if outcome==outcome[_n-1]
replace outcome = . if tmp==1
drop tmp


** -----------------------------------------------------------------------------
** output
** -----------------------------------------------------------------------------
local cdate: display %tdCCYYNNDD date(c(current_date), "DMY")	
export excel "${outfilepath}\\`cdate'_ircounts_${vaccine}_v1.xlsx", replace firstrow(variables)
export delimited using "${outfilepath}\\`cdate'_ircounts_${vaccine}_v1.csv", replace
