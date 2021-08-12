/*******************************************************************************
Description: This do-file reformats the data for supp table 2: Fully adjusted hazard ratios (HRs) for the association of ChAdOx1-S and BNT162b2 with venous and arterial events 1-28 and >28 days after vaccination, within pre-specified subgroups
Author: Thomas Bolton
*******************************************************************************/


** -----------------------------------------------------------------------------
** filepaths
** -----------------------------------------------------------------------------
global infilepath "U:\BU\CVD-COVID-UK\CCU002\20210624\inputFiles"
global outfilepath  "U:\BU\CVD-COVID-UK\CCU002\20210624\outputFiles"


** -----------------------------------------------------------------------------
** import
** -----------------------------------------------------------------------------
** import HRs
global infilename "Hazard Ratios Interaction Terms for Venous and Arterial Events_TABLE1"
import delimited "${infilepath}\\${infilename}.csv", varnames(1) clear 

** import pvalues
tempfile tmppP
preserve
	global infilename "Hazard Ratios Interaction Terms for Venous and Arterial Events_TABLE2"
	import delimited "${infilepath}\\${infilename}.csv", varnames(1) clear 
	save `tmppP', replace
restore

** VX numbers
tempfile tmppC
preserve
	global infilename "ccu002_vacc_table_S3_S4_210624"
	import excel "${infilepath}\\${infilename}.xlsx", sheet("Sheet1") firstrow clear
	save `tmppC', replace
restore


** -----------------------------------------------------------------------------
** tidy
** -----------------------------------------------------------------------------
** drop vars
drop v1 adjpvalue statistic
tab nullvalue, mi
assert nullvalue==0
drop nullvalue
rename vac_str vac

** extract term and val
rename interacting_feat var
gen term = regexs(1) if regexm(contrast, "^(week(1_4|5_39)) \+ week(1_4|5_39):" + var + "(.*)$")
replace term = contrast if term=="" & inlist(contrast,"week1_4","week5_39")
assert term!=""
gen val = regexs(3) if regexm(contrast, "^week(1_4|5_39) \+ week(1_4|5_39):" + var + "(.*)$")
replace val = "refcat" if val=="" & inlist(contrast,"week1_4","week5_39")
assert val!=""

** order
order event vac term var val estimate stderror
drop contrast


** -----------------------------------------------------------------------------
** add pvalues (per vacc var and term)
** -----------------------------------------------------------------------------
preserve
	use `tmppP', clear
	keep event interacting_feat vac_str week pvalue
	rename (interacting_feat vac_str week) (var vac term)
	order vac event term var pvalue
	save `tmppP', replace
restore
merge m:1 event vac term var using `tmppP'
tab _merge, mi
assert _merge==3
drop _merge

** create hr95ci - data
rename estimate beta
rename stderror se
gen hr = exp(beta)
gen lb = exp(beta - invnorm(0.975)*se)
gen ub = exp(beta + invnorm(0.975)*se)
drop beta se

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
label define time 1 "≤28" 2 ">28"
label values time time
tab term time, mi
drop term

** hr95ci
gen hr95ci = string(hr,"%9.2f") + " (" + string(lb,"%9.2f") +  "-" + string(ub,"%9.2f")+ ")" 
replace hr95ci = string(hr,"%9.1f") + " (" + string(lb,"%9.1f") +  "-" + string(ub,"%9.1f")+ ")" if hr!=. & hr>10
replace hr95ci = "-" if hr95ci=="0.00 (.-.)"
replace hr95ci = "-" if hr95ci=="0.00 (0.00-0.00)"
replace hr95ci = "" if hr95ci==". (.-.)"
local ii = ustrunescape("\u2013")
replace hr95ci = subinstr(hr95ci,"-","`ii'",.) if hr95ci!="-"
drop hr lb ub

** pvalue
rename pvalue pval
gen pvalstr = string(pval, "%9.3f") 
replace pvalstr = "" if pval==.
replace pvalstr = "<0.0001" if pvalstr == "0.000" & pval!=. & pval < 0.0001
replace pvalstr = "<0.001" if pvalstr == "0.000" & pval!=. & pval < 0.001
drop pval
rename pvalstr pvalue


** -----------------------------------------------------------------------------
** order, varlab, group
** -----------------------------------------------------------------------------
tempfile tmppv
preserve
	import excel "${infilepath}\\reference.xlsx", sheet("Sheet1") firstrow clear
	rename (varSI valSI) (var val)
	gen indxv = _n
	save `tmppv', replace
restore	
merge m:1 var val using `tmppv'
tab _merge, mi
assert _merge==3
drop _merge

** sort
sort indxv event vacc time 
order indxv, before(event)

** tidy
drop var val
order varTB valTB, before(time)
rename (varTB valTB) (var val)

** reshape on time
rename (hr95ci pvalue) (hr95ci_time pvalue_time)
reshape wide hr95ci_time pvalue_time, i(indxv vacc event var val varvalVW) j(time)
rename (hr95ci_time1 hr95ci_time2) (hr95ci_time1_ hr95ci_time2_)
rename (pvalue_time1 pvalue_time2) (pvalue_time1_ pvalue_time2_)

** add counts
preserve
	use `tmppC', clear
	rename (arterial_astrazeneca arterial_pfizer venous_astrazeneca venous_pfizer) ///
	       (NArterial_eventAZ NArterial_eventPfizer NVenous_eventAZ NVenous_eventPfizer)
	rename category varvalVW
	reshape long NArterial_event NVenous_event, i(varvalVW) j(vacc) string
	reshape long N, i(varvalVW vacc) j(event) string
	
	gen flag = 1 if inlist(varvalVW,"Thrombocytopenia","Thrombophilia","Venous_event","COVID_infection","OCP")
	replace flag = 1 if inlist(varvalVW,"HRT","Anticoagulant","Antiplatelet","MI_Stroke","Diabetes")
	replace varvalVW = varvalVW + "_Yes" if flag==1
	
	save `tmppC', replace
	
	gen tmp = N if varvalVW=="N"
	bysort event vacc (varvalVW): egen tmpm = max(tmp)
	drop tmp
	keep if flag==1
	replace N = tmpm - N
	replace varvalVW = regexs(1) + "_No" if regexm(varvalVW,"^(.*)_Yes$")
	drop tmpm
	
	append using `tmppC'	
	drop flag
	
	gen Ns = string(N)
	replace Ns = "<5" if inlist(Ns,"1","2","3","4")
	drop N
	rename Ns N
	order event vacc varvalVW N
	save `tmppC', replace		   
restore
merge 1:1 event vacc varvalVW using `tmppC'
tab _merge, mi
assert _merge==3|(_merge==2 & varvalVW=="N")
replace var = "N" if _merge==2 & varvalVW=="N"
replace indxv = 0 if _merge==2 & varvalVW=="N"
drop _merge
drop varvalVW
order N, before(hr95ci_time1_)

** reshape on vacc
reshape wide N hr95ci_time1_ pvalue_time1_ hr95ci_time2_ pvalue_time2_, i(indxv event var val) j(vacc) string
sort event indxv

** dummy rows
gen dummy = .
set more off
local tt = _N
forvalues i=1(1)`tt' {	
	if(var[`i'-1]!=var[`i']){ 
		di "   Previous var: " var[`i'-1] ", New var: " var[`i']
		** add line
		set obs `=_N+1'	
		replace dummy = 1 in `=_N'
		replace indx = (indx[`i'] - 1) + 0.5 in `=_N'
		replace event = event[`i'] in `=_N'
		replace var = var[`i'] in `=_N'
		replace val = var[`i'] in `=_N'
	}
}

** sort
sort event indx 

** tidy pvalues
ds pval*
local pp "`r(varlist)'"
foreach p in `pp' {
	replace `p' = `p'[_n+1] if event==event[_n+1] & var==var[_n+1] & dummy==1
	replace `p' = "" if dummy!=1
}

** tidy
drop if var=="N" & val=="N"
drop indxv var dummy


** -----------------------------------------------------------------------------
** output
** -----------------------------------------------------------------------------
local cdate: display %tdCCYYNNDD date(c(current_date), "DMY")	
export excel "${outfilepath}\\`cdate'_suppTable3n4_v1.xlsx", replace firstrow(variables)
export delimited using "${outfilepath}\\`cdate'_suppTable3n4_v1.csv", replace





