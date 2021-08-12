/*******************************************************************************
Description: This do-file reformats the data for supp table X1: Historical event counts 2018, 2019, 2020
Author: Thomas Bolton
*******************************************************************************/
		
		
** -----------------------------------------------------------------------------
** filepaths
** -----------------------------------------------------------------------------
global directory    "U:\BU\CVD-COVID-UK\CCU002\20210604_2"
global infilepath   "${directory}\inputFiles"
global outfilepath  "${directory}\outputFiles"


**------------------------------------------------------------------------------
** print for log
**------------------------------------------------------------------------------
di "...infilepath     ${infilepath}"
di "...outfilepath    ${outfilepath}"


** -----------------------------------------------------------------------------
** parameters
** -----------------------------------------------------------------------------
** client
global client "MHRA" // MHRA | internal

** infilename
global infilename "export_20210604_VW.csv"

** event_with
global event_with "event with thrombocytopenia" // "event only" | "event with thrombocytopenia" | "event with death in 28 days"


** -----------------------------------------------------------------------------
** settings
** -----------------------------------------------------------------------------
if("${client}"=="MHRA") {
	global per = 1000000 
	global pers "million" 
}
else if("${client}"=="internal") {
	global per = 100000 
	global pers "100,000"
}


** -----------------------------------------------------------------------------
** get data - mid yr popn estimates (mype) 2019
** -----------------------------------------------------------------------------
import excel "${infilepath}\\midYearPopnEstimates2019_TB.xlsx", sheet("Sheet1") clear 

** reformat
drop B C D
rename A sex
rename E age_all
ds F-CR
local vlist "`r(varlist)'"
set more off
foreach var in `vlist' {
	local vary = subinstr("`=`var'[1]'","+","",.)
	di "renaming `var' age_`vary'"
	rename `var' age_`vary'
}
drop if _n==1
destring age_all,  replace
destring age_90,  replace
reshape long age_, i(sex) j(age) 
rename age_ mype

** check
bysort sex (age): egen double tmp = sum(mype) 
format tmp %11.0g
assert tmp==age_all
drop tmp age_all

** age groupings
egen ageb = cut(age), at(0,19,31,40,50,60,70,80,90,91)
tabstat age, by(ageb) stats(min max)
bysort sex ageb (age): egen age_min = min(age)
bysort sex ageb (age): egen age_max = max(age)
gen tmp = string(age_min) + "-" + string(age_max)
replace tmp = "90+" if tmp=="90-90"
drop age_min age_max
collapse (sum)mype=mype, by(sex ageb tmp)
drop age
encode tmp, gen(age)
drop tmp
order age, after(sex)
tab age, mi
tab age, mi nolab

** add combined m&f
tempfile tmppe
preserve
	collapse (sum)mype=mype, by(age)
	gen sex = "Combined"
	save `tmppe', replace
restore
append using `tmppe'
sort sex age

** sex
replace sex = "Men" if sex=="Male"
replace sex = "Women" if sex=="Female"
assert inlist(sex,"Combined","Men","Women")
preserve
	gen ind = 1
	keep sex ind
	collapse (count)n=ind, by(sex)
	assert _N==3
	assert n==9
restore

** save
tempfile mype
save `mype', replace


** -----------------------------------------------------------------------------
** get data - counts
** -----------------------------------------------------------------------------
//import excel "${infilepath}\\${prianyf}", sheet("${prianys}") firstrow clear
import delimited "${infilepath}\\${infilename}", varnames(1) clear 

** filter
tab event_with, mi
assert inlist(event_with,"event only","event with thrombocytopenia","event with death in 28 days")
if("${event_with}"=="event only") keep if event_with=="event only"
else if("${event_with}"=="event with thrombocytopenia") keep if event_with=="event with thrombocytopenia"
else if("${event_with}"=="event with death in 28 days") keep if event_with=="event with death in 28 days"
else assert _N==0 
drop event_with

** renaming
rename (event year record) (evt yr source)

** sex
tab sex, mi
assert inlist(sex,"","Men","Women")
replace sex = "null" if sex==""
preserve
	keep if sex=="null"
	if("${event_with}"=="event only") {
		assert _N==10
		assert n==1|n==2
	}
	else if("${event_with}"=="event with thrombocytopenia") {
		assert _N==0
	}
	else if("${event_with}"=="event with death in 28 days") {
		assert _N==0
	}
	else assert _N==0
restore

** age
encode age_band, gen(age)
tab age age_band, mi
tab age age_band, mi nolab

** reorder null age
replace age = 0 if age_band==""
label define age 0 "null", add
tab age age_band, mi
tab age age_band, mi nolab
drop age_band

** tidy
order evt sex age yr source n
sort evt sex age yr source n

** collapse source 
collapse (sum)n=n, by(evt sex age yr)

** get missing rows for sex (e.g., for male "Thrombosis_during_pregnancy")
reshape wide n, i(yr age evt) j(sex) string
reshape long

** get missing rows for age (e.g., for male "Thrombosis_during_pregnancy")
reshape wide n, i(yr sex evt) j(age) 
reshape long
order evt sex age yr n
sort evt sex age yr n

** missings to zero
//br if n==.
tab evt age if n==. & sex=="null", mi
tab evt age if n==. & sex!="null", mi
replace n = 0 if n==.

** add combined men and women - includes null sex (which we drop afterwards)
tempfile tmppj
preserve
	collapse (sum)n=n, by(evt age yr)
	gen sex = "Combined"
	save `tmppj', replace
restore
append using `tmppj'
drop if sex=="null"
order evt yr sex age n
sort evt yr sex age

** check evt
tab evt, mi
preserve
	keep evt
	bysort evt: keep if _n==1
	
	if("${event_with}"=="event only") {
		assert _N==18
	}
	else if("${event_with}"=="event with thrombocytopenia") {
		assert _N==17
		assert evt!="thrombocytopenia"
	}
	else if("${event_with}"=="event with death in 28 days") {
		assert _N==18
	}
	else assert _N==0
		
	** ordering 
	gen ord = .
	replace ord =  1 if evt=="cerebral venous thrombosis" // was "intracranial venous thrombosis"
	replace ord =  2 if evt=="portal vein thrombosis"
	replace ord =  3 if evt=="other deep vein thrombosis" // was "other vein thrombosis"
	replace ord =  4 if evt=="deep vein thrombosis" // was "lower limb thrombosis" - see email
	replace ord =  5 if evt=="pulmonary embolism"
	replace ord =  6 if evt=="mesenteric thrombus"
	replace ord =  7 if evt=="thrombosis during pregnancy and puerperium"	
	replace ord =  8 if evt=="incident myocardial infarction"
	replace ord =  9 if evt=="ischaemic stroke"	
	replace ord = 10 if evt=="stroke of unknown type"	
	replace ord = 11 if evt=="retinal infarction"	
	replace ord = 12 if evt=="spinal stroke"
	replace ord = 13 if evt=="intracerebral haemorrhage" // was "haemorrhagic stroke"
	replace ord = 14 if evt=="stroke, subarachnoid haemorrhage" // added
	replace ord = 15 if evt=="other arterial embolism" // was "other arterial thrombosis"
	replace ord = 16 if evt=="thrombocytopenia" // added
	replace ord = 17 if evt=="disseminated intravascular coagulation" // added
	replace ord = 18 if evt=="thrombotic thrombocytopenic purpura" // added
	
	assert ord!=.
	sort ord
	
	local nn = _N
	global nn = `nn'
	forvalues i=1(1)`nn' {
		local ord_`i' = "`=ord[`i']'"
		
		if("${event_with}"=="event only") {
			assert "`ord_`i''"=="`i'"
		}
		else if("${event_with}"=="event with thrombocytopenia") {
			// missing thrombocytopenia
			assert "`ord_`i''"=="`i'" if `i'<=15
			assert "`ord_`i''"=="`=`i' + 1'" if `i'>15
		}
		else if("${event_with}"=="event with death in 28 days") {
			assert "`ord_`i''"=="`i'"
		}		
		
		local evt_`i' = "`=evt[`i']'"
		di "evt_`i' = `evt_`i''"
	}
restore


** -----------------------------------------------------------------------------
** loop over evts
** -----------------------------------------------------------------------------
set more off
tempfile tmppF tmppM tmppT
save `tmppF', replace
local vvv = 0

forvalues i=1(1)`nn' {
	di "########################################################################"
	di "`i'/`nn' `evt_`i''"
	
	** get data
	use `tmppF', clear
	
	** filter
	tab evt, mi
	keep if evt=="`evt_`i''"

	** check number of rows
	assert _N==90 // 10 age groups (0(null), 1-9) * 3 years (2018, 2019, 2020) * 3 sex (m, w, combined)
	
	** add ntot
	bysort evt sex yr (age): egen ntot = sum(n)	
	
	** add mype and mypetot
	merge m:1 sex age using `mype'
	assert _merge==3 & age!="null":age | _merge==1 & age=="null":age
	drop _merge
	bysort evt sex yr (age): egen double mypetot = sum(mype)
	
	** add rows: n totals mype totals over age and add headers
	gen nheader1 = .
	gen nheader2 = .
	gen nheader3 = .
	gen nheader4 = .
	gen nheader5 = .
	rename n nn
	rename mype mypen
	reshape long n mype, i(evt sex yr age) j(tmp) string
	drop if tmp=="tot" & age!="90+":age 
	replace age = 999 if tmp=="tot" 
	label define age 999 "Any age", add
	drop if tmp=="header1" & age!="null":age 
	drop if tmp=="header2" & age!="null":age
	drop if tmp=="header3" & age!="null":age
	drop if tmp=="header4" & age!="null":age
	drop if tmp=="header5" & age!="null":age
	replace age = -5 if tmp=="header5" 
	replace age = -4 if tmp=="header4" 
	replace age = -3 if tmp=="header3"
	replace age = -2 if tmp=="header2"
	replace age = -1 if tmp=="header1"
	label define age -5 "", add
	label define age -4 "", add
	label define age -3 "", add
	label define age -2 "", add
	label define age -1 "", add
	drop tmp

	** calculate incidence
	gen inc = n/(mype/${per})
	format inc %9.1f
	gen incm = inc/12
	format incm %9.1f
	drop mype
	
	** convert n to string
	tostring n, gen(ns)
	drop n
	replace ns = "" if ns=="."
	
	** censoring
	replace ns = "<5" if ns=="4"
	replace ns = "<5" if ns=="3"
	replace ns = "<5" if ns=="2"
	replace ns = "<5" if ns=="1"
	
	replace inc = round(inc,0.1)
	tostring inc, gen(incs) format(%7.1f) force
	drop inc
	replace incs = "" if incs=="."
	replace incm = round(incm,0.1)
	tostring incm, gen(incms) format(%9.1f) force
	drop incm
	replace incms = "" if incms=="."	
	
	
	reshape wide ns incs incms, i(evt sex age) j(yr) 
	reshape wide ns* incs* incms*, i(evt age) j(sex) string

	** add headers
	ds ns* incs* incms*
	local vlist "`r(varlist)'"
	di "`vlist'"
	foreach var in `vlist' {
		di "`var'"
		** col
		if(regexm("`var'","^ns(.*)$")){
			replace `var' = "N" if age==-1
			** yr
			if(regexm("`var'","^ns2018(.*)$")) replace `var' = "2018" if age==-2
			else if(regexm("`var'","^ns2019(.*)$")) replace `var' = "2019" if age==-2
			else if(regexm("`var'","^ns2020(.*)$")) replace `var' = "2020" if age==-2
			** sex
			if(regexm("`var'","^ns2018Combined$")) replace `var' = "Total" if age==-3
			else if(regexm("`var'","^ns2018Men$")) replace `var' = "Men" if age==-3 
			else if(regexm("`var'","^ns2018Women$")) replace `var' = "Women" if age==-3 
		}
		else if(regexm("`var'","^incs(.*)$")) replace `var' = "Incidence per ${pers} per year" if age==-1
		else if(regexm("`var'","^incms(.*)$")) replace `var' = "Incidence per ${pers} per month" if age==-1
	}	
	
	
	label define age -1 "Age group", modify
	preserve
		keep evt
		bysort evt: keep if _n==1
		assert _n==1
	restore	
	local evt = evt[1]
	local evt = upper(substr("`evt'", 1, 1)) + lower(substr("`evt'", 2, .)) 
	label define age -5 "`evt'", modify
	drop evt

	** age to string
	decode age, gen(ages)
	order ages, after(age)
	
	** drop incm
	if("${client}"=="internal") drop incms*

	
	** -------------------------------------------------------------------------
	** long format
	** -------------------------------------------------------------------------
	local bb = _N
	local bb = `bb' + 2
	set obs `bb'
	
	** save
	if(`i'>1)	{
		save `tmppT', replace
		use `tmppM', clear
		append using `tmppT'
	}
	save `tmppM', replace
}

drop if age=="null":age
drop age


** -----------------------------------------------------------------------------
** output
** -----------------------------------------------------------------------------
local cdate: display %tdCCYYNNDD date(c(current_date), "DMY")	
local event_with = subinstr("${event_with}"," ","_",.)
export delimited using "${outfilepath}\\`cdate'_`event_with'_${client}_v1_censor.csv", replace
