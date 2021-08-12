/*******************************************************************************
Description: This do-file produces the Figure 1: Adjusted Hazard ratios for (all venous thromboses, intracranial venous thromboses, pulmonary embolism and deep vein thromboses / all arterial thromboses, myocardial infarction, ischemic stroke and other arterial thromboses / thrombocytopenia, hemorrhagic stroke, mesenteric thrombosis, lower limb fracture) after ChAdOx1-S or BNT162b2 vaccine
Author: Thomas Bolton
*******************************************************************************/


** -----------------------------------------------------------------------------
** filepaths
** -----------------------------------------------------------------------------
global infilepath "U:\BU\CVD-COVID-UK\CCU002\20210618\inputFiles"
global outfilepath  "U:\BU\CVD-COVID-UK\CCU002\20210618\outputFiles"


** -----------------------------------------------------------------------------
** import
** -----------------------------------------------------------------------------
** import file 1
global infilename "Hazard Ratios Unadjusted, Agesex-adjusted, fully adjusted (using backwards selection)"
import delimited "${infilepath}\\${infilename}.csv", varnames(1) clear 
drop if event=="Venous_event"

** import file 2
tempfile tmpp
preserve
	global infilename "Hazard Ratios venous event update – fully adjusted (using backwards selection)"
	import delimited "${infilepath}\\${infilename}.csv", varnames(1) clear 
	assert event=="Venous_event"
	save `tmpp', replace
restore
append using `tmpp'
	
** check	
tab event, mi 

** tidy
keep if term=="week1_4"|term=="week5_39"
drop if sex=="all_interact"
drop v1 stderror robustse statistic pvalue
rename event outcome 
rename agegp age
rename vac vacc
rename (estimate conflow confhigh) (hr lb ub)
rename term time

** vacc
tab vacc, mi 
assert vacc=="vac_az"|vacc=="vac_pf"
replace vacc = "AZ" if vacc=="vac_az"
replace vacc = "Pfizer" if vacc=="vac_pf"
replace vacc = "ZZZComb" if vacc=="vac_all"
drop if vacc=="ZZZComb"
tab vacc, mi 

** time
tab time, mi
assert time=="week1_4"|time=="week5_39"
tempfile tmpp
preserve
	keep if time=="week1_4"
	replace time = "no"
	replace hr = 1
	replace lb = 1
	replace ub = 1
	save `tmpp', replace
restore
append using `tmpp'
tab time, mi
replace time = "Post, weeks 1-4" if time=="week1_4"
replace time = "Post, weeks 5-39" if time=="week5_39"
replace time = "No vaccination" if time=="no"
tab time, mi


** -----------------------------------------------------------------------------
** outcome
** -----------------------------------------------------------------------------
tab outcome, mi
drop if outcome=="ICVT_pregnancy"
drop if outcome=="DVT_pregnancy"
drop if outcome=="Haematological_event"
drop if outcome=="THROMBO_plus_ART"
drop if outcome=="THROMBO_plus_VEN"
tab outcome, mi
gen outcome_old = outcome

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

replace outcome = "Disseminated intravascular coagulation" if outcome=="DIC"
replace outcome = "Thrombotic thrombocytopenic purpura" if outcome=="TTP"
replace outcome = "Any thrombocytopenia" if outcome=="thrombocytopenia"

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

//replace ord = 14 if outcome=="Haematological" // HEADER
replace ord = 15 if outcome=="Disseminated intravascular coagulation"
replace ord = 16 if outcome=="Thrombotic thrombocytopenic purpura"
replace ord = 17 if outcome=="Any thrombocytopenia"

//replace ord = 18 if outcome=="Other" & tmp==1 // HEADER
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
order outcome_old outcome sex age time vacc hr lb ub
sort outcome sex age vacc time  

tempfile hr
save `hr', replace


** -----------------------------------------------------------------------------
** add event numberes
** -----------------------------------------------------------------------------
import delimited "${infilepath}\\Event Counts and Incidence Rates per 100,000 person-yrs.csv", varnames(1) clear 
drop v1 ndays* ir*
drop if vac=="vac_all"
rename agegroup age

** vacc
assert vaccine=="vac_az"|vaccine=="vac_pf"
rename vaccine vacc
tab vacc, mi
replace vacc = "AZ" if vacc=="vac_az"
replace vacc = "Pfizer" if vacc=="vac_pf"
tab vacc, mi

** time
tab periodwrtexpo, mi
assert periodwrtexpo=="28 days post-expo"|periodwrtexpo==">28 days post-expo"|periodwrtexpo=="unexposed"
rename periodwrtexpo time
tab time, mi
replace time = "Post, weeks 1-4" if time=="28 days post-expo"
replace time = "Post, weeks 5-39" if time==">28 days post-expo"
replace time = "No vaccination" if time=="unexposed"
tab time, mi

order outcome age time vacc 
sort outcome age time vacc
reshape long nevents, i(outcome age time vacc) j(sex) string
rename nevents nevt
replace sex = "all" if sex=="total"
replace sex = "1" if sex=="male"
replace sex = "2" if sex=="female"
order outcome sex age time vacc
sort outcome sex age time vacc 

rename outcome outcome_old
drop if age=="all"
drop if outcome_old=="ICVT_pregnancy"
drop if outcome_old=="DVT_pregnancy"
drop if outcome_old=="Haematological_event"
drop if outcome_old=="THROMBO_plus_ART"
drop if outcome_old=="THROMBO_plus_VEN"
replace outcome_old = "ICVT_summ_event" if outcome_old=="DVT_ICVT"
replace outcome_old = "DVT_summ_event" if outcome_old=="DVT_DVT"

merge 1:1 outcome_old sex age time vacc using `hr'
tab _merge, mi
if("${infilename}"=="Hazard Ratios venous event update – fully adjusted (using backwards selection)") {
	assert inlist(_merge,1,3)
	tab outcome_old if _merge==1, mi
	assert inlist(outcome_old,"DIC","TTP") if _merge==1
	keep if _merge==3
}
assert _merge==3|_merge==1 
tab outcome_old age if _merge==1, mi
assert outcome_old=="DIC" | outcome_old=="TTP" | (outcome_old=="ICVT_summ_event" & vacc=="AZ" & age=="<70") if _merge==1
drop _merge

** temp save
tempfile tmppc
save `tmppc', replace


** -----------------------------------------------------------------------------
** flag out of range HRs
** -----------------------------------------------------------------------------
gen tmp = 1 if hr<0.001
list if tmp==1
replace hr = . if tmp==1
replace lb = . if tmp==1
list if tmp==1 & ub>=0.001
replace ub = . if tmp==1 & ub<0.003
drop tmp

gen flag = .
replace flag = 1 if lb!=. & lb<0.25
replace flag = 2 if ub!=. & ub>8
replace flag = 3 if lb!=. & lb<0.25 & ub!=. & ub>8
replace flag = 4 if hr!=. & hr<0.25
replace flag = 5 if hr!=. & hr>8
replace flag = 6 if hr!=. & hr<0.25 & ub!=. & ub<0.25
replace flag = 7 if hr!=. & hr>8 & lb!=. & lb>8
replace flag = 8 if flag==5 & hr!=. & hr>8 & lb!=. & lb<0.25 & ub!=. & ub>8
tab outcome flag, mi

replace hr = . if hr<0.25
replace hr = . if hr>8
gen lbtmp = 0.25
gen ubtmp = 8
gen lbtmpx = 0.25 + 0.001
gen ubtmpx = 8 - 0.001


** -----------------------------------------------------------------------------
** create event list
** -----------------------------------------------------------------------------
preserve
	keep outcome outcome_old
	bysort outcome_old outcome: keep if _n==1
	if("${infilename}"=="Hazard Ratios Unadjusted, Agesex-adjusted, fully adjusted (using backwards selection), Suppl table 34") {
		drop if outcome_old=="ICVT_summ_event" & outcome==.
	}
	bysort outcome_old: keep if _n==1
	sort outcome
	local nn = _N
	local list_outcome_old ""
	forvalues i = 1(1)`nn' {
		local list_outcome_old "`list_outcome_old' `=outcome_old[`i']'"
	}
	di "`nn' list_outcome_old = `list_outcome_old'"
restore


** -----------------------------------------------------------------------------
** loop
** -----------------------------------------------------------------------------
** temp save
tempfile tmppM
save `tmppM', replace

local hhh = 0
foreach outcome_old in `list_outcome_old' {
	di "outcome_old = `outcome_old'"
	
	local ++hhh
	
	** get data
	use `tmppM', clear
	
	** filter data
	keep if outcome_old=="`outcome_old'"
	

	** -----------------------------------------------------------------------------
	** recodes
	** -----------------------------------------------------------------------------
	replace sex = "0" if sex=="all"
	sort outcome sex age time vacc 
	rename sex sexx
	encode sexx, gen(sex)

	rename outcome_old outcomex 
	rename outcome outcome_new
	encode outcomex, gen(outcome)

	rename age agex
	encode agex, gen(age)

	rename vacc vaccx
	encode vaccx, gen(vacc)

	rename time timex
	sencode timex, gen(time)
	drop timex
	rename time timep

	local tmp = 0.05 
	replace timep = timep - `tmp' if vacc=="AZ":vacc
	replace timep = timep + `tmp' if vacc=="Pfizer":vacc
	
	order sex outcome age vacc time

	gen byvar = outcomex + "_" + sexx + "_" + agex
	encode byvar, gen(byvarn)


	**--------------------------------------------------------------------------
	** plot options
	**--------------------------------------------------------------------------
	global ytit "Hazard ratio (95% CI)" 
	global ylab = "0.25 0.5 1 2 4 8" 
	global yfor = "%9.0f"
	global ymin = 0.25
	global ymax = 8
	global ylog = "log"

	global xtit "Vaccination status" 
	global xlab "1 2 3" 
	global xfor "" 
	global xmin = 0.66
	global xmax = 3.33
	global xlog "" 

	global ysizetb = 12 
	global xsizetb = 7 

	global txtsize "*0.94"
	global txtsize2 "*0.60"

	global colb "navy"
	global colr "maroon"
	global colg "forest_green"

	local infilenamexx = subinstr(substr("${infilename}",1,8)," ","",.)	
	local outcomexx = substr("`outcome_old'",1,8)
	local tmpname "a`infilenamexx'_`outcomexx'"

	local lwid "*0.9"
	twoway ///
		(scatter hr timep if vacc=="AZ":vacc, col("${colb}") msym(D) msize(*0.88) c(l) lpattern(shortdash) lwidth(*0.7) lcol("${colb}") yline(1, lpattern(vshortdash) lcol(black) lwidth(*0.75))) /// 
		(rcap lb ub timep if vacc=="AZ":vacc & flag==., col("${colb}") msize(*1.1) lwidth(`lwid')) ///	
		(rcap hr ub timep if vacc=="AZ":vacc & flag==1, col("${colb}") msize(*1.1) lwidth(`lwid')) ///	
		(pcarrow hr timep lbtmp timep if vacc=="AZ":vacc & flag==1, col("${colb}") msize(*1) lwidth(`lwid') mlwidth(`lwid') barbsize(0)) ///	
		(rcap lb hr timep if vacc=="AZ":vacc & flag==2, col("${colb}") msize(*1.1) lwidth(`lwid')) ///	
		(pcarrow hr timep ubtmp timep if vacc=="AZ":vacc & flag==2, col("${colb}") msize(*1) lwidth(`lwid') mlwidth(`lwid') barbsize(0)) ///		
		(pcarrow hr timep lbtmp timep if vacc=="AZ":vacc & flag==3, col("${colb}") msize(*1) lwidth(`lwid') mlwidth(`lwid') barbsize(0)) ///	
		(pcarrow hr timep ubtmp timep if vacc=="AZ":vacc & flag==3, col("${colb}") msize(*1) lwidth(`lwid') mlwidth(`lwid') barbsize(0)) ///		
		(rcap ub ub timep if vacc=="AZ":vacc & flag==4, col("${colb}") msize(*1.1) lwidth(`lwid')) ///	
		(pcarrow ub timep lbtmp timep if vacc=="AZ":vacc & flag==4, col("${colb}") msize(*1) lwidth(`lwid') mlwidth(`lwid') barbsize(0)) ///	
		(rcap lb lb timep if vacc=="AZ":vacc & flag==5, col("${colb}") msize(*1.1) lwidth(`lwid')) ///	
		(pcarrow lb timep ubtmp timep if vacc=="AZ":vacc & flag==5, col("${colb}") msize(*1) lwidth(`lwid') mlwidth(`lwid') barbsize(0)) ///			
		(pcarrow lbtmpx timep lbtmp timep if vacc=="AZ":vacc & flag==6, col("${colb}") msize(*1) lwidth(`lwid') mlwidth(`lwid') barbsize(0)) ///			
		(pcarrow ubtmpx timep ubtmp timep if vacc=="AZ":vacc & flag==7, col("${colb}") msize(*1) lwidth(`lwid') mlwidth(`lwid') barbsize(0)) ///
		(pcarrow ubtmp timep lbtmp timep if vacc=="AZ":vacc & flag==8, col("${colb}") msize(*1) lwidth(`lwid') mlwidth(`lwid') barbsize(0)) ///	
		(pcarrow lbtmp timep ubtmp timep if vacc=="AZ":vacc & flag==8, col("${colb}") msize(*1) lwidth(`lwid') mlwidth(`lwid') barbsize(0)) ///			
		(scatteri . . if vacc=="AZ":vacc, msym(D) msize(*0.40) col("${colb}") c(l) lpattern(vshortdash) lwidth(*0.4) lcol("${colb}")) /// 
		///
		(scatter hr timep if vacc=="Pfizer":vacc, col("${colr}") msym(S) msize(*0.88) c(l) lpattern(shortdash) lwidth(*0.7) lcol("${colr}")) /// 
		(rcap lb ub timep if vacc=="Pfizer":vacc & flag==., col("${colr}") msize(*1.1) lwidth(`lwid')) ///		
		(rcap hr ub timep if vacc=="Pfizer":vacc & flag==1, col("${colr}") msize(*1.1) lwidth(`lwid')) ///		
		(pcarrow hr timep lbtmp timep if vacc=="Pfizer":vacc & flag==1, col("${colr}") msize(*1) lwidth(`lwid') mlwidth(`lwid') barbsize(0)) ///		
		(rcap lb hr timep if vacc=="Pfizer":vacc & flag==2, col("${colr}") msize(*1.1) lwidth(`lwid')) ///		
		(pcarrow hr timep ubtmp timep if vacc=="Pfizer":vacc & flag==2, col("${colr}") msize(*1) lwidth(`lwid') mlwidth(`lwid') barbsize(0)) ///			
		(pcarrow hr timep lbtmp timep if vacc=="Pfizer":vacc & flag==3, col("${colr}") msize(*1) lwidth(`lwid') mlwidth(`lwid') barbsize(0)) ///		
		(pcarrow hr timep ubtmp timep if vacc=="Pfizer":vacc & flag==3, col("${colr}") msize(*1) lwidth(`lwid') mlwidth(`lwid') barbsize(0)) ///			
		(rcap ub ub timep if vacc=="Pfizer":vacc & flag==4, col("${colr}") msize(*1.1) lwidth(`lwid')) ///	
		(pcarrow ub timep lbtmp timep if vacc=="Pfizer":vacc & flag==4, col("${colr}") msize(*1) lwidth(`lwid') mlwidth(`lwid') barbsize(0)) ///			
		(rcap lb lb timep if vacc=="Pfizer":vacc & flag==5, col("${colr}") msize(*1.1) lwidth(`lwid')) ///	
		(pcarrow lb timep ubtmp timep if vacc=="Pfizer":vacc & flag==5, col("${colr}") msize(*1) lwidth(`lwid') mlwidth(`lwid') barbsize(0)) ///					
		(pcarrow lbtmpx timep lbtmp timep if vacc=="Pfizer":vacc & flag==6, col("${colr}") msize(*1) lwidth(`lwid') mlwidth(`lwid') barbsize(0)) ///					
		(pcarrow ubtmpx timep ubtmp timep if vacc=="Pfizer":vacc & flag==7, col("${colr}") msize(*1) lwidth(`lwid') mlwidth(`lwid') barbsize(0)) ///					
		(pcarrow ubtmp timep lbtmp timep if vacc=="Pfizer":vacc & flag==8, col("${colr}") msize(*1) lwidth(`lwid') mlwidth(`lwid') barbsize(0)) ///		
		(pcarrow lbtmp timep ubtmp timep if vacc=="Pfizer":vacc & flag==8, col("${colr}") msize(*1) lwidth(`lwid') mlwidth(`lwid') barbsize(0)) ///		
		(scatteri . . if vacc=="Pfizer":vacc, msym(S) msize(*0.40) col("${colr}") c(l) lpattern(vshortdash) lwidth(*0.4) lcol("${colr}")) ///
		, ///
		by(byvarn, col(6) iscale(*0.79) ix ixtitle iy iytitle graphregion(color(white)) ///
			note("", size(${txtsize})) ///
			subtitle(, bfcolor(white) nobox size(*0.82) justification(left)) ///
			title("", size(${txtsize}) col(black)) ///
			legend(ring(0) position(12)) /// 
		) ///
		legend(order(17 34) col(1) size(*0.52) region(style(none)) rowgap(0.7) keygap(*0.5) symxsize(*0.3) symysize(*0.5) /// 
			label(17 "ChAdOx1-S") /// 
			label(34 "BNT162b2") /// 
		) ///	
		graphregion(color(white)) /// 
		plotregion(margin(none)) ///
		ytitle("${ytit}" " ", size(${txtsize})) ///
		ylab(${ylab}, angle(horiz) format(${yfor}) labsize(${txtsize}) nogrid) ///
		yscale(range(${ymin} ${ymax}) ${ylog}) ///
		xtitle(" " "${xtit}", size(${txtsize})) ///
		xlab(${xlab}, angle(horiz) format(${xfor}) valuelab labsize(${txtsize})) ///
		xscale(range(${xmin} ${xmax}) ${xlog}) /// 
		note(, size(*0.45)) ///	
		ysize(${ysizetb}) xsize(${xsizetb}) ///
		subtitle(, bfcolor(white) nobox size(*0.82) justification(left))  ///
		name("`tmpname'", replace) 	
		
		
	** -----------------------------------------------------------------------------
	** gredit
	** -----------------------------------------------------------------------------
	** sizing
	gr_edit .style.editstyle declared_ysize(5) editcopy
	gr_edit .style.editstyle declared_xsize(18) editcopy

	** ratio
	gr_edit .gmetric_mult = 1.6

	** remove main axis titles
	gr_edit .l1title.draw_view.setstyle, style(no)
	gr_edit .b1title.draw_view.setstyle, style(no)
		
	** y-axis reposition
	gr_edit .plotregion1.yaxis1[1].title.xoffset = 1.5
		
	** y-axis titles remove
	local xx "2 3 4 5 6"
	foreach i in `xx' { 
		gr_edit .plotregion1.yaxis1[`i'].title.text = {}
		gr_edit .plotregion1.yaxis1[`i'].title.text.Arrpush `" "'
	}	

	** y-axis titles add space
	local xx "3 5"
	foreach i in `xx' { 
		gr_edit .plotregion1.yaxis1[`i'].title.text = {}
		gr_edit .plotregion1.yaxis1[`i'].title.text.Arrpush `" "'
		gr_edit .plotregion1.yaxis1[`i'].title.text.Arrpush `" "'
		//gr_edit .plotregion1.yaxis1[`i'].title.draw_view.setstyle, style(no)
	}

	** y-axis labels remove
	local xx "2 4 6"
	foreach i in `xx' { 
		gr_edit .plotregion1.yaxis1[`i'].EditCustomStyle , j(-1) style(majorstyle(tickstyle(show_labels(no))))
	}

	** x-axis labels
	local xx "1 2 3 4 5 6"
	foreach i in `xx' { 
		gr_edit .plotregion1.xaxis1[`i'].major.num_rule_ticks = 0
		gr_edit .plotregion1.xaxis1[`i'].edit_tick 1 1 `""Pre" " ""', tickset(major)
		gr_edit .plotregion1.xaxis1[`i'].major.num_rule_ticks = 0
		gr_edit .plotregion1.xaxis1[`i'].edit_tick 2 2 `""Post" "≤28 days""', tickset(major)
		gr_edit .plotregion1.xaxis1[`i'].major.num_rule_ticks = 0
		gr_edit .plotregion1.xaxis1[`i'].edit_tick 3 3 `""Post" ">28 days""', tickset(major)
	}

	** get counts
	preserve
		use `tmppc', clear
		keep if outcome_old=="`outcome_old'"
		bysort outcome_old sex age time (vacc): egen nevt_max = max(nevt)
		
		** do not double count "No vaccination"
		drop if time=="No vaccination" & nevt!=nevt_max
		bysort outcome_old sex age time (vacc): gen tmp = _n 
		drop if time=="No vaccination" & tmp==2
		
		tab sex, mi
		drop if sex==""
		
		collapse (sum)nevt_sum=nevt, by(outcome_old sex age)
		
		assert _N==6
		forvalues i=1(1)6 {
			     if(outcome[`i']=="`outcome_old'" & sex[`i']=="all" & age[`i']=="<70" ) local sa_al "`=nevt_sum[`i']'" 
			else if(outcome[`i']=="`outcome_old'" & sex[`i']=="all" & age[`i']==">=70") local sa_ag "`=nevt_sum[`i']'" 
			else if(outcome[`i']=="`outcome_old'" & sex[`i']=="1"   & age[`i']=="<70" ) local sm_al "`=nevt_sum[`i']'" 
			else if(outcome[`i']=="`outcome_old'" & sex[`i']=="1"   & age[`i']==">=70") local sm_ag "`=nevt_sum[`i']'" 
			else if(outcome[`i']=="`outcome_old'" & sex[`i']=="2"   & age[`i']=="<70" ) local sw_al "`=nevt_sum[`i']'" 
			else if(outcome[`i']=="`outcome_old'" & sex[`i']=="2"   & age[`i']==">=70") local sw_ag "`=nevt_sum[`i']'" 		
		}
		di "sa_al `sa_al'"
		di "sa_ag `sa_ag'"
		di "sm_al `sm_al'"
		di "sm_ag `sm_ag'"
		di "sw_al `sw_al'"
		di "sw_ag `sw_ag'"
	restore

	** subtitles
	gr_edit .plotregion1.subtitle[1].text = {}
	preserve		
		if("${infilename}"=="Fully adjusted using AMI’s backwards selected complementary covariates") {
			use `tmppc', clear
			keep if outcome_old=="`outcome_old'"
			keep hr outcome
			rename outcome outcome_new
		}
		
		collapse (sum)hr=hr, by(outcome_new)
		if("${infilename}"=="Fully adjusted using AMI’s backwards selected complementary covariates" ///
		   | "${infilename}"=="Hazard Ratios Unadjusted, Agesex-adjusted, fully adjusted (using backwards selection), Suppl table 34" ///
		) {
			drop if outcome_new==.
		}
		assert _N==1		
		keep outcome_new
		decode outcome_new, gen(jjj)
		local jjj "`=jjj[1]'"
		if("`outcome_old'"=="other_DVT" & "`jjj'"=="Other") local jjj "Other venous"
		di "`jjj'"
	restore
	gr_edit .plotregion1.subtitle[1].text.Arrpush `"{bf:`jjj'}"'

	gr_edit .plotregion1.subtitle[1].text.Arrpush `"{bf:}"'
	gr_edit .plotregion1.subtitle[1].text.Arrpush `"{bf:Overall}"'
	gr_edit .plotregion1.subtitle[1].text.Arrpush `"{bf:}"'
	gr_edit .plotregion1.subtitle[1].text.Arrpush `"{bf:Age<70}, `sa_al' events"'

	gr_edit .plotregion1.subtitle[2].text = {}
	gr_edit .plotregion1.subtitle[2].text.Arrpush `"{bf:Age≥70}, `sa_ag' events"'

	gr_edit .plotregion1.subtitle[3].text = {}
	gr_edit .plotregion1.subtitle[3].text.Arrpush `"{bf:Men}"'
	gr_edit .plotregion1.subtitle[3].text.Arrpush `"{bf:}"'
	gr_edit .plotregion1.subtitle[3].text.Arrpush `"{bf:Age<70}, `sm_al' events"'

	gr_edit .plotregion1.subtitle[4].text = {}
	gr_edit .plotregion1.subtitle[4].text.Arrpush `"{bf:Age≥70}, `sm_ag' events"'

	gr_edit .plotregion1.subtitle[5].text = {}
	gr_edit .plotregion1.subtitle[5].text.Arrpush `"{bf:Women}"'
	gr_edit .plotregion1.subtitle[5].text.Arrpush `"{bf:}"'
	gr_edit .plotregion1.subtitle[5].text.Arrpush `"{bf:Age<70}, `sw_al' events"'

	gr_edit .plotregion1.subtitle[6].text = {}
	gr_edit .plotregion1.subtitle[6].text.Arrpush `"{bf:Age≥70}, `sw_ag' events"'

	
	** -----------------------------------------------------------------------------
	** export
	** -----------------------------------------------------------------------------
	local cdate: display %tdCCYYNNDD date(c(current_date), "DMY")
	local infilename = subinstr("${infilename}"," ","_",.)
	local infilename = subinstr("`infilename'",",","_",.)
	local infilename = substr("`infilename'",1,20)
	if("${infilename}"=="Completely unadjusted, all outcomes") local infilename "Unadj"
	if("${infilename}"=="Adjusted for age and sex only") local infilename "AdjAgeSex"
	if("${infilename}"=="Fully adjusted using AMI’s backwards selected complementary covariates") local infilename "AdjFull"
	if("${infilename}"=="Hazard Ratios Unadjusted, Agesex-adjusted, fully adjusted (using backwards selection), Suppl table 34") local infilename "AdjFull"
	
	if(`hhh'<10) local hhh "0`hhh'"
	graph export "${outfilepath}\\`cdate'_`infilename'_`hhh'_`outcome_old'_v5.emf", replace
	graph export "${outfilepath}\\`cdate'_`infilename'_`hhh'_`outcome_old'_v5.png", replace width(4000)	
}
