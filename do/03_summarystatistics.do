

*********************************************************************
*	This file creates summary statistics for Public Costs project	*
*********************************************************************


clear all

		
********************************************************************************

	******************************
	* Run the Matching Procedure *
	******************************

** Randomly match controls to cases 
forvalues x=1/3750 {
  
use "$data/CasesAndControls.dta", clear
  keep if matchgrp==`x'
  keep hhidpn bid_hrs_22 dementiacase dementia eligiblecase hhmemberiscase casecontrol ///
  matchgrp allyearseligibility indexdate indexmthyear dxdatehhmember ///
  dxhhmembermthyear dupecase ragender rabyear waveentered caseid
  
  *sort in descending order
  gsort -casecontrol hhidpn caseid 
  set seed 81979
  *apply random count to cases
  g rnumcases=runiform() if casecontrol==1
  set seed 21279
  *apply random count to controls
  g rnumcontrols=runiform() if casecontrol==0
  g rnum=.
	  replace rnum=rnumcases if casecontrol==1
	  replace rnum=rnumcontrols if casecontrol==0
  *sort in descending order by casecontrol ascending order by rnum
  gsort -casecontrol +rnum hhidpn caseid 
 
  *generate count of cases, saved to r(N)
  count if casecontrol
  loc anyassigned=1

	while `anyassigned' {
	// clear anyassigned for a run through of all cases/controls
	loc anyassigned=0

	loc h=1
	// loop through all cases
	while `h'<=r(N) {
		// i starts at first control
		loc i=r(N)+1
		// loop through all controls, breaking at first eligible for current case (h)
		while `i'<=_N {
			// is this control eligible for the current case?
			loc match=substr(allyearseligibility[`i'],(year(indexdate[`h'])-1992)*12+month(indexdate[`h'])-12,14)=="11111111111111"
			loc nodx=(dementiacase[`i']==0 | (indexmthyear[`i']-indexmthyear[`h']>=72))
			loc nohhdx=(hhmemberiscase[`i']==0 | (dxhhmembermthyear[`i']-indexmthyear[`h']>=72))
			
			// if yes, AND caseid hasn't been assigned, replace the caseid with the current case
			loc assign = caseid[`i']==. & `match' & `nodx' & `nohhdx' & caseid[`h']<5
			replace caseid=hhidpn[`h'] if (_n==`i' & `assign'==1)
			di "`i'"
			
			// progress control loop
			loc i=`i'+1
			
			// break if match since the case was assigned
			if `assign' {
				loc anyassigned=1
				replace caseid=caseid[`h']+1 if _n==`h'
				loc i=_N+1 
			}
		}
		
		// progress case loop
		loc h=`h'+1
	}
  }
	*finding the first match
	drop if casecontrol==1|caseid==.
	bys caseid: g dup = cond(_N==1, 0, _n)
	drop if dup>1
	drop dup
	
	tempfile MatchGroup`x'
	sa `MatchGroup`x''
	di "`x'"
	clear
}

** Append all of the match group datasets together
use `MatchGroup1'
forvalues y=2/3750 {
	capture append using `MatchGroup`y'', gen(append`y')
	di "`y'"
}
drop append*

** Keep only necessary variables, sort, save and clear
	keep hhidpn casecontrol caseid
	so hhidpn casecontrol

		*label each as the first match and assign time -1
		g time = -1
		g first = 1
		sa "$data/first_match_claimsupdate.dta", replace	



********************************************************************************
	*for the summary statistics	
	use "$data/AnalyticFileLongBasuVariables_ss_claims.dta", clear
	keep if time==-1
	cap drop _merge
	merge 1:1 hhidpn casecontrol caseid time using "$data/first_match_claimsupdate.dta"
	
	so hhidpn casecontrol 
	
	bys hhidpn casecontrol: egen medicare_total = sum(total_reimb)
	bys hhidpn casecontrol: egen medicaid_total = sum(totalmax_reimb)	
	
	drop if (_merge==1&casecontrol==0)|time!=-1
	
	*need enrollment indicators - part D, Medicaid
	merge m:1 bid_hrs_22 using "$data/PD_wide.dta", keep(matched master) nogen
	rename hhidpn hhidpn_master
	merge m:1 bid_hrs_22 using "$data/Medicaid_wide.dta", keep(matched master) nogen
	
	*identify month of indexdate and death - eligibility in month of diagnosis, and death
	format deathmthyear %td	
	
	g medicaid_eligible_mth=0
		*(1-((1999-1960)*12))
		replace medicaid_eligible_mth=1 if substr(medicaid_elig,indexmthyear-467,1)=="1"
		replace medicaid_eligible_mth=. if substr(medicaid_elig,indexmthyear-467,1)=="."
		*missing if diagnosed before 1/1999 ((1999-1960)*12)
		replace medicaid_eligible_mth=. if indexmthyear<468
		*missing if diagnosed after 12/2012 (((2012-1960)*12)+11)
		replace medicaid_eligible_mth=. if indexmthyear>635

	g medicaid_eligible_dth=0
		replace medicaid_eligible_dth=1 if substr(medicaid_elig,deathmthyear-467,1)=="1"
		replace medicaid_eligible_dth=. if deathmthyear<468
		replace medicaid_eligible_dth=. if deathmthyear>635

			
	g partd_eligible_mth=0
		replace partd_eligible_mth=1 if substr(partd_elig,indexmthyear-551,1)=="1"
		*missing if diagnosed before 1/2006
		replace partd_eligible_mth=. if indexmthyear<552
		*missing if diagnosed after 12/2015
		replace partd_eligible_mth=. if indexmthyear>671

	g partd_eligible_dth=0
		replace partd_eligible_dth=1 if substr(partd_elig,deathmthyear-551,1)=="1"
		replace partd_eligible_dth=. if deathmthyear<552
		replace partd_eligible_dth=. if deathmthyear>671
	
	ta race, g(race_)
	ta maritalstatus, g(maritalstatus_)
	ta education, g(education_)
	
	*number that died, for table notes
	recode timeofdeath (.=0) (else=1), g(died)
	ta casecontrol died	
	
*apply variable labels	
	label var ageatindex "Age at diagnosis in years, mean (sd)"
	label var male "Male, %"
	
	label var race_1 "Non-Hispanic White"
	label var race_2 "Non-Hispanic Black"
	label var race_3 "Hispanic"
	label var race_4 "Non-Hispanic other"

	label var maritalstatus "Marital status at diagnosis, %"
	label var maritalstatus_1 "Married"
	label var maritalstatus_2 "Separated/divorced"
	label var maritalstatus_3 "Widowed"
	label var maritalstatus_4 "Never married"
	label var maritalstatus_5 "Unknown marital status"
	
	label var education "Educational attainment, %"
	label var education_1 "Less than high school"
	label var education_2 "High school graduate"
	label var education_3 "Some college"
	label var education_4 "College and above"
	label var ravetrn "Veteran, %"
	
	label var anemia_final "Anemia"
	label var arthritis "Arthritis"	
	label var atrialfib "Atrial fibrillation"
	label var cancer "Cancer"	
	label var renaldisease "Chronic kidney disease"
	label var copd_final "COPD"
	label var depression_final "Depression"
	label var diabetes_final "Diabetes"
	label var heartfailure "Heart failure"
	label var hyperlipidemia "Hyperlipidemia"
	label var hypertension "Hypertension"
	label var heartdisease "Ischemic heart disease"
	label var stroke "Stroke/TIA"
	label var baselinetotal_reimb "Total Medicare reimbursement, mean (sd)"
	
	label var partd_eligible_mth "Medicare Part D, %"
	label var medicaid_eligible_mth "Medicaid, %"
	label var partd_eligible_dth "Medicare Part D, %"
	label var medicaid_eligible_dth "Medicaid, %"
	
	*death counts for table notes
	ta died casecontrol, miss
	
	
	loc sum_vars ageatindex male race race_* maritalstatus maritalstatus_* education education_* ravetrn anemia_final arthritis atrialfib cancer renaldisease copd_final depression_final diabetes_final heartfailure hyperlipidemia hypertension heartdisease stroke baselinetotal_reimb partd_eligible_mth medicaid_eligible_mth partd_eligible_dth medicaid_eligible_dth
	
	estpost su `sum_vars' if casecontrol==1
	est sto A		
	estpost su `sum_vars' if casecontrol==0
	est sto B
	
	ttest ageatindex, by(casecontrol)
		replace ageatindex = r(p)
	foreach y in male race maritalstatus education ravetrn anemia_final arthritis atrialfib cancer renaldisease copd_final depression_final diabetes_final heartfailure hyperlipidemia hypertension heartdisease stroke {
	di "`y'"
	tab `y' casecontrol, col chi2
	replace `y' = r(p)
	}	
	
	ttest baselinetotal_reimb, by(casecontrol)
		replace baselinetotal_reimb = r(p)	
	foreach z in partd_eligible_mth medicaid_eligible_mth partd_eligible_dth medicaid_eligible_dth {
	di "`z'"
	tab `z' casecontrol, col chi2
	replace `z' = r(p)
	}	
	tab partd_eligible_mth casecontrol, col chi2
	tab medicaid_eligible_mth casecontrol, col chi2
	tab partd_eligible_dth casecontrol, col chi2
	tab medicaid_eligible_dth casecontrol, col chi2
	
	estpost su `sum_vars'
	est sto C

	esttab A B C using "$output/Public_Costs_SS.rtf", ///
	ti("Table 1. Characteristics of dementia cases and the first matched control") ///
	mtitles("Participants with dementia diagnosis (N=4,073)" "First matched control (N=4,073)" "p-value") ///
	addnote("Notes: *The baseline period was defined as the 12 months prior to the diagnosis date. ** Among the sub-sample where death is observed in the data. N=XXXX and XXX in dementia and first matched control cohorts, respectively.") ///
	cells("mean(fmt(a3))") l replace




*graph for figure 1
use "$data/AnalyticFileLongBasuVariables_ss_claims.dta", clear
	collapse (mean) meancarereimb=total_reimb (mean) meanmaxreimb=totalmax_reimb, by(casecontrol time)

	line meancarereimb time if casecontrol==0 || line meancarereimb time if casecontrol==1 || line meanmaxreimb time if casecontrol==0 || line meanmaxreimb time if casecontrol==1, title("Figure 1. Unadjusted mean FFS Medicare and Medicaid expenditures" "for participants with and without dementia diagnosis", size(medsmall) color(black) margin(medium)) ytitle("Average monthly expenditures", size(small) margin(large)) xtitle("Time (months)", size(small)) lwidth(medthick medthick medthick medthick) legend(position(3) cols(1) region(lcolor(white)) label(1 "Medicare - Controls") label(2 "Medicare - Dementia") label(3 "Medicaid - Controls") label(4 "Medicaid - Dementia") size(vsmall)) ylabel(, labsize(vsmall)) xlabel(-12(12)60, labsize(vsmall)) graphregion(color(white)) sort

graph save "Graph" "/project/coe_costofalz/Oney/Public_Costs/output/Figure1.gph", replace
clear


