

*********************************************************************
*		This file runs estimation for Public Costs project			*
*********************************************************************

clear all

gl y "total_reimb totalmax_reimb medicare_nhhhcare_reimb inpt_reimb pde_reimb medicaid_nhhhcare_reimb maxip_reimb maxrx_reimb total_reimb_dxduringstay"
loc table1 "Medicare_T2"
loc format "main(mean) ti("Table 2. Period-specific absolute and incremental costs to FFS Medicare and Medicaid - FFS Medicare") addnote("Notes: * Month 1 is the month in which the diagnosis date occurred")"


foreach outcome in $y {	

set obs 1
gen simul=.
sa "$data/bootdata_`outcome'.dta", replace

forv iter = 1(1)1000 {

clear all
eststo clear

use "$data/Medicare.dta", clear

bsample, cluster(idnew) idcluster(freshid)
sort freshid time

rename stroke rstroke 
global xlist "casecontrol ageatindex male anemia_final renaldisease copd_final depression_final diabetes_final hypertension arthritis rstroke combinedheart notmarried unknownmarital i.basereimbquart anycollege black hispanic otherrace"


/* Note that for Anirban's code: cost outcomes are zero for all intervals after death
cost outcomes can be zero or missing for all intervals after censoring */
replace `outcome'=0 if (ineligibledeathcensored==2 & time>timeofdeath) 

** Changing outcome to binary
recode `outcome' (0=0) (.=.) (else=1), g(ind)


* BASU and MANNING estimation

cap preserve

********************************************************************************
** First part of two part model

logit ind $xlist casetime time duration yfromdind2 yfromdind3 yfromdind4 yfromdind5 ///
yfromdtime2 yfromdtime3 yfromdtime4 yfromdtime5 if obs==1, cluster(freshid)

if _rc==0 & e(converged)==1 {
mat b=e(b)

restore

** PREDICT FOR ALL PATIENT TIME INTERVALS as If patient was alive during the entirity of the interval
cap drop p2* 
cap drop xb

replace duration=30 if time==deathtime | time==centime | duration==0
predict p2,p

	replace casecontrol=0
	replace casetime=casecontrol*time
	mat score xb=b
	gen p2_0 = exp(xb)/(1+ exp(xb))
	drop xb
	
	replace casecontrol=1
	replace casetime=casecontrol*time
	mat score xb=b
	gen p2_1 = exp(xb)/(1+ exp(xb))
	drop xb
	
replace duration=durold
replace casecontrol=casecontrolold
replace casetime=casecontrol*time

cap preserve

** SECOND PART OF TWO PART MODEL

*keep if fullobs==1 & `outcome' >0 & `outcome' !=. 
keep if obs==1 & `outcome' >0 & `outcome' !=. 
cap compress
glm `outcome' $xlist time casetime duration yfromdind2 yfromdind3 yfromdind4 yfromdind5 ///
yfromdtime2 yfromdtime3 yfromdtime4 yfromdtime5, cluster(freshid) link(log) family(gamma) iterate(50) difficult

mat bg=e(b)
capture restore


** PREDICT FOR ALL PATIENT TIME INTERVALS as If patient was alive during the entirity of the interval

replace duration=30 if time==deathtime | time==centime | duration==0

	replace casecontrol=0
	replace casetime=casecontrol*time
	mat score xb=bg
	gen mu2_0 = exp(xb)
	drop xb

	replace casecontrol=1
	replace casetime=casecontrol*time
	mat score xb=bg
	gen mu2_1 = exp(xb)
	drop xb

replace duration=durold
replace casecontrol=casecontrolold
replace casetime=casecontrol*time


// full predictions for E(Y| No Death)
forv i=0/1 {
capture gen mu2p_`i' = p2_`i'*mu2_`i'
}


********************************************************************************
// MODELING COSTS IN INTERVALS OF OBSERVED DEATH

capture drop mu1 mu1_pred

cap preserve
keep if death==1 & time==deathtime 
cap compress

capture logit ind $xlist casetime time duration yfromdind2 yfromdind3 yfromdind4 yfromdind5 yfromdtime2 yfromdtime3 yfromdtime4 yfromdtime5, cluster(freshid)

if _rc==0 & e(converged)==1 {
mat bg = e(b)
restore

 
// Make predictions after integrating out time of death within an interval
proportion duration if time==deathtime   
mat prop=e(b)

replace casecontrol=0
replace casetime=casecontrol*time
gen pu1p_0=0
local k=1

forv i=1(1)31 {		
	replace duration=`i'
	mat score xbg = bg
		g tg = invlogit(xbg) 
		gen prod = prop[1,`k']*tg
		replace pu1p_0 = pu1p_0+prod
	drop tg prod xbg
local k = `k' + 1
}

replace casecontrol=1
replace casetime=casecontrol*time
gen pu1p_1=0
local k=1

forv i=1(1)31 {
	replace duration=`i'
	mat score xbg = bg
		g tg = invlogit(xbg) 
		gen prod = prop[1,`k']*tg
		replace pu1p_1 = pu1p_1+prod
	drop tg prod xbg
local k = `k' + 1
}

replace duration=durold
replace casecontrol=casecontrolold
replace casetime=casecontrol*time
}

cap preserve 

/*SECOND PART OF 2 PART MODEL*/
keep if `outcome' >0 & `outcome' !=. 
keep if death==1 & time==deathtime 
cap compress

glm `outcome' $xlist duration time casetime yfromdind2 yfromdind3 yfromdind4 yfromdind5 ///
yfromdtime2 yfromdtime3 yfromdtime4 yfromdtime5, link(log) family(gamma) robust iterate(50) difficult

mat bg = e(b)
capture restore
 
// Make predictions after integrating out time of death within an interval
proportion duration if time==deathtime   
mat prop=e(b)

replace casecontrol=0
replace casetime=casecontrol*time
g mu1_0=0

loc k=1
forv i=1(1)31 {		
	replace duration=`i'
	mat score xbg = bg
		g tg = invlogit(xbg) 
		gen prod = prop[1,`k']*tg
		replace mu1_0 = mu1_0+prod
	drop tg prod xbg
local k = `k' + 1
}

replace casecontrol=1
replace casetime=casecontrol*time
g mu1_1=0

loc k=1
forv i=1(1)31 {
	replace duration=`i'
	mat score xbg = bg
		g tg = invlogit(xbg) 
		gen prod = prop[1,`k']*tg
		replace mu1_1 = mu1_1+prod
	drop tg prod xbg
local k = `k' + 1
}


// full predictions for E(Y| No Death)
	forv i=0/1 {
	cap g mu1p_`i' = pu1p_`i'*mu1_`i'
	}

replace duration=durold
replace casecontrol=casecontrolold
replace casetime=casecontrol*time


********************************************************************************
// SURVIVAL ESTIMATORS

	capture drop _*
	stset time if obs2==1
	streset if obs2==1, id(freshid) failure(death==1) 

	capture drop s0
	capture drop st
	cap compress
	streg $xlist if obs2==1, cluster(freshid) dist(lognormal) time iterate(50)
	
	replace _st=1 if obs2==0
	replace _d=0 if obs2==0
	replace _t=time if obs2==0
	replace _t0=time-1 if obs2==0
	
	predict surv, surv 
	predict csurv, csurv oos
	gen haz=1-surv
	
	replace casecontrol=0
	predict surv0, surv 
	predict csurv0, csurv oos
	gen haz0=1-surv0
	
	replace casecontrol=1
	predict surv1, surv 
	predict csurv1, csurv oos
	gen haz1=1-surv1
	
	replace casecontrol=casecontrolold
	

// OVERALL PREDICTIONS

forv i =0/1 {
 capture gen mu_`i' = csurv`i'*(haz`i'*mu1p_`i' + (1-haz`i')*mu2p_`i')
}


if "`outcome'"=="totalmax_reimb" {
recode medicaid_month (.=0)
}

cap gen ie10_survcons = csurv1*(haz1*mu1p_1 + (1-haz1)*mu2p_1) - csurv1*(haz0*mu1p_0 + (1-haz0)*mu2p_0)
cap gen ie10_costcons = csurv1*(haz0*mu1p_0 + (1-haz0)*mu2p_0) - csurv0*(haz0*mu1p_0 + (1-haz0)*mu2p_0)

keep if casecontrol==1

if "`outcome'"=="totalmax_reimb" {
cap collapse (mean) `outcome' csurv* haz* mu* ie* medicaid_month, by(time)
}
else {
cap collapse (mean) `outcome' csurv* haz* mu* ie*, by(time)
}

g simul = `iter'

append using "$data/bootdata_`outcome'.dta"
save "$data/bootdata_`outcome'.dta", replace
noi di `iter'

}  /* end 2nd logit */

	else {
		clear
	}
} /* end 1st logit */
clear	
	else {
		clear
	}	
}
	
	
********************************************************************************
* Process the results into tables
********************************************************************************
	
clear all

foreach outcome in $y {	
	
use "$data/bootdata_`outcome'.dta", clear

*this is necessary when some iterations don't converge - confirm 97% convergence in Medicare
drop if ie10_survcons==. | ie10_costcons==.


g incrementalcost=ie10_survcons+ie10_costcons
g incrementalcostalive=mu2p_1-mu2p_0

recode time (1/12=1) (13/24=2) (25/36=3) (37/48=4) (49/60=5) (else=0), g(year)

if "`outcome'"=="totalmax_reimb"  {
	g incrementalcostpea=incrementalcostalive/(medicaid_month/csurv1)
	bysort year simul: egen cumincrementalcostpea=total(incrementalcostpea)
	bysort simul: egen cumincrementalcostpea_total=total(incrementalcostpea)
}

*period-specific estimates
	bysort year simul: egen cummu0=total(mu_0)
	bysort year simul: egen cummu1=total(mu_1)
	bysort year simul: egen cumsurvcons=total(ie10_survcons)
	bysort year simul: egen cumcostcons=total(ie10_costcons)
	bysort year simul: egen cumincrementalcost=total(incrementalcost)
	bysort year simul: egen cumincrementalcostalive=total(incrementalcostalive)

*5-year totals
	bysort simul: egen cummu0_total=total(mu_0)
	bysort simul: egen cummu1_total=total(mu_1)
	bysort simul: egen cumsurvcons_total=total(ie10_survcons)
	bysort simul: egen cumcostcons_total=total(ie10_costcons)
	bysort simul: egen cumincrementalcost_total=total(incrementalcost)
	bysort simul: egen cumincrementalcostalive_total=total(incrementalcostalive)

loc list cummu1 cummu0 cumsurvcons cumcostcons cumincrementalcost cumincrementalcostalive cumincrementalcostpea
loc count 1 2 3 4 5
	foreach u in `list' {
	    g `u'_lb=.
	    g `u'_ub=.
		foreach w in `count' {
			cap _pctile `u' if year==`w', p(2.5, 97.5)
			return list
			cap replace `u'_lb = r(r1) if year==`w'
			cap replace `u'_ub = r(r2) if year==`w'
		}
	}

loc list cummu1_total cummu0_total cumsurvcons_total cumcostcons_total cumincrementalcost_total cumincrementalcostalive_total cumincrementalcostpea_total
	foreach v in `list' {
	    g `v'_lb=.
	    g `v'_ub=.
			cap _pctile `v', p(2.5, 97.5)
			return list
			cap replace `v'_lb = r(r1)
			cap replace `v'_ub = r(r2)
		}
	
*collapse and concatenate to get final formatting ready
collapse (mean) cum*, by(year)


rename cumincrementalcostalive_total cuminccostalive_total
rename cumincrementalcostalive_total_lb cuminccostalive_total_lb
rename cumincrementalcostalive_total_ub cuminccostalive_total_ub

cap rename cumincrementalcostpea_total cuminccostpea_total 
cap rename cumincrementalcostpea_total_lb cuminccostpea_total_lb
cap rename cumincrementalcostpea_total_ub cuminccostpea_total_ub


loc list cummu1 cummu0 cumsurvcons cumcostcons cumincrementalcost cumincrementalcostalive cumincrementalcostpea cummu1_total cummu0_total cumsurvcons_total cumcostcons_total cumincrementalcost_total cuminccostalive_total cuminccostpea_total
foreach var in `list' {	

	cap g `var'_st = string(`var', "%3.0f")	
	cap g `var'_lb_st = string(`var'_lb, "%3.0f")	
	cap g `var'_ub_st = string(`var'_ub, "%3.0f")	
		cap g `var'_final = "$" + `var'_st + "" + "(" + `var'_lb_st + ";" + "" + `var'_ub_st + ")"	
		
}	


keep year *final

tempfile orig
sa `orig'
	keep *total*
	keep if _n==1
	rename *_total* **
	cap rename cuminccostalive_final cumincrementalcostalive_final
	cap rename cuminccostpea_total cumincrementalcostpea_total
append using `orig'

drop *_total* 
replace year=6 if year==.
order year 
so year

*export data as a table to excel 
export excel using "$data/exp_`outcome'.xlsx", firstrow(variables) replace

clear all

cap log close
}



