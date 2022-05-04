
*********************************************************************
*		This file cleans/merges data for Public Costs project		*
*********************************************************************


clear all


********************************************************************************

 	****************
	* 1. RAND data *
 	****************
		
** Start with RAND dataset to get some necessary variables
use pn *hhid* raspct raspid1 raspid2 raspid3 raspid4 *gender *bmonth *byear ///
	inw* *iwstat r*mstat *shlt *adla *iadla *iadlza hacohort ///
	racohbyr *racem *hispan ravetrn *educ s*vetrn using "$data/randhrs1992_2016v1.dta", clear
	
	so hhidpn
sa "$data/CasesAndControls_RAND.dta", replace
clear

*merge in tracker data for FIRST wave
	use "$data/trk2018tr_r.dta", clear
	destring hhidpn, replace
	merge 1:1 hhidpn using 	"$data/CasesAndControls_RAND.dta", keep(matched) nogen
sa "$data/CasesAndControls_RAND.dta", replace
clear

********************************************************************************

	****************
	* 2. BASF data *
	****************
	
use "$datasource/basf_1992_2015.dta", clear
rename BID_HRS_22 bid_hrs_22
rename START_DT start_dt
rename END_DT end_dttracker
rename ALZH_DEMEN alzh_demen
rename ALZH_EVER alzh_ever
rename ALZH_DEMEN_EVER alzh_demen_ever
rename A_MO_CNT a_mo_cnt
rename B_MO_CNT b_mo_cnt
rename HMO_MO hmo_mo
rename BUYIN_MO buyin_mo

keep bid_hrs_22 start_dt end_dt alzh alzh_demen alzh_ever alzh_demen_ever a_mo_cnt b_mo_cnt hmo_mo buyin_mo STATE_CD
	drop if STATE_CD==""|STATE_CD>="54"
	rename alzh_ever firstalzhdate
	rename alzh_demen_ever firstdemendate
	
** Need to get BASF data down to the beneficiary level - so go wide
	g year=year(start_dt)
	drop start_dt end_dt
tempfile BASF1992_2015Long
sa `BASF1992_2015Long', replace

forval x = 1992 (1) 2015 {
	use `BASF1992_2015Long'
	keep if year==`x'
		rename alzh alzh`x'
		rename alzh_demen alzh_demen`x'
		rename firstalzhdate firstalzhdate`x'
		rename firstdemendate firstdemendate`x'
		rename a_mo_cnt a_mo_cnt`x'
		rename b_mo_cnt b_mo_cnt`x'
		rename hmo_mo hmo_mo`x'
		rename buyin_mo buyin_mo`x'
	drop year
	sort bid_hrs_22
	tempfile BASF`x'
	sa `BASF`x''
	clear
}
use `BASF1992', clear
	forval y = 1993 (1) 2015 {
		merge 1:1 bid_hrs_22 using `BASF`y'', gen(merge`y')
	}
	drop merge*
	so bid_hrs_22
sa "$data/BASF1992_2015Wide.dta", replace
clear

********************************************************************************

	***************************************
	* 3. Merge RAND, BASF & Medicare XRef *
	***************************************
	
** Extract smaller dataset from full Medicare link file with only needed variables and save as a new dataset 
use hhidpn BID_HRS_22 using "/origdata/Coe_HRS/Medicare/xref2015medicare.dta", clear

** Medicare dataset has a string hhidpn. In order to link to the RAND file, where hhidpn is numeric, need to destring
	rename hhidpn hhidpn_string
	destring hhidpn_string, g(hhidpn)
	format hhidpn %12.0g
	so hhidpn

	merge 1:1 hhidpn using "$data/CasesAndControls_RAND.dta", keep(match master) nogen
	
** Now merge the RAND file with the BASF file
	rename BID_HRS_22 bid_hrs_22
	so bid_hrs_22
	merge 1:1 bid_hrs_22 using "$data/BASF1992_2015Wide.dta", nogen

** Identify the group of people with a dementia dx somewhere in the BASF file between 1991 and 2015
	g dementiacase  = 0
		replace dementiacase = 1 if inlist(alzh_demen1992, 1, 3) | inlist(alzh_demen1993, 1, 3) ///
		| inlist(alzh_demen1994, 1, 3) | inlist(alzh_demen1995, 1, 3) | inlist(alzh_demen1996, 1, 3) ///
		| inlist(alzh_demen1997, 1, 3) | inlist(alzh_demen1998, 1, 3) | inlist(alzh_demen1999, 1, 3) ///
		| inlist(alzh_demen2000, 1, 3) | inlist(alzh_demen2001, 1, 3) | inlist(alzh_demen2002, 1, 3) ///
		| inlist(alzh_demen2003, 1, 3) | inlist(alzh_demen2004, 1, 3) | inlist(alzh_demen2005, 1, 3) ///
		| inlist(alzh_demen2006, 1, 3) | inlist(alzh_demen2007, 1, 3) | inlist(alzh_demen2008, 1, 3) ///
		| inlist(alzh_demen2009, 1, 3) | inlist(alzh_demen2010, 1, 3) | inlist(alzh_demen2011, 1, 3) ///
		| inlist(alzh_demen2012, 1, 3) | inlist(alzh_demen2013, 1, 3) | inlist(alzh_demen2014, 1, 3) ///
		| inlist(alzh_demen2015, 1, 3) ///
		| inlist(alzh1992, 1, 3) | inlist(alzh1993, 1, 3) | inlist(alzh1994, 1, 3) ///
		| inlist(alzh1995, 1, 3) | inlist(alzh1996, 1, 3) | inlist(alzh1997, 1, 3) ///
		| inlist(alzh1998, 1, 3) | inlist(alzh1999, 1, 3) | inlist(alzh2000, 1, 3) ///
		| inlist(alzh2001, 1, 3) | inlist(alzh2002, 1, 3) | inlist(alzh2003, 1, 3) ///
		| inlist(alzh2004, 1, 3) | inlist(alzh2005, 1, 3) | inlist(alzh2006, 1, 3) ///
		| inlist(alzh2007, 1, 3) | inlist(alzh2008, 1, 3) | inlist(alzh2009, 1, 3) ///
		| inlist(alzh2010, 1, 3) | inlist(alzh2011, 1, 3) | inlist(alzh2012, 1, 3) ///
		| inlist(alzh2013, 1, 3) | inlist(alzh2014, 1, 3) | inlist(alzh2015, 1, 3)

	** Create a dementia indicator variable for ever diagnosed
	g dementia=dementiacase 
	
** Now, identify all household members of dementia cases						
	duplicates tag hhid, g(multiplehh)
	bysort hhid: egen meanhhcase=mean(dementiacase)
	g hhmemberiscase=0
		replace hhmemberiscase=1 if (dementiacase==0 & multiplehh>0 & meanhhcase>0)
		replace hhmemberiscase=1 if (dementiacase==1 & multiplehh>0 & meanhhcase==1)
		replace hhmemberiscase=1 if (dementiacase==1 & multiplehh>0 & (meanhhcase>0.39 & meanhhcase<0.5))
		replace hhmemberiscase=1 if (dementiacase==1 & multiplehh>0 & (meanhhcase>0.5 & meanhhcase<1))
		replace hhmemberiscase=1 if (dementiacase==1 & multiplehh==3 & meanhhcase==0.5)

** Determine index date for cases - date on which person first meets clinical criteria for having dementia - first diagnosis code
	egen indexdate=rowmin(firstalzhdate* firstdemendate*) if dementiacase==1
		format indexdate %td
	
** Determine diagnosis date of household members who are cases - if in a household with multiple cases, take earliest date
	so hhid indexdate
	by hhid: gen firstdate=indexdate[1]
	g dxdatehhmember=firstdate if hhmemberiscase==1
		format dxdatehhmember %td
	drop firstdate
sa "$data/CasesAndControls.dta", replace


********************************************************************************

	********************************************
	* 4. Create Monthly Eligibility Indicators *
	********************************************

****************
* PDE datasets *
****************
*Need to know proportion covered by Part D at baseline
	forval x = 2006/2015 {
	use "$datasource/pde_`x'.dta", clear	
	rename BID_HRS_22 bid_hrs_22 
	rename RX_DOS_DT rx_dos_dt
	keep bid_hrs_22 rx_dos_dt
	g month = mofd(rx_dos_dt)
	
		so bid_hrs_22 month
		bys bid_hrs_22 month: g dup = cond(_N==1, 0, _n)		
		drop if dup>1
		drop dup
		
	if `x'==2006 {
	replace month = month-551
	}	
	if `x'==2007 {
	replace month = month-563
	}	
	if `x'==2008 {
	replace month = month-575
	}	
	if `x'==2009 {
	replace month = month-587
	}	
	if `x'==2010 {
	replace month = month-599
	}	
	if `x'==2011 {
	replace month = month-611
	}	
	if `x'==2012 {
	replace month = month-623
	}	
	if `x'==2013 {
	replace month = month-635
	}	
	if `x'==2014 {
	replace month = month-647
	}	
	if `x'==2015 {
	replace month = month-659
	}	
		
	g jan = month==1
	g feb = month==2
	g mar = month==3
	g apr = month==4
	g may = month==5
	g jun = month==6
	g jul = month==7
	g aug = month==8
	g sep = month==9
	g oct = month==10
	g nov = month==11
	g dec = month==12
	
	bys bid_hrs_22: egen jan`x'=max(jan)
	bys bid_hrs_22: egen feb`x'=max(feb)
	bys bid_hrs_22: egen mar`x'=max(mar)
	bys bid_hrs_22: egen apr`x'=max(apr)
	bys bid_hrs_22: egen may`x'=max(may)
	bys bid_hrs_22: egen jun`x'=max(jun)
	bys bid_hrs_22: egen jul`x'=max(jul)
	bys bid_hrs_22: egen aug`x'=max(aug)
	bys bid_hrs_22: egen sep`x'=max(sep)
	bys bid_hrs_22: egen oct`x'=max(oct)
	bys bid_hrs_22: egen nov`x'=max(nov)
	bys bid_hrs_22: egen dec`x'=max(dec)
			
		so bid_hrs_22
		bys bid_hrs_22: g dup = cond(_N==1, 0, _n)		
		drop if dup>1
		drop dup
		
	tempfile PDE`x'
	sa `PDE`x''
	clear
	
	}
	
use `PDE2006'
	forval z = 2007/2015 {
	merge 1:1 bid_hrs_22 using `PDE`z'', nogen
	}
	
	egen c1 = rowmax(jan2006 feb2006 mar2006)
	egen c2 = rowmax(jan2006 feb2006 mar2006)
	egen c3 = rowmax(feb2006 mar2006 apr2006)
	egen c4 = rowmax(mar2006 apr2006 may2006)
	egen c5 = rowmax(apr2006 may2006 jun2006)
	egen c6 = rowmax(may2006 jun2006 jul2006)
	egen c7 = rowmax(jun2006 jul2006 aug2006)
	egen c8 = rowmax(jul2006 aug2006 sep2006)
	egen c9 = rowmax(aug2006 sep2006 oct2006)
	egen c10 = rowmax(sep2006 oct2006 nov2006)
	egen c11 = rowmax(oct2006 nov2006 dec2006)
	egen c12 = rowmax(nov2006 dec2006 jan2007)
		
loc n = 13
forval x = 2006/2013 {	
loc y = `x'+1
loc z = `x'+2
	egen c`n' = rowmax(dec`x' jan`y' feb`y')
		loc n = `n'+1
	egen c`n' = rowmax(jan`y' feb`y' mar`y')
		loc n = `n'+1
	egen c`n' = rowmax(feb`y' mar`y' apr`y')
		loc n = `n'+1
	egen c`n' = rowmax(mar`y' apr`y' may`y')
		loc n = `n'+1
	egen c`n' = rowmax(apr`y' may`y' jun`y')
		loc n = `n'+1
	egen c`n' = rowmax(may`y' jun`y' jul`y')
		loc n = `n'+1
	egen c`n' = rowmax(jun`y' jul`y' aug`y')
		loc n = `n'+1
	egen c`n' = rowmax(jul`y' aug`y' sep`y')
		loc n = `n'+1
	egen c`n' = rowmax(aug`y' sep`y' oct`y')
		loc n = `n'+1
	egen c`n' = rowmax(sep`y' oct`y' nov`y')
		loc n = `n'+1
	egen c`n' = rowmax(oct`y' nov`y' dec`y')
		loc n = `n'+1
	egen c`n' = rowmax(nov`y' dec`y' jan`z')
		loc n = `n'+1	
}
	egen c109 = rowmax(dec2014 jan2015 feb2015)
	egen c110 = rowmax(jan2015 feb2015 mar2015)
	egen c111 = rowmax(feb2015 mar2015 apr2015)
	egen c112 = rowmax(mar2015 apr2015 may2015)
	egen c113 = rowmax(apr2015 may2015 jun2015)
	egen c114 = rowmax(may2015 jun2015 jul2015)
	egen c115 = rowmax(jun2015 jul2015 aug2015)
	egen c116 = rowmax(jul2015 aug2015 sep2015)
	egen c117 = rowmax(aug2015 sep2015 oct2015)
	egen c118 = rowmax(sep2015 oct2015 nov2015)
	egen c119 = rowmax(oct2015 nov2015 dec2015)
	egen c120 = rowmax(oct2015 nov2015 dec2015)
	
forval z = 1/120 {
replace c`z' = 0 if c`z'==.
}	
		egen partd_elig=concat(c1-c120)	
	
	keep bid* partd_elig c*
	
sa "$data/PD_wide.dta", replace


*******************
* Non PS datasets *
*******************

*Need to know proportion covered by Medicaid at baseline  - use DN & MBSF for buyin variables
*DN files
	forval x = 1992/2012 {
	use "$datasource/dn_`x'.dta", clear
	rename BID_HRS_22 bid_hrs_22
	rename BUYIN_MO buyin_mo
	rename BUYIN12 buyin12
	keep bid_hrs_22 buyin12 buyin_mo


	  forval y = 1/12 {
	  g buyin_`y' = substr(buyin12, `y', 1)
	  g ind`y' = 1 if buyin_`y'=="A" | buyin_`y'=="B" | buyin_`y'=="C"
	  replace ind`y' = 0 if ind`y'==.
	  }
	egen buyin_`x' = concat(ind1 ind2 ind3 ind4 ind5 ind6 ind7 ind8 ind9 ind10 ind11 ind12)
	
		rename buyin_mo count`x'
		
		bys bid_hrs_22: g dup = cond(_N==1, 0, _n)
		drop if dup>1
		
	keep bid_hrs_22 buyin_`x' count`x'	

	tempfile Denominator`x'
	sa `Denominator`x''
	clear
	}
*MBSF files
	forval y = 2013/2015 {
	use "$datasource/mbsf_`y'.dta", clear
	rename BID_HRS_22 bid_hrs_22
	rename DUAL_ELGBL_MONS dual_elgbl_mons
		foreach x in 01 02 03 04 05 06 07 08 09 10 11 12 {	
		rename MDCR_ENTLMT_BUYIN_IND_`x' mdcr_entlmt_buyin_ind_`x'
		}
	
	keep bid_hrs_22 mdcr_entlmt_buyin_ind_* dual_elgbl_mons

		foreach x in 01 02 03 04 05 06 07 08 09 10 11 12 {
		g buyin_`x' = 1 if mdcr_entlmt_buyin_ind_`x'=="A" | mdcr_entlmt_buyin_ind_`x'=="B" | mdcr_entlmt_buyin_ind_`x'=="C"
		replace buyin_`x' = 0 if buyin_`x'==.		
		}
		egen buyin_`y' = concat(buyin_01 buyin_02 buyin_03 buyin_04 buyin_05 buyin_06 buyin_07 buyin_08 buyin_09 buyin_10 buyin_11 buyin_12)
		
		rename dual_elgbl_mons count`y'

		bys bid_hrs_22: g dup = cond(_N==1, 0, _n)
		drop if dup>1
		
	keep bid_hrs_22 buyin_`y' count`y'	

	tempfile Denominator`y'
	sa `Denominator`y''
	clear
	}
	
	use `Denominator1992'
	forval z = 1993/2015 {
	merge 1:1 bid_hrs_22 using `Denominator`z'', gen(merge`z')
	}
	drop merge*
	forval x = 1992/2015 {
	replace buyin_`x' = "000000000000" if buyin_`x'==""
	}
		
	keep bid_hrs_22 buyin_*
	order bid_hrs_22 buyin_*
egen medicaid_elig = concat(buyin_1992-buyin_2015)
	
sa "$data/Medicaid_wide.dta", replace



********************************************************************************
/*  Next, we need to determine whether each of the cases has the necessary amount 
of Medicare data (1 year prior to index date and 1 month post index date) */

*DN files
	forval x = 1992/2012 {
	use "$datasource/dn_`x'.dta", clear
	
	rename BID_HRS_22 bid_hrs_22 
	rename A_MO_CNT a_mo_cnt 
	rename B_MO_CNT b_mo_cnt 
	rename HMO_MO hmo_mo 
	rename BUYIN12 buyin12 
	rename HMOIND12 hmoind12
	
	keep bid_hrs_22 a_mo_cnt b_mo_cnt hmo_mo buyin12 hmoind12
		foreach var of varlist a_mo_cnt-hmoind12 {
		   rename `var' `var'`x'
		}
		bys bid_hrs_22: g dup = cond(_N==1, 0, _n)
		drop if dup>1
		drop dup
	tempfile Denominator`x'
	sa `Denominator`x''
	clear
	}
			
*MBSF files
	forval y = 2013/2015 {
	use "$datasource/mbsf_`y'.dta", clear
	
	rename BID_HRS_22 bid_hrs_22 
	rename BENE_HI_CVRAGE_TOT_MONS bene_hi_cvrage_tot_mons 
	rename BENE_SMI_CVRAGE_TOT_MONS bene_smi_cvrage_tot_mons 
	rename BENE_HMO_CVRAGE_TOT_MONS bene_hmo_cvrage_tot_mons 
	rename PTD_PLAN_CVRG_MONS ptd_plan_cvrg_mons
	foreach val in 01 02 03 04 05 06 07 08 09 10 11 12 {
		rename MDCR_ENTLMT_BUYIN_IND_`val' mdcr_entlmt_buyin_ind_`val'
		rename HMO_IND_`val' hmo_ind_`val'
		rename PTD_SGMT_ID_`val' ptd_sgmt_id_`val'
	}

	keep bid_hrs_22 bene_hi_cvrage_tot_mons bene_smi_cvrage_tot_mons bene_hmo_cvrage_tot_mons mdcr_entlmt_buyin_ind_* hmo_ind_* ptd_plan_cvrg_mons ptd_sgmt_id_* 
	order _all, alpha
		rename bene_hi_cvrage_tot_mons a_mo_cnt`y'
		rename bene_smi_cvrage_tot_mons b_mo_cnt`y'
		rename bene_hmo_cvrage_tot_mons hmo_mo`y'
		egen buyin12`y'=concat(mdcr_entlmt_buyin_ind_01-mdcr_entlmt_buyin_ind_12)
		egen hmoind12`y'=concat(hmo_ind_01-hmo_ind_12)
		drop mdcr_entlmt_buyin_ind_* hmo_ind_*
		bys bid_hrs_22: g dup = cond(_N==1, 0, _n)
		drop if dup>1
		drop dup
	tempfile Denominator`y'
	sa `Denominator`y''
	clear
	}
	
	use `Denominator1992'
	forval z = 1993/2015 {
	merge 1:1 bid_hrs_22 using `Denominator`z'', gen(merge`z')
	}
	order _all, alpha
	order bid_hrs_22
	drop merge*
		
	** Create monthly eligibility indicators
	loc y 1992 1993 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015 
	loc z jan feb mar apr may jun jul aug sep oct nov dec
	loc w 1 2 3 4 5 6 7 8 9 10 11 12 
	loc n : word count `z'
	foreach year in `y' {
		forval i = 1/`n' {
			loc a : word `i' of `z'
			loc b : word `i' of `w'			
			g elig`a'`year'=0	
			replace elig`a'`year'=1 if (substr(buyin12`year',`b',1)=="3" | substr(buyin12`year',`b',1)=="C") & substr(hmoind12`year',`b',1)=="0"
		}
	}		
		
** Concatenate all of the eligibility indicators together into one long string 
egen allyearseligibility=concat(eligjan1992-eligdec2015)

********************************************************************************

	**************************************
	* 5. Create the CaseControl Variable *
	**************************************

** Save just the concatenated variable and the BID to merge with the combined dataset
	keep bid_hrs_22 allyearseligibility
	so bid_hrs_22
	merge 1:1 bid_hrs_22 using "$data/CasesAndControls.dta", nogen 
sa "$data/CasesAndControls.dta", replace


** Determine which cases have the requisite amount of Medicare data to be eligible						 
g eligiblecase=.
	replace eligiblecase=0 if (dementiacase==1 & (indexdate<mdy(1,1,1993) | indexdate>mdy(11,30,2015)))
	replace eligiblecase=1 if (dementiacase==1 & (indexdate>=mdy(1,1,1993) & indexdate<=mdy(11,30,2015)) & substr(allyearseligibility,(year(indexdate)-1992)*12+month(indexdate)-12,14)=="11111111111111")
	replace eligiblecase=0 if (dementiacase==1 & (indexdate>=mdy(1,1,1993) & indexdate<=mdy(11,30,2015)) & substr(allyearseligibility,(year(indexdate)-1992)*12+month(indexdate)-12,14)!="11111111111111")

** Assign all of the eligible cases to the matching categories based on gender, birth year, interview wave entered on
** Create a variable that indicates the interview wave in which the respondent entered - first wave they were a respondent
recode FIRSTIW (1992=1) (1993/1994=2) (1995/1996=3) (1998=4) (2000=5) (2002=6) (2004=7) (2006=8) (2008=9) (2010=10) (2012=11) (2014=12) (2016=13), g(entry_wave)

g waveentered=.
	forval x = 1/13 {
		replace waveentered=`x' if (missing(waveentered) & inw`x'==1)
	}	
	
** Create a matching group variable for all of the eligible cases and all controls
	drop if ragender==.|rabyear==.|entry_wave==.|raracem==.|raracem==.m|rahispan==.|rahispan==.m
	egen birth = cut(rabyear), at(1890, 1895, 1900, 1905, 1910, 1915, 1920, 1925, 1930, 1935, 1940, 1945, 1950, 1955, 1960, 1965, 1970, 1975, 1980, 1985, 1990, 1995, 2000)
	recode raracem (1=1) (2/3=0) (else=.), g(white)
		replace white=0 if rahispan==1

	egen matchgrp=group(ragender birth entry_wave white STATE_CD) if ((dementiacase==1&eligiblecase==1)|(dementiacase==0|(dementia==1&eligiblecase==0))), label
	drop birth white
	
** Create an indexdate variable in month year format
	g indexmthyear=mofd(indexdate)
	format indexmthyear %tm

** Create a household member dx date in month year format
	g dxhhmembermthyear=mofd(dxdatehhmember)
	format dxhhmembermthyear %tm

		** Create a copy of dataset as eligible cases only 
		sa "$data/CasesAndControls.dta", replace
			keep if dementiacase==1 & eligiblecase==1
		append using "$data/CasesAndControls.dta", gen(dupecase)

		** Generate a case/control indicator variable
		g casecontrol=0
			replace casecontrol=1 if (dementiacase==1 & eligiblecase==1 & dupecase==0)

		** Generate a variable that will hold the assigned hhidpn during random selection of controls
		g long caseid=0 if casecontrol==1
			format caseid %12.0g
		*drop if hhidpn==.	
		sa "$data/CasesAndControls.dta", replace
		
********************************************************************************

	*********************************
	* 6. Run the Matching Procedure *
	*********************************

* Randomly match controls to cases 
forvalues x=1/3750 {
  
use "$data/CasesAndControls.dta", clear
  keep if matchgrp==`x'
  keep hhidpn bid_hrs_22 dementiacase dementia eligiblecase hhmemberiscase casecontrol ///
  matchgrp allyearseligibility indexdate indexmthyear dxdatehhmember ///
  dxhhmembermthyear dupecase ragender rabyear entry_wave caseid
  
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

	tempfile CaseControlMatching
	sa `CaseControlMatching'
	clear

********************************************************************************

	*********************************************************
	* 7. Merge matches with  eligibility and diagnosis data *
	*********************************************************
	
** Merge the list of case control matches onto the full dataset
use "$data/CasesAndControls.dta", clear
	drop caseid
	so hhidpn casecontrol									
merge 1:1 hhidpn casecontrol using `CaseControlMatching', gen(mergeMatches)

** Save dataset as final cases and controls list. Then drop controls that are unmatched and drop cases that were unmatched
	drop if (caseid==.) | (casecontrol==1 & caseid==0)
sa "$data/FinalCasesControls1992-2015.dta", replace


********************************************************************************

	*****************************************************************
	* 8. Merge index date to matches & create Unique/Duplicate files *
	*****************************************************************

	** Index date for controls needs to be filled in with index date from matched case
	keep if casecontrol==1
	keep hhidpn indexdate
	rename hhidpn caseid
	rename indexdate indexdatecontrol
	so caseid
tempfile IndexDates
sa `IndexDates'
clear

use "$data/FinalCasesControls1992-2015.dta", clear
	so caseid
	merge m:1 caseid using `IndexDates', gen(mergeIndexDates)
	replace indexdate=indexdatecontrol if casecontrol==0
	drop if bid_hrs_22==""
sa "$data/FinalCasesControls1992-2015.dta", replace

** Create 2 cases and controls lists to use as a cross-check against all claims 
** First one is a list of cases + non-duplicate controls (should be 13,099 unique people - the duplicates kept should be all the of cases)
	gsort bid_hrs_22 -casecontrol
	duplicates drop bid_hrs_22, force
	keep hhidpn bid_hrs_22 dementiacase eligiblecase casecontrol dupecase indexdate indexdatecontrol
	so bid_hrs_22
sa "$data/UniquePatients.dta", replace
clear

** Next one is a list of controls that are also serving as cases (1,771 duplicates - should all be controls)
use "$data/FinalCasesControls1992-2015.dta", clear
	duplicates tag bid_hrs_22, gen(test)
	keep if (casecontrol==0 & test>0)
	keep hhidpn bid_hrs_22 dementiacase eligiblecase casecontrol dupecase indexdate indexdatecontrol
	so bid_hrs_22
sa "$data/DuplicatePatients.dta", replace
clear

********************************************************************************

	*******************************
	* 9. Create Expenditures File *
	*******************************

************************************
* DM, PB, HH, HS, OP & IP datasets *
************************************
loc files dm pb hh hs op sn ip 
foreach w of loc files {
	forval x = 1992 (1) 2015 {
		use "$datasource/`w'_`x'.dta"
		tempfile `w'`x'
		sa ``w'`x''
	clear
	}
	use ``w'1992'
	forval y = 1993 (1) 2015 {
		append using ``w'`y''
	}

	rename SGMT_NUM sgmt_num
	rename BID_HRS_22 bid_hrs_22
	rename PMT_AMT pmt_amt
	rename THRU_DT thru_dt
	
	** Keep only first segments
	keep if sgmt_num==1		

	** Merge the with the list of unique people and list of duplicates to keep claims for people in analysis
	sort bid_hrs_22
	preserve

	merge m:1 bid_hrs_22 using "$data/UniquePatients.dta", nogen
	tempfile unique
	sa `unique'
	clear
	restore

	merge m:1 bid_hrs_22 using "$data/DuplicatePatients.dta", nogen
		append using `unique', gen(appendUnique)
		
	** Keep only the necessary variables
		if "`w'"=="sn" | "`w'"=="ip" {	
		keep bid_hrs_22 FROM_DT thru_dt pmt_amt hhidpn dementiacase eligiblecase casecontrol dupecase indexdate 
		}
		else {
		keep bid_hrs_22 thru_dt pmt_amt hhidpn dementiacase eligiblecase casecontrol dupecase indexdate 
		}

	** Explore negative payment amounts - they should be zero (beneficiary cost-sharing covered whole bill)
		su pmt_amt
		replace pmt_amt=0 if pmt_amt<0

	** Create another index date variable that is in the format of year and month
		g indexmthyear=mofd(indexdate)
			format indexmthyear %tm
		g dateofservice=mofd(thru_dt)
			format dateofservice %tm
		g servicedateyr=yofd(thru_dt)
			
	** Sort data and examine 
		so bid_hrs_22 casecontrol dateofservice

	** Generate a time period variable that translates the dates of service to a month in relation to the index date
		g time=dateofservice-indexmthyear

	** Keep only time periods from -12 months on - from 1 year prior to index date and anytime after
		keep if time>=-12

	** Adjust costs for inflation - convert to 2017 dollars using the Personal Consumption Expenditures Health (BEA - per Dunn HSR article)
		g inflatedpmt_amt=.
		
		loc x 1.90 1.80	1.73 1.67 1.63 1.60 1.57 1.53 1.49 1.44	1.41 1.36 1.31 1.27 1.23 1.19 1.16 1.12	1.10 1.08 1.06 1.04 1.03 1.03
		loc y 1992 1993 1994 1995 1996 1997 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015
		loc n: word count `x' 
		forval i = 1/`n'{
			loc a: word `i' of `x'
			loc b: word `i' of `y'
			replace inflatedpmt_amt=pmt_amt*`a' if servicedateyr==`b'
		}
		
	*Flag diagnoses received during IP/SNF stays for appendix table
		if "`w'"=="sn" | "`w'"=="ip" {	
		g diag_snip=((indexdate>=FROM_DT)&(indexdate<=thru_dt))
		}
		else {
		g diag_snip=.
		}
	** Collapse data, summing inflated payment amounts within time periods for each subject
		collapse (sum) inflatedpmt_amt (max) diag_snip, by(bid_hrs_22 casecontrol time)

	** Sort, save, and close down for later merging
		so bid_hrs_22 casecontrol time
	
		if "`w'"=="dm" {
		rename inflatedpmt_amt dme_reimb
		sa "$data/DME1992_2015.dta", replace
		}
		if "`w'"=="pb" {
		rename inflatedpmt_amt carrier_reimb
		sa "$data/Carrier1992_2015.dta", replace
		}
		if "`w'"=="hh" {
		rename inflatedpmt_amt hh_reimb
		sa "$data/HH1992_2015.dta", replace
		}
		if "`w'"=="hs" {	
		rename inflatedpmt_amt hospice_reimb
		sa "$data/Hospice1992_2015.dta", replace
		}
		if "`w'"=="op" {	
		rename inflatedpmt_amt outpt_reimb
		sa "$data/Outpt1992_2015.dta", replace
		}
		if "`w'"=="sn" {	
		rename inflatedpmt_amt snf_reimb
		rename diag_snip diag_snip_sn
		sa "$data/SNF1992_2015.dta", replace
		}
		if "`w'"=="ip" {	
		rename inflatedpmt_amt inpt_reimb
		rename diag_snip diag_snip_ip
		sa "$data/Inpt1992_2015.dta", replace
		}
	clear
}	

*sensitivity analysis - removing costs from stay where bene received diagnosis
loc files ip sn
foreach w of loc files {
	forval x = 1992 (1) 2015 {
	use "$datasource/`w'_`x'.dta"
	tempfile `w'`x'
	sa ``w'`x''
	clear
	}
	use ``w'1992'
	forval y = 1993 (1) 2015 {
		append using ``w'`y''
	}

	rename SGMT_NUM sgmt_num
	rename BID_HRS_22 bid_hrs_22
	rename PMT_AMT pmt_amt
	rename THRU_DT thru_dt
	
	** Keep only first segments
	keep if sgmt_num==1		

	** Merge the with the list of unique people and list of duplicates to keep claims for people in analysis
	sort bid_hrs_22
	preserve

	merge m:1 bid_hrs_22 using "$data/UniquePatients.dta", nogen
	tempfile unique
	sa `unique'
	clear
	restore

	merge m:1 bid_hrs_22 using "$data/DuplicatePatients.dta", nogen
		append using `unique', gen(appendUnique)
		
		*remove diagnoses received during IP/SNF stays for appendix table
		g diag_snip=((indexdate>=FROM_DT)&(indexdate<=thru_dt))
		*drop if diag_snip==1
		collapse (max) diag_snip, by(bid_hrs_22 casecontrol)

	** Sort, save, and close down for later merging
		so bid_hrs_22
	
		if "`w'"=="sn" {
			rename diag_snip diag_snip_sn
		tempfile sn_snip
		sa `sn_snip'
		}
		if "`w'"=="ip" {	
			rename diag_snip diag_snip_ip
		tempfile ip_snip
		sa `ip_snip'
		}
	clear
}	
use `sn_snip'
	merge 1:1 bid_hrs_22 casecontrol using `ip_snip', nogen
	g diag_snip = 1 if (diag_snip_sn==1|diag_snip_ip==1)
	merge 1:1 bid_hrs_22 casecontrol using "$data/FinalCasesControls1992-2015.dta", keep(matched using)
	*find percentage of cases that received a diagnosis during an IP/SNF stay - 40.3
	ta diag_snip if casecontrol==1, miss
	clear

****************
* PDE datasets *
****************
	forval x = 2006 (1) 2015 {
		use "$datasource/pde_`x'.dta"
		tempfile PDE`x'
		sa `PDE`x''
	clear
	}
	use `PDE2006'
	forval y = 2007 (1) 2015 {
		append using `PDE`y''
	}
	rename BID_HRS_22 bid_hrs_22
	rename RX_DOS_DT rx_dos_dt
	rename TOTAL_CST total_cst
	rename PATIENT_PAY_AMT patient_pay_amt
	sort bid_hrs_22
	preserve
	merge m:1 bid_hrs_22 using "$data/UniquePatients.dta", keep(match) nogen
	tempfile unique
	sa `unique'
	clear

	restore

	merge m:1 bid_hrs_22 using "$data/DuplicatePatients.dta", keep(match) nogen
	append using `unique', gen(appendUnique)
	drop appendUnique

	** Keep only the necessary variables
		cap drop quantity_dispensed days_supply prod_service_id indexdatecontrol mergeUnique mergeDupes

	** Create another index date variable that is in the format of year and month
		g indexmthyear=mofd(indexdate)
			format indexmthyear %tm

	** Generate a date of service variable in year and month format
		g dateofservice=mofd(rx_dos_dt)
			format dateofservice %tm

	** Sort data and examine
		so bid_hrs_22 casecontrol dateofservice

	** Generate a time period variable that translates the dates of service to a month in relation to the index date
		g time=dateofservice-indexmthyear

	** Keep only time periods from -12 months on - from 1 year prior to index date and anytime after
		keep if time>=-12

	** Calculate the medicare portion of the drug costs (make sure there are no negative payment amounts)
		su total_cst, detail
		su patient_pay_amt, detail
		g pmt_amt=total_cst-patient_pay_amt

		su pmt_amt, detail
		replace pmt_amt=0 if pmt_amt<0.01

	** Adjust costs for inflation - convert to 2012 dollars using the Personal Consumption Expenditures Health (BEA - per Dunn HSR article)
		g inflatedpmt_amt=.

		loc x 1.23 1.19 1.16 1.12 1.10 1.08 1.06 1.04 1.03 1.03
		loc y 2006 2007 2008 2009 2010 2011 2012 2013 2014 2015
			loc n : word count `x'
				forval i=1/`n' {
					loc a : word `i' of `x'
					loc b : word `i' of `y'
				replace inflatedpmt_amt=pmt_amt*`a' if year(rx_dos_dt)==`b'
				}

	** Collapse data, summing inflated payment amounts within time periods for each subject
		collapse (sum) inflatedpmt_amt, by(bid_hrs_22 casecontrol time)
		rename inflatedpmt_amt pde_reimb

	** Sort, save, and close down for later merging
		so bid_hrs_22 casecontrol time
	
	sa "$data/PDE2006_2015.dta", replace	
	
********************************************************************************

** Merge all of the different types of cost components together 
use "$data/DME1992_2015.dta", clear

	so bid_hrs_22 casecontrol time
merge 1:1 bid_hrs_22 casecontrol time using "$data/HH1992_2015.dta", gen(mergeHomeHealth)
	so bid_hrs_22 casecontrol time
merge 1:1 bid_hrs_22 casecontrol time using "$data/Hospice1992_2015.dta", gen(mergeHospice)
	so bid_hrs_22 casecontrol time
merge 1:1 bid_hrs_22 casecontrol time using "$data/Outpt1992_2015.dta", gen(mergeOutpt)
	so bid_hrs_22 casecontrol time
merge 1:1 bid_hrs_22 casecontrol time using "$data/SNF1992_2015.dta", gen(mergeNursing)
	so bid_hrs_22 casecontrol time
merge 1:1 bid_hrs_22 casecontrol time using "$data/Carrier1992_2015.dta", gen(mergeCarrier)
	so bid_hrs_22 casecontrol time
merge 1:1 bid_hrs_22 casecontrol time using "$data/Inpt1992_2015.dta", gen(mergeInpt)
	so bid_hrs_22 casecontrol time
merge 1:1 bid_hrs_22 casecontrol time using "$data/PDE2006_2015.dta", gen(mergePDE)
	so bid_hrs_22 casecontrol time	
	
drop merge*

** Calculate part A costs and part B costs
	*egen parta_reimb=rowtotal(inpt_reimb snf_reimb hospice_reimb hh_reimb)
	egen partb_reimb=rowtotal(carrier_reimb outpt_reimb dme_reimb)
** Code diag_snip	
	replace diag_snip=1 if (diag_snip_sn==1 | diag_snip_ip==1)
	drop diag_snip_*	
** Calculate a total expenditures for each time period >>> after merging, no PDE reimbursement 
	egen total_reimb=rowtotal(inpt_reimb snf_reimb hospice_reimb hh_reimb partb_reimb pde_reimb)
	g total_reimb_dxduringstay = total_reimb
	replace total_reimb_dxduringstay = (total_reimb - inpt_reimb) if diag_snip==1 & inpt_reimb!=.
	replace total_reimb_dxduringstay = (total_reimb - snf_reimb) if diag_snip==1 & snf_reimb!=.
	drop diag_snip

sa "$data/Expenditures1992_2015.dta", replace
clear


********************************************************************************

	***************************
	* X. Medicaid MAX Files  *
	***************************

********************
* HRS MAX datasets *
********************
loc files ip lt ot rx 
foreach w of loc files {
	forval x = 1999 (1) 2012 {
		use "$datasource/hrs_max_`w'_`x'.dta"
		tempfile MAX`w'`x'
		sa `MAX`w'`x''
	clear
	}
	use `MAX`w'1999'
	forval y = 2000 (1) 2012 {
		append using `MAX`w'`y'', force
	}
			
	** Merge the 1991-2012 file with the list of unique people and list of duplicates to keep claims for people in analysis
		rename BID_HRS_23 bid_hrs_23
		rename Type_of_claim type_of_claim 
		rename Medicaid_payment_amount medicaid_payment_amount 
		cap rename Ending_date_of_service ending_date_of_service 
		cap rename SMRF_Type_of_Service smrf_type_of_service 
		cap rename Prescription_fill_date prescription_fill_date 
		
		sort bid_hrs_23

			*get hhidpn for unique/duplicate data
			merge m:1 bid_hrs_23 using "/origdata/Coe_HRS/Medicaid/Medicaid2016Xref.dta", keep(match master) nogen
			destring hhidpn, replace
			
		preserve

		merge m:1 hhidpn using "$data/UniquePatients.dta", keep(match) nogen
		tempfile unique
		sa `unique'
		clear
		restore

		merge m:1 hhidpn using "$data/DuplicatePatients.dta", gen(mergeDupes)
		keep if mergeDupes==3
		append using `unique', gen(appendUnique)
		drop appendUnique

	** Drop "dummy records". Just a placeholder for utilization on a capitated claim.
		drop if type_of_claim==3

	** Explore negative payment amounts - they should be zero 
		su medicaid_payment_amount
		replace medicaid_payment_amount=0 if medicaid_payment_amount<0

	** Create a date of service variable in year month date format - current format is numeric all run together

		if "`w'" == "ip" |"`w'" == "lt" {
			keep bid_hrs_23 hhidpn  type_of_claim medicaid_payment_amount ///
			hhidpn bid_hrs_22 dementiacase eligiblecase casecontrol dupecase indexdate ///
			ending_date_of_service 	
			
			tostring ending_date_of_service, g(enddatestring)
		}
		if "`w'" == "ot" {
			keep bid_hrs_23 hhidpn  type_of_claim medicaid_payment_amount ///
			hhidpn bid_hrs_22 dementiacase eligiblecase casecontrol dupecase indexdate smrf_type_of_service ///
			ending_date_of_service 	
			
			tostring ending_date_of_service, g(enddatestring)
			recode smrf_type_of_service (13=1) (else=0), g(home_health)
		}
		if "`w'" == "rx" {
			keep bid_hrs_23 hhidpn  type_of_claim medicaid_payment_amount ///
			hhidpn bid_hrs_22 dementiacase eligiblecase casecontrol dupecase indexdate ///
			prescription_fill_date 
			tostring prescription_fill_date, g(enddatestring)
		}
			
		g servicedatemth=substr(enddatestring,5,2)
		g servicedateyr=substr(enddatestring,1,4)
			destring servicedatemth servicedateyr, replace
		g dateofservice=ym(servicedateyr,servicedatemth)
			format dateofservice %tm

	** Create another index date variable that is in the format of year and month
		g indexmthyear=mofd(indexdate)
			format indexmthyear %tm

	** Sort data and examine 
		so bid_hrs_23 casecontrol dateofservice

	** Generate a time period variable that translates the dates of service to a month in relation to the index date
		g time=dateofservice-indexmthyear

	** Keep only time periods after -12 months - 1 year prior to index date and anytime after
		keep if time>=-12

	** Adjust costs for inflation - convert to 2017 dollars using the Personal Consumption Expenditures Health (BEA - per Dunn HSR article)
		g inflatedpmt_amt=.

		loc x 1.57 1.53 1.49 1.44 1.41 1.36 1.31 1.27 1.23 1.19 1.16 1.12 1.10 1.08 1.06
		loc y 1998 1999 2000 2001 2002 2003 2004 2005 2006 2007 2008 2009 2010 2011 2012
			loc n : word count `x'
				forval i=1/`n' {
					loc a : word `i' of `x'
					loc b : word `i' of `y'
				replace inflatedpmt_amt=medicaid_payment_amount*`a' if servicedateyr==`b'
				}			
			
** Collapse data, summing payment amounts within time periods for each subject
	
	if "`w'" == "ip" {
	collapse (sum) inflatedpmt_amt, by(bid_hrs_23 hhidpn casecontrol time)
		rename inflatedpmt_amt maxip_reimb
	}
	if "`w'" == "lt" {
	collapse (sum) inflatedpmt_amt, by(bid_hrs_23 hhidpn casecontrol time)
		rename inflatedpmt_amt maxlt_reimb
	}
	if "`w'" == "ot" {
	preserve 
	keep if home_health==0
	collapse (sum) inflatedpmt_amt, by(bid_hrs_23 hhidpn casecontrol time)
		rename inflatedpmt_amt maxot_reimb_other
	tempfile other
	sa `other'
	restore
	keep if home_health==1
	collapse (sum) inflatedpmt_amt, by(bid_hrs_23 hhidpn casecontrol time)
		rename inflatedpmt_amt maxot_reimb_hh
	merge 1:1 bid_hrs_23 hhidpn casecontrol time using `other', nogen
		replace maxot_reimb_hh=0 if maxot_reimb_hh==.&maxot_reimb_other!=.
		replace maxot_reimb_other=0 if maxot_reimb_other==.&maxot_reimb_hh!=.
	}	
	
	if "`w'" == "rx" {
	collapse (sum) inflatedpmt_amt, by(bid_hrs_23 hhidpn casecontrol time)
		rename inflatedpmt_amt maxrx_reimb
	}
	
	sa "$data/Max`w'1999_2012.dta", replace
clear
}

* Merge all of the different Medicaid cost components together
use "$data/Maxip1999_2012.dta", clear
foreach file in lt ot rx {
	merge 1:1 bid_hrs_23 casecontrol time using "$data/Max`file'1999_2012.dta", nogen
		so bid_hrs_23 casecontrol time
		}

sa "$data/MedicaidExp1999_2012.dta", replace

********************************************************************************

	*********************
	* 10. Comorbidities *
	*********************

/* Need to get several covariates from the Medicare beneficiary file - date of death, CCW conditions (in index year) */
use "$datasource/basf_1992_2015.dta", clear

	rename BID_HRS_22 bid_hrs_22
	rename START_DT start_dt
	rename END_DT end_dt
	rename BENE_DOD bene_dod
	rename ATRIAL_FIB atrial_fib
	rename HIP_FRACTURE hip_fracture
	rename RA_OA ra_oa
	rename STROKE_TIA stroke_tia
	rename CANCER_BREAST cancer_breast
	rename CANCER_COLORECTAL cancer_colorectal
	rename CANCER_PROSTATE cancer_prostate
	rename CANCER_LUNG cancer_lung
	rename CANCER_ENDOMETRIAL cancer_endometrial
	
	keep bid_hrs_22 start_dt end_dt bene_dod hypoth-CANCER_ENDOMETRIAL_EVER
	drop alzh ALZH_DEMEN ALZH_EVER ALZH_DEMEN_EVER
sa "$data/DthConditions.dta", replace


** Merge with the list of unique patients and list of duplicates to only keep the data for people in our analysis 
sort bid_hrs_22
	merge m:1 bid_hrs_22 using "$data/UniquePatients.dta", gen(mergeUnique)
	keep if mergeUnique==3
tempfile BASFUnique
sa `BASFUnique'
clear

use "$data/DthConditions.dta", clear
	so bid_hrs_22
	merge m:1 bid_hrs_22 using "$data/DuplicatePatients.dta", nogen 

** Append both sets of BASF files together
	append using `BASFUnique'
sa "$data/DthConditions.dta", replace

** Keep only the year of data which matches the index date year
	keep if year(indexdate)==year(start_dt)

** Make a death date variable in month year format for later comparison to indexdate
	g deathmthyear=mofd(bene_dod)
		format deathmthyear %tm

** Create clean condition indicators for all 25 CCW conditions
	foreach x in hypoth ami anemia asthma atrial_fib cataract chronickidney copd ///
	depression diabetes glaucoma chf hip_fracture hyperl hypert ischemicheart ///
	osteoporosis ra_oa stroke_tia cancer_breast cancer_colorectal cancer_prostate ///
	cancer_lung cancer_endometrial hyperp {
		g `x'clean=`x'==1|`x'==3
	}

** Create another index date variable that is in the format of year and month
	g indexmthyear=mofd(indexdate)
		format indexmthyear %tm

** Make the ever condition variable (the date on which the subject first met the condition criteria) into an actual date variable (is str8)
	rename HYPOTH_EVER hypoth_ever 
	rename AMI_EVER ami_ever
	rename ANEMIA_EVER anemia_ever
	rename ASTHMA_EVER asthma_ever
	rename ATRIAL_FIB_EVER atrial_fib_ever
	rename HYPERP_EVER hyperp_ever 
	rename CATARACT_EVER cataract_ever
	rename CHRONICKIDNEY_EVER chronickidney_ever
	rename COPD_EVER copd_ever
	rename DEPRESSION_EVER depression_ever
	rename DIABETES_EVER diabetes_ever
	rename GLAUCOMA_EVER glaucoma_ever
	rename CHF_EVER chf_ever 
	rename HIP_FRACTURE_EVER hip_fracture_ever
	rename HYPERL_EVER hyperl_ever
	rename HYPERT_EVER hypert_ever 
	rename ISCHEMICHEART_EVER ischemicheart_ever
	rename OSTEOPOROSIS_EVER osteoporosis_ever
	rename RA_OA_EVER ra_oa_ever
	rename STROKE_TIA_EVER stroke_tia_ever
	rename CANCER_BREAST_EVER cancer_breast_ever
	rename CANCER_COLORECTAL_EVER cancer_colorectal_ever
	rename CANCER_PROSTATE_EVER cancer_prostate_ever
	rename CANCER_LUNG_EVER cancer_lung_ever
	rename CANCER_ENDOMETRIAL_EVER cancer_endometrial_ever

	foreach var of varlist hypoth_ever-cancer_endometrial_ever {
		gen `var'date=`var'
	}

** Replace the condition variables with zero if the date on which the subject first met the condition criteria is after the index date
	foreach x in hypoth ami anemia asthma atrial_fib cataract chronickidney copd ///
	depression diabetes glaucoma chf hip_fracture hyperl hypert ischemicheart ///
	osteoporosis ra_oa stroke_tia cancer_breast cancer_colorectal cancer_prostate ///
	cancer_lung cancer_endometrial {
		replace `x'clean=0 if `x'_everdate>indexdate
	}

** Combine the cleaned 25 indicators into the 19 overall condition indicators desired
	g hypothyroidism=hypothclean
	g heartdisease=amiclean==1 | ischemicheartclean==1
	g anemia_final=anemiaclean
	g asthma_final=asthmaclean
	g atrialfib=atrial_fibclean
	g prostatichyperplasia=hyperpclean
	g eyedisorders=cataractclean==1 | glaucomaclean==1
	g renaldisease=chronickidneyclean
	g copd_final=copdclean
	g depression_final=depressionclean
	g diabetes_final=diabetesclean
	g heartfailure=chfclean
	g fracture=hip_fractureclean
	g hyperlipidemia=hyperlclean
	g hypertension=hypertclean
	g osteoporosis_final=osteoporosisclean
	g arthritis=ra_oaclean
	g stroke=stroke_tiaclean
	g cancer=cancer_breastclean==1 | cancer_colorectalclean==1 | cancer_prostateclean==1 | cancer_lungclean==1 | cancer_endometrialclean==1
	g combinedheart=heartdisease==1 | atrialfib==1 | heartfailure==1

** Keep only needed variables, sort, save and close down for later merging
	keep bid_hrs_22 casecontrol bene_dod deathmthyear hypothyroidism-combinedheart
	so bid_hrs_22 casecontrol

** Merge death date and conditions
	so bid_hrs_22 casecontrol
	merge 1:1 bid_hrs_22 casecontrol using "$data/FinalCasesControls1992-2015.dta", nogen
	
** Calculate age at index date
	drop indexmthyear
	g indexmthyear=mofd(indexdate)
		format indexmthyear %tm
	g birthmthyear=ym(rabyear,rabmonth)
		format birthmthyear %tm
	g ageatindex=(indexmthyear-birthmthyear)/12

** Calculate time period of death (number of months after diagnosis, some are negative?)
	g timeofdeath=deathmthyear-indexmthyear

** Calculate time period in which person became ineligible - stopped having Medicare data			288 (12*24 for 1992-2015)
** Determine the string length from the month after the index date
	g stringlength=288-((year(indexdate)-1992)*12+month(indexdate))

** Grab out the substring of eligibility indicators from the month after the index date
	g eligibilityafterindex=substr(allyearseligibility,(year(indexdate)-1992)*12+month(indexdate)+1,stringlength)

** Now find the first month, if any, in which the person becomes ineligible
	g timeineligible=strpos(eligibilityafterindex,"0")

** Create an indicator for eligible from index date through Dec 2012
*	g eligiblethroughdec12=timeineligible==0

** Calculate an indicator for died between index date and 12/31/2012 (end of follow-up)
	g diedbtwnindexfollowup=(bene_dod>indexdate & bene_dod<=mdy(12,31,2015))

** Create a categorical variable that describes reason for ineligibility - death or not Medicare FFS anymore
	g ineligibledeathcensored=3
	
	label define ineligiblelabel 1 "data through 60 mths followup (no death or censoring)" 2 "data until died (death occurred before mth 60)" ///
	3 "data until dropped FFS coverage (occurring before 60 mths)" 4 "data until 12/31/12 (which occurred before mth 60)"
	label values ineligibledeathcensored ineligiblelabel
	replace ineligibledeathcensored=1 if (timeineligible>59 | (timeineligible==0 & stringlength>=59))
	replace ineligibledeathcensored=2 if ((timeineligible-timeofdeath==1) & timeofdeath<60)
	replace ineligibledeathcensored=4 if timeineligible==0 & stringlength<59
	replace ineligibledeathcensored=2 if (ineligibledeathcensored==4 & diedbtwnindexfollowup==1)

** Sort, save and clear for later merging
	so bid_hrs_22 casecontrol
sa "$data/AnalyticFileWide.dta", replace
clear

********************************************************************************

	**************************************************
	* 11. Merge with Expenditures, Reshape, & Clean  *
	**************************************************

** Open expenditures file and take the file wide
use "$data/Expenditures1992_2015.dta", clear
	drop if time>60
	replace time=time+12
	reshape wide dme_reimb-total_reimb, i(bid_hrs_22 casecontrol) j(time)

** Sort, save and clear for later merging
	so bid_hrs_22 casecontrol 

** Merge the analytic wide file with the Expenditures wide file
merge 1:1 bid_hrs_22 casecontrol using "$data/AnalyticFileWide.dta", gen(mergeExpend)
sa "$data/AnalyticFileWide.dta", replace
*
use "$data/PDE2006_2015.dta", clear
	drop if time>60
	replace time=time+12
	reshape wide pde_reimb, i(bid_hrs_22 casecontrol) j(time)
merge 1:1 bid_hrs_22 casecontrol using "$data/AnalyticFileWide.dta", gen(mergePDE)
sa "$data/AnalyticFileWide.dta", replace

use "$data/MedicaidExp1999_2012.dta", clear
	drop if time>60
	replace time=time+12
	
	reshape wide maxip_reimb-maxrx_reimb, i(bid_hrs_23 casecontrol) j(time)
merge 1:1 hhidpn casecontrol using "$data/AnalyticFileWide.dta", gen(mergeMAX)

sa "$data/AnalyticFileWide.dta", replace

** Clean up some of the covariates
** Create an indicator for male
	g male=ragender==1

** Create a condensed categorical variable indicating highest degree plus indicator variables
	g education=raeduc
		recode education (3=2) (4=3) (5=4)
		label define educationlabel 1 "less than high school" 2 "high school graduate or GED" 3 "some college" 4 "college and above"
		label values education educationlabel

	g hsgrad=education==2
	g somecollege=education==3
	g collegegrad=education==4
	g anycollege=(education==3|education==4)

	tostring casecontrol, g(casecontrolstr)
	g idnew = bid_hrs_22+casecontrolstr
	
** Create a combined race/ethnicity variable and indicator variables

	g race=.
		replace race=1 if (raracem==1 & rahispan==0)
		replace race=2 if (raracem==2 & rahispan==0)
		replace race=3 if rahispan==1
		replace race=4 if (raracem==3 & rahispan==0)
		
		label define racelabel 1 "non-hispanic white" 2 "non-hispanic black" 3 "hispanic" 4 "non-hispanic other"
		label values race racelabel

	g black=race==2
	g hispanic=race==3
	g otherrace=race==4

** Clean up indicator variable for veteran
	recode ravetrn (.m=.)

** Calculate the total number of comorbid conditions as of the index date - combinedheart?
	egen numcomorbidities=rowtotal(hypothyroidism-cancer)
	order numcomorbidities, after(cancer)

** Calculate the total medicare reimbursement amount for the year prior to the index date
	egen baselinetotal_reimb=rowtotal(total_reimb0 total_reimb1 total_reimb2 total_reimb3 total_reimb4 total_reimb5 total_reimb6 total_reimb7 total_reimb8 total_reimb9 total_reimb10 total_reimb11)
	order baselinetotal_reimb, after(total_reimb11)

** Calculate the time period in which 12/31/2012 occurs (relative to indexdate)
	g endfollowupmthyr=ym(2015,12)
		format endfollowupmthyr %tm

	g timeendfollowup=endfollowupmthyr-indexmthyear

** Generate a summary marital status variable and replace with the data that is closest to the subject's index date		
	g maritalstatus=.
		replace maritalstatus=r1mstat if year(indexdate)==1992 
		replace maritalstatus=r2mstat if (year(indexdate)==1992 & missing(maritalstatus))
		
	loc year 1993
	forval x = 1/11 {
		loc next1 = `x'+1
		loc next2 = `x'+2
		loc nextyr = `year'+1
			replace maritalstatus=r`next1'mstat if (year(indexdate)==`year' | year(indexdate)==`nextyr')
			replace maritalstatus=r`x'mstat if ((year(indexdate)==`year' | year(indexdate)==`nextyr') & missing(maritalstatus))
			replace maritalstatus=r`next2'mstat if ((year(indexdate)==`year' | year(indexdate)==`nextyr') & missing(maritalstatus))
		loc year = `year'+2
	}

** Recode to condense the categories
	recode maritalstatus (1/3=1) (4/6=2) (7=3) (8=4) (.=9)
		label define maritallabel 1 "married/partnered" 2 "separated/divorced" 3 "widowed" 4 "never married" 9 "unknown"
		label values maritalstatus maritallabel

** Generate some marital status indicator variables 
	g divorced=maritalstatus==2
	g widowed=maritalstatus==3
	g nevermarried=maritalstatus==4
	g unknownmarital=maritalstatus==9

** Create a categorical variable (and accompanying indicator variables) that further condenses marital status
	g maritalcondensed=maritalstatus
		recode maritalcondensed (3/4=2)
		label define maritalcondensedlabel 1 "married/partnered" 2 "not married/partnered" 9 "unknown"
		label values maritalcondensed maritalcondensedlabel

	g notmarried=maritalcondensed==2
	
*	keep hhidpn casecontrol *reimb* ineligibledeathcensored timeofdeath timeineligible timeendfollowup indexdate bene_dod

** Go long
	reshape long maxip_reimb maxlt_reimb maxot_reimb_hh maxot_reimb_other  ///
	maxrx_reimb pde_reimb dme_reimb hh_reimb hospice_reimb outpt_reimb ///
	snf_reimb carrier_reimb inpt_reimb parta_reimb partb_reimb total_reimb, i(hhidpn casecontrol) j(time)

*generate the categories
	egen medicare_nhhhcare_reimb = rowtotal(snf_reimb hh_reimb) 
	egen medicaid_nhhhcare_reimb = rowtotal(maxlt_reimb maxot_reimb_hh)
		replace maxip_reimb=0 if bid_hrs_23==""
		replace maxrx_reimb=0 if bid_hrs_23==""

	egen totalmax_reimb = rowtotal(maxip_reimb maxlt_reimb maxot_reimb_hh maxot_reimb_other maxrx_reimb)
	egen total_reimb_dxduringstay = rowtotal(parta_reimb partb_reimb pde_reimb) if diag_snip!=1
	
	sa "$data/AnalyticFileLong.dta", replace	
	
	
** Replace time with time-12
	replace time=time-12

** Now, we want to fill in all of the missing prescription drug costs that happen before part d became available
	gen partdtime=ym(2006,1)-indexmthyear

	replace pde_reimb=0 if time<partdtime
	
** Now, we want to fill in all of the missing costs appropriately - zero when person is eligible, missing for all times after ineligible
** Start with those who had data all through 60 months of followup
	g indexyear = yofd(indexdate)
	
	foreach var of varlist total_reimb medicare_nhhhcare_reimb inpt_reimb pde_reimb total_reimb_dxduringstay {
		replace `var'=0 if (ineligibledeathcensored==1 & missing(`var'))
	}
	foreach var of varlist totalmax_reimb medicaid_nhhhcare_reimb maxip_reimb maxrx_reimb {
		replace `var'=0 if (ineligibledeathcensored==1 & missing(`var') & (indexyear>=1999 & indexyear<=2012))
	}

** Next, do those who had data until they died
	foreach var of varlist total_reimb medicare_nhhhcare_reimb inpt_reimb pde_reimb total_reimb_dxduringstay {
		replace `var'=0 if (ineligibledeathcensored==2 & time<=timeofdeath & missing(`var'))
		replace `var'=. if (ineligibledeathcensored==2 & time>timeofdeath)
	}
	foreach var of varlist totalmax_reimb medicaid_nhhhcare_reimb maxip_reimb maxrx_reimb {
		 replace `var'=0 if (ineligibledeathcensored==2 & time<=timeofdeath & missing(`var') & (indexyear>=1999 & indexyear<=2012))
		 replace `var'=. if (ineligibledeathcensored==2 & time>timeofdeath & (indexyear>=1999 & indexyear<=2012))
	}

** Next, do those who had data until they dropped FFS coverage
	foreach var of varlist total_reimb medicare_nhhhcare_reimb inpt_reimb pde_reimb total_reimb_dxduringstay {
		replace `var'=0 if (ineligibledeathcensored==3 & time<timeineligible & missing(`var'))
		replace `var'=. if (ineligibledeathcensored==3 & time>=timeineligible)
	}
	foreach var of varlist totalmax_reimb medicaid_nhhhcare_reimb maxip_reimb maxrx_reimb {
		 replace `var'=0 if (ineligibledeathcensored==3 & time<timeineligible & missing(`var') & (indexyear>=1999 & indexyear<=2012))
		 replace `var'=. if (ineligibledeathcensored==3 & time>=timeineligible & (indexyear>=1999 & indexyear<=2012))
}

** Finally, do those who had data until end of data period
	foreach var of varlist total_reimb medicare_nhhhcare_reimb inpt_reimb pde_reimb total_reimb_dxduringstay {
		replace `var'=0 if (ineligibledeathcensored==4 & time<=timeendfollowup & missing(`var'))
		replace `var'=. if (ineligibledeathcensored==4 & time>timeendfollowup)
	}
	foreach var of varlist totalmax_reimb medicaid_nhhhcare_reimb maxip_reimb maxrx_reimb {
		 replace `var'=0 if (ineligibledeathcensored==4 & time<=timeendfollowup & missing(`var') & (indexyear>=1999 & indexyear<=2012))
		 replace `var'=. if (ineligibledeathcensored==4 & time>timeendfollowup & (indexyear>=1999 & indexyear<=2012))
}


	
	
********************************************************************************

	************************************
	* 12. Format for Anirban's Method  *
	************************************

	*for the summary statistics	
	sa "$data/AnalyticFileLongBasuVariables_ss_claims.dta", replace	

/* Now we need to calculate some extra variables that are needed to run Anirban's 
code. We also need to shift the times forward one month. In the regular wide and 
long dataset, the indexdate occurs at time==0. In Anirban's analysis, the 
diagnosis should occur at time==1. */


** Anirban's analysis only requires data from diagnosis on
	drop if time<0
	drop if time>59


** Fullobs variable - fullobs==1 if patient-interval was observed over the entirity (no death or censoring within interval)
	g fullobs=1
		replace fullobs = 0 if (ineligibledeathcensored==2 & time>=timeofdeath)
		replace fullobs = 0 if (ineligibledeathcensored==3 & time>=timeineligible)
		replace fullobs = 0 if (ineligibledeathcensored==4 & time>timeendfollowup)

** Obs variable - Indicator of excess data - for intervals beyond censoring and death
	g obs=1
		replace obs = 0 if (ineligibledeathcensored==2 & time>timeofdeath)
		replace obs = 0 if (ineligibledeathcensored==3 & time>=timeineligible)
		replace obs = 0 if (ineligibledeathcensored==4 & time>timeendfollowup)

	g obs2=obs
		replace obs2=. if ineligibledeathcensored==3
		replace obs2=0 if (ineligibledeathcensored==3 & stringlength<59 & timeofdeath>stringlength & time>timeendfollowup)
		replace obs2=1 if (ineligibledeathcensored==3 & stringlength<59 & timeofdeath>stringlength & time<=timeendfollowup)
		replace obs2=0 if (ineligibledeathcensored==3 & timeofdeath<60 & timeofdeath<=stringlength & time>timeofdeath)
		replace obs2=1 if (ineligibledeathcensored==3 & timeofdeath<60 & timeofdeath<=stringlength & time<=timeofdeath)
		replace obs2=1 if (ineligibledeathcensored==3 & obs2==.)
		
** Censored variable - censored = 0/1: is 1 for the interval where censoring begins and all intervals thereafter for a patient
	g censored=0
		replace censored=1 if ((ineligibledeathcensored==3 & time>=timeineligible)|(ineligibledeathcensored==4 & time>timeendfollowup))

** Death variable - death = 0/1: is 1 for the interval where death occurs and all intervals thereafter for the patient
	g death=0
		replace death=1 if (ineligibledeathcensored==2 & time>=timeofdeath)
		replace death=1 if (ineligibledeathcensored==3 & timeofdeath<60 & timeofdeath<=stringlength & time>=timeofdeath)
		
** Duration variable - duration = time to death within an interval or full interval length (e.g.60 DAYS)
	g duration=30
		replace duration=day(bene_dod) if (ineligibledeathcensored==2 & time==timeofdeath)
		replace duration=0 if ((ineligibledeathcensored==2 & time>timeofdeath)|(ineligibledeathcensored==2 & time>timeofdeath)|(ineligibledeathcensored==3 & time>=timeineligible)|(ineligibledeathcensored==4 & time>timeendfollowup))
		
** Years from diagnosis variable - yfromind*  = yearly indicators from diagnosis (eg. yfromdind2 yfromdind3 yfromdind4)
	g yfromdind1=(time>=0 & time<=11)
	g yfromdind2=(time>=12 & time<=23)
	g yfromdind3=(time>=24 & time<=35)
	g yfromdind4=(time>=36 & time<=47)
	g yfromdind5=(time>=48 & time<=59)

** Generate a variable that contains the duration as a backup
	g durold=duration

** Generate a variable that contains the case control status as a backup
	g casecontrolold=casecontrol

** Calculate quartiles for baseline total reimbursements
	xtile basereimbquart=baselinetotal_reimb, nq(4)

** Now, we need to shift the times
	replace time=time+1
	replace timeofdeath=timeofdeath+1
	replace timeineligible=timeineligible+1
	replace timeendfollowup=timeendfollowup+1

** Interaction of yearly indicators with time - yfromdtime2 = inteaction of yearly indicators with continuous time (eg. yfromdtime2 yfromdtime3 yfromdtime4)
	forval z = 1/5 {
		g yfromdtime`z'=yfromdind`z'*time
	}

** Time (interval number) at which censoring occurs - centime
	g centime=.
		replace centime=timeineligible if ineligibledeathcensored==3
		replace centime=timeendfollowup+1 if ineligibledeathcensored==4

** Time (interval number) at which death occurs - deathtime = interval number where death occurs
	g deathtime=.
		replace deathtime=timeofdeath if ineligibledeathcensored==2

** Interaction between case status and time
	g casetime=casecontrol*time

sa "$data/Medicare.dta", replace
clear

** Add in Medicaid eligibility
*PS files
	forval x = 1999/2012 {
	use "$datasource/hrs_max_ps_`x'.dta", clear
	rename BID_HRS_23 bid_hrs_23
	foreach val in 01 02 03 04 05 06 07 08 09 10 11 12 {
		g elig_`val' = "1"
		replace elig_`val'="0" if SMRF_uniform_eligibility_mo_`val'=="00" | Managed_care_combinations_mo_`val'!=16
		replace elig_`val'="." if SMRF_uniform_eligibility_mo_`val'=="99"
		*only count full duals - crossover variable only monthly from 2005 onward
		*replace elig_`val'="0" if elig_`val'=="1" & (crossover_mo_`val'!="02"|crossover_mo_`val'=="04"|crossover_mo_`val'=="08"|crossover_mo_`val'=="52"|crossover_mo_`val'=="54"|crossover_mo_`val'=="58")
		*only use FFS, exclude HMO
		*EL_PPH_PLN_MO_CNT_CMCP/BMCP/LTCM/PCCM
		*MANAGED_CARE_COMBINATIONS_MO_01
 }
	egen SMRF_uniform_eligibility = concat(elig_01 elig_02 elig_03 elig_04 elig_05 elig_06 elig_07 elig_08 elig_09 elig_10 elig_11 elig_12)
	keep bid_hrs_23 SMRF_uniform_eligibility
	

	loc z jan feb mar apr may jun jul aug sep oct nov dec
	loc w 1 2 3 4 5 6 7 8 9 10 11 12 
	loc n : word count `z'
		forval i = 1/`n' {
			loc a : word `i' of `z'
			loc b : word `i' of `w'			
	g medicaid_elig`a'`x'=0
	format medicaid_elig`a'`x' %tm
	replace medicaid_elig`a'`x'= ((`b'-1)+(`x'-1960)*12) if (substr(SMRF_uniform_eligibility,`b',1)=="1")
	replace medicaid_elig`a'`x'=. if medicaid_elig`a'`x'==0
		}

		duplicates drop 
		so bid_hrs_23 *jan* *feb* *mar* *apr* *may* *jun* *jul* *aug* *sep* *oct* *nov* *dec*
		bys bid_hrs_23: g dup = cond(_N==1, 0, _n)
		drop if dup>1
	tempfile Denominator`x'
	sa `Denominator`x''
	clear
	}
	
	use `Denominator1999'
	forval z = 2000/2012 {
	merge 1:1 bid_hrs_23 using `Denominator`z'', gen(merge`z')
	}
	order _all, alpha
	order bid_hrs_23
	drop merge*		
		
	merge 1:m bid_hrs_23 using "$data/Medicare.dta", keep(using matched) nogen
	
	g medicaid_month = .
	forval x = 1999/2012 {
		foreach y in jan feb mar apr may jun jul aug sep oct nov dec {
		replace medicaid_month=1 if (medicaid_elig`y'`x'-indexmthyear)==(time-12)
	}
	}

*keep only necessary variables to reduce size of dataset
keep *reimb total_reimb_dxduringstay idnew time stroke casecontrol ageatindex male anycollege black hispanic *race anemia_final renaldisease copd_final depression_final diabetes_final hypertension arthritis combinedheart notmarried unknownmarital basereimbquart yfromdind* yfromdtime* obs* ineligibledeathcensored timeofdeath casetime duration death* centime *old medicaid_month
	
sa "$data/Medicare.dta", replace	


*death statistics for draft
	*death by the end of 5 years (37.5 %)
	ta death if time==60, miss
	*death overall (67.2 %)
	recode timeofdeath (2/276=1) (else=0), g(check)
	ta check

