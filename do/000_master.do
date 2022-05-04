
********************************************************************************
*						PUBLIC COSTS MASTER DO FILE			 			       *
********************************************************************************

/*
GENERAL NOTES:
- This is the master do-file for the Public Costs project.
- This do-file defines folder and data globals and allows to choose which sections and tables to run.
- Adjust your folderpaths and globals in the respective fields.
*/

********************************************************************************

*DATA GLOBALS

if 1 {

*select path
gl name 1

	if $name {
	gl folder					""
	gl data						""
	gl datasource				""
	}

}

*FOLDER GLOBALS

gl do			   				"$folder\do"
gl output		  				"$folder\output"
gl log			   				"$folder\log"
gl subfolder					"$do\createdataset"

*CHOOSE SECTIONS TO RUN
	
loc clean_merge					0 	/* Activate this section to run "01_clean.do" */
loc estimation					0	/* Activate this section to run "02_est.do"	*/ 
loc tables						0	/* Activate this section to run "03_summarystatistics.do"	*/


gl reps 						1000



********************************************************************************
*					   PART 1:  RUN DO-FILES								   *
********************************************************************************

	
* PART 1: CLEAN AND MERGE

	if `clean_merge' {
		do "$do\01_clean.do"
	}
		

*PART 2: RUN ANALYSIS	
	
	if `estimation' {
		do "$do\02_est.do"
	}

* PART 3: CREATE TABLES	

	if `tables' {
		do "$do\03_summarystatistics.do"
	}

	