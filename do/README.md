**README file for posted estimation files**

**&quot;Public Spending on Acute and Long-Term Care for Alzheimer&#39;s and Related Dementias&quot;**

by Norma B. Coe, Lindsay White, Melissa Oney, Anirban Basu, and Eric B. Larson

**Overview:**

Before running the code:

- Copy file contents into project folder with the following subfolders: do, output, log, and data
- Change the file path of the folder global (&quot;gl folder&quot;) in 000\_master.do to the location of the project folder
- Save all data in the data subfolder (and additional subfolders as described below)

Once these changes have been made, running the master file will produce the tables corresponding to the selected sections (noted after each local in 000\_master.do). The number of replications and iterations can be adjusted in &quot;gl reps&quot; of 000\_master.do.

For questions about the code, please contact NAME (chuxuan.sun@pennmedicine.upenn.edu).

**Data required:**

Register for access to the HRS and RAND HRS data on the HRS website ([https://hrs.isr.umich.edu/data-products](https://hrs.isr.umich.edu/data-products)), then download the following files:

- HRS tracker file: trk2018tr\_r.dta
- RAND HRS Longitudinal File: randhrs1992\_2016v1.dta

Files located on the HSRDC server (&quot;/origdata/HRS\_CMS/output&quot;) include:

- BASF: basf\_1992\_2015.dta
- Part D: pde\__YEAR_.dta, for each _YEAR_ from 2006-2015
- Denominator files: dn\__YEAR_.dta, for each _YEAR_ from 1992-2012
- MBSF: mbsf\_2013.dta, mbsf\_2014.dta, mbsf\_2015.dta
- Part A and B files: dm\ __YEAR_.dta, pb\__ YEAR_.dta, hh\ __YEAR_.dta, hs\__ YEAR_.dta, hs\ __YEAR_.dta, op\__ YEAR_.dta, sn\ __YEAR_.dta, ip\__ YEAR_.dta, for each _YEAR_ from 1992-2015
- Medicaid MAX files: hrs\_max\_ip\ __YEAR_.dta, hrs\_max\_lt\__ YEAR_.dta, hrs\_max\_ot\ __YEAR_.dta, hrs\_max\_rx\__ YEAR_.dta, for each _YEAR_ from 1999-2012
- Medicaid MAX Personal Summary files: hrs\_max\_ps\__YEAR_.dta, for each _YEAR_ from 1999-2012

Finally, supplementary files include:

- Medicare cross-reference file: &quot;/origdata/Coe\_HRS/Medicare/xref2015medicare.dta&quot;

Place all publicly accessible data files in the data folder. Do not place any data files in your home directory on the server, only in project folder.

**Running the code:**

This code is for Stata, and has been verified to run in version 17. The estout package is required to output tables.

**Description of files:**

The following describes how the files correspond to the inputs and output:

| File | Description | Inputs/Outputs | Notes |
| --- | --- | --- | --- |
| 000\_master.do | Sets macros for all variables, specifications, and replications used in the other files | | Only edit the global folder and the individual global macros |
| 01\_clean.do | Cleans and merges all raw data files | Inputs: HRS tracker file, randhrs1992\_2016v1.dta, HRS-CMS linked data files listed above (located on the server) Output: Medicare.dta | |
| 02\_est.do | Runs Basu and Manning method | Input: Medicare.dta Output: Cost-specific tables in both word and excel | Copy and paste excel output into draft tables for updates |
| 03\_summarystatistics.do | Creates summary statistics | Inputs (produced in 01\_clean.do): CasesAndControls.dta, AnalyticFileLongBasuVariables\_ss\_claims.dta, PD\_wide.dta, and Medicaid\_wide.dta Output: Summary statistics table and Figure 1 | If matching criteria changes (matchgrp variable) make sure to update the through value on line 18 to reflect max value |
