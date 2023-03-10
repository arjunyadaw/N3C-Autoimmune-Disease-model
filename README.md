# Autoimmune Disease Project
## Introduction:
This repository contains reproducible code for our paper “Pre-existing autoimmunity associates with increased severity of COVID-19 patients: A retrospective cohort   study using data from the National COVID Cohort Collaborative (N3C) data”, which usage the data from the National COVID Cohort Collaborative’s (N3C) EHR repository to identify potential autoimmune disease patients prior to COVID-19 diagnosis and patients with long-term usage of immunosuppressants prior to COVID-19 diagnosis. Please cite following paper if you use the code:\
https://doi.org/10.1101/2023.02.02.23285353
***
## Purpose of this code: 
This code is designed to identified patients with pre-existing autoimmune diseases and/or patients with long-term usage of immunosuppressants prior to COVID-19 diagnosis. We also created a cohort of patients with selected antiviral (Paxlovid (Nirmatrelvir/ritonavir), LAGEVRIO (molnupiravir)) or one monoclonal antibody (bebtelovimab) treatment. For details description, you can see above mentioned paper. 

## Prerequisites:
In order to run this code (reproduce results), you will need following:
  * Electronic health record  data table (version 90) in the OMOP data model
  * Laboratory confirmed positive COVID-19 diagnosis based on a positive SARS-CoV-2 polymerase chain reaction (PCR) or antigen (Ag) test based cohort
  * Autoimmune disease patients cohort based on its SNOMET-CT code
  * Cohort of immunosuppressants user based 15 classes of immunosuppressants by using its drug_concept_id
  * Cohort of pre-existing comorbidities
  * Cohort of vaccinated and selected antiviral treatment
  
The SQL code in this repository is written in the Spark SQL. The Python code in few repositories are written in PySpark. Pre-existing comorbidities extracted based on it's codeset_id in concept_set_members table by using SQL.


## Python Libraries Used:
The following Python version and packages are required to execute this in palantir foundary:

Python 3.6.7\
numpy   1.19.5\
pandas  0.25.3\
statsmodel 0.12.2\
patsy  0.5.3\
matplotlib 2.2.4\
Seaborn 0.11.2\
Spark SQL 3.2.1-palantir.35\
tableone 0.7.12


## Running our model:

To produce the results, One has to create cohorts of (autoimmune disease patients, cohort of immunosuppressants user, cohort of COVID-19 positive patients, cohort of antiviral treatment & cohort of preexisting comorbidities) then run AID_model by using these input tables.

