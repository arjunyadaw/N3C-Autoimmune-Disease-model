

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c3efe690-072e-4474-a88c-a1ec424b8e31"),
    drug_exposure_after01012020=Input(rid="ri.foundry.main.dataset.44739c27-18cc-4407-b005-b7741600e2b6")
)
-- drug concept ids taken from Hythem Sidky. codework book for Bebtalovimab
SELECT *
FROM drug_exposure_after01012020
WHERE drug_concept_id IN (726330,
726665,
727154,
727611,
1759073,
1759074,
1759075,
1759076,
1759077,
1759259,
1999375,
42632508)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.787917f6-40fe-4f53-8f6d-2e23959fe773"),
    covid_positive_on_immunosuppressants=Input(rid="ri.foundry.main.dataset.702264e6-16fc-4324-8695-9042ced0b9ef")
)
--- condsider patients with drug exposure 14 days before covid-19 diagnosis date.
SELECT *
FROM covid_positive_on_immunosuppressants
WHERE days_immuno_before_visit >= 14;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7ab920cc-c524-4bf1-8c43-c2465c078dbe"),
    Covid_pos_immuno_14days_before=Input(rid="ri.foundry.main.dataset.787917f6-40fe-4f53-8f6d-2e23959fe773")
)
---Creating a table with the person_id for unique people with an immunosuppressive drug present at admission, and an indicator variable for immunosuppressed for the joining
SELECT person_id,
    SUM (immunosupp) as immunosupp_sum,
    SUM (anthracyclines) as anthracyclines_sum,
    SUM (checkpoint_inhibitor) as checkpoint_inhibitor_sum,
    SUM (cyclophosphamide) as cyclophosphamide_sum, 
    SUM (pk_inhibitor) as pk_inhibitor_sum,
    SUM (monoclonal_other) as monoclonal_other_sum,
    SUM (l01_other) as l01_other_sum,
    SUM (azathioprine) as azathioprine_sum,
    SUM (calcineurin_inhibitor) as calcineurin_inhibitor_sum, 
    SUM (il_inhibitor) as il_inhibitor_sum,
    SUM (jak_inhibitor) as jak_inhibitor_sum,
    SUM (rituximab) as rituximab_sum,
    SUM (mycophenol) as mycophenol_sum,
    SUM (tnf_inhibitor) as tnf_inhibitor_sum, 
    SUM (l04_other) as l04_other_sum,
    SUM (glucocorticoid) as glucocorticoid_sum,
    SUM (steroids_before_covid) as steroids_before_covid_sum,
    SUM (steroids_during_covid) as steroids_during_covid_sum, 
    SUM (gluco_dose_known_high) as gluco_dose_known_high_sum
FROM Covid_pos_immuno_14days_before
GROUP BY person_id;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.ad1a2ec9-00c3-415e-b11a-170c80219806"),
    Immuno_sum=Input(rid="ri.foundry.main.dataset.7ab920cc-c524-4bf1-8c43-c2465c078dbe")
)
---Purpose: the summation in the previous step counts drug records, but we just want an indicator of yes/no so anything >=1 is present and null means not present
SELECT *,
CASE 
    WHEN (immunosupp_sum >0 ) then 1
    END AS immunosupp, 
CASE
    WHEN (anthracyclines_sum >0 ) then 1
    ELSE 0
    END AS anthracyclines, 
CASE
    WHEN (checkpoint_inhibitor_sum >0 ) then 1
    ELSE 0
    END AS checkpoint_inhibitor,
CASE
    WHEN (cyclophosphamide_sum >0 ) then 1
    ELSE 0
    END AS cyclophosphamide,  
CASE
    WHEN (pk_inhibitor_sum >0 ) then 1
    ELSE 0
    END AS pk_inhibitor, 
CASE
    WHEN (monoclonal_other_sum >0 ) then 1
    ELSE 0
    END AS monoclonal_other, 
CASE
    WHEN (rituximab_sum >0 ) then 1
    ELSE 0
    END AS rituximab, 
CASE
    WHEN (l01_other_sum >0 ) then 1
    ELSE 0
    END AS l01_other, 
CASE
    WHEN (azathioprine_sum >0 ) then 1
    ELSE 0
    END AS azathioprine,
CASE
    WHEN (calcineurin_inhibitor_sum >0 ) then 1
    ELSE 0
    END AS calcineurin_inhibitor, 
CASE
    WHEN (il_inhibitor_sum >0 ) then 1
    ELSE 0
    END AS il_inhibitor, 
CASE
    WHEN (jak_inhibitor_sum >0 ) then 1
    ELSE 0
    END AS jak_inhibitor,
CASE
    WHEN (mycophenol_sum >0 ) then 1
    ELSE 0
    END AS mycophenol,  
CASE
    WHEN (tnf_inhibitor_sum >0 ) then 1
    ELSE 0
    END AS tnf_inhibitor, 
CASE
    WHEN (l04_other_sum >0 ) then 1
    ELSE 0
    END AS l04_other, 
CASE
    WHEN (glucocorticoid_sum >0 ) then 1
    ELSE 0
    END AS glucocorticoid, 
CASE
    WHEN (steroids_before_covid_sum >0 ) then 1
    ELSE 0
    END AS steroids_before_covid,
CASE
    WHEN (steroids_during_covid_sum >0 ) then 1
    ELSE 0
    END AS steroids_during_covid, 
CASE
    WHEN (gluco_dose_known_high_sum >0 ) then 1
    ELSE 0
    END AS gluco_dose_known_high

FROM Immuno_sum

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.55bbd22f-7f36-43db-9d3b-ae4e063ed2ef"),
    Cohort_covid_final=Input(rid="ri.foundry.main.dataset.9fda0be9-ceab-47ae-b159-bec157742c13"),
    patients_on_immunosupressants=Input(rid="ri.foundry.main.dataset.82472975-bf67-45df-8155-560e994e781c")
)
--- Here severity of patients mapped by using COVID-19 cohort with severity and immunosuppressants_cohorts tables
SELECT A.*, B.Severity_Type
FROM patients_on_immunosupressants A
inner join Cohort_covid_final B
on A.person_id = B.person_id;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.702264e6-16fc-4324-8695-9042ced0b9ef"),
    Cohort_covid_final=Input(rid="ri.foundry.main.dataset.9fda0be9-ceab-47ae-b159-bec157742c13"),
    patients_on_immunosuppressant=Input(rid="ri.foundry.main.dataset.f95bd294-8261-4f82-9381-7173ca919998")
)
---PURPOSE: Immunosuppression present at the time of admisssion
SELECT i.person_id, i.drug_concept_name, i.drug_exposure_start_date, c.COVID_first_PCR_or_AG_lab_positive,i.drug_exposure_end_date, i.anthracyclines, i.checkpoint_inhibitor, i.cyclophosphamide, i.pk_inhibitor, i.monoclonal_other, i.l01_other, i.rituximab, i.azathioprine, i.calcineurin_inhibitor, i.il_inhibitor, i.jak_inhibitor, i.mycophenol, i.tnf_inhibitor, i.l04_other, i.glucocorticoid, i.gluco_dose_known_high, DATEDIFF(c.COVID_first_PCR_or_AG_lab_positive, i.drug_exposure_start_date) as days_immuno_before_visit, 
---Creating indicator variable for immunosuppression
CASE 
    WHEN (i.drug_exposure_start_date IS NOT NULL) then 1 else 0
    END AS immunosupp, 
---To help rule out steroids FOR covid, we are interested in chronic prescriptions present before COVID
---June 14: adding the 14 days prior to admission criteria, to get rid of precipitating factors like pred for COPD flare which is really COVID
CASE WHEN (i.glucocorticoid=1 AND i.drug_exposure_start_date < c.COVID_first_PCR_or_AG_lab_positive) then 1 else 0
    END AS steroids_before_covid,
CASE WHEN (i.glucocorticoid=1 AND i.drug_exposure_start_date >= c.COVID_first_PCR_or_AG_lab_positive) then 1 else 0
    END AS steroids_during_covid
---Merging the immunosuppression exposures table with the COVID+ hospitalizations 
FROM patients_on_immunosuppressant i
inner join Cohort_covid_final c
on i.person_id = c.person_id
---Restricting to pre-admission meds
and c.COVID_first_PCR_or_AG_lab_positive > i.drug_exposure_start_date
---Making sure they weren't stopped prior to admisssion  
and (i.drug_exposure_end_date is NULL or c.COVID_first_PCR_or_AG_lab_positive <= i.drug_exposure_end_date)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.44739c27-18cc-4407-b005-b7741600e2b6"),
    drug_exposure=Input(rid="ri.foundry.main.dataset.ec252b05-8f82-4f7f-a227-b3bb9bc578ef")
)
SELECT *
FROM drug_exposure
WHERE drug_exposure_start_date >='2020-01-01' and data_partner_id NOT IN ('117','565','655','38','285','966','224','41','578','901','181','170')

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.25cebf04-8ac0-4291-8e1d-17916d00a4e6"),
    concept=Input(rid="ri.foundry.main.dataset.5cb3c4a3-327a-47bf-a8bf-daf0cafe6772")
)
---https://doi.org/10.1016/S2665-9913(21)00325-8 ( drug concept is taken from this paper)

SELECT concept_id, concept_name
FROM concept
WHERE /*atc code l04aa - selective immunosuppressants*/ 
lcase(concept_name) like '%muromonab-cd3%'
or lcase(concept_name) like '%antilymphocyte immunoglobulin (horse)%'
or lcase(concept_name) like '%antithmyocyte immunoglobulin (rabbit)%'
or lcase(concept_name) like '%mycophenolic acid%'
or lcase(concept_name) like '%mycophenolate sodium%'
or lcase(concept_name) like '%mycophenolate mofetil%'
or lcase(concept_name) like '%sirolimus%'
or lcase(concept_name) like '%leflunomide%'
or lcase(concept_name) like '%alefacept%'
or lcase(concept_name) like '%everolimus%'
or lcase(concept_name) like '%gusperimus%'
or lcase(concept_name) like '%efalizumab%'
or lcase(concept_name) like '%abetimus%' 
or lcase(concept_name) like '%natalizumab%'
or lcase(concept_name) like '%abatacept%'
or lcase(concept_name) like '%eculizumab%'
or lcase(concept_name) like '%belimumab%'
or lcase(concept_name) like '%fingolimod%'
or lcase(concept_name) like '%belatacept%'
or lcase(concept_name) like '%tofacitinib%'
or lcase(concept_name) like '%teriflunomide%'
or lcase(concept_name) like '%apremilast%'
or lcase(concept_name) like '%vedolizumab%'
or lcase(concept_name) like '%alemtuzumab%'
or lcase(concept_name) like '%begelomab%'
or lcase(concept_name) like '%ocrelizumab%'
or lcase(concept_name) like '%baricitinib%'
or lcase(concept_name) like '%ozanimod%'
or lcase(concept_name) like '%emapalumab%'
or lcase(concept_name) like '%cladribine%'
or lcase(concept_name) like '%imlifidase%'
or lcase(concept_name) like '%siponimod%'
or lcase(concept_name) like '%ravulizumab%'
or lcase(concept_name) like '%upadacitinib%' 
/*atc code l04ab - tnf alpha inhibitors*/
or lcase(concept_name) like '%etanercept%'
or lcase(concept_name) like '%infliximab%'
or lcase(concept_name) like '%afelimomab%'
or lcase(concept_name) like '%adalimumab%'
or lcase(concept_name) like '%certolizumab pegol%'
or lcase(concept_name) like '%golimumab%'
or lcase(concept_name) like '%opinercept%' 
/*atc code l04ac - interleukin inhibitors*/
or lcase(concept_name) like '%daciluzumab%'
or lcase(concept_name) like '%basiliximab%'
or lcase(concept_name) like '%anakinra%'
or lcase(concept_name) like '%rilonacept%'
or lcase(concept_name) like '%ustekinumab%'
or lcase(concept_name) like '%tocilizumab%'
or lcase(concept_name) like '%canakinumab%'
or lcase(concept_name) like '%briakinumab%'
or lcase(concept_name) like '%secukinumab%'
or lcase(concept_name) like '%siltuximab%'
or lcase(concept_name) like '%brodalumab%'
or lcase(concept_name) like '%ixekizumab%'
or lcase(concept_name) like '%sarilumab%'
or lcase(concept_name) like '%sirukumab%'
or lcase(concept_name) like '%guselkumab%'
or lcase(concept_name) like '%tildrakizumab%'
or lcase(concept_name) like '%risankizumab%'
/*atc code l04ad - calcineurin inhibitors*/
or lcase(concept_name) like '%ciclosporin%'
or lcase(concept_name) like '%cyclosporin%'
or lcase(concept_name) like '%tacrolimus%'
or lcase(concept_name) like '%voclosporin%' 
/*atc code l04ax - other immunosuppressants*/
or lcase(concept_name) like '%azathioprine%'
or lcase(concept_name) like '%thalidomide%'
or lcase(concept_name) like '%lenalidomide%'
or lcase(concept_name) like '%pirfenidone%'
or lcase(concept_name) like '%pomalidomide%'
or lcase(concept_name) like '%dimethyl fumarate%'
or lcase(concept_name) like '%darvadstrocel%'
/*oral glucocorticoids*/
or lcase(concept_name) like '%dexamethasone%'
or lcase(concept_name) like '%prednisone%'
or lcase(concept_name) like '%prednisolone%'
or lcase(concept_name) like '%methylprednisolone%'
/*l01aa nitrogen mustard analogues*/
or lcase(concept_name) like '%cyclophosphamide%'
or lcase(concept_name) like '%chlorambucil%'
or lcase(concept_name) like '%melphalan%'
or lcase(concept_name) like '%chlormethine%'
or lcase(concept_name) like '%ifosfamide%'
or lcase(concept_name) like '%trofosfamide%'
or lcase(concept_name) like '%prednimustine%'
or lcase(concept_name) like '%bendamustine%' 
/*l01ab alkyl sulfonates*/
or lcase(concept_name) like '%busulfan%'
or lcase(concept_name) like '%treosulfan%'
or lcase(concept_name) like '%mannosulfan%' 
/*l01ac ethylene imines*/
or lcase(concept_name) like '%thiotepa%'
or lcase(concept_name) like '%triaziquone%'
or lcase(concept_name) like '%carboquone%' 
/*l01ad nitrosoureas*/
or lcase(concept_name) like '%carmustine%'
or lcase(concept_name) like '%lomustine%'
or lcase(concept_name) like '%semustine%'
or lcase(concept_name) like '%streptozocin%'
or lcase(concept_name) like '%fotemustine%'
or lcase(concept_name) like '%nimustine%'
or lcase(concept_name) like '%ranimustine%'
or lcase(concept_name) like '%uramustine%' 
/*l01ag epoxides*/
or lcase(concept_name) like '%etoglucid%'
/*l01ax other alkylating agents*/
or lcase(concept_name) like '%mitobronitol%'
or lcase(concept_name) like '%pipobroman%'
or lcase(concept_name) like '%temozolomide%'
or lcase(concept_name) like '%dacarbazine%' 
/*l01ba folic acid analogues*/
or lcase(concept_name) like '%methotrexate%'
or lcase(concept_name) like '%raltitrexed%'
or lcase(concept_name) like '%pemetrexed%'
or lcase(concept_name) like '%pralatrexate%' 
/*l01bb purine analogues*/
or lcase(concept_name) like '%mercaptopurine%'
or lcase(concept_name) like '%tioguanine%'
or lcase(concept_name) like '%cladribine%'
or lcase(concept_name) like '%fludarabine%'
or lcase(concept_name) like '%clofarabine%'
or lcase(concept_name) like '%nelarabine%'
or lcase(concept_name) like '%rabacfosadine%' 
/*l01bc pyrimidine analogues*/
or lcase(concept_name) like '%cytarabine%'
or lcase(concept_name) like '%fluorouracil%'
or lcase(concept_name) like '%tegafur%'
or lcase(concept_name) like '%carmofur%'
or lcase(concept_name) like '%gemcitabine%'
or lcase(concept_name) like '%capecitabine%'
or lcase(concept_name) like '%azacitidine%'
or lcase(concept_name) like '%decitabine%'
or lcase(concept_name) like '%floxuridine%'
or lcase(concept_name) like '%fluorouracil%'
or lcase(concept_name) like '%tegafur%'
or lcase(concept_name) like '%trifluridine%' 
/*l01ca vinca alkaloids and analogues*/
or lcase(concept_name) like '%vinblastine%'
or lcase(concept_name) like '%vincristine%'
or lcase(concept_name) like '%vindesine%'
or lcase(concept_name) like '%vinorelbine%'
or lcase(concept_name) like '%vinflunine%'
or lcase(concept_name) like '%vintafolide%' 
/*l01cb podophyllotoxin derivatives*/
or lcase(concept_name) like '%etoposide%'
or lcase(concept_name) like '%teniposide%' 
/*l01cc colchicine derivatives*/
or lcase(concept_name) like '%demecolcine%' 
/*l01cd taxanes*/
or lcase(concept_name) like '%paclitaxel%'
or lcase(concept_name) like '%docetaxel%'
or lcase(concept_name) like '%paclitaxel poliglumex%'
or lcase(concept_name) like '%cabazitaxel%' 
/*l01cx other plant alkaloids and natural products*/
or lcase(concept_name) like '%trabectedin%' 
/*l01da actinomycines*/
or lcase(concept_name) like '%dactinomycin%' 
/*l01db anthracyclines and related substances*/
or lcase(concept_name) like '%doxorubicin%'
or lcase(concept_name) like '%daunorubicin%'
or lcase(concept_name) like '%epirubicin%'
or lcase(concept_name) like '%aclarubicin%'
or lcase(concept_name) like '%zorubicin%'
or lcase(concept_name) like '%idarubicin%'
or lcase(concept_name) like '%mitoxantrone%'
or lcase(concept_name) like '%pirarubicin%' 
or lcase(concept_name) like '%valrubicin%'
or lcase(concept_name) like '%amrubicin%'
or lcase(concept_name) like '%pixantrone%' 
/*l01dc other cytotoxic antibiotics*/
or lcase(concept_name) like '%bleomycin%'
or lcase(concept_name) like '%plicamycin%'
or lcase(concept_name) like '%mitomycin%'
or lcase(concept_name) like '%ixabepilone%' 
/*l01xa platinum compounds*/
or lcase(concept_name) like '%cisplatin%'
or lcase(concept_name) like '%carboplatin%'
or lcase(concept_name) like '%oxaliplatin%'
or lcase(concept_name) like '%satraplatin%'
or lcase(concept_name) like '%polyplatillen%' 
/*l01xb methylhydrazines*/
or lcase(concept_name) like '%procarbazine%' 
/*l01xc monoclonal antibodies*/ 
or lcase(concept_name) like '%edrecolomab%'
or lcase(concept_name) like '%rituximab%'
or lcase(concept_name) like '%trastuzumab%'
or lcase(concept_name) like '%gemtuzumab ozogamicin%'
or lcase(concept_name) like '%cetuximab%'
or lcase(concept_name) like '%bevacizumab%'
or lcase(concept_name) like '%panitumumab%'
or lcase(concept_name) like '%catumaxomab%'
or lcase(concept_name) like '%ofatumumab%' 
or lcase(concept_name) like '%ipilimumab%'
or lcase(concept_name) like '%brentuximab vedotin%'
or lcase(concept_name) like '%pertuzumab%'
or lcase(concept_name) like '%trastuzumab emtansine%'
or lcase(concept_name) like '%obinutuzumab%'
or lcase(concept_name) like '%dinutuximab beta%'
or lcase(concept_name) like '%nivolumab%'
or lcase(concept_name) like '%pembrolizumab%'
or lcase(concept_name) like '%blinatumomab%' 
or lcase(concept_name) like '%ramucirumab%'
or lcase(concept_name) like '%necitumumab%'
or lcase(concept_name) like '%elotuzumab%'
or lcase(concept_name) like '%daratumumab%'
or lcase(concept_name) like '%mogamulizumab%'
or lcase(concept_name) like '%inotuzumab ozogamicin%'
or lcase(concept_name) like '%olaratumab%'
or lcase(concept_name) like '%durvalumab%'
or lcase(concept_name) like '%bermekimab%'
or lcase(concept_name) like '%avelumab%' 
or lcase(concept_name) like '%atezolizumab%'
or lcase(concept_name) like '%cemiplimab%' 
/*l01xd sensitizers used in photodynamic/radiation therapy*/
or lcase(concept_name) like '%porfimer sodium%'
or lcase(concept_name) like '%methyl aminolevulinate%'
or lcase(concept_name) like '%aminolevulinic acid%'
or lcase(concept_name) like '%temoporfin%'
or lcase(concept_name) like '%efaproxiral%'
or lcase(concept_name) like '%padeliporfin%' 
/*l01xe protein kinase inhibitors*/
or lcase(concept_name) like '%imatinib%'
or lcase(concept_name) like '%gefitinib%'
or lcase(concept_name) like '%erlotinib%'
or lcase(concept_name) like '%sunitinib%'
or lcase(concept_name) like '%sorafenib%'
or lcase(concept_name) like '%dasatinib%'
or lcase(concept_name) like '%lapatinib%'
or lcase(concept_name) like '%nilotinib%'
or lcase(concept_name) like '%temsirolimus%'
or lcase(concept_name) like '%everolimus%' 
or lcase(concept_name) like '%pazopanib%'
or lcase(concept_name) like '%vandetanib%'
or lcase(concept_name) like '%afatinib%'
or lcase(concept_name) like '%bosutinib%'
or lcase(concept_name) like '%vemurafenib%'
or lcase(concept_name) like '%crizotinib%'
or lcase(concept_name) like '%axitinib%'
or lcase(concept_name) like '%ruxolitinib%'
or lcase(concept_name) like '%ridaforolimus%'
or lcase(concept_name) like '%regorafenib%'
or lcase(concept_name) like '%masitinib%' 
or lcase(concept_name) like '%dabrafenib%'
or lcase(concept_name) like '%ponatinib%'
or lcase(concept_name) like '%trametinib%'
or lcase(concept_name) like '%cabozantinib%'
or lcase(concept_name) like '%ibrutinib%'
or lcase(concept_name) like '%ceritinib%'
or lcase(concept_name) like '%lenvatinib%'
or lcase(concept_name) like '%nintedanib%'
or lcase(concept_name) like '%cediranib%'
or lcase(concept_name) like '%palbociclib%'
or lcase(concept_name) like '%tivozanib%' 
or lcase(concept_name) like '%osimertinib%'
or lcase(concept_name) like '%alectinib%'
or lcase(concept_name) like '%rociletinib%'
or lcase(concept_name) like '%cobimetinib%'
or lcase(concept_name) like '%midostaurin%'
or lcase(concept_name) like '%olmutinib%'
or lcase(concept_name) like '%binimetinib%'
or lcase(concept_name) like '%ribociclib%'
or lcase(concept_name) like '%brigatinib%'
or lcase(concept_name) like '%lorlatinib%'
or lcase(concept_name) like '%neratinib%' 
or lcase(concept_name) like '%encorafenib%'
or lcase(concept_name) like '%dacomitinib%'
or lcase(concept_name) like '%icotinib%'
or lcase(concept_name) like '%abemaciclib%'
or lcase(concept_name) like '%acalabrutinib%'
or lcase(concept_name) like '%quizartinib%'
or lcase(concept_name) like '%larotrectinib%'
or lcase(concept_name) like '%gilteritinib%'
or lcase(concept_name) like '%entrectinib%'
or lcase(concept_name) like '%fedratinib%' 
or lcase(concept_name) like '%toceranib%' 
/*l01xx other antineoplastic agents*/
or lcase(concept_name) like '%amsacrine%'
or lcase(concept_name) like '%asparaginase%'
or lcase(concept_name) like '%altretamine%'
or lcase(concept_name) like '%hydroxycarbamide%'
or lcase(concept_name) like '%lonidamine%'
or lcase(concept_name) like '%pentostatin%'
or lcase(concept_name) like '%masoprocol%'
or lcase(concept_name) like '%estramustine%'
/*'tretinoin',*/ 
or lcase(concept_name) like '%mitoguazone%'
or lcase(concept_name) like '%topotecan%'
or lcase(concept_name) like '%tiazofurine%'
or lcase(concept_name) like '%irinotecan%'
or lcase(concept_name) like '%alitretinoin%'
or lcase(concept_name) like '%mitotane%'
or lcase(concept_name) like '%pegaspargase%'
or lcase(concept_name) like '%bexarotene%'
or lcase(concept_name) like '%arsenic trioxide%'
or lcase(concept_name) like '%denileukin diftitox%' 
or lcase(concept_name) like '%bortezomib%'
or lcase(concept_name) like '%anagrelide%'
or lcase(concept_name) like '%oblimersen%'
or lcase(concept_name) like '%sitimagene ceradenovec%'
or lcase(concept_name) like '%vorinostat%'
or lcase(concept_name) like '%romidepsin%'
or lcase(concept_name) like '%omacetaxine mepesuccinate%'
or lcase(concept_name) like '%eribulin%'
or lcase(concept_name) like '%panobinostat%' 
or lcase(concept_name) like '%vismodegib%'
or lcase(concept_name) like '%aflibercept%'
or lcase(concept_name) like '%carfilzomib%'
or lcase(concept_name) like '%olaparib%'
or lcase(concept_name) like '%idelalisib%'
or lcase(concept_name) like '%sonidegib%'
or lcase(concept_name) like '%belinostat%'
or lcase(concept_name) like '%ixazomib%'
or lcase(concept_name) like '%talimogene laherparepvec%'
or lcase(concept_name) like '%venetoclax%' 
or lcase(concept_name) like '%vosaroxin%'
or lcase(concept_name) like '%niraparib%'
or lcase(concept_name) like '%rucaparib%'
or lcase(concept_name) like '%etirinotecan pegol%'
or lcase(concept_name) like '%plitidepsin%'
or lcase(concept_name) like '%epacadostat%'
or lcase(concept_name) like '%enasidenib%'
or lcase(concept_name) like '%talazoparib%'
or lcase(concept_name) like '%copanlisib%'
or lcase(concept_name) like '%ivosidenib%' 
or lcase(concept_name) like '%glasdegib%'
or lcase(concept_name) like '%entinostat%'
or lcase(concept_name) like '%alpelisib%'
or lcase(concept_name) like '%selinexor%'
or lcase(concept_name) like '%tagraxofusp%'
or lcase(concept_name) like '%belotecan%'
or lcase(concept_name) like '%tigilanol tiglate%' 
/*l01xy combinations of antineoplastic agents*/
or lcase(concept_name) like '%cytarabine%';

---303 unique drugs

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1fa8bfdb-7ea6-4f9b-a47f-2e87c157c613"),
    drug_exposure=Input(rid="ri.foundry.main.dataset.ec252b05-8f82-4f7f-a227-b3bb9bc578ef"),
    immunosuppressive_drugs_data=Input(rid="ri.foundry.main.dataset.25cebf04-8ac0-4291-8e1d-17916d00a4e6")
)
--- Here we have mapped drug concept id with immunosupressants concept ids
SELECT d.person_id, d.drug_concept_name, d.drug_exposure_start_date, d.drug_exposure_end_date, i.concept_name
FROM drug_exposure d
inner join immunosuppressive_drugs_data i
    on d.drug_concept_id = i.concept_id;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c9225f30-c260-4a9e-86b9-00d517ae02c7"),
    drug_exposure_after01012020=Input(rid="ri.foundry.main.dataset.44739c27-18cc-4407-b005-b7741600e2b6")
)
-- drug concept ids taken from Hythem Sidky codework book for Molnupiravir
SELECT *
FROM drug_exposure_after01012020
WHERE drug_concept_id IN (702536,
702537,
702538,
702539,
702540,
702541,
1109978,
1145015,
36802190,
36900085,
36900086,
36900651,
36901949)

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f95bd294-8261-4f82-9381-7173ca919998"),
    immunosuppressive_drugs_exposures=Input(rid="ri.foundry.main.dataset.1fa8bfdb-7ea6-4f9b-a47f-2e87c157c613")
)
/*
Here we cotegorised 303 immunosuppressants into 15 cotegories ( anthracyclines, checkpoint_inhibitor, pk_inhibitor, etc. )
*/
select *,
case when /*l01db anthracyclines and related substances*/
(lcase(concept_name) like '%doxorubicin%'
or lcase(concept_name) like '%daunorubicin%'
or lcase(concept_name) like '%epirubicin%'
or lcase(concept_name) like '%aclarubicin%'
or lcase(concept_name) like '%zorubicin%'
or lcase(concept_name) like '%idarubicin%'
or lcase(concept_name) like '%mitoxantrone%'
or lcase(concept_name) like '%pirarubicin%' 
or lcase(concept_name) like '%valrubicin%'
or lcase(concept_name) like '%amrubicin%'
or lcase(concept_name) like '%pixantrone%') then 1 else 0
end as anthracyclines,

case when /*selected from l01xc*/
(lcase(concept_name) like '%ipilimumab%'
or lcase(concept_name) like '%nivolumab%'
or lcase(concept_name) like '%pembrolizumab%'
or lcase(concept_name) like '%avelumab%' 
or lcase(concept_name) like '%atezolizumab%'
or lcase(concept_name) like '%cemiplimab%' 
or lcase(concept_name) like '%durvalumab%') then 1 else 0
end as checkpoint_inhibitor,

case when lcase(concept_name) like '%cyclophosphamide%' then 1 else 0
end as cyclophosphamide,

case when /*l01xe protein kinase inhibitors*/
(lcase(concept_name) like '%imatinib%'
or lcase(concept_name) like '%gefitinib%'
or lcase(concept_name) like '%erlotinib%'
or lcase(concept_name) like '%sunitinib%'
or lcase(concept_name) like '%sorafenib%'
or lcase(concept_name) like '%dasatinib%'
or lcase(concept_name) like '%lapatinib%'
or lcase(concept_name) like '%nilotinib%'
or lcase(concept_name) like '%temsirolimus%'
or lcase(concept_name) like '%everolimus%' 
or lcase(concept_name) like '%pazopanib%'
or lcase(concept_name) like '%vandetanib%'
or lcase(concept_name) like '%afatinib%'
or lcase(concept_name) like '%bosutinib%'
or lcase(concept_name) like '%vemurafenib%'
or lcase(concept_name) like '%crizotinib%'
or lcase(concept_name) like '%axitinib%'
or lcase(concept_name) like '%ruxolitinib%'
or lcase(concept_name) like '%ridaforolimus%'
or lcase(concept_name) like '%regorafenib%'
or lcase(concept_name) like '%masitinib%' 
or lcase(concept_name) like '%dabrafenib%'
or lcase(concept_name) like '%ponatinib%'
or lcase(concept_name) like '%trametinib%'
or lcase(concept_name) like '%cabozantinib%'
or lcase(concept_name) like '%ibrutinib%'
or lcase(concept_name) like '%ceritinib%'
or lcase(concept_name) like '%lenvatinib%'
or lcase(concept_name) like '%nintedanib%'
or lcase(concept_name) like '%cediranib%'
or lcase(concept_name) like '%palbociclib%'
or lcase(concept_name) like '%tivozanib%' 
or lcase(concept_name) like '%osimertinib%'
or lcase(concept_name) like '%alectinib%'
or lcase(concept_name) like '%rociletinib%'
or lcase(concept_name) like '%cobimetinib%'
or lcase(concept_name) like '%midostaurin%'
or lcase(concept_name) like '%olmutinib%'
or lcase(concept_name) like '%binimetinib%'
or lcase(concept_name) like '%ribociclib%'
or lcase(concept_name) like '%brigatinib%'
or lcase(concept_name) like '%lorlatinib%'
or lcase(concept_name) like '%neratinib%' 
or lcase(concept_name) like '%encorafenib%'
or lcase(concept_name) like '%dacomitinib%'
or lcase(concept_name) like '%icotinib%'
or lcase(concept_name) like '%abemaciclib%'
or lcase(concept_name) like '%acalabrutinib%'
or lcase(concept_name) like '%quizartinib%'
or lcase(concept_name) like '%larotrectinib%'
or lcase(concept_name) like '%gilteritinib%'
or lcase(concept_name) like '%entrectinib%'
or lcase(concept_name) like '%fedratinib%' 
or lcase(concept_name) like '%toceranib%' ) then 1 else 0
end as pk_inhibitor,

case when
lcase(concept_name) like '%rituximab%' then 1 else 0
end as rituximab,

case when ( /*other monoclonals: will separate out when sample size permits*/
/*l01xc monoclonal antibodies*/ 
lcase(concept_name) like '%edrecolomab%'
or lcase(concept_name) like '%trastuzumab%'
or lcase(concept_name) like '%gemtuzumab ozogamicin%'
or lcase(concept_name) like '%cetuximab%'
or lcase(concept_name) like '%bevacizumab%'
or lcase(concept_name) like '%panitumumab%'
or lcase(concept_name) like '%catumaxomab%'
or lcase(concept_name) like '%ofatumumab%' 
or lcase(concept_name) like '%brentuximab vedotin%'
or lcase(concept_name) like '%pertuzumab%'
or lcase(concept_name) like '%trastuzumab emtansine%'
or lcase(concept_name) like '%obinutuzumab%'
or lcase(concept_name) like '%dinutuximab beta%'
or lcase(concept_name) like '%blinatumomab%' 
or lcase(concept_name) like '%ramucirumab%'
or lcase(concept_name) like '%necitumumab%'
or lcase(concept_name) like '%elotuzumab%'
or lcase(concept_name) like '%daratumumab%'
or lcase(concept_name) like '%mogamulizumab%'
or lcase(concept_name) like '%inotuzumab ozogamicin%'
or lcase(concept_name) like '%olaratumab%'
or lcase(concept_name) like '%bermekimab%') then 1 else 0
end as monoclonal_other,

case when /*leftover from the l01 category*/
(/*l01aa nitrogen mustard analogues*/
lcase(concept_name) like '%chlorambucil%'
or lcase(concept_name) like '%melphalan%'
or lcase(concept_name) like '%chlormethine%'
or lcase(concept_name) like '%ifosfamide%'
or lcase(concept_name) like '%trofosfamide%'
or lcase(concept_name) like '%prednimustine%'
or lcase(concept_name) like '%bendamustine%' 
/*l01ab alkyl sulfonates*/
or lcase(concept_name) like '%busulfan%'
or lcase(concept_name) like '%treosulfan%'
or lcase(concept_name) like '%mannosulfan%' 
/*l01ac ethylene imines*/
or lcase(concept_name) like '%thiotepa%'
or lcase(concept_name) like '%triaziquone%'
or lcase(concept_name) like '%carboquone%' 
/*l01ad nitrosoureas*/
or lcase(concept_name) like '%carmustine%'
or lcase(concept_name) like '%lomustine%'
or lcase(concept_name) like '%semustine%'
or lcase(concept_name) like '%streptozocin%'
or lcase(concept_name) like '%fotemustine%'
or lcase(concept_name) like '%nimustine%'
or lcase(concept_name) like '%ranimustine%'
or lcase(concept_name) like '%uramustine%' 
/*l01ag epoxides*/
or lcase(concept_name) like '%etoglucid%'
/*l01ax other alkylating agents*/
or lcase(concept_name) like '%mitobronitol%'
or lcase(concept_name) like '%pipobroman%'
or lcase(concept_name) like '%temozolomide%'
or lcase(concept_name) like '%dacarbazine%' 
/*l01ba folic acid analogues*/
or lcase(concept_name) like '%methotrexate%'
or lcase(concept_name) like '%raltitrexed%'
or lcase(concept_name) like '%pemetrexed%'
or lcase(concept_name) like '%pralatrexate%' 
/*l01bb purine analogues*/
or lcase(concept_name) like '%mercaptopurine%'
or lcase(concept_name) like '%tioguanine%'
or lcase(concept_name) like '%cladribine%'
or lcase(concept_name) like '%fludarabine%'
or lcase(concept_name) like '%clofarabine%'
or lcase(concept_name) like '%nelarabine%'
or lcase(concept_name) like '%rabacfosadine%' 
/*l01bc pyrimidine analogues*/
or lcase(concept_name) like '%cytarabine%'
or lcase(concept_name) like '%fluorouracil%'
or lcase(concept_name) like '%tegafur%'
or lcase(concept_name) like '%carmofur%'
or lcase(concept_name) like '%gemcitabine%'
or lcase(concept_name) like '%capecitabine%'
or lcase(concept_name) like '%azacitidine%'
or lcase(concept_name) like '%decitabine%'
or lcase(concept_name) like '%floxuridine%'
or lcase(concept_name) like '%fluorouracil%'
or lcase(concept_name) like '%tegafur%'
or lcase(concept_name) like '%trifluridine%' 
/*l01ca vinca alkaloids and analogues*/
or lcase(concept_name) like '%vinblastine%'
or lcase(concept_name) like '%vincristine%'
or lcase(concept_name) like '%vindesine%'
or lcase(concept_name) like '%vinorelbine%'
or lcase(concept_name) like '%vinflunine%'
or lcase(concept_name) like '%vintafolide%' 
/*l01cb podophyllotoxin derivatives*/
or lcase(concept_name) like '%etoposide%'
or lcase(concept_name) like '%teniposide%' 
/*l01cc colchicine derivatives*/
or lcase(concept_name) like '%demecolcine%' 
/*l01cd taxanes*/
or lcase(concept_name) like '%paclitaxel%'
or lcase(concept_name) like '%docetaxel%'
or lcase(concept_name) like '%paclitaxel poliglumex%'
or lcase(concept_name) like '%cabazitaxel%' 
/*l01cx other plant alkaloids and natural products*/
or lcase(concept_name) like '%trabectedin%' 
/*l01da actinomycines*/
or lcase(concept_name) like '%dactinomycin%' 
/*l01dc other cytotoxic antibiotics*/
or lcase(concept_name) like '%bleomycin%'
or lcase(concept_name) like '%plicamycin%'
or lcase(concept_name) like '%mitomycin%'
or lcase(concept_name) like '%ixabepilone%' 
/*l01xa platinum compounds*/
or lcase(concept_name) like '%cisplatin%'
or lcase(concept_name) like '%carboplatin%'
or lcase(concept_name) like '%oxaliplatin%'
or lcase(concept_name) like '%satraplatin%'
or lcase(concept_name) like '%polyplatillen%' 
/*l01xb methylhydrazines*/
or lcase(concept_name) like '%procarbazine%'
/*l01xd sensitizers used in photodynamic/radiation therapy*/
or lcase(concept_name) like '%porfimer sodium%'
or lcase(concept_name) like '%methyl aminolevulinate%'
or lcase(concept_name) like '%aminolevulinic acid%'
or lcase(concept_name) like '%temoporfin%'
or lcase(concept_name) like '%efaproxiral%'
or lcase(concept_name) like '%padeliporfin%' 
/*l01xx other antineoplastic agents*/
or lcase(concept_name) like '%amsacrine%'
or lcase(concept_name) like '%asparaginase%'
or lcase(concept_name) like '%altretamine%'
or lcase(concept_name) like '%hydroxycarbamide%'
or lcase(concept_name) like '%lonidamine%'
or lcase(concept_name) like '%pentostatin%'
or lcase(concept_name) like '%masoprocol%'
or lcase(concept_name) like '%estramustine%'
/*'tretinoin',*/ 
or lcase(concept_name) like '%mitoguazone%'
or lcase(concept_name) like '%topotecan%'
or lcase(concept_name) like '%tiazofurine%'
or lcase(concept_name) like '%irinotecan%'
or lcase(concept_name) like '%alitretinoin%'
or lcase(concept_name) like '%mitotane%'
or lcase(concept_name) like '%pegaspargase%'
or lcase(concept_name) like '%bexarotene%'
or lcase(concept_name) like '%arsenic trioxide%'
or lcase(concept_name) like '%denileukin diftitox%' 
or lcase(concept_name) like '%bortezomib%'
or lcase(concept_name) like '%anagrelide%'
or lcase(concept_name) like '%oblimersen%'
or lcase(concept_name) like '%sitimagene ceradenovec%'
or lcase(concept_name) like '%vorinostat%'
or lcase(concept_name) like '%romidepsin%'
or lcase(concept_name) like '%omacetaxine mepesuccinate%'
or lcase(concept_name) like '%eribulin%'
or lcase(concept_name) like '%panobinostat%' 
or lcase(concept_name) like '%vismodegib%'
or lcase(concept_name) like '%aflibercept%'
or lcase(concept_name) like '%carfilzomib%'
or lcase(concept_name) like '%olaparib%'
or lcase(concept_name) like '%idelalisib%'
or lcase(concept_name) like '%sonidegib%'
or lcase(concept_name) like '%belinostat%'
or lcase(concept_name) like '%ixazomib%'
or lcase(concept_name) like '%talimogene laherparepvec%'
or lcase(concept_name) like '%venetoclax%' 
or lcase(concept_name) like '%vosaroxin%'
or lcase(concept_name) like '%niraparib%'
or lcase(concept_name) like '%rucaparib%'
or lcase(concept_name) like '%etirinotecan pegol%'
or lcase(concept_name) like '%plitidepsin%'
or lcase(concept_name) like '%epacadostat%'
or lcase(concept_name) like '%enasidenib%'
or lcase(concept_name) like '%talazoparib%'
or lcase(concept_name) like '%copanlisib%'
or lcase(concept_name) like '%ivosidenib%' 
or lcase(concept_name) like '%glasdegib%'
or lcase(concept_name) like '%entinostat%'
or lcase(concept_name) like '%alpelisib%'
or lcase(concept_name) like '%selinexor%'
or lcase(concept_name) like '%tagraxofusp%'
or lcase(concept_name) like '%belotecan%'
or lcase(concept_name) like '%tigilanol tiglate%' 
/*l01xy combinations of antineoplastic agents*/
or lcase(concept_name) like '%cytarabine%') then 1 else 0
end as l01_other,

case when (lcase(concept_name) like '%azathioprine%') then 1 else 0
end as azathioprine,

case
    when (lcase(concept_name) like '%ciclosporin%'
or lcase(concept_name) like '%cyclosporin%'
or lcase(concept_name) like '%tacrolimus%'
or lcase(concept_name) like '%voclosporin%') then 1 else 0
end as calcineurin_inhibitor,

case
    when (lcase(concept_name) like '%daciluzumab%'
or lcase(concept_name) like '%basiliximab%'
or lcase(concept_name) like '%anakinra%'
or lcase(concept_name) like '%rilonacept%'
or lcase(concept_name) like '%ustekinumab%'
or lcase(concept_name) like '%tocilizumab%'
or lcase(concept_name) like '%canakinumab%'
or lcase(concept_name) like '%briakinumab%'
or lcase(concept_name) like '%secukinumab%'
or lcase(concept_name) like '%siltuximab%'
or lcase(concept_name) like '%brodalumab%'
or lcase(concept_name) like '%ixekizumab%'
or lcase(concept_name) like '%sarilumab%'
or lcase(concept_name) like '%sirukumab%'
or lcase(concept_name) like '%guselkumab%'
or lcase(concept_name) like '%tildrakizumab%'
or lcase(concept_name) like '%risankizumab%') then 1 else 0
end as il_inhibitor ,

case when (/*janus kinase (jak) inhibitors*/
lcase(concept_name) like '%tofacitinib%'
or lcase(concept_name) like '%baricitinib%'
or lcase(concept_name) like '%upadacitinib%' ) then 1 else 0
end as jak_inhibitor,

case when /*mycophenolate and derivates*/
(lcase(concept_name) like '%mycophenolic acid%'
or lcase(concept_name) like '%mycophenolate sodium%'
or lcase(concept_name) like '%mycophenolate mofetil%') then 1 else 0
end as mycophenol,

case 
    when (lcase(concept_name) like '%etanercept%'
or lcase(concept_name) like '%infliximab%'
or lcase(concept_name) like '%afelimomab%'
or lcase(concept_name) like '%adalimumab%'
or lcase(concept_name) like '%certolizumab pegol%'
or lcase(concept_name) like '%golimumab%'
or lcase(concept_name) like '%opinercept%')  then 1 else 0
end as tnf_inhibitor,

case when /*all else left from l04*/
(/*atc code l04aa - selective immunosuppressants*/ 
lcase(concept_name) like '%muromonab-cd3%'
or lcase(concept_name) like '%antilymphocyte immunoglobulin (horse)%'
or lcase(concept_name) like '%antithmyocyte immunoglobulin (rabbit)%'
or lcase(concept_name) like '%sirolimus%'
or lcase(concept_name) like '%leflunomide%'
or lcase(concept_name) like '%alefacept%'
or lcase(concept_name) like '%everolimus%'
or lcase(concept_name) like '%gusperimus%'
or lcase(concept_name) like '%efalizumab%'
or lcase(concept_name) like '%abetimus%' 
or lcase(concept_name) like '%natalizumab%'
or lcase(concept_name) like '%abatacept%'
or lcase(concept_name) like '%eculizumab%'
or lcase(concept_name) like '%belimumab%'
or lcase(concept_name) like '%fingolimod%'
or lcase(concept_name) like '%belatacept%'
or lcase(concept_name) like '%teriflunomide%'
or lcase(concept_name) like '%apremilast%'
or lcase(concept_name) like '%vedolizumab%'
or lcase(concept_name) like '%alemtuzumab%'
or lcase(concept_name) like '%begelomab%'
or lcase(concept_name) like '%ocrelizumab%'
or lcase(concept_name) like '%ozanimod%'
or lcase(concept_name) like '%emapalumab%'
or lcase(concept_name) like '%cladribine%'
or lcase(concept_name) like '%imlifidase%'
or lcase(concept_name) like '%siponimod%'
or lcase(concept_name) like '%ravulizumab%'
/*atc code l04ax - other immunosuppressants*/
or lcase(concept_name) like '%thalidomide%'
or lcase(concept_name) like '%lenalidomide%'
or lcase(concept_name) like '%pirfenidone%'
or lcase(concept_name) like '%pomalidomide%'
or lcase(concept_name) like '%dimethyl fumarate%'
or lcase(concept_name) like '%darvadstrocel%') then 1 else 0
end as l04_other,

case when (lcase(concept_name) like '%dexamethasone%'
or lcase(concept_name) like '%prednisone%'
or lcase(concept_name) like '%prednisolone%'
or lcase(concept_name) like '%methylprednisolone%') then 1 else 0
end as glucocorticoid,

case when (
lcase(drug_concept_name) like '%prednisone 5 mg delayed release oral tablet%' 
or lcase(drug_concept_name) like'prednisone 10 mg oral tablet'
or lcase(drug_concept_name) like 'prednisone 20 mg oral tablet [deltasone]'
or lcase(drug_concept_name) like 'prednisone 50 mg oral tablet'
or lcase(drug_concept_name) like 'prednisone 20 mg oral tablet [predone]'
or lcase(drug_concept_name) like 'prednisone 10 mg oral tablet [predone]'
or lcase(drug_concept_name) like '{21 (prednisone 10 mg oral tablet{ } pack'
or lcase(drug_concept_name) like '{10 (prednisone 10 mg oral tablet) } pack'
or lcase(drug_concept_name) like 'prednisone 50 mg oral tablet [orasone]'
or lcase(drug_concept_name) like 'methylprednisolone 8 mg oral tablet'
or lcase(drug_concept_name) like 'methylprednisolone 32 mg oral tablet'
or lcase(drug_concept_name) like 'prednisone 10 mg oral tablet [deltasone]'
or lcase(drug_concept_name) like 'methylprednisolone 16 mg oral tablet'
or lcase(drug_concept_name) like '{25 (prednisone 10 mg oral tablet) } pack'
or lcase(drug_concept_name) like 'methylprednisolone 16 mg oral tablet [medrol]'
or lcase(drug_concept_name) like '{48 (prednisone 10 mg oral tablet) } pack'
or lcase(drug_concept_name) like 'methylprednisolone 8 mg oral tablet [medrol]'
or lcase(drug_concept_name) like 'methylprednisolone 32 mg oral tablet [medrol]'
or lcase(drug_concept_name) like 'prednisolone 10 mg disintegrating oral tablet [orapred]'
or lcase(drug_concept_name) like 'prednisone 50 mg oral tablet [deltasone]'
or lcase(drug_concept_name) like 'prednisolone 30 mg disintegrating oral tablet [orapred]'
or lcase(drug_concept_name) like 'prednisolone 15 mg disintegrating oral tablet'
or lcase(drug_concept_name) like 'prednisolone 10 mg disintegrating oral tablet'
or lcase(drug_concept_name) like 'prednisolone 30 mg disintegrating oral tablet'
or lcase(drug_concept_name) like 'prednisolone 15 mg disintegrating oral tablet [orapred]'
or lcase(drug_concept_name) like '{21 (prednisone 10 mg oral tablet [sterapred ds]) } pack [sterapred ds uni-pack]'
or lcase(drug_concept_name) like 'methylprednisolone 100 mg'
or lcase(drug_concept_name) like 'methylprednisolone 40 mg'
or lcase(drug_concept_name) like 'dexamethasone 4 mg oral tablet'
or lcase(drug_concept_name) like 'dexamethasone 2 mg oral tablet'
or lcase(drug_concept_name) like 'dexamethaonse 6 mg oral tablet [decadron]'
or lcase(drug_concept_name) like 'dexamethasone 4 mg oral tablet [hexadrol]'
or lcase(drug_concept_name) like 'dexamethasone 4 mg oral tablet [decadron]'
or lcase(drug_concept_name) like 'dexamethasone 6 mg oral tablet'
or lcase(drug_concept_name) like 'dexamethasone 1.5 mg oral tablet [decadron]'
or lcase(drug_concept_name) like 'dexamethasone 20 mg oral tablet'
or lcase(drug_concept_name) like '{21 (dexamethasone 1.5 mg oral tablet) } pack'
or lcase(drug_concept_name) like '{25 (dexamethasone 1.5 mg oral tablet) } pack'
or lcase(drug_concept_name) like 'dexamethasone 20 mg oral tablet [hemady]'
or lcase(drug_concept_name) like '{21 (dexamethasone 1.5 mg oral tablet) } pack [dexpak taperpak 6 day]'
or lcase(drug_concept_name) like '{35 (dexamethasone 1.5 mg oral tablet) } pack'
or lcase(drug_concept_name) like '{21 (dexamethasone 1.5 mg oral tablet) } pack [zema pak 6 day]'
or lcase(drug_concept_name) like '{41 (dexamethasone 1.5 mg oral tablet) } pack'
or lcase(drug_concept_name) like '{51 (dexamethasone 1.5 mg oral tablet) } pack'
or lcase(drug_concept_name) like '25 (dexamethasone 1.5 mg oral tablet) } pack [zcort 7 day taper]'
) then 1 else 0
end as gluco_dose_known_high

FROM immunosuppressive_drugs_exposures

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.7d90d06b-f217-440c-ae83-dca5b6af99e3"),
    drug_exposure_after01012020=Input(rid="ri.foundry.main.dataset.44739c27-18cc-4407-b005-b7741600e2b6")
)
-- drug concept ids taken from Hythem Sidky codework book for paxlovid
--- branded drugs 
SELECT *
FROM drug_exposure_after01012020
where drug_concept_id IN (702578 ,702577,749191,1632579,36900156)
UNION
--component ingredients that occurred on the same day
(SELECT nirm.* FROM
(SELECT *
FROM drug_exposure_after01012020
where drug_concept_id IN (702530, 702531, 702532,702533, 702534, 702535,36501120)) nirm --nirmatrelvir
JOIN
(SELECT *
FROM drug_exposure_after01012020
where drug_concept_id IN (1748921, 1748954, 1748957, 1748959, 1748960, 1748982, 1748984, 1592434, 1592435, 19082373, 19088562, 40171778, 40171780, 40220792, 40080334, 40080337)) rit --ritonavir
ON nirm.person_id = rit.person_id AND nirm.drug_exposure_start_date = rit.drug_exposure_start_date)

