

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.3f9688d7-91b1-4b51-9171-29a2e10c345b"),
    cohort_covid_final=Input(rid="ri.foundry.main.dataset.9fda0be9-ceab-47ae-b159-bec157742c13"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86")
)
-- Created comorbidities table of final covid-19 positive patients by using it's reviewed codeset Ids
SELECT
distinct
x.person_id, cast( x.MI as INTEGER) , cast( x.CHF as INTEGER) , cast( x.PVD as INTEGER) , cast( x.stroke as INTEGER) , cast( x.dementia as INTEGER) , cast( x.pulmonary as INTEGER) , cast( x.PUD as INTEGER) , cast( x.liver_mild as INTEGER) , cast( x.liversevere as INTEGER) , cast( x.type2_diabetes as INTEGER) , cast( x.paralysis as INTEGER) , cast( x.renal as INTEGER) , cast( x.cancer as INTEGER) ,  cast( x.mets as INTEGER) ,  cast( x.hiv as INTEGER) , cast( x.multiple as INTEGER) 

FROM
(
SELECT
distinct
    person_id,
    sum(case when comorbidity = 'MI' then 1 else 0 end) MI ,
    sum(case when comorbidity = 'CHF' then 1 else 0 end) CHF ,
    sum(case when comorbidity = 'PVD' then 1 else 0 end) PVD ,
    sum(case when comorbidity = 'Stroke' then 1 else 0 end) stroke ,
    sum(case when comorbidity = 'Dementia' then 1 else 0 end) dementia ,
    sum(case when comorbidity = 'Pulmonary' then 1 else 0 end) pulmonary ,
    sum(case when comorbidity = 'PUD' then 1 else 0 end) PUD ,
    sum(case when comorbidity = 'LiverMild' then 1 else 0 end) liver_mild ,
    sum(case when comorbidity = '[VSAC] Type II Diabetes' then 1 else 0 end) type2_diabetes ,
    sum(case when comorbidity = 'Paralysis' then 1 else 0 end) paralysis ,
    sum(case when comorbidity = 'Renal' then 1 else 0 end) renal ,
    sum(case when comorbidity = 'Cancer' then 1 else 0 end) cancer ,
    sum(case when comorbidity = 'LiverSevere' then 1 else 0 end) liversevere ,
    sum(case when comorbidity = 'Mets' then 1 else 0 end) mets ,   
  --  sum(case when comorbidity = 'HIV' then 1 else 0 end) hiv, 
    sum(case when comorbidity = 'hiv infection' then 1 else 0 end) hiv,    
    case when count(*) > 1 then 1 else 0 end multiple
FROM (
SELECT 
distinct
cp.person_id,cp.COVID_first_PCR_or_AG_lab_positive,
--replace(cs.concept_set_name, 'Charlson - ','') comorbidity 
case when cs.codeset_id = 382527336 then 'hiv infection'
     when cs.codeset_id = 767642548 then 'Type 2 diabetes mellitus'
     else replace(cs.concept_set_name, 'Charlson - ','') end as comorbidity
FROM cohort_covid_final cp
left outer join condition_occurrence co on (cp.person_id = co.person_id and co.condition_start_date < cp.COVID_first_PCR_or_AG_lab_positive)
left outer join concept_set_members cs on ( cs.concept_id = co.condition_source_concept_id or cs.concept_id = co.condition_concept_id )
and cs.is_most_recent_version = true
and cs.codeset_id in ( 259495957,359043664,376881697,652711186,78746470,514953976,510748896,494981955,
 484742674,489555336,220495690,535274723,248333963,378462283,382527336 )
) t
group by t.person_id
) x

/*
Myocardial infarction	     259495957
Congestive heart failure	 359043664
Peripheral vascular disease	 376881697
Stroke	                     652711186
Dementia	                 78746470
Pulmonary disorder           514953976
Rheumatic disease	         765004404
Peptic ulcer disease	     510748896
Liver disease, mild	         494981955
Type 2Diabetes	             796221531, 484742674 [VSAC] Type II Diabetes(V2) 
Paralysis	                 489555336
Renal disease	             220495690
Cancer	                     535274723
Liver disease, severe	     248333963
Metastatic cancer	         378462283
HIV infection	             382527336 */

