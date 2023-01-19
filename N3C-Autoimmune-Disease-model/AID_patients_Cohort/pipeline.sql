

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.20f35db1-0d5e-4eaa-b8ca-cb7720c5ad8a"),
    condition_occurance_before_covid=Input(rid="ri.foundry.main.dataset.8347f79b-877d-4951-880b-ff92e4ce780c")
)
--- To extract AID patients data based on AID concept_id of eTable 2 in supplementary  
SELECT *
FROM condition_occurance_before_covid
where  (condition_concept_id in ('80809','4035611','4117686','4115161','4116151','36684997','42534836','37209323','4114444','36685023','36687003',
'37209322','36685017','374919','4145049','4102337','4178929','4137855','257628','4285717','4344158','255891','4219859','4324123','4295179','4291306',
'4316373','4178133','4066824','4281109','138711','36674282','4137430','318397','4102469','4103532','134442','4135937','255304','40485046','4103019',
'4319301','4185187','200762','36687200','441269','4160887','4146936','79833','36685172','320749','4344489','4347062','201606','81893','4074815',
'76685','43531560','374366','138717','254443','314962','255348','432588','194992','4098292','137829','437082','80182','381009','4135822',
'4290976','4058821','80800','132703','375801','135215','4160059','141933','380995','4164770','436642','432295','4146087','201257','252365','4232076',
'4101602','379008','440740','314381','46273631','4170723','4100184','4216873','436956','4116447','44808422','4142060','4182711','139899','135338',
'434621','4344165','4080146','312939','4058841','4108968','140487','4343935','4237155','137520','380747','45765493','4083100','4223448','4305666',
'37311078','443394','4148690','440108','379027','4034815','4321746','4318558','4308085','443904','4179347','4346977','4053289','4291435','4214309',
'134330','40490446','4029439','4046110','764228','43530713','4215003','760840','4185522','4048079','4301243','4048024','4041672','4297816','4080933',
'36717646','36716199','4318267'));

/*
###################################
Auto Immunedisease names:SNOMED CT ID
##################################### and  (condition_start_date <= '2022-06-30')
Rheumatoid arthritis	80809
Seropositive rheumatoid arthritis	4035611
Rheumatoid arthritis of multiple joints	4117686
Rheumatoid arthritis - hand joint	4115161
Rheumatoid arthritis of knee	4116151
Rheumatoid factor positive rheumatoid arthritis	36684997
Rheumatoid arthritis of right hand	42534836
Bilateral rheumatoid arthritis of hands	37209323
Flare of rheumatoid arthritis	4114444
Rheumatoid arthritis of right foot	36685023
Rheumatoid arthritis of bilateral hips	36687003
Bilateral rheumatoid arthritis of knees	37209322
Rheumatoid arthritis of left ankle	36685017
Multiple sclerosis	374919
Relapsing remitting multiple sclerosis	4145049
Exacerbation of multiple sclerosis	4102337
Primary progressive multiple sclerosis	4178929
Secondary progressive multiple sclerosis	4137855
Systemic lupus erythematosus	257628
SLE glomerulonephritis syndrome	4285717
Systemic lupus erythematosus with organ/system involvement	4344158
Lupus erythematosus	255891
Systemic lupus erythematosus-related syndrome	4219859
Cutaneous lupus erythematosus	4324123
Acute systemic lupus erythematosus	4295179
Lupus erythematosus overlap syndrome	4291306
Neonatal lupus erythematosus	4316373
SLE glomerulonephritis syndrome, WHO class V	4178133
Discoid lupus erythematosus	4066824
Autoimmune thyroiditis	4281109
Fibrous autoimmune thyroiditis	138711
Steroid-responsive encephalopathy associated with autoimmune thyroiditis	36674282
Idiopathic thrombocytopenic purpura	4137430
Chronic idiopathic thrombocytopenic purpura	318397
Acute idiopathic thrombocytopenic purpura	4102469
Immune thrombocytopenia	4103532
Systemic sclerosis	134442
CREST syndrome	4135937
Lung disease with systemic sclerosis	255304
Progressive systemic sclerosis	40485046
Limited systemic sclerosis	4103019
Sclerodema	4319301
Systemic sclerosis with limited cutaneous involvement	4185187
Autoimmune hepatitis	200762
Chronic autoimmune hepatitis	36687200
Autoimmune hemolytic anemia	441269
Cold autoimmune hemolytic anemia	4160887
Drug-induced autoimmune hemolytic anemia	4146936
Ménière's disease	79833
Meniere's disease of left inner ear	36685172
Polyarteritis nodosa	320749
Microscopic polyarteritis nodosa	4344489
Cutaneous polyarteritis nodosa	4347062
Crohn's disease	201606
Ulcerative colitis	81893
Inflammatory bowel disease	4074815
Myasthenia gravis	76685
Myasthenia gravis with exacerbation	43531560
Sensorineural hearing loss	374366
Toxic diffuse goiter	138717
Sjogren's syndrome	254443
Raynaud's disease	314962
Polymyalgia rheumatica	255348
Megaloblastic anemia due to vitamin B12 deficiency	432588
Celiac disease	194992
Antiphospholipid syndrome	4098292
Aplastic anemia	137829
Ankylosing spondylitis	437082
** removed {Cholangitis	195856} based on Dave's suggestions
Dermatomyositis	80182
Chronic inflammatory demyelinating polyradiculoneuropathy	381009
Primary biliary cholangitis	4135822
Temporal arteritis	4290976
Primary sclerosing cholangitis	4058821
Polymyositis	80800
Lichen planus	132703
Demyelinating disease of central nervous system	375801
Hashimoto thyroiditis	135215
Primary adrenocortical insufficiency	4160059
Alopecia areata	141933
Neuromyelitis optica	380995
Guillain-BarrÈ syndrome	4164770
Behcet's syndrome	436642
Pernicious anemia	432295
Constitutional red cell aplasia and hypoplasia	4146087
Disorder of endocrine ovary	201257
Membranous glomerulonephritis	252365
Graves' disease	4232076
Immunoglobulin A vasculitis	4101602
Stiff-man syndrome	379008
Takayasu's disease	440740
Acute febrile mucocutaneous lymph node syndrome	314381
Pulmonary disease due to allergic granulomatosis angiitis	46273631
Pemphigus vulgaris	4170723
Pustular psoriasis of palms and soles	4100184
Relapsing polychondritis	4216873
Evans syndrome	436956
Systemic onset juvenile chronic arthritis	4116447
Microscopic polyangiitis	44808422
Benign mucous membrane pemphigoid	4142060
Vasculitis of the skin	4182711
Pemphigoid	139899
Pemphigus	135338
Autoimmune disease	434621
Undifferentiated connective tissue disease	4344165
Autonomic neuropathy	4080146
Thromboangiitis obliterans	312939
Nephrotic syndrome, diffuse membranous glomerulonephritis	4058841
Vogt-Koyanagi-Harada disease	4108968
Dermatitis herpetiformis	140487
Giant cell arteritis with polymyalgia rheumatica	4343935
Eaton-Lambert syndrome	4237155
Chronic thyroiditis	137520
Cerebral arteritis	380747
Autoimmune lymphoproliferative syndrome	45765493
Fasciitis with eosinophilia syndrome	4083100
Autoimmune polyendocrinopathy	4223448
Eosinophilic granulomatosis with polyangiitis	4305666
Delayed postmyocardial infarction pericarditis	37311078
Addison's disease	443394
Pemphigus foliaceus	4148690
Thyrotoxic exophthalmos	440108
Progressive external ophthalmoplegia	379027
Autoimmune hypothyroidism	4034815
Endolymphatic hydrops	4321746
Autoimmune encephalitis	4318558
Asteatotic eczema	4308085
Transverse myelopathy syndrome	443904
Psoriasiform dermatitis	4179347
Polymyositis associated with autoimmune disease	4346977
Spongiotic dermatitis	4053289
Pemphigus paraneoplastica	4291435
Exophthalmos due to thyroid eye disease	4214309
Idiopathic transverse myelitis	134330
Autoimmune pancreatitis	40490446
ACTH deficiency	4029439
Balo concentric sclerosis	4046110
Autoimmune encephalitis caused by N-methyl D-aspartate receptor antibody	764228
Acute inflammatory demyelinating polyneuropathy	43530713
Myasthenic crisis	4215003
Myelofibrosis due to another disorder	760840
Toxic diffuse goiter with exophthalmos	4185522
Pustular eczema	4048079
Granulomatous dermatophytosis	4301243
Chronic inflammatory demyelinating polyradiculoneuropathy with central nervous system demyelination	4048024
Rasmussen syndrome	4041672
Idiopathic vitiligo	4297816
Psoriasiform eczema	4080933
Secondary hypoparathyroidism	36717646
Idiopathic membranous glomerulonephritis	36716199
Erythema nodosum, acute form	4318267
*/

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8f848245-5aff-401c-81a5-b7aa980faa92"),
    AIDs_cohort=Input(rid="ri.foundry.main.dataset.86c1065a-5e27-4c74-94ce-fbf0cc07a4c7"),
    Cohort_covid_final=Input(rid="ri.foundry.main.dataset.9fda0be9-ceab-47ae-b159-bec157742c13")
)
--- Here severity of patients mapped by using COVID-19 cohort with severity and AID_cohorts tables
SELECT A.*, B.Severity_Type
FROM AIDs_cohort A
inner join Cohort_covid_final B
on A.person_id = B.person_id;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.57249ea3-8b16-4c41-9fc8-75b8da59e876"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
/* Create Psoriasis concept ids based on codeset id 
psoriasis: 531043866 from "concept_set_members" table */

SELECT *
FROM concept_set_members
where (codeset_id ='531043866');

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.c7dadda5-dfed-4db7-92a5-ea4bda53ce81"),
    Psoriasis=Input(rid="ri.foundry.main.dataset.57249ea3-8b16-4c41-9fc8-75b8da59e876"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86")
)
/* Create patients cohort of "Psoriasis" by maping concept_id with 
condition_concept_id of condition_occurance table */

SELECT A.*, B.concept_id
FROM condition_occurrence A
inner join Psoriasis B
on A.condition_concept_id = B.concept_id;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.caadaffa-fdbe-4a22-b871-5f6c625c0e0a"),
    concept_set_members=Input(rid="ri.foundry.main.dataset.e670c5ad-42ca-46a2-ae55-e917e3e161b6")
)
/* Create Type 1 diabetes mellitus concept ids based on codeset id 
Type 1 diabetes mellitus: 678552689 from "concept_set_members" table */

SELECT *
FROM concept_set_members
where codeset_id ='678552689' ;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.eb208948-4eca-4cde-b47d-b55fcbdaf909"),
    Type1_DM=Input(rid="ri.foundry.main.dataset.caadaffa-fdbe-4a22-b871-5f6c625c0e0a"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86")
)
/* Create patients cohort of "Type 1 diabetes mellitus " by maping concept_id with 
condition_concept_id of condition_occurance table */
SELECT A.*, B.concept_id
FROM condition_occurrence A
inner join Type1_DM B
on A.condition_concept_id = B.concept_id;

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.1cf04aaf-b81e-4ba7-bd9d-d9b7083a342c"),
    Cohort_covid_final=Input(rid="ri.foundry.main.dataset.9fda0be9-ceab-47ae-b159-bec157742c13"),
    type1_DM_AID=Input(rid="ri.foundry.main.dataset.aefb7cd1-92ca-461c-9a94-51e8737b5f34")
)
/*
Following code is mapping Type1 DM patients with Cohort of COVID-19 positive patients 
where Type1 DM condition start date should be prior to COVID-19 diagnosis date
*/
SELECT A.*, B.COVID_first_PCR_or_AG_lab_positive
FROM type1_DM_AID A
inner join Cohort_covid_final B
on A.person_id = B.person_id and (A.condition_start_date < B.COVID_first_PCR_or_AG_lab_positive);

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.8347f79b-877d-4951-880b-ff92e4ce780c"),
    Cohort_covid_final=Input(rid="ri.foundry.main.dataset.9fda0be9-ceab-47ae-b159-bec157742c13"),
    condition_occurrence=Input(rid="ri.foundry.main.dataset.900fa2ad-87ea-4285-be30-c6b5bab60e86")
)
-- select patients data whose condition started before covid diagnosis date (AID prior covid)
SELECT A.*,B.person_id as covid_pid, B.COVID_first_PCR_or_AG_lab_positive
FROM condition_occurrence A
inner join Cohort_covid_final B
on A.person_id = B.person_id and (A.condition_start_date < B.COVID_first_PCR_or_AG_lab_positive); 

@transform_pandas(
    Output(rid="ri.foundry.main.dataset.f2827e14-7f2d-477c-97d7-5e3486dc3460"),
    Cohort_covid_final=Input(rid="ri.foundry.main.dataset.9fda0be9-ceab-47ae-b159-bec157742c13"),
    psoriasis_AID=Input(rid="ri.foundry.main.dataset.991a45a4-89ce-4e99-9c4b-2afabf7f4639")
)
/*
Following code is mapping Psoriasis patients with Cohort of COVID-19 positive patients 
where Psoriasis condition start date should be prior to COVID-19 diagnosis date
*/

SELECT A.*,  B.COVID_first_PCR_or_AG_lab_positive
FROM psoriasis_AID A
inner join Cohort_covid_final B
on A.person_id = B.person_id and (A.condition_start_date < B.COVID_first_PCR_or_AG_lab_positive);

