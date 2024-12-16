--create a new insurance_plan table consisting of the payer_source_value and aggregated health insurance plan for each model 1 cohort patient on their index date
select mt.subject_id, ppp.payer_source_value,
case
	when ppp.payer_concept_id = 420 then 'Medicare Gap'
	when ppp.payer_concept_id in (282,283,280) then 'Medicare Advantage'
	when ppp.payer_concept_id in (330, 329,331) or ppp.payer_source_value = 'Self-Insured|Preferred Provider Organization' then 'Commercial PPO/HMO'
	when ppp.payer_source_value = 'Commercial|Consumer Directed Health Care' then 'Commercial HDHP'
	when ppp.payer_concept_id in (291,289) then 'Medicaid'
	when ppp.payer_source_value in ('Unknown/Missing|Unknown/Missing', 'Commercial|Unknown/Missing', 'Unknown/Missing|Health Maintenance Organization') then 'Missing'	
end as insurance_plan into {model_1_insurance}
from work_tilton_ca204.{model_1_target} mt 
inner join {stroke_cohort} sc
on mt.subject_id = sc.subject_id 
inner join omop_cdm_53_pmtx_202203.payer_plan_period ppp 
on mt.subject_id = ppp.person_id
where ppp.payer_plan_period_start_date <= sc.cohort_start_date
and ppp.payer_plan_period_end_date >= sc.cohort_start_date;


