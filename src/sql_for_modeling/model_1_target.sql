--creating a new table of only patients with 1 or more speech disorders, and their SLP-treatment status
with multiple_conditions as
(
  -- List of person_ids with multiple conditions
  select subject_id, cohort_start_date
  from {stroke_cohort} -- write name of stroke cohort table in your schema
  where coalesce(has_aphasia, 0) = 1
   or coalesce(has_dysphagia, 0) = 1
   or coalesce(has_dysarthria, 0) = 1
),
valid_slp_procedure as
(
  -- Filtered SLP procedures where procedure_date is on or after index_date for each person_id
  select mc.subject_id
  from multiple_conditions mc
  join omop_cdm_53_pmtx_202203.procedure_occurrence po 
    on mc.subject_id = po.person_id
    and po.procedure_date >= mc.cohort_start_date
    and po.procedure_source_value in (92507, 92508, 92520, 92521, 92522, 92523, 92524,
                                      92526, 92597, 92605, 92606, 92607, 92608, 92609,
                                      92610, 92611, 92612, 92613, 92614, 92615, 92616,
                                      92617, 96105, 97129, 97130)
  group by mc.subject_id
)
-- Final select to classify person_ids as With_SLP or Without_SLP
select mc.subject_id, case when vsp.subject_id is not null then 'SLP_treatment' else 'without_SLP_treatment' end as slp_treatment_status
into {model_1_target} -- write name of table to write into your schema
from multiple_conditions mc
left join valid_slp_procedure vsp 
  on mc.subject_id = vsp.subject_id;