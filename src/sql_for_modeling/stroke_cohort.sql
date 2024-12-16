--creating an intermediate table for all condition occurrences that match the stroke-related concept ids for inpatient visits

select co.condition_occurrence_id, co.person_id, co.condition_concept_id, co.condition_start_date, co.condition_end_date, vo.visit_start_date, vo.visit_end_date, vo.discharge_to_concept_id 
into {stroke_occurrences} --write a table name from your personal schema
from omop_cdm_53_pmtx_202203.condition_occurrence co
inner join omop_cdm_53_pmtx_202203.visit_occurrence vo 
on co.visit_occurrence_id = vo.visit_occurrence_id
where vo.visit_concept_id IN 
(
select ca.descendant_concept_id from omop_cdm_53_pmtx_202203.concept_ancestor ca 
inner join omop_cdm_53_pmtx_202203.concept c on 
ca.descendant_concept_id = c.concept_id 
where ancestor_concept_id IN (9201, 9203, 262) -- inpatient visit codes and all descendants
)
and co.condition_concept_id in 
(
select ca.descendant_concept_id from omop_cdm_53_pmtx_202203.concept_ancestor ca 
inner join omop_cdm_53_pmtx_202203.concept c on 
ca.descendant_concept_id = c.concept_id 
where ancestor_concept_id IN (372924,375557,376713,443454,441874,439847,432923)); -- stroke occurrence codes and all descendants


-- creating a cohort with aphasia, dysarthria, and dysphagia indicators
with first_stroke_occurrence as 
(
  -- Select all stroke occurrences and assign row numbers for each person based on the condition_start_date
  select condition_occurrence_id, 
         person_id, 
         condition_concept_id, 
         condition_start_date, 
         condition_end_date,
         visit_start_date,
         visit_end_date,
         discharge_to_concept_id,
         row_number() over(partition by person_id order by condition_start_date asc) as row_num
  from {stroke_occurrences}
),
distinct_stroke_occurrence as 
(
  -- Select distinct stroke occurrences by condition_start_date and rank them
  select condition_occurrence_id, 
         person_id, 
         condition_concept_id, 
         condition_start_date, 
         condition_end_date, 
         dense_rank() over(partition by person_id order by condition_start_date asc) as distinct_rank
  from first_stroke_occurrence
),
multiple_stroke_occurrence as
(
  -- Select person_ids with more than one distinct occurrence within 180 days
  select f1.person_id
  from distinct_stroke_occurrence f1
  join distinct_stroke_occurrence f2 
    on f1.person_id = f2.person_id 
   and f1.distinct_rank = 1  -- First distinct stroke
   and f2.distinct_rank = 2  -- Second distinct stroke
  where datediff(day, f1.condition_start_date, f2.condition_start_date) <= 180
  group by f1.person_id
),
stroke_cohort as
(
  -- Select first stroke occurrence and filter based on observation period
  select f.person_id as subject_id,
         f.condition_occurrence_id,  
         f.condition_concept_id, 
         f.condition_start_date as cohort_start_date,  -- Renaming condition_start_date to cohort_start_date for CCI merge later in pipeline
         f.condition_end_date, 
         dateadd(day, 180, f.condition_start_date) as six_months_after_index, -- Adding 180 days to condition_start_date
         f.visit_start_date,
         f.visit_end_date,
         f.discharge_to_concept_id,
         CASE 
         WHEN f.discharge_to_concept_id IN (581476, 0, 38004519, 9202) THEN 'Home'
         WHEN f.discharge_to_concept_id IN (38004285, 8920) THEN 'Inpatient Rehabilitation Facility'
         WHEN f.discharge_to_concept_id IN (8863,38004277,8676) THEN 'SNF/LTC'
         when f.discharge_to_concept_id in (8717,32254,32276) then 'Acute Care'
         when f.discharge_to_concept_id in (8546,8951,38004284,38003619) then 'Other'
    	 END AS discharge_location,
         op.observation_period_start_date, 
         op.observation_period_end_date as cohort_end_date, 
         op.observation_period_id  
  from first_stroke_occurrence f
  inner join omop_cdm_53_pmtx_202203.observation_period op 
    on op.person_id = f.person_id
  where f.person_id in (select person_id from multiple_stroke_occurrence)
    and f.row_num = 1  -- Select only the first stroke occurrence for each person
    and f.condition_start_date >= dateadd(day, 180, op.observation_period_start_date)
    and op.observation_period_end_date >= dateadd(day, 180, f.condition_start_date)
),
aphasia_occurrence as
(
  -- Find aphasia occurrences for each person (aphasia concept ids: 440424, 40480002)
  select condition_occurrence_id, 
         person_id, 
         condition_start_date, 
         dense_rank() over(partition by person_id order by condition_start_date asc) as aphasia_rank
  from omop_cdm_53_pmtx_202203.condition_occurrence
  where condition_concept_id in (440424, 40480002)
),
valid_aphasia_occurrence as
(
  -- Select person_ids where aphasia occurrence is on or after the stroke occurrence date
  select ao.person_id
  from stroke_cohort sc
  join aphasia_occurrence ao 
    on sc.subject_id = ao.person_id
    and ao.condition_start_date >= sc.cohort_start_date
  group by ao.person_id
),
multiple_aphasia_occurrence as
(
  -- Select person_ids with more than one distinct aphasia occurrence on different days
  select f1.person_id
  from aphasia_occurrence f1
  join aphasia_occurrence f2 
    on f1.person_id = f2.person_id 
   and f1.aphasia_rank = 1  -- First distinct aphasia occurrence
   and f2.aphasia_rank = 2  -- Second distinct aphasia occurrence
  group by f1.person_id
),
aphasia_indicator as
(
  -- Combine the valid aphasia and multiple aphasia occurrences into a binary indicator
  select sc.subject_id,
         case when va.person_id is not null and ma.person_id is not null then 1 else 0 end as has_aphasia
  from stroke_cohort sc
  left join valid_aphasia_occurrence va on sc.subject_id = va.person_id
  left join multiple_aphasia_occurrence ma on sc.subject_id = ma.person_id
),
dysphagia_occurrence as
(
  -- Find dysphagia occurrences for each person 
  select condition_occurrence_id, 
         person_id, 
         condition_start_date, 
         dense_rank() over(partition by person_id order by condition_start_date asc) as dysphagia_rank
  from omop_cdm_53_pmtx_202203.condition_occurrence
  where condition_concept_id in (31317, 440530, 26823, 443465, 45757559, 4198185) 
),
valid_dysphagia_occurrence as
(
  -- Select person_ids where dysphagia occurrence is on or after the stroke occurrence date
  select dyo.person_id
  from stroke_cohort sc
  join dysphagia_occurrence dyo 
    on sc.subject_id = dyo.person_id
    and dyo.condition_start_date >= sc.cohort_start_date
  group by dyo.person_id
),
multiple_dysphagia_occurrence as
(
  -- Select person_ids with more than one distinct dysphagia occurrence on different days
  select dys1.person_id
  from dysphagia_occurrence dys1
  join dysphagia_occurrence dys2 
    on dys1.person_id = dys2.person_id 
   and dys1.dysphagia_rank = 1  -- First distinct dysphagia occurrence
   and dys2.dysphagia_rank = 2  -- Second distinct dysphagia occurrence
  group by dys1.person_id
),
dysphagia_indicator as
(
  -- Combine the valid dysphagia and multiple dysphagia occurrences into a binary indicator
  select sc.subject_id,
         case when vdyo.person_id is not null and mdyo.person_id is not null then 1 else 0 end as has_dysphagia
  from stroke_cohort sc
  left join valid_dysphagia_occurrence vdyo on sc.subject_id = vdyo.person_id
  left join multiple_dysphagia_occurrence mdyo on sc.subject_id = mdyo.person_id
),
dysarthria_occurrence as
(
  -- Find dysarthria occurrences for each person
  select condition_occurrence_id, 
         person_id, 
         condition_start_date, 
         dense_rank() over(partition by person_id order by condition_start_date asc) as dysarthria_rank
  from omop_cdm_53_pmtx_202203.condition_occurrence
  where condition_concept_id in (4196636, 43530687)
),
valid_dysarthria_occurrence as
(
  -- Select person_ids where dysarthria occurrence is on or after the stroke occurrence date
  select dya.person_id
  from stroke_cohort sc
  join dysarthria_occurrence dya 
    on sc.subject_id = dya.person_id
    and dya.condition_start_date >= sc.cohort_start_date
  group by dya.person_id
),
multiple_dysarthria_occurrence as
(
  -- Select person_ids with more than one distinct dysarthria occurrence on different days
  select dar1.person_id
  from dysarthria_occurrence dar1
  join dysarthria_occurrence dar2 
    on dar1.person_id = dar2.person_id 
   and dar1.dysarthria_rank = 1  -- First distinct dysarthria occurrence
   and dar2.dysarthria_rank = 2  -- Second distinct dysarthria occurrence
  group by dar1.person_id
),
dysarthria_indicator as
(
  -- Combine the valid dysarthria and multiple dysarthria occurrences into a binary indicator
  select sc.subject_id,
         case when vdy.person_id is not null and mdy.person_id is not null then 1 else 0 end as has_dysarthria
  from stroke_cohort sc
  left join valid_dysarthria_occurrence vdy on sc.subject_id = vdy.person_id
  left join multiple_dysarthria_occurrence mdy on sc.subject_id = mdy.person_id
)
-- Final select to return the cohort with aphasia, dysphagia, and dysarthria binary indicators
select sc.*, ai.has_aphasia, di.has_dysphagia, dri.has_dysarthria, 1 as cohort_definition_id
into {stroke_cohort} --write a table name from your personal schema
from stroke_cohort sc
left join aphasia_indicator ai on sc.subject_id = ai.subject_id
left join dysphagia_indicator di on sc.subject_id = di.subject_id
left join dysarthria_indicator dri on sc.subject_id = dri.subject_id;


