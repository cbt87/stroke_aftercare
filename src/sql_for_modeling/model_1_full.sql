-- CTE for initial_ip_duration and severity
WITH initial_ip_data AS (
    SELECT subject_id,
           visit_end_date - cohort_start_date AS initial_ip_duration,
           CASE 
               WHEN visit_end_date - cohort_start_date <= 7 THEN 'mild stroke'
               WHEN visit_end_date - cohort_start_date > 7 THEN 'severe stroke'
               ELSE NULL 
           END AS severity
    FROM {stroke_cohort}
),
-- CTE for Parkinson's disease
parkinson_subset AS (
    SELECT DISTINCT sc.subject_id
    FROM {stroke_cohort} sc
    INNER JOIN {model_1_target} mt 
        ON sc.subject_id = mt.subject_id
    INNER JOIN omop_cdm_53_pmtx_202203.condition_occurrence co 
        ON mt.subject_id = co.person_id
    WHERE co.condition_source_concept_id IN (
        SELECT c.concept_id 
        FROM omop_cdm_53_pmtx_202203.concept AS c
        WHERE c.concept_code ~ '^G20(\\.[A-Za-z0-9]|$)' 
           OR c.concept_code ~ '^G21(\\.[A-Za-z0-9]|$)'
           OR c.concept_code ~ '^G22(\\.[A-Za-z0-9]|$)'
    )
    AND (
        co.condition_start_date BETWEEN sc.cohort_start_date - INTERVAL '180 days'
                                    AND sc.cohort_start_date
        OR co.condition_start_date BETWEEN sc.cohort_start_date 
                                    AND sc.cohort_start_date + INTERVAL '30 days'
    )
),
-- CTE for migraine
migraine_subset AS (
    SELECT DISTINCT sc.subject_id
    FROM {stroke_cohort} sc
    INNER JOIN {model_1_target} mt 
        ON sc.subject_id = mt.subject_id
    INNER JOIN omop_cdm_53_pmtx_202203.condition_occurrence co 
        ON mt.subject_id = co.person_id
    WHERE co.condition_source_concept_id IN (
        SELECT c.concept_id 
        FROM omop_cdm_53_pmtx_202203.concept AS c
        WHERE c.concept_code ~ '^G43(\\.[A-Za-z0-9]|$)' 
           OR c.concept_code ~ '^G44(\\.[A-Za-z0-9]|$)'
    )
    AND (
        co.condition_start_date BETWEEN sc.cohort_start_date - INTERVAL '180 days'
                                    AND sc.cohort_start_date
        OR co.condition_start_date BETWEEN sc.cohort_start_date 
                                    AND sc.cohort_start_date + INTERVAL '30 days'
    )
),
-- CTE for depression
depression_subset AS (
    SELECT DISTINCT sc.subject_id
    FROM {stroke_cohort} sc
    INNER JOIN {model_1_target} mt 
        ON sc.subject_id = mt.subject_id
    INNER JOIN omop_cdm_53_pmtx_202203.condition_occurrence co 
        ON mt.subject_id = co.person_id
    WHERE co.condition_source_concept_id IN (
        SELECT c.concept_id 
        FROM omop_cdm_53_pmtx_202203.concept AS c
        WHERE c.concept_code ~ '^F32(\\.[A-Za-z0-9]|$)' 
           OR c.concept_code ~ '^F33(\\.[A-Za-z0-9]|$)'
    )
    AND (
        co.condition_start_date BETWEEN sc.cohort_start_date - INTERVAL '180 days'
                                    AND sc.cohort_start_date
        OR co.condition_start_date BETWEEN sc.cohort_start_date 
                                    AND sc.cohort_start_date + INTERVAL '30 days'
    )
),
-- CTE for insomnia
insomnia_subset AS (
    SELECT DISTINCT sc.subject_id
    FROM {stroke_cohort} sc
    INNER JOIN {model_1_target} mt 
        ON sc.subject_id = mt.subject_id
    INNER JOIN omop_cdm_53_pmtx_202203.condition_occurrence co 
        ON mt.subject_id = co.person_id
    WHERE co.condition_source_concept_id IN (
        SELECT c.concept_id 
        FROM omop_cdm_53_pmtx_202203.concept AS c
        WHERE c.concept_code ~ '^G47(\\.[A-Za-z0-9]|$)' 
           OR c.concept_code ~ '^F51(\\.[A-Za-z0-9]|$)'
    )
    AND (
        co.condition_start_date BETWEEN sc.cohort_start_date - INTERVAL '180 days'
                                    AND sc.cohort_start_date
        OR co.condition_start_date BETWEEN sc.cohort_start_date 
                                    AND sc.cohort_start_date + INTERVAL '30 days'
    )
),
-- CTE for dementia
dementia_subset AS (
    SELECT DISTINCT sc.subject_id
    FROM {stroke_cohort} sc
    INNER JOIN {model_1_target} mt 
        ON sc.subject_id = mt.subject_id
    INNER JOIN omop_cdm_53_pmtx_202203.condition_occurrence co 
        ON mt.subject_id = co.person_id
    WHERE co.condition_source_concept_id IN (
        SELECT c.concept_id 
        FROM omop_cdm_53_pmtx_202203.concept AS c
        WHERE c.concept_code ~ '^F00(\\.[A-Za-z0-9]|$)' 
           OR c.concept_code ~ '^F01(\\.[A-Za-z0-9]|$)'
           OR c.concept_code ~ '^F02(\\.[A-Za-z0-9]|$)'
           OR c.concept_code ~ '^F03(\\.[A-Za-z0-9]|$)'
           OR c.concept_code ~ '^G30(\\.[A-Za-z0-9]|$)'
           OR c.concept_code ~ '^G31(\\.[A-Za-z0-9]|$)'
           OR c.concept_code ~ '^R54(\\.[A-Za-z0-9]|$)'
           OR c.concept_code = 'F05.1'
    )
    AND (
        co.condition_start_date BETWEEN sc.cohort_start_date - INTERVAL '180 days'
                                    AND sc.cohort_start_date
        OR co.condition_start_date BETWEEN sc.cohort_start_date 
                                    AND sc.cohort_start_date + INTERVAL '30 days'
    )
),
-- CTE for paralysis
paralysis_subset AS (
    SELECT DISTINCT sc.subject_id
    FROM {stroke_cohort} sc
    INNER JOIN {model_1_target} mt 
        ON sc.subject_id = mt.subject_id
    INNER JOIN omop_cdm_53_pmtx_202203.condition_occurrence co 
        ON mt.subject_id = co.person_id
    WHERE co.condition_source_concept_id IN (
        SELECT c.concept_id 
        FROM omop_cdm_53_pmtx_202203.concept AS c
        WHERE c.concept_code ~ '^G81(\\.[A-Za-z0-9]|$)' 
           OR c.concept_code ~ '^G82(\\.[A-Za-z0-9]|$)'
           OR c.concept_code ~ '^G83(\\.[A-Za-z0-9]|$)'
    )
    AND co.condition_start_date BETWEEN sc.cohort_start_date AND sc.cohort_start_date + INTERVAL '30 days'
),
-- CTE for ischemic stroke
ischemic_stroke_subset AS (
    SELECT DISTINCT sc.subject_id
    FROM {stroke_cohort} sc
    INNER JOIN {model_1_target} mt 
        ON sc.subject_id = mt.subject_id
    INNER JOIN omop_cdm_53_pmtx_202203.condition_occurrence co 
        ON mt.subject_id = co.person_id
    WHERE co.condition_source_concept_id IN (
        SELECT c.concept_id 
        FROM omop_cdm_53_pmtx_202203.concept AS c
        WHERE c.concept_code ~ '^I63(\\.[A-Za-z0-9]|$)'
    )
    AND co.condition_start_date BETWEEN sc.cohort_start_date AND sc.cohort_start_date + INTERVAL '30 days'
),
-- CTE for patient_age and gender
patient_data AS (
    SELECT 
        sc.subject_id,
        EXTRACT(YEAR FROM sc.cohort_start_date) AS index_year,
        p.year_of_birth,
        EXTRACT(YEAR FROM sc.cohort_start_date) - p.year_of_birth AS patient_age,
        p.gender_source_value AS gender
    FROM {stroke_cohort} sc
    INNER JOIN omop_cdm_53_pmtx_202203.person p 
        ON sc.subject_id = p.person_id
)
-- Main query
SELECT mt.*,
       CASE WHEN parkinson_subset.subject_id IS NOT NULL THEN 1 ELSE 0 END AS has_parkinsons,
       CASE WHEN migraine_subset.subject_id IS NOT NULL THEN 1 ELSE 0 END AS has_migraine,
       CASE WHEN depression_subset.subject_id IS NOT NULL THEN 1 ELSE 0 END AS has_depression,
       CASE WHEN insomnia_subset.subject_id IS NOT NULL THEN 1 ELSE 0 END AS has_insomnia,
       CASE WHEN dementia_subset.subject_id IS NOT NULL THEN 1 ELSE 0 END AS has_dementia,
       CASE WHEN paralysis_subset.subject_id IS NOT NULL THEN 1 ELSE 0 END AS has_paralysis,
       CASE WHEN ischemic_stroke_subset.subject_id IS NOT NULL THEN 1 ELSE 0 END AS has_ischemic_stroke,
       ip.initial_ip_duration,
       ip.severity,
       pd.patient_age,
       pd.gender
into {full_model_1} -- write new table in your schema
FROM {model_1_target} mt
LEFT JOIN parkinson_subset 
    ON mt.subject_id = parkinson_subset.subject_id
LEFT JOIN migraine_subset 
    ON mt.subject_id = migraine_subset.subject_id
LEFT JOIN depression_subset 
    ON mt.subject_id = depression_subset.subject_id
LEFT JOIN insomnia_subset 
    ON mt.subject_id = insomnia_subset.subject_id
LEFT JOIN dementia_subset 
    ON mt.subject_id = dementia_subset.subject_id
LEFT JOIN paralysis_subset 
    ON mt.subject_id = paralysis_subset.subject_id
LEFT JOIN ischemic_stroke_subset 
    ON mt.subject_id = ischemic_stroke_subset.subject_id
LEFT JOIN initial_ip_data ip
    ON mt.subject_id = ip.subject_id
LEFT JOIN patient_data pd
    ON mt.subject_id = pd.subject_id;
