import redshift_connector
import sys
import os


sys.path.append(os.path.abspath(os.path.join('..')))

import config

#Connect to the redshift database

conn = redshift_connector.connect(
     host=config.DB_HOST,
     port=5439,
     database=config.DB_NAME,
     user=config.DB_USER,
     password=config.DB_PASSWORD
  )


def create_cohort(intermediate_table, final_table):
    inpatient_stroke_query = f"""select co.condition_occurrence_id, co.person_id, co.condition_concept_id, co.condition_start_date, co.condition_end_date, vo.visit_start_date, vo.visit_end_date, vo.discharge_to_concept_id 
    into {intermediate_table}
    from omop_cdm_53_pmtx_202203.condition_occurrence co
    inner join omop_cdm_53_pmtx_202203.visit_occurrence vo 
    on co.visit_occurrence_id = vo.visit_occurrence_id
    where vo.visit_concept_id IN 
    (
    select ca.descendant_concept_id from omop_cdm_53_pmtx_202203.concept_ancestor ca 
    inner join omop_cdm_53_pmtx_202203.concept c on 
    ca.descendant_concept_id = c.concept_id 
    where ancestor_concept_id IN (9201, 9203, 262)
    )
    and co.condition_concept_id in 
    (
    select ca.descendant_concept_id from omop_cdm_53_pmtx_202203.concept_ancestor ca 
    inner join omop_cdm_53_pmtx_202203.concept c on 
    ca.descendant_concept_id = c.concept_id 
    where ancestor_concept_id IN (372924,375557,376713,443454,441874,439847,432923));"""

    
    # # Create a Cursor object
    cursor = conn.cursor()

    # Query and receive result set
    cursor.execute(inpatient_stroke_query)
    conn.commit()


    create_cohort_w_conditions_query = f"""with first_stroke_occurrence as 
    (
    select condition_occurrence_id, 
            person_id, 
            condition_concept_id, 
            condition_start_date, 
            condition_end_date,
            visit_start_date,
            visit_end_date,
            discharge_to_concept_id,
            row_number() over(partition by person_id order by condition_start_date asc) as row_num
    from {intermediate_table}
    ),
    distinct_stroke_occurrence as 
    (
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
    select f1.person_id
    from distinct_stroke_occurrence f1
    join distinct_stroke_occurrence f2 
        on f1.person_id = f2.person_id 
    and f1.distinct_rank = 1
    and f2.distinct_rank = 2
    where datediff(day, f1.condition_start_date, f2.condition_start_date) <= 180
    group by f1.person_id
    ),
    stroke_cohort as
    (
    select f.person_id as subject_id,
            f.condition_occurrence_id,  
            f.condition_concept_id, 
            f.condition_start_date as cohort_start_date,
            f.condition_end_date, 
            dateadd(day, 180, f.condition_start_date) as six_months_after_index,
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
        and f.row_num = 1
        and f.condition_start_date >= dateadd(day, 180, op.observation_period_start_date)
        and op.observation_period_end_date >= dateadd(day, 180, f.condition_start_date)
    ),
    aphasia_occurrence as
    (
    select condition_occurrence_id, 
            person_id, 
            condition_start_date, 
            dense_rank() over(partition by person_id order by condition_start_date asc) as aphasia_rank
    from omop_cdm_53_pmtx_202203.condition_occurrence
    where condition_concept_id in (440424, 40480002)
    ),
    valid_aphasia_occurrence as
    (
    select ao.person_id
    from stroke_cohort sc
    join aphasia_occurrence ao 
        on sc.subject_id = ao.person_id
        and ao.condition_start_date >= sc.cohort_start_date
    group by ao.person_id
    ),
    multiple_aphasia_occurrence as
    (
    select f1.person_id
    from aphasia_occurrence f1
    join aphasia_occurrence f2 
        on f1.person_id = f2.person_id 
    and f1.aphasia_rank = 1
    and f2.aphasia_rank = 2 
    group by f1.person_id
    ),
    aphasia_indicator as
    (
    select sc.subject_id,
            case when va.person_id is not null and ma.person_id is not null then 1 else 0 end as has_aphasia
    from stroke_cohort sc
    left join valid_aphasia_occurrence va on sc.subject_id = va.person_id
    left join multiple_aphasia_occurrence ma on sc.subject_id = ma.person_id
    ),
    dysphagia_occurrence as
    ( 
    select condition_occurrence_id, 
            person_id, 
            condition_start_date, 
            dense_rank() over(partition by person_id order by condition_start_date asc) as dysphagia_rank
    from omop_cdm_53_pmtx_202203.condition_occurrence
    where condition_concept_id in (31317, 440530, 26823, 443465, 45757559, 4198185) 
    ),
    valid_dysphagia_occurrence as
    (
    select dyo.person_id
    from stroke_cohort sc
    join dysphagia_occurrence dyo 
        on sc.subject_id = dyo.person_id
        and dyo.condition_start_date >= sc.cohort_start_date
    group by dyo.person_id
    ),
    multiple_dysphagia_occurrence as
    (
    select dys1.person_id
    from dysphagia_occurrence dys1
    join dysphagia_occurrence dys2 
        on dys1.person_id = dys2.person_id 
    and dys1.dysphagia_rank = 1
    and dys2.dysphagia_rank = 2
    group by dys1.person_id
    ),
    dysphagia_indicator as
    (
    select sc.subject_id,
            case when vdyo.person_id is not null and mdyo.person_id is not null then 1 else 0 end as has_dysphagia
    from stroke_cohort sc
    left join valid_dysphagia_occurrence vdyo on sc.subject_id = vdyo.person_id
    left join multiple_dysphagia_occurrence mdyo on sc.subject_id = mdyo.person_id
    ),
    dysarthria_occurrence as
    (
    select condition_occurrence_id, 
            person_id, 
            condition_start_date, 
            dense_rank() over(partition by person_id order by condition_start_date asc) as dysarthria_rank
    from omop_cdm_53_pmtx_202203.condition_occurrence
    where condition_concept_id in (4196636, 43530687)
    ),
    valid_dysarthria_occurrence as
    (
    select dya.person_id
    from stroke_cohort sc
    join dysarthria_occurrence dya 
        on sc.subject_id = dya.person_id
        and dya.condition_start_date >= sc.cohort_start_date
    group by dya.person_id
    ),
    multiple_dysarthria_occurrence as
    (
    select dar1.person_id
    from dysarthria_occurrence dar1
    join dysarthria_occurrence dar2 
        on dar1.person_id = dar2.person_id 
    and dar1.dysarthria_rank = 1
    and dar2.dysarthria_rank = 2
    group by dar1.person_id
    ),
    dysarthria_indicator as
    (
    select sc.subject_id,
            case when vdy.person_id is not null and mdy.person_id is not null then 1 else 0 end as has_dysarthria
    from stroke_cohort sc
    left join valid_dysarthria_occurrence vdy on sc.subject_id = vdy.person_id
    left join multiple_dysarthria_occurrence mdy on sc.subject_id = mdy.person_id
    )
    select sc.*, ai.has_aphasia, di.has_dysphagia, dri.has_dysarthria, 1 as cohort_definition_id
    into {final_table}
    from stroke_cohort sc
    left join aphasia_indicator ai on sc.subject_id = ai.subject_id
    left join dysphagia_indicator di on sc.subject_id = di.subject_id
    left join dysarthria_indicator dri on sc.subject_id = dri.subject_id;"""

    cursor.execute(create_cohort_w_conditions_query)
    conn.commit()

    cursor.execute(f"select * from {final_table} limit 5")

    df = cursor.fetch_dataframe()
    print("cohort table")
    print(df)
    


def create_model_1(cohort_table, model_target_table, full_model_table):
    model_1_target_query = f"""with multiple_conditions as
    (
    select subject_id, cohort_start_date
    from {cohort_table}
    where coalesce(has_aphasia, 0) = 1
    or coalesce(has_dysphagia, 0) = 1
    or coalesce(has_dysarthria, 0) = 1
    ),
    valid_slp_procedure as
    (
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
    select mc.subject_id, case when vsp.subject_id is not null then 'SLP_treatment' else 'without_SLP_treatment' end as slp_treatment_status
    into {model_target_table}
    from multiple_conditions mc
    left join valid_slp_procedure vsp 
    on mc.subject_id = vsp.subject_id;"""

    cursor = conn.cursor()

    # Query and receive result set
    cursor.execute(model_1_target_query)
    conn.commit()

    cursor.execute(f"select * from {model_target_table} limit 5")

    df = cursor.fetch_dataframe()
    print("model_1 target table")
    print(df)

    full_model_1_query = f"""WITH initial_ip_data AS (
    SELECT subject_id,
           visit_end_date - cohort_start_date AS initial_ip_duration,
           CASE 
               WHEN visit_end_date - cohort_start_date <= 7 THEN 'mild stroke'
               WHEN visit_end_date - cohort_start_date > 7 THEN 'severe stroke'
               ELSE NULL 
           END AS severity
    FROM {cohort_table}
    ),
    parkinson_subset AS (
        SELECT DISTINCT sc.subject_id
        FROM {cohort_table} sc
        INNER JOIN {model_target_table} mt 
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
    migraine_subset AS (
        SELECT DISTINCT sc.subject_id
        FROM {cohort_table} sc
        INNER JOIN {model_target_table} mt 
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
    depression_subset AS (
        SELECT DISTINCT sc.subject_id
        FROM {cohort_table} sc
        INNER JOIN {model_target_table} mt 
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
    insomnia_subset AS (
        SELECT DISTINCT sc.subject_id
        FROM {cohort_table} sc
        INNER JOIN {model_target_table} mt 
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
    dementia_subset AS (
        SELECT DISTINCT sc.subject_id
        FROM {cohort_table} sc
        INNER JOIN {model_target_table} mt 
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
    paralysis_subset AS (
        SELECT DISTINCT sc.subject_id
        FROM {cohort_table} sc
        INNER JOIN {model_target_table} mt 
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
    ischemic_stroke_subset AS (
        SELECT DISTINCT sc.subject_id
        FROM {cohort_table} sc
        INNER JOIN {model_target_table} mt 
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
    patient_data AS (
        SELECT 
            sc.subject_id,
            EXTRACT(YEAR FROM sc.cohort_start_date) AS index_year,
            p.year_of_birth,
            EXTRACT(YEAR FROM sc.cohort_start_date) - p.year_of_birth AS patient_age,
            p.gender_source_value AS gender
        FROM {cohort_table} sc
        INNER JOIN omop_cdm_53_pmtx_202203.person p 
            ON sc.subject_id = p.person_id
    )
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
    into {full_model_table}
    FROM {model_target_table} mt
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
        ON mt.subject_id = pd.subject_id;"""

    cursor.execute(full_model_1_query)
    conn.commit()

    cursor.execute(f"select * from {full_model_table} limit 5")

    df = cursor.fetch_dataframe()
    print("model 1 full table")
    print(df)

def create_insurance_table(cohort_table,model_target_table,insurance_table):
    model_1_insurance_query = f"""select mt.subject_id, ppp.payer_source_value,
    case
        when ppp.payer_concept_id = 420 then 'Medicare Gap'
        when ppp.payer_concept_id in (282,283,280) then 'Medicare Advantage'
        when ppp.payer_concept_id in (330, 329,331) or ppp.payer_source_value = 'Self-Insured|Preferred Provider Organization' then 'Commercial PPO/HMO'
        when ppp.payer_source_value = 'Commercial|Consumer Directed Health Care' then 'Commercial HDHP'
        when ppp.payer_concept_id in (291,289) then 'Medicaid'
        when ppp.payer_source_value in ('Unknown/Missing|Unknown/Missing', 'Commercial|Unknown/Missing', 'Unknown/Missing|Health Maintenance Organization') then 'Missing'	
    end as insurance_plan into {insurance_table}
    from {model_target_table} mt 
    inner join {cohort_table} sc
    on mt.subject_id = sc.subject_id 
    inner join omop_cdm_53_pmtx_202203.payer_plan_period ppp 
    on mt.subject_id = ppp.person_id
    where ppp.payer_plan_period_start_date <= sc.cohort_start_date
    and ppp.payer_plan_period_end_date >= sc.cohort_start_date;
    """
    cursor = conn.cursor()
    cursor.execute(model_1_insurance_query)
    conn.commit()

    cursor.execute(f"select * from {insurance_table} limit 5")

    df = cursor.fetch_dataframe()
    print("model 1 insurance table")
    print(df)

    conn.close()


in_patient_stroke_table = "work_tilton_ca204.inpatient_stroke_demo" #change the name of this variable to a table in your schema
cohort_table = "work_tilton_ca204.stroke_cohort_w_conditions_demo" #change the name of this variable to a table in your schema
model_target_table = "work_tilton_ca204.model_1_target_demo" #change the name of this variable to a table in your schema
full_model_table = "work_tilton_ca204.model_1_full_demo" #change the name of this variable to a table in your schema
insurance_table = "work_tilton_ca204.model_1_insurance_demo" #change the name of this variable to a table in your schema
create_cohort(intermediate_table=in_patient_stroke_table, final_table=cohort_table)
create_model_1(cohort_table=cohort_table, model_target_table=model_target_table, full_model_table = full_model_table)
create_insurance_table(cohort_table=cohort_table, model_target_table=model_target_table, insurance_table = insurance_table)
