
---count of patients for every year of birth
---why does the year 1937 have the highest count? 
select year_of_birth , count(*) from omop_cdm_53_pmtx_202203.person
group by year_of_birth
order by count desc;

---top 10 patients with the most observations
select person_id, count(*) from omop_cdm_53_pmtx_202203.observation
group by person_id 
order by count desc
limit 20;

---breakdown of the visits of the patient with the most observations, patient 209840053
select VO.count, VO.visit_concept_id, VN.concept_name from 
(select count(*),visit_concept_id  from omop_cdm_53_pmtx_202203.visit_occurrence
where person_id =  209840053
group by visit_concept_id) VO 
inner join omop_cdm_53_pmtx_202203.concept VN on VO.visit_concept_id = VN.concept_id 
order by VO.count desc;


---patients who had the measurement for covid19
select * from omop_cdm_53_pmtx_202203.measurement  
where measurement_concept_id = 704059
order by measurement_date asc
limit 100 ;

---measurement breakdown of patient 209840053
select m.count, m.measurement_concept_id, c.concept_name from
(select count(*),measurement_concept_id from omop_cdm_53_pmtx_202203.measurement  
where person_id = 209840053
group by measurement_concept_id) m
inner join omop_cdm_53_pmtx_202203.concept c on m.measurement_concept_id = c.concept_id
order by m.count desc;

-- patients with the most device exposures

select person_id, count(*) from omop_cdm_53_pmtx_202203.device_exposure de 
group by person_id
order by count desc
limit 20;

--breakdown of device exposures for the patient with the most exposures
select dc.count,dc.device_concept_id,c.concept_name from
(select device_concept_id, count(*) from omop_cdm_53_pmtx_202203.device_exposure de 
where person_id = 101843992
group by device_concept_id) dc
inner join omop_cdm_53_pmtx_202203.concept c on dc.device_concept_id = c.concept_id 
order by dc.count desc;

--patient's breakdown of condition occurrences 
select con.condition_concept_id,con.count, c.concept_name from
(select condition_concept_id,count(*) from omop_cdm_53_pmtx_202203.condition_occurrence
where person_id = 101843992
group by condition_concept_id) con
left join omop_cdm_53_pmtx_202203.concept c on con.condition_concept_id = c.concept_id 
order by count desc;




-- count of distinct patients that have had an occurence of a stroke-related condition 
select count(distinct person_id) from omop_cdm_53_pmtx_202203.condition_occurrence co 
where condition_concept_id  in (372924,375557,376713,443454,441874,439847,432923);

--select condition occurrences per person where the number of occurences per person is less than 3
select count(*) from (select person_id, count(*) from omop_cdm_53_pmtx_202203.condition_occurrence co 
where condition_concept_id  in (372924,375557,376713,443454,441874,439847,432923)
group by person_id)
where count < 3;


select * from omop_cdm_53_pmtx_202203.condition_occurrence co 
where condition_concept_id  in (372924,375557,376713,443454,441874,439847,432923) limit 100;

--selecting the first stroke-related condition occurrence per person_id
--problem is that a condition_concept_id could take place more than once on the earliest condition_start_date
--not sure why
select co.* from omop_cdm_53_pmtx_202203.condition_occurrence co
inner join (select person_id, min(condition_start_date) as first_condition_occurrence from omop_cdm_53_pmtx_202203.condition_occurrence
where condition_concept_id  in (372924,375557,376713,443454,441874,439847,432923)
group by person_id) f
on co.person_id = f.person_id and
co.condition_start_date = f.first_condition_occurrence
where condition_concept_id  in (372924,375557,376713,443454,441874,439847,432923)
limit 100;


