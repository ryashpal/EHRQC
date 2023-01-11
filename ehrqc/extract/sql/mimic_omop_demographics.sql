with t1 as (
	select
	per.person_id as person_id,
	(floor(date_part('day', vo.visit_start_datetime - make_timestamp(per.year_of_birth, 1, 1, 0, 0, 0))/365.0) + pat.anchor_age) as age,
	vo.visit_start_datetime as visit_start_datetime,
	mmt_wt.value_as_number as weight,
	mmt_wt.measurement_datetime as wt_measurement_datetime,
	mmt_ht.value_as_number as height,
	mmt_ht.measurement_datetime as ht_measurement_datetime,
	per.gender_source_value as gender,
	coalesce(per.race_source_value, per.ethnicity_source_value) as ethnicity,
	concat(year_of_birth::varchar, '-01-01')::date AT TIME ZONE 'Australia/Melbourne' as dob,
	dth.death_date as dod
	from
	omop_cdm.person per
	inner join mimiciv.patients pat
	on pat.subject_id = per.person_source_value::int
	inner join omop_cdm.visit_occurrence vo
	on vo.person_id = per.person_id
	inner join omop_cdm.measurement mmt_wt
	on mmt_wt.person_id = per.person_id and mmt_wt.measurement_concept_id = 3025315 -- Body weight
	inner join omop_cdm.measurement mmt_ht
	on mmt_ht.person_id = per.person_id and mmt_ht.measurement_concept_id = 3036277 -- Body height
	left join omop_cdm.death dth
	on dth.person_id = per.person_id
)
select
distinct(person_id) as person_id,
first_value(age) over (partition by person_id order by visit_start_datetime desc),
first_value(weight) over (partition by person_id order by wt_measurement_datetime desc),
first_value(height) over (partition by person_id order by ht_measurement_datetime desc),
gender,
ethnicity,
dob,
dod
from
t1
;
