with q1 as (
    select
    per.person_id as person_id,
    (floor(date_part('day', vo.visit_start_datetime - make_timestamp(per.year_of_birth, 1, 1, 0, 0, 0))/365.0) + pat.anchor_age) as age,
    per.gender_source_value as gender,
    coalesce(per.race_source_value, per.ethnicity_source_value) as ethnicity,
    vo.visit_start_datetime as visit_start_datetime
    from
    __schema_name__.person per
    inner join __schema_name__.cohort coh
    on coh.person_id = per.person_id
    inner join mimiciv.patients pat
    on pat.subject_id = per.person_source_value::int
    left join __schema_name__.visit_occurrence vo
    on vo.person_id = per.person_id
    left join __schema_name__.death dth
    on dth.person_id = per.person_id
)
select
person_id,
min(age) as age,
min(gender) as gender,
min(ethnicity) as ethnicity
from
q1
group by person_id
;
