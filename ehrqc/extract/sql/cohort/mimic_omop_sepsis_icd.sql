select
per.person_id,
min(vis.visit_start_datetime) as anchor_time
from
__schema_name__.condition_occurrence con
inner join __schema_name__.visit_occurrence vis
on con.visit_occurrence_id = vis.visit_occurrence_id
inner join mimiciv.admissions adm
on adm.hadm_id = split_part(vis.visit_source_value, '|', 2)::int
inner join mimiciv.patients pat
on pat.subject_id = adm.subject_id
inner join __schema_name__.person per
on per.person_id = con.person_id
where vis.visit_source_value not like '%-%'
and (floor(date_part('day', vis.visit_start_datetime - make_timestamp(pat.anchor_year, 1, 1, 0, 0, 0))/365.0) + pat.anchor_age) > 18
and (con.condition_source_value in ('99591', '99592', '78552', 'A419', 'R6520', 'R6521'))
group by per.person_id
;
