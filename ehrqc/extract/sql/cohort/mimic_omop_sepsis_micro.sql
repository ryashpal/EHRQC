with stg1 as
(
	select
	subject_id
	, min(chartdate) as chartdate
	, min(charttime) as charttime
	from
	mimiciv.microbiologyevents
	where
	spec_type_desc = 'BLOOD CULTURE'
	and org_itemid != 90760
	and org_name is not null
	group by subject_id
),
stg2 as (
	select
	per.person_id
	, coalesce(stg1.charttime, stg1.chartdate) as chart_time
	from stg1
	inner join mimiciv.patients pat
	on stg1.subject_id = pat.subject_id
	inner join __schema_name__.person per
	on per.person_source_value::int = pat.subject_id
	where (floor(date_part('day', stg1.chartdate - make_timestamp(pat.anchor_year, 1, 1, 0, 0, 0))/365.0) + pat.anchor_age) > 18
)
select
*
from
stg2
;
