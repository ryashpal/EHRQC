select
coh.person_id as person_id
, coh.anchor_time
, dth.death_datetime
from
__schema_name__.cohort coh
left join __schema_name__.death dth
on dth.person_id = coh.person_id
;
