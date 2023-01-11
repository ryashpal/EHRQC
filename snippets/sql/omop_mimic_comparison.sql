WITH id_map AS (
	SELECT
	visit_occurrence_id AS visit_occurrence_id,
	COALESCE(convert_to_integer(SUBSTR(trace_id, 12, 9)), 0) AS hadm_id
	FROM etl_dataset.cdm_visit_detail
),
mimic_ids AS (
	SELECT 
		DISTINCT idm.visit_occurrence_id AS visit_occurrence_id
	FROM mimiciv.icustays icu
	INNER JOIN mimiciv.admissions adm
	ON adm.hadm_id = icu.hadm_id
	INNER JOIN mimiciv.chartevents cev
	ON cev.stay_id = icu.stay_id
	AND cev.charttime >= icu.intime
	AND cev.charttime <= icu.intime + interval '24 hour'
	INNER JOIN id_map idm
	ON idm.hadm_id = icu.hadm_id
	WHERE cev.itemid IN
	(
		220045 -- heartrate
		, 220050, 220179 -- sysbp
		, 220051, 220180 -- diasbp
		, 220052, 220181, 225312 -- meanbp
		, 220210, 224688, 224689, 224690 -- resprate
		, 223761, 223762 -- tempc
		, 220277 -- SpO2
		, 220739 -- gcseye
		, 223900 -- gcsverbal
		, 223901 -- gscmotor
	)
	AND valuenum IS NOT null
),
omop_ids AS(
	SELECT
		DISTINCT vo.visit_occurrence_id AS visit_occurrence_id
	FROM omop_cdm.visit_occurrence vo
	INNER JOIN omop_cdm.measurement mmt
		ON vo.visit_occurrence_id = mmt.visit_occurrence_id
		AND mmt.measurement_datetime >= vo.visit_start_datetime
		AND mmt.measurement_datetime <= vo.visit_start_datetime + interval '24 hour'
		AND mmt.measurement_source_value IN
		(
			'220045' -- heartrate
			, '220050', '220179' -- sysbp
			, '220051', '220180' -- diasbp
			, '220052', '220181', '225312' -- meanbp
			, '220210', '224688', '224689', '224690' -- resprate
			, '223761', '223762' -- tempc
			, '220277' -- SpO2
			, '220739' -- gcseye
			, '223900' -- gcsverbal
			, '223901' -- gscmotor
		)
		AND mmt.value_as_number IS NOT null
)
SELECT
COUNT(DISTINCT(mimic_ids.visit_occurrence_id)) AS mimic_count,
COUNT(DISTINCT(omop_ids.visit_occurrence_id)) AS omop_count
FROM
mimic_ids, omop_ids
;
