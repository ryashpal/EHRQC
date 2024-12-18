WITH vitals_stg_1 AS
(
SELECT
co.person_id, mmt.measurement_datetime
    , CASE
            WHEN mmt.measurement_source_value = '223761' THEN (mmt.value_as_number-32)/1.8
        ELSE mmt.value_as_number
    END AS valuenum
    , CASE
            WHEN mmt.measurement_source_value = '220045' THEN 'HEARTRATE'
            WHEN mmt.measurement_source_value = '220050' THEN 'SYSBP'
            WHEN mmt.measurement_source_value = '220179' THEN 'SYSBP'
            WHEN mmt.measurement_source_value = '220051' THEN 'DIASBP'
            WHEN mmt.measurement_source_value = '220180' THEN 'DIASBP'
            WHEN mmt.measurement_source_value = '220052' THEN 'MEANBP'
            WHEN mmt.measurement_source_value = '220181' THEN 'MEANBP'
            WHEN mmt.measurement_source_value = '225312' THEN 'MEANBP'
            WHEN mmt.measurement_source_value = '220210' THEN 'RESPRATE'
            WHEN mmt.measurement_source_value = '224688' THEN 'RESPRATE'
            WHEN mmt.measurement_source_value = '224689' THEN 'RESPRATE'
            WHEN mmt.measurement_source_value = '224690' THEN 'RESPRATE'
            WHEN mmt.measurement_source_value = '223761' THEN 'TEMPC'
            WHEN mmt.measurement_source_value = '223762' THEN 'TEMPC'
            WHEN mmt.measurement_source_value = '220277' THEN 'SPO2'
            WHEN mmt.measurement_source_value = '220739' THEN 'GCSEYE'
            WHEN mmt.measurement_source_value = '223900' THEN 'GCSVERBAL'
            WHEN mmt.measurement_source_value = '223901' THEN 'GCSMOTOR'
        ELSE null
        END AS label
FROM __schema_name__.visit_occurrence vo
INNER JOIN __schema_name__.measurement mmt
    ON vo.visit_occurrence_id = mmt.visit_occurrence_id
INNER JOIN __schema_name__.cohort co
	ON co.person_id = mmt.person_id
    AND mmt.measurement_datetime >= co.anchor_time - interval '__before_time__ hour'
    AND mmt.measurement_datetime <= co.anchor_time + interval '__after_time__ hour'
WHERE mmt.measurement_source_value IN
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
, vitals_stg_2 AS
(
SELECT
    person_id, valuenum, label
    , ROW_NUMBER() OVER (PARTITION BY person_id, label ORDER BY measurement_datetime) AS rn
FROM vitals_stg_1
)
, vitals_stg_3 AS
(
SELECT
    person_id
    , rn
    , COALESCE(MAX(CASE WHEN label = 'HEARTRATE' THEN valuenum ELSE null END)) AS heartrate
    , COALESCE(MAX(CASE WHEN label = 'SYSBP' THEN valuenum ELSE null END)) AS sysbp
    , COALESCE(MAX(CASE WHEN label = 'DIASBP' THEN valuenum ELSE null END)) AS diabp
    , COALESCE(MAX(CASE WHEN label = 'MEANBP' THEN valuenum ELSE null END)) AS meanbp
    , COALESCE(MAX(CASE WHEN label = 'RESPRATE' THEN valuenum ELSE null END)) AS resprate
    , COALESCE(MAX(CASE WHEN label = 'TEMPC' THEN valuenum ELSE null END)) AS tempc
    , COALESCE(MAX(CASE WHEN label = 'SPO2' THEN valuenum ELSE null END)) AS spo2
    , COALESCE(MAX(CASE WHEN label = 'GCSEYE' THEN valuenum ELSE null END)) AS gcseye
    , COALESCE(MAX(CASE WHEN label = 'GCSVERBAL' THEN valuenum ELSE null END)) AS gcsverbal
    , COALESCE(MAX(CASE WHEN label = 'GCSMOTOR' THEN valuenum ELSE null END)) AS gcsmotor
FROM vitals_stg_2
GROUP BY person_id, rn
)
, vitals_stg_4 AS
(
    SELECT
    person_id
    , __agg_function__(heartrate) AS heartrate
    , __agg_function__(sysbp) AS sysbp
    , __agg_function__(diabp) AS diabp
    , __agg_function__(meanbp) AS meanbp
    , __agg_function__(resprate) AS resprate
    , __agg_function__(tempc) AS tempc
    , __agg_function__(spo2) AS spo2
    , __agg_function__(gcseye) AS gcseye
    , __agg_function__(gcsverbal) AS gcsverbal
    , __agg_function__(gcsmotor) AS gcsmotor
    FROM vitals_stg_3
    GROUP BY person_id
)
SELECT * FROM vitals_stg_4
