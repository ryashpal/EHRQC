WITH vitals_stg_1 AS
(
    SELECT icu.stay_id, cev.charttime
    , CASE
            WHEN itemid = 223761 THEN (cev.valuenum-32)/1.8
        ELSE cev.valuenum
    END AS valuenum
    , CASE
            WHEN itemid = 220045 THEN 'HEARTRATE'
            WHEN itemid = 220050 THEN 'SYSBP'
            WHEN itemid = 220179 THEN 'SYSBP'
            WHEN itemid = 220051 THEN 'DIASBP'
            WHEN itemid = 220180 THEN 'DIASBP'
            WHEN itemid = 220052 THEN 'MEANBP'
            WHEN itemid = 220181 THEN 'MEANBP'
            WHEN itemid = 225312 THEN 'MEANBP'
            WHEN itemid = 220210 THEN 'RESPRATE'
            WHEN itemid = 224688 THEN 'RESPRATE'
            WHEN itemid = 224689 THEN 'RESPRATE'
            WHEN itemid = 224690 THEN 'RESPRATE'
            WHEN itemid = 223761 THEN 'TEMPC'
            WHEN itemid = 223762 THEN 'TEMPC'
            WHEN itemid = 220277 THEN 'SPO2'
            WHEN itemid = 220739 THEN 'GCSEYE'
            WHEN itemid = 223900 THEN 'GCSVERBAL'
            WHEN itemid = 223901 THEN 'GCSMOTOR'
        ELSE null
        END AS label
    FROM __schema_name__.icustays icu
    INNER JOIN __schema_name__.chartevents cev
    ON cev.stay_id = icu.stay_id
    AND cev.charttime >= icu.intime
    AND cev.charttime <= icu.intime + interval '24 hour'
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
)
, vitals_stg_2 AS
(
SELECT
    stay_id, valuenum, label
    , ROW_NUMBER() OVER (PARTITION BY stay_id, label ORDER BY charttime) AS rn
FROM vitals_stg_1
)
, vitals_stg_3 AS
(
SELECT
    stay_id
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
GROUP BY stay_id, rn
)
, vitals_stg_4 AS
(
    SELECT
    stay_id,
    AVG(heartrate) AS heartrate
    , AVG(sysbp) AS sysbp
    , AVG(diabp) AS diabp
    , AVG(meanbp) AS meanbp
    , AVG(resprate) AS resprate
    , AVG(tempc) AS tempc
    , AVG(spo2) AS spo2
    , AVG(gcseye) AS gcseye
    , AVG(gcsverbal) AS gcsverbal
    , AVG(gcsmotor) AS gcsmotor
    FROM vitals_stg_3
    GROUP BY stay_id
)
SELECT * FROM vitals_stg_4
