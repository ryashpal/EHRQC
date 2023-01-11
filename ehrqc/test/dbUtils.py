import os
import psycopg2
import pandas as pd

def _getDemographics():
    # information used to create a database connection
    sqluser = 'postgres'
    dbname = 'mimic4'
    hostname = 'localhost'
    port_number = 5434
    schema_name = 'mimiciv'

    # Connect to postgres with a copy of the MIMIC-III database
    con = psycopg2.connect(dbname=dbname, user=sqluser, host=hostname, port=port_number, password='mysecretpassword')

    # the below statement is prepended to queries to ensure they select from the right schema
    query_schema = 'set search_path to ' + schema_name + ';'

    query = query_schema + \
    """
    WITH ht AS
    (
    SELECT 
        c.subject_id, c.stay_id, c.charttime,
        -- Ensure that all heights are in centimeters, and fix data as needed
        CASE
            -- rule for neonates
            WHEN pt.anchor_age = 0
            AND (c.valuenum * 2.54) < 80
            THEN c.valuenum * 2.54
            -- rule for adults
            WHEN pt.anchor_age > 0
            AND (c.valuenum * 2.54) > 120
            AND (c.valuenum * 2.54) < 230
            THEN c.valuenum * 2.54
            -- set bad data to NULL
            ELSE NULL
        END AS height
        , ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY charttime DESC) AS rn
    FROM mimiciv.chartevents c
    INNER JOIN mimiciv.patients pt
        ON c.subject_id = pt.subject_id
    WHERE c.valuenum IS NOT NULL
    AND c.valuenum != 0
    AND c.itemid IN
    (
        226707 -- Height (measured in inches)
        -- note we intentionally ignore the below ITEMID in metavision
        -- these are duplicate data in a different unit
        -- , 226730 -- Height (cm)
    )
    )
    , wt AS
    (
        SELECT
            c.stay_id
        , c.charttime
        -- TODO: eliminate obvious outliers if there is a reasonable weight
        , c.valuenum as weight
        , ROW_NUMBER() OVER (PARTITION BY stay_id ORDER BY charttime DESC) AS rn
        FROM mimiciv.chartevents c
        WHERE c.valuenum IS NOT NULL
        AND c.itemid = 226512 -- Admit Wt
        AND c.stay_id IS NOT NULL
        AND c.valuenum > 0
    )
    SELECT
    ie.subject_id, ie.hadm_id, ie.stay_id
    , pat.gender AS gender
    , FLOOR(DATE_PART('day', adm.admittime - make_timestamp(pat.anchor_year, 1, 1, 0, 0, 0))/365.0) + pat.anchor_age as age
    , adm.ethnicity AS ethnicity
    , ht.height as height
    , wt.weight as weight
    , make_timestamp(pat.anchor_year, 1, 1, 0, 0, 0) as dob
    , pat.dod as dod
    FROM mimiciv.icustays ie
    INNER JOIN mimiciv.admissions adm
    ON ie.hadm_id = adm.hadm_id
    INNER JOIN mimiciv.patients pat
    ON ie.subject_id = pat.subject_id
    LEFT JOIN ht
    ON ie.stay_id = ht.stay_id AND ht.rn = 1
    LEFT JOIN wt
    ON ie.stay_id = wt.stay_id AND wt.rn = 1
    """

    static = pd.read_sql_query(query, con)

    return static


def _getVitals():
    # information used to create a database connection
    sqluser = 'postgres'
    dbname = 'mimic4'
    hostname = 'localhost'
    port_number = 5434
    schema_name = 'mimiciv'

    # Connect to postgres with a copy of the MIMIC-III database
    con = psycopg2.connect(dbname=dbname, user=sqluser, host=hostname, port=port_number, password='mysecretpassword')

    # the below statement is prepended to queries to ensure they select from the right schema
    query_schema = 'set search_path to ' + schema_name + ';'

    query = query_schema + \
    """
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
        FROM mimiciv.icustays icu
        INNER JOIN mimiciv.chartevents cev
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
    """

    vitals = pd.read_sql_query(query, con)

    return vitals


def _getLabMeasurements():
    # information used to create a database connection
    sqluser = 'postgres'
    dbname = 'mimic4'
    hostname = 'localhost'
    port_number = 5434

    # Connect to postgres with a copy of the MIMIC-III database
    con = psycopg2.connect(dbname=dbname, user=sqluser, host=hostname, port=port_number, password='mysecretpassword')

    curDir = os.path.dirname(__file__)
    mimicOmopSelectedLabMeasurementsPath = os.path.join(curDir, '../extract/sql/mimic_omop_selected_lab_measurements.sql')
    mimicOmopSelectedLabMeasurementsFile = open(mimicOmopSelectedLabMeasurementsPath)
    mimicOmopSelectedLabMeasurementsDf = pd.read_sql_query(mimicOmopSelectedLabMeasurementsFile.read(), con)
    return mimicOmopSelectedLabMeasurementsDf
