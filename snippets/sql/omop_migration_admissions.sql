DO language plpgsql $$

BEGIN

RAISE NOTICE '%: Beginning of the transcaction', now();
RAISE NOTICE '%: Inserting in to etl_dataset_temp.src_admissions', now();

DROP TABLE IF EXISTS etl_dataset_temp.src_admissions;
CREATE TABLE etl_dataset_temp.src_admissions AS
SELECT
    hadm_id                             AS hadm_id, -- PK
    subject_id                          AS subject_id,
    admittime                           AS admittime,
    dischtime                           AS dischtime,
    deathtime                           AS deathtime,
    admission_type                      AS admission_type,
    admission_location                  AS admission_location,
    discharge_location                  AS discharge_location,
    ethnicity                           AS ethnicity,
    edregtime                           AS edregtime,
    insurance                           AS insurance,
    marital_status                      AS marital_status,
    language                            AS language,
    -- edouttime
    -- hospital_expire_flag
    --
    'admissions'                        AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id)                                  AS trace_id
FROM
    mimiciv.admissions
;



RAISE NOTICE '%: Inserting in to etl_dataset_temp.lk_admissions_clean', now();

DROP TABLE IF EXISTS etl_dataset_temp.lk_admissions_clean;
CREATE TABLE etl_dataset_temp.lk_admissions_clean AS
SELECT
    src.subject_id                                  AS subject_id,
    src.hadm_id                                     AS hadm_id,
	CASE 
		WHEN src.edregtime < src.admittime THEN src.edregtime
		ELSE src.admittime
		END 										AS start_datetime,
	
	
    src.dischtime                                   AS end_datetime,
    src.admission_type                              AS admission_type, -- current location
    src.admission_location                          AS admission_location, -- to hospital
    src.discharge_location                          AS discharge_location, -- from hospital
	CASE 
		WHEN src.edregtime IS NULL THEN FALSE
		ELSE TRUE
		END 										 AS is_er_admission, -- create visit_detail if TRUE
    -- 
    'admissions'                    AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
   etl_dataset_temp.src_admissions src -- adm
;





RAISE NOTICE '%: Inserting in to etl_dataset_temp.lk_visit_no_hadm_all', now();

DROP TABLE IF EXISTS etl_dataset_temp.lk_visit_no_hadm_all;
CREATE TABLE etl_dataset_temp.lk_visit_no_hadm_all AS
-- labevents
SELECT
    src.subject_id                                  AS subject_id,
    CAST(src.start_datetime AS DATE)                AS date_id,
    src.start_datetime                              AS start_datetime,
    -- 
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    etl_dataset.lk_meas_labevents_mapped src
WHERE
    src.hadm_id IS NULL
UNION ALL
-- specimen
SELECT
    src.subject_id                                  AS subject_id,
    CAST(src.start_datetime AS DATE)                AS date_id,
    src.start_datetime                              AS start_datetime,
    -- 
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    etl_dataset.lk_specimen_mapped src
WHERE
    src.hadm_id IS NULL
UNION ALL
-- organism
SELECT
    src.subject_id                                  AS subject_id,
    CAST(src.start_datetime AS DATE)                AS date_id,
    src.start_datetime                              AS start_datetime,
    -- 
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    etl_dataset.lk_meas_organism_mapped src
WHERE
    src.hadm_id IS NULL
UNION ALL
-- antibiotics
SELECT
    src.subject_id                                  AS subject_id,
    CAST(src.start_datetime AS DATE)                AS date_id,
    src.start_datetime                              AS start_datetime,
    -- 
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    etl_dataset.lk_meas_ab_mapped src
WHERE
    src.hadm_id IS NULL
-- UNION ALL
-- waveforms
-- SELECT
--     src.subject_id                                  AS subject_id,
--     CAST(src.start_datetime AS DATE)                AS date_id,
--     src.start_datetime                              AS start_datetime,
--     -- 
--     src.unit_id                     AS unit_id,
--     src.load_table_id               AS load_table_id,
--     src.load_row_id                 AS load_row_id,
--     src.trace_id                    AS trace_id
-- FROM
--     etl_dataset.lk_meas_waveform_mapped src
-- WHERE
--     src.hadm_id IS NULL
;

RAISE NOTICE '%: Inserting in to etl_dataset_temp.lk_visit_no_hadm_dist', now();

DROP TABLE IF EXISTS etl_dataset_temp.lk_visit_no_hadm_dist;
CREATE TABLE etl_dataset_temp.lk_visit_no_hadm_dist AS
SELECT
    src.subject_id                                  AS subject_id,
    src.date_id                                     AS date_id,
    MIN(src.start_datetime)                         AS start_datetime,
    MAX(src.start_datetime)                         AS end_datetime,
    'AMBULATORY OBSERVATION'                        AS admission_type, -- outpatient visit
    CAST(NULL AS TEXT)                            AS admission_location, -- to hospital
    CAST(NULL AS TEXT)                            AS discharge_location, -- from hospital
    -- 
    'no_hadm'                       AS unit_id,
    'lk_visit_no_hadm_all'          AS load_table_id,
    0                               AS load_row_id,
    jsonb_build_object('subject_id', src.subject_id, 'date_id', src.date_id )                                AS trace_id
FROM
    etl_dataset_temp.lk_visit_no_hadm_all src
GROUP BY
    src.subject_id,
    src.date_id
;





RAISE NOTICE '%: Inserting in to etl_dataset_temp.lk_visit_clean', now();

DROP TABLE IF EXISTS etl_dataset_temp.lk_visit_clean;
CREATE TABLE etl_dataset_temp.lk_visit_clean AS
SELECT
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int               AS visit_occurrence_id,
    src.subject_id                                  AS subject_id,
    src.hadm_id                                     AS hadm_id,
    CAST(NULL AS DATE)                              AS date_id,
    src.start_datetime                              AS start_datetime,
    src.end_datetime                                AS end_datetime,
    src.admission_type                              AS admission_type, -- current location
    src.admission_location                          AS admission_location, -- to hospital
    src.discharge_location                          AS discharge_location, -- from hospital
    CONCAT(
        CAST(src.subject_id AS TEXT), '|',
        CAST(src.hadm_id AS TEXT)
    )                                               AS source_value,
    -- 
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    etl_dataset_temp.lk_admissions_clean src -- adm
UNION ALL
SELECT
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int               AS visit_occurrence_id,
    src.subject_id                                  AS subject_id,
    CAST(NULL AS INTEGER)                             AS hadm_id,
    src.date_id                                     AS date_id,
    src.start_datetime                              AS start_datetime,
    src.end_datetime                                AS end_datetime,
    src.admission_type                              AS admission_type, -- current location
    src.admission_location                          AS admission_location, -- to hospital
    src.discharge_location                          AS discharge_location, -- from hospital
    CONCAT(
        CAST(src.subject_id AS TEXT), '|',
        CAST(src.date_id AS TEXT)
    )                                               AS source_value,
    -- 
    src.unit_id                     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM
    etl_dataset_temp.lk_visit_no_hadm_dist src -- adm
;




RAISE NOTICE '%: Inserting in to etl_dataset_temp.cdm_visit_occurrence', now();

DROP TABLE IF EXISTS etl_dataset_temp.cdm_visit_occurrence;
CREATE TABLE etl_dataset_temp.cdm_visit_occurrence
(
    visit_occurrence_id           INTEGER     not null ,
    person_id                     INTEGER     not null ,
    visit_concept_id              INTEGER     not null ,
    visit_start_date              DATE      not null ,
    visit_start_datetime          TIMESTAMP           ,
    visit_end_date                DATE      not null ,
    visit_end_datetime            TIMESTAMP           ,
    visit_type_concept_id         INTEGER     not null ,
    provider_id                   INTEGER              ,
    care_site_id                  INTEGER              ,
    visit_source_value            TEXT             ,
    visit_source_concept_id       INTEGER              ,
    admitting_source_concept_id   INTEGER              ,
    admitting_source_value        TEXT             ,
    discharge_to_concept_id       INTEGER              ,
    discharge_to_source_value     TEXT             ,
    preceding_visit_occurrence_id INTEGER              ,
    -- 
    unit_id                       TEXT,
    load_table_id                 TEXT,
    load_row_id                   INTEGER,
    trace_id                      TEXT
)
;


INSERT INTO etl_dataset_temp.cdm_visit_occurrence
SELECT
    src.visit_occurrence_id                 AS visit_occurrence_id,
    per.person_id                           AS person_id,
    COALESCE(lat.target_concept_id, 0)      AS visit_concept_id,
    CAST(src.start_datetime AS DATE)        AS visit_start_date,
    src.start_datetime                      AS visit_start_datetime,
    CAST(src.end_datetime AS DATE)          AS visit_end_date,
    src.end_datetime                        AS visit_end_datetime,
    32817                                   AS visit_type_concept_id,   -- EHR   Type Concept    Standard                          
    CAST(NULL AS INTEGER)                     AS provider_id,
    cs.care_site_id                         AS care_site_id,
    src.source_value                        AS visit_source_value, -- it should be an ID for visits
    COALESCE(lat.source_concept_id, 0)      AS visit_source_concept_id, -- it is where visit_concept_id comes from
   CASE
		WHEN src.admission_location IS NOT NULL THEN COALESCE(la.target_concept_id, 0)
		ELSE NULL 
	END 									AS admitting_source_concept_id,
    src.admission_location                  AS admitting_source_value,
	CASE
		WHEN src.discharge_location IS NOT NULL THEN COALESCE(ld.target_concept_id, 0)
		ELSE NULL
	END 									 AS discharge_to_concept_id,         
    src.discharge_location                  AS discharge_to_source_value,
    LAG(src.visit_occurrence_id) OVER ( 
        PARTITION BY subject_id, hadm_id 
        ORDER BY start_datetime
    )                                   AS preceding_visit_occurrence_id,
    --
    CONCAT('visit.', src.unit_id)   AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM 
    etl_dataset_temp.lk_visit_clean src
INNER JOIN
    etl_dataset.cdm_person per
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
LEFT JOIN 
    etl_dataset.lk_visit_concept lat
        ON lat.source_code = src.admission_type
LEFT JOIN 
    etl_dataset.lk_visit_concept la 
        ON la.source_code = src.admission_location
LEFT JOIN 
    etl_dataset.lk_visit_concept ld
        ON ld.source_code = src.discharge_location
LEFT JOIN 
    etl_dataset.cdm_care_site cs
        ON care_site_name = 'BIDMC' -- Beth Israel hospital for all
;




RAISE NOTICE '%: Inserting in to etl_dataset_temp.visit_occurrence', now();

CREATE TABLE etl_dataset_temp.visit_occurrence AS 
SELECT
    visit_occurrence_id,
    person_id,
    visit_concept_id,
    visit_start_date,
    visit_start_datetime,
    visit_end_date,
    visit_end_datetime,
    visit_type_concept_id,
    provider_id,
    care_site_id,
    visit_source_value,
    visit_source_concept_id,
    admitting_source_concept_id,
    admitting_source_value,
    discharge_to_concept_id,
    discharge_to_source_value,
    preceding_visit_occurrence_id
FROM etl_dataset_temp.cdm_visit_occurrence;

RAISE NOTICE '%: End of the transcaction', now();

END
$$;
