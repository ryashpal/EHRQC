DO language plpgsql $$
BEGIN

RAISE NOTICE '%: Beginning of the transcaction', now();
RAISE NOTICE '%: Inserting in to etl_dataset_temp.src_patients', now();

DROP TABLE IF EXISTS etl_dataset_temp.src_patients;
CREATE TABLE etl_dataset_temp.src_patients AS
SELECT
    subject_id                          AS subject_id,
    anchor_year                         AS anchor_year,
    anchor_age                          AS anchor_age,
    anchor_year_group                   AS anchor_year_group,
    gender                              AS gender,

    'patients'                          AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('subject_id',subject_id)                                  AS trace_id
FROM
    mimiciv.patients
;

RAISE NOTICE '%: Inserting in to etl_dataset_temp.tmp_subject_ethnicity', now();

DROP TABLE IF EXISTS etl_dataset_temp.tmp_subject_ethnicity;
CREATE TABLE etl_dataset_temp.tmp_subject_ethnicity AS
SELECT DISTINCT
    src.subject_id                      AS subject_id,
    FIRST_VALUE(src.ethnicity) OVER (
        PARTITION BY src.subject_id
        ORDER BY src.admittime ASC)   AS ethnicity_first
FROM
  etl_dataset_temp.src_admissions src
;

RAISE NOTICE '%: Inserting in to etl_dataset_temp.lk_pat_ethnicity_concept', now();

DROP TABLE IF EXISTS etl_dataset_temp.lk_pat_ethnicity_concept;
CREATE TABLE etl_dataset_temp.lk_pat_ethnicity_concept AS
SELECT DISTINCT
    src.ethnicity_first   AS source_code,
    vc.concept_id         AS source_concept_id,
    vc.vocabulary_id      AS source_vocabulary_id,
    vc1.concept_id        AS target_concept_id,
    vc1.vocabulary_id     AS target_vocabulary_id -- look here to distinguish Race and Ethnicity
FROM
    etl_dataset_temp.tmp_subject_ethnicity src
LEFT JOIN
  -- gcpt_ethnicity_to_concept -> mimiciv_per_ethnicity
    voc_dataset.concept vc
      ON UPPER(vc.concept_code) = UPPER(src.ethnicity_first) -- do the custom mapping
        AND vc.domain_id IN ('Race', 'Ethnicity')
LEFT JOIN
   voc_dataset.concept_relationship cr1
        ON  cr1.concept_id_1 = vc.concept_id
        AND cr1.relationship_id = 'Maps to'
LEFT JOIN
    voc_dataset.concept vc1
        ON  cr1.concept_id_2 = vc1.concept_id
        AND vc1.invalid_reason IS NULL
        AND vc1.standard_concept = 'S'
;

RAISE NOTICE '%: Inserting in to etl_dataset_temp.cdm_person', now();

DROP TABLE if exists etl_dataset_temp.cdm_person;
--HINT DISTRIBUTE_ON_KEY(person_id)
CREATE TABLE etl_dataset_temp.cdm_person
(
    person_id                 INTEGER   not null ,
    gender_concept_id         INTEGER   not null ,
    year_of_birth             INTEGER   not null ,
    month_of_birth            INTEGER           ,
    day_of_birth              INTEGER           ,
  birth_DATETIME              TIMESTAMP        ,
    race_concept_id           INTEGER   not null,
    ethnicity_concept_id      INTEGER   not null,
    location_id               INTEGER           ,
    provider_id               INTEGER           ,
    care_site_id              INTEGER           ,
    person_source_value       TEXT          ,
    gender_source_value       TEXT          ,
    gender_source_concept_id  INTEGER           ,
    race_source_value         TEXT          ,
    race_source_concept_id    INTEGER           ,
    ethnicity_source_value    TEXT          ,
    ethnicity_source_concept_id INTEGER           ,
  --
    unit_id                     TEXT,
    load_table_id               TEXT,
    load_row_id                 INTEGER,
    trace_id                    TEXT
)
;

INSERT INTO etl_dataset_temp.cdm_person
SELECT
 
  ('x'||substr(md5(random():: text),1,8))::bit(32)::int AS person_id,
  CASE
        WHEN p.gender = 'F' THEN 8532 -- FEMALE
        WHEN p.gender = 'M' THEN 8507 -- MALE
        ELSE 0
    END                           AS gender_concept_id,
    p.anchor_year                 AS year_of_birth,
    CAST(NULL AS INTEGER)             AS month_of_birth,
    CAST(NULL AS INTEGER)             AS day_of_birth,
    CAST(NULL AS TIMESTAMP)          AS birth_DATETIME,
    COALESCE(
        CASE
            WHEN map_eth.target_vocabulary_id <> 'Ethnicity'
                THEN map_eth.target_concept_id
            ELSE NULL
        END, 0)                               AS race_concept_id,
    COALESCE(
        CASE
            WHEN map_eth.target_vocabulary_id = 'Ethnicity'
                THEN map_eth.target_concept_id
            ELSE NULL
        END, 0)                   AS ethnicity_concept_id,
    CAST(NULL AS INTEGER)             AS location_id,
    CAST(NULL AS INTEGER)             AS provider_id,
    CAST(NULL AS INTEGER)             AS care_site_id,
  CAST(p.subject_id AS TEXT)  AS person_source_value,
    p.gender                      AS gender_source_value,
  0                             AS gender_source_concept_id,
  CASE
        WHEN map_eth.target_vocabulary_id <> 'Ethnicity'
            THEN eth.ethnicity_first
        ELSE NULL
    END                           AS race_source_value,
    COALESCE(
        CASE
            WHEN map_eth.target_vocabulary_id <> 'Ethnicity'
                THEN map_eth.source_concept_id
            ELSE NULL
        END, 0)                     AS race_source_concept_id,
  CASE
        WHEN map_eth.target_vocabulary_id = 'Ethnicity'
            THEN eth.ethnicity_first
        ELSE NULL
    END                           AS ethnicity_source_value,
    COALESCE(
        CASE
            WHEN map_eth.target_vocabulary_id = 'Ethnicity'
                THEN map_eth.source_concept_id
            ELSE NULL
        END, 0)                   AS ethnicity_source_concept_id,
  --
    'person.patients'             AS unit_id,
    p.load_table_id               AS load_table_id,
    p.load_row_id                 AS load_row_id,
    p.trace_id                    AS trace_id
FROM
  etl_dataset_temp.src_patients p
LEFT JOIN
    etl_dataset_temp.tmp_subject_ethnicity eth
        ON  p.subject_id = eth.subject_id
LEFT JOIN
    etl_dataset_temp.lk_pat_ethnicity_concept map_eth
        ON  eth.ethnicity_first = map_eth.source_code
;

DROP TABLE etl_dataset_temp.tmp_subject_ethnicity;

RAISE NOTICE '%: Inserting in to etl_dataset_temp.tmp_observation_period_clean', now();

DROP TABLE IF EXISTS etl_dataset_temp.tmp_observation_period_clean;

RAISE NOTICE '%: Inserting in to etl_dataset_temp.tmp_observation_period_clean from etl_dataset_temp.cdm_visit_occurrence', now();

CREATE TABLE etl_dataset_temp.tmp_observation_period_clean AS
SELECT
    src.person_id               AS person_id,
    MIN(src.visit_start_date)   AS start_date,
    MAX(src.visit_end_date)     AS end_date,
    src.unit_id                 AS unit_id
FROM
    etl_dataset_temp.cdm_visit_occurrence src
GROUP BY
    src.person_id, src.unit_id
;

RAISE NOTICE '%: Inserting in to etl_dataset_temp.tmp_observation_period_clean from etl_dataset.cdm_condition_occurrence', now();

INSERT INTO etl_dataset_temp.tmp_observation_period_clean
SELECT
    src.person_id               AS person_id,
    MIN(src.condition_start_date)   AS start_date,
    MAX(src.condition_end_date)     AS end_date,
    src.unit_id                 AS unit_id
FROM
    etl_dataset.cdm_condition_occurrence src
GROUP BY
    src.person_id, src.unit_id
;

RAISE NOTICE '%: Inserting in to etl_dataset_temp.tmp_observation_period_clean from etl_dataset.cdm_procedure_occurrence', now();

INSERT INTO etl_dataset_temp.tmp_observation_period_clean
SELECT
    src.person_id               AS person_id,
    MIN(src.procedure_date)   AS start_date,
    MAX(src.procedure_date)     AS end_date,
    src.unit_id                 AS unit_id
FROM
    etl_dataset.cdm_procedure_occurrence src
GROUP BY
    src.person_id, src.unit_id
;

RAISE NOTICE '%: Inserting in to etl_dataset_temp.tmp_observation_period_clean from etl_dataset.cdm_drug_exposure', now();

INSERT INTO etl_dataset_temp.tmp_observation_period_clean
SELECT
    src.person_id               AS person_id,
    MIN(src.drug_exposure_start_date)   AS start_date,
    MAX(src.drug_exposure_end_date)     AS end_date,
    src.unit_id                 AS unit_id
FROM
    etl_dataset.cdm_drug_exposure src
GROUP BY
    src.person_id, src.unit_id
;

RAISE NOTICE '%: Inserting in to etl_dataset_temp.tmp_observation_period_clean from etl_dataset.cdm_device_exposure', now();

INSERT INTO etl_dataset_temp.tmp_observation_period_clean
SELECT
    src.person_id               AS person_id,
    MIN(src.device_exposure_start_date)   AS start_date,
    MAX(src.device_exposure_end_date)     AS end_date,
    src.unit_id                 AS unit_id
FROM
    etl_dataset.cdm_device_exposure src
GROUP BY
    src.person_id, src.unit_id
;

RAISE NOTICE '%: Inserting in to etl_dataset_temp.tmp_observation_period_clean from etl_dataset.cdm_measurement', now();

INSERT INTO etl_dataset_temp.tmp_observation_period_clean
SELECT
    src.person_id               AS person_id,
    MIN(src.measurement_date)   AS start_date,
    MAX(src.measurement_date)     AS end_date,
    src.unit_id                 AS unit_id
FROM
    etl_dataset.cdm_measurement src
GROUP BY
    src.person_id, src.unit_id
;

RAISE NOTICE '%: Inserting in to etl_dataset_temp.tmp_observation_period_clean from etl_dataset.cdm_specimen', now();

INSERT INTO etl_dataset_temp.tmp_observation_period_clean
SELECT
    src.person_id               AS person_id,
    MIN(src.specimen_date)   AS start_date,
    MAX(src.specimen_date)     AS end_date,
    src.unit_id                 AS unit_id
FROM
    etl_dataset.cdm_specimen src
GROUP BY
    src.person_id, src.unit_id
;

RAISE NOTICE '%: Inserting in to etl_dataset_temp.tmp_observation_period_clean from etl_dataset.cdm_observation', now();

INSERT INTO etl_dataset_temp.tmp_observation_period_clean
SELECT
    src.person_id               AS person_id,
    MIN(src.observation_date)   AS start_date,
    MAX(src.observation_date)     AS end_date,
    src.unit_id                 AS unit_id
FROM
    etl_dataset.cdm_observation src
GROUP BY
    src.person_id, src.unit_id
;

RAISE NOTICE '%: Inserting in to etl_dataset_temp.tmp_observation_period_clean from etl_dataset.cdm_death', now();

INSERT INTO etl_dataset_temp.tmp_observation_period_clean
SELECT
    src.person_id               AS person_id,
    MIN(src.death_date)         AS start_date,
    MAX(src.death_date)         AS end_date,
    src.unit_id                 AS unit_id
FROM
    etl_dataset.cdm_death src
GROUP BY
    src.person_id, src.unit_id
;

RAISE NOTICE '%: Inserting in to etl_dataset_temp.tmp_observation_period', now();

DROP TABLE IF EXISTS etl_dataset_temp.tmp_observation_period;
CREATE TABLE etl_dataset_temp.tmp_observation_period AS
SELECT
    src.person_id               AS person_id,
    MIN(src.start_date)   AS start_date,
    MAX(src.end_date)     AS end_date,
    src.unit_id                 AS unit_id
FROM
    etl_dataset_temp.tmp_observation_period_clean src
GROUP BY
    src.person_id, src.unit_id
;

RAISE NOTICE '%: Inserting in to etl_dataset_temp.cdm_observation_period', now();

DROP TABLE IF EXISTS etl_dataset_temp.cdm_observation_period;
CREATE TABLE etl_dataset_temp.cdm_observation_period
(
    observation_period_id             INTEGER   not null ,
    person_id                         INTEGER   not null ,
    observation_period_start_date     DATE    not null ,
    observation_period_end_date       DATE    not null ,
    period_type_concept_id            INTEGER   not null ,
    -- 
    unit_id                       TEXT,
    load_table_id                 TEXT,
    load_row_id                   INTEGER,
    trace_id                      TEXT
)
;

INSERT INTO etl_dataset_temp.cdm_observation_period
SELECT
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int          AS observation_period_id,
    src.person_id                               AS person_id,
    MIN(src.start_date)                         AS observation_period_start_date,
    MAX(src.end_date)                           AS observation_period_end_date,
    32828                                       AS period_type_concept_id,  -- 32828    OMOP4976901 EHR episode record
    --
    'observation_period'                        AS unit_id,
    'event tables'                              AS load_table_id,
    0                                           AS load_row_id,
    CAST(NULL AS TEXT)                        AS trace_id
FROM 
    etl_dataset_temp.tmp_observation_period src
GROUP BY
    src.person_id
;

RAISE NOTICE '%: Inserting in to etl_dataset_temp.tmp_person', now();

DROP TABLE IF EXISTS etl_dataset_temp.tmp_person;
CREATE TABLE etl_dataset_temp.tmp_person AS
SELECT per.*
FROM 
    etl_dataset_temp.cdm_person per
INNER JOIN
    etl_dataset_temp.cdm_observation_period op
        ON  per.person_id = op.person_id
;

RAISE NOTICE '%: Truncating table etl_dataset_temp.cdm_person', now();

TRUNCATE TABLE etl_dataset_temp.cdm_person;

RAISE NOTICE '%: Truncating table etl_dataset_temp.cdm_person from etl_dataset_temp.tmp_person', now();

INSERT INTO etl_dataset_temp.cdm_person
SELECT per.*
FROM
    etl_dataset_temp.tmp_person per
;

DROP TABLE IF EXISTS etl_dataset_temp.tmp_person;

RAISE NOTICE '%: Truncating table etl_dataset_temp.cdm_person from etl_dataset_temp.person', now();

DROP TABLE IF EXISTS etl_dataset_temp.person;
CREATE TABLE etl_dataset_temp.person AS 
SELECT
    person_id,
    gender_concept_id,
    year_of_birth,
    month_of_birth,
    day_of_birth,
    birth_datetime,
    race_concept_id,
    ethnicity_concept_id,
    location_id,
    provider_id,
    care_site_id,
    person_source_value,
    gender_source_value,
    gender_source_concept_id,
    race_source_value,
    race_source_concept_id,
    ethnicity_source_value,
    ethnicity_source_concept_id
FROM etl_dataset_temp.cdm_person;

RAISE NOTICE '%: end of the transcaction', now();

END
$$;
