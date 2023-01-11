DROP TABLE IF EXISTS etl_dataset_temp.src_labevents;
CREATE TABLE etl_dataset_temp.src_labevents AS
SELECT
    labevent_id                         AS labevent_id,
    subject_id                          AS subject_id,
    charttime                           AS charttime,
    hadm_id                             AS hadm_id,
    itemid                              AS itemid,
    valueuom                            AS valueuom,
    value                               AS value,
    flag                                AS flag,
    ref_range_lower                     AS ref_range_lower,
    ref_range_upper                     AS ref_range_upper,
    --
    'labevents'                         AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('labevent_id',labevent_id)                                 AS trace_id
FROM
    mimiciv.labevents
;




DROP TABLE IF EXISTS etl_dataset_temp.lk_meas_labevents_clean;
CREATE TABLE etl_dataset_temp.lk_meas_labevents_clean AS
SELECT
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int       AS measurement_id,
    src.subject_id                          AS subject_id,
    src.charttime                           AS start_datetime, -- measurement_datetime,
    src.hadm_id                             AS hadm_id,
    src.itemid                              AS itemid,
    src.value                               AS value, -- value_source_value
    REGEXP_MATCHES(src.value, '^(\<=|\>=|\>|\<|=|)')   AS value_operator,
    REGEXP_MATCHES(src.value, '[-]?[\d]+[.]?[\d]*')    AS value_number, -- assume "-0.34 etc"
    CASE
    WHEN TRIM(src.valueuom) <> '' THEN src  .valueuom 
    ELSE NULL    
    END AS valueuom, -- unit_source_value,
    src.ref_range_lower                     AS ref_range_lower,
    src.ref_range_upper                     AS ref_range_upper,
    'labevents'                             AS unit_id,
    --
    src.load_table_id       AS load_table_id,
    src.load_row_id         AS load_row_id,
    src.trace_id            AS trace_id
FROM
    etl_dataset_temp.src_labevents src
INNER JOIN
    etl_dataset_temp.src_d_labitems dlab
        ON src.itemid = dlab.itemid
;




DROP TABLE IF EXISTS etl_dataset_temp.lk_meas_labevents_hadm_id;
CREATE TABLE etl_dataset_temp.lk_meas_labevents_hadm_id AS
SELECT
    src.trace_id                        AS event_trace_id, 
    adm.hadm_id                         AS hadm_id,
    ROW_NUMBER() OVER (
        PARTITION BY src.trace_id
        ORDER BY adm.start_datetime
    )                                   AS row_num
FROM  
    etl_dataset_temp.lk_meas_labevents_clean src
INNER JOIN 
    etl_dataset_temp.lk_admissions_clean adm
        ON adm.subject_id = src.subject_id
        AND src.start_datetime BETWEEN adm.start_datetime AND adm.end_datetime
WHERE
    src.hadm_id IS NULL
;




DROP TABLE IF EXISTS etl_dataset_temp.lk_meas_labevents_mapped;
CREATE TABLE etl_dataset_temp.lk_meas_labevents_mapped AS
SELECT
    src.measurement_id                      AS measurement_id,
    src.subject_id                          AS subject_id,
    COALESCE(src.hadm_id, hadm.hadm_id)     AS hadm_id,
    CAST(src.start_datetime AS DATE)        AS date_id,
    src.start_datetime                      AS start_datetime,
    src.itemid                              AS itemid,
    CAST(src.itemid AS TEXT)              AS source_code, -- change working source code to the representation
    labc.source_vocabulary_id               AS source_vocabulary_id,
    labc.source_concept_id                  AS source_concept_id,
    COALESCE(labc.target_domain_id, 'Measurement')  AS target_domain_id,
    labc.target_concept_id                  AS target_concept_id,
    src.valueuom                            AS unit_source_value,
    CASE 
    WHEN src.valueuom IS NOT NULL THEN COALESCE(uc.target_concept_id, 0)
    ELSE NULL
    END    AS unit_concept_id,
    src.value_operator                      AS operator_source_value,
    opc.target_concept_id                   AS operator_concept_id,
    src.value                               AS value_source_value,
    src.value_number                        AS value_as_number,
    CAST(NULL AS INTEGER)                     AS value_as_concept_id,
    src.ref_range_lower                     AS range_low,
    src.ref_range_upper                     AS range_high,
    --
    CONCAT('meas.', src.unit_id)    AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM  
    etl_dataset_temp.lk_meas_labevents_clean src
INNER JOIN 
    etl_dataset.lk_meas_d_labitems_concept labc
        ON labc.itemid = src.itemid
LEFT JOIN 
    etl_dataset.lk_meas_operator_concept opc
        ON opc.source_code = src.value_operator[1]
LEFT JOIN 
    etl_dataset.lk_meas_unit_concept uc
        ON uc.source_code = src.valueuom
LEFT JOIN 
    etl_dataset_temp.lk_meas_labevents_hadm_id hadm
        ON hadm.event_trace_id = src.trace_id
        AND hadm.row_num = 1
;



INSERT INTO etl_dataset_temp.cdm_measurement
SELECT
    src.measurement_id                      AS measurement_id,
    per.person_id                           AS person_id,
    COALESCE(src.target_concept_id, 0)      AS measurement_concept_id,
    CAST(src.start_datetime AS DATE)        AS measurement_date,
    src.start_datetime                      AS measurement_datetime,
    CAST(NULL AS TEXT)                    AS measurement_time,
    32856                                   AS measurement_type_concept_id, -- OMOP4976929 Lab
    src.operator_concept_id                 AS operator_concept_id,
    CAST(src.value_as_number[1] AS FLOAT)    AS value_as_number,  -- to move CAST to mapped/clean
    CAST(NULL AS INTEGER)                     AS value_as_concept_id,
    src.unit_concept_id                     AS unit_concept_id,
    src.range_low                           AS range_low,
    src.range_high                          AS range_high,
    CAST(NULL AS INTEGER)                     AS provider_id,
    vis.visit_occurrence_id                 AS visit_occurrence_id,
    CAST(NULL AS INTEGER)                     AS visit_detail_id,
    src.source_code                         AS measurement_source_value,
    src.source_concept_id                   AS measurement_source_concept_id,
    src.unit_source_value                   AS unit_source_value,
    src.value_source_value                  AS value_source_value,
    --
    CONCAT('measurement.', src.unit_id)     AS unit_id,
    src.load_table_id               AS load_table_id,
    src.load_row_id                 AS load_row_id,
    src.trace_id                    AS trace_id
FROM  
    etl_dataset_temp.lk_meas_labevents_mapped src -- 107,209 
INNER JOIN
    etl_dataset_temp.cdm_person per -- 110,849
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
INNER JOIN
    etl_dataset_temp.cdm_visit_occurrence vis -- 116,559
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS TEXT), '|', 
                COALESCE(CAST(src.hadm_id AS TEXT), CAST(src.date_id AS TEXT)))
WHERE
    src.target_domain_id = 'Measurement' -- 115,272
;




INSERT INTO etl_dataset_temp.measurement
SELECT
    measurement_id,
    person_id,
    measurement_concept_id,
    measurement_date,
    measurement_datetime,
    measurement_time,
    measurement_type_concept_id,
    operator_concept_id,
    value_as_number,
    value_as_concept_id,
    unit_concept_id,
    range_low,
    range_high,
    provider_id,
    visit_occurrence_id,
    visit_detail_id,
    measurement_source_value,
    measurement_source_concept_id,
    unit_source_value,
    value_source_value
FROM etl_dataset_temp.cdm_measurement;

