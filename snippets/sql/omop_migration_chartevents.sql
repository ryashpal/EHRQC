DO language plpgsql $$
BEGIN

RAISE NOTICE '%: Beginning of the transcaction', now();
RAISE NOTICE '%: Inserting in to etl_dataset_temp.src_chartevents', now();

DROP TABLE IF EXISTS etl_dataset_temp.src_chartevents;
CREATE TABLE etl_dataset_temp.src_chartevents AS
SELECT
    subject_id  AS subject_id,
    hadm_id     AS hadm_id,
    stay_id     AS stay_id,
    itemid      AS itemid,
    charttime   AS charttime,
    value       AS value,
    valuenum    AS valuenum,
    valueuom    AS valueuom,
    --
    'chartevents'                       AS load_table_id,
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
    jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id, 'stay_id',  stay_id, 'charttime', charttime)                                 AS trace_id
FROM
    mimiciv.chartevents
;

RAISE NOTICE '%: Inserting in to etl_dataset_temp.lk_chartevents_clean', now();

DROP TABLE IF EXISTS etl_dataset_temp.lk_chartevents_clean;
CREATE TABLE etl_dataset_temp.lk_chartevents_clean AS
SELECT
    src.subject_id                  AS subject_id,
    src.hadm_id                     AS hadm_id,
    src.stay_id                     AS stay_id,
    src.itemid                      AS itemid,
    CAST(src.itemid AS TEXT)      AS source_code,
    di.label                        AS source_label,
    src.charttime                   AS start_datetime,
    TRIM(src.value)                 AS value,
	CASE
		WHEN REGEXP_MATCH(TRIM(src.value), '^[-]?[\d]+[.]?[\d]*[ ]*[a-z]+$') IS NOT NULL THEN CAST((REGEXP_MATCH(src.value, '[-]?[\d]+[.]?[\d]*'))[1] AS FLOAT)
		ELSE src.valuenum																			   
    END                        AS valuenum,
	
	CASE
		WHEN REGEXP_MATCH(TRIM(src.value), '^[-]?[\d]+[.]?[\d]*[ ]*[a-z]+$') IS NOT NULL THEN REGEXP_MATCH(src.value, '[a-z]+')::character varying(20)
		ELSE src.valueuom
	END                AS valueuom, -- unit of measurement
    --
    'chartevents'           AS unit_id,
    src.load_table_id       AS load_table_id,
    src.load_row_id         AS load_row_id,
    src.trace_id            AS trace_id
FROM
    etl_dataset_temp.src_chartevents src -- ce
INNER JOIN
    etl_dataset.src_d_items di
        ON  src.itemid = di.itemid
WHERE
    di.label NOT LIKE '%Temperature'
    OR di.label LIKE '%Temperature' 
    AND 
	CASE
		WHEN valueuom LIKE '%F%' THEN (valuenum - 32) * 5 / 9
		ELSE valuenum
	END BETWEEN 25 and 44
;

RAISE NOTICE '%: Inserting in to etl_dataset_temp.lk_chartevents_mapped', now();

DROP TABLE IF EXISTS etl_dataset_temp.lk_chartevents_mapped;
CREATE TABLE etl_dataset_temp.lk_chartevents_mapped AS
SELECT
    ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS measurement_id,
    src.subject_id                              AS subject_id,
    src.hadm_id                                 AS hadm_id,
    src.stay_id                                 AS stay_id,
    src.start_datetime                          AS start_datetime,
    32817                                       AS type_concept_id,  -- OMOP4976890 EHR
    src.itemid                                  AS itemid,
    src.source_code                             AS source_code,
    src.source_label                            AS source_label,
    c_main.source_vocabulary_id                 AS source_vocabulary_id,
    c_main.source_domain_id                     AS source_domain_id,
    c_main.source_concept_id                    AS source_concept_id,
    c_main.target_domain_id                     AS target_domain_id,
    c_main.target_concept_id                    AS target_concept_id,
    CASE
		WHEN src.valuenum IS NULL THEN src.value 
		ELSE NULL
	END											 AS value_source_value,
	
	CASE
		WHEN  (src.valuenum IS NULL) AND (src.value IS NOT NULL)  THEN COALESCE(c_value.target_concept_id, 0)
		ELSE  NULL
	END 										 AS value_as_concept_id,

        src.valuenum                                AS value_as_number,
    src.valueuom                                AS unit_source_value, -- unit of measurement

   CASE
		WHEN src.valueuom IS NOT NULL THEN  COALESCE(uc.target_concept_id, 0)
		ELSE NULL								
	END 						AS unit_concept_id,

    --
    CONCAT('meas.', src.unit_id)                AS unit_id,
    src.load_table_id       AS load_table_id,
    src.load_row_id         AS load_row_id,
    src.trace_id            AS trace_id
FROM
    etl_dataset_temp.lk_chartevents_clean src -- ce
LEFT JOIN
    etl_dataset.lk_chartevents_concept c_main -- main
        ON c_main.source_code = src.source_code 
        AND c_main.source_vocabulary_id = 'mimiciv_meas_chart'
LEFT JOIN
    etl_dataset.lk_chartevents_concept c_value -- values for main
        ON c_value.source_code = src.value
        AND c_value.source_vocabulary_id = 'mimiciv_meas_chartevents_value'
        AND c_value.target_domain_id = 'Meas Value'
LEFT JOIN 
    etl_dataset.lk_meas_unit_concept uc
        ON uc.source_code = src.valueuom
;

RAISE NOTICE '%: Inserting in to etl_dataset_temp.cdm_measurement', now();

DROP TABLE IF EXISTS etl_dataset_temp.cdm_measurement;
CREATE TABLE etl_dataset_temp.cdm_measurement
(
    measurement_id                INTEGER     not null ,
    person_id                     INTEGER     not null ,
    measurement_concept_id        INTEGER     not null ,
    measurement_date              DATE      not null ,
    measurement_datetime          TIMESTAMP           ,
    measurement_time              TEXT             ,
    measurement_type_concept_id   INTEGER     not null ,
    operator_concept_id           INTEGER              ,
    value_as_number               FLOAT            ,
    value_as_concept_id           INTEGER              ,
    unit_concept_id               INTEGER              ,
    range_low                     FLOAT            ,
    range_high                    FLOAT            ,
    provider_id                   INTEGER              ,
    visit_occurrence_id           INTEGER              ,
    visit_detail_id               INTEGER              ,
    measurement_source_value      TEXT             ,
    measurement_source_concept_id INTEGER              ,
    unit_source_value             TEXT             ,
    value_source_value            TEXT             ,
    -- 
    unit_id                       TEXT,
    load_table_id                 TEXT,
    load_row_id                   INTEGER,
    trace_id                      TEXT  
)
;

INSERT INTO etl_dataset_temp.cdm_measurement
SELECT
    src.measurement_id                      AS measurement_id,
    per.person_id                           AS person_id,
    COALESCE(src.target_concept_id, 0)      AS measurement_concept_id,
    CAST(src.start_datetime AS DATE)        AS measurement_date,
    src.start_datetime                      AS measurement_datetime,
    CAST(NULL AS TEXT)                    AS measurement_time,
    src.type_concept_id                     AS measurement_type_concept_id,
    CAST(NULL AS INTEGER)                     AS operator_concept_id,
    src.value_as_number                     AS value_as_number,
    src.value_as_concept_id                 AS value_as_concept_id,
    src.unit_concept_id                     AS unit_concept_id,
    CAST(NULL AS INTEGER)                     AS range_low,
    CAST(NULL AS INTEGER)                     AS range_high,
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
    etl_dataset_temp.lk_chartevents_mapped src
INNER JOIN
    etl_dataset.cdm_person per
        ON CAST(src.subject_id AS TEXT) = per.person_source_value
INNER JOIN
    etl_dataset.cdm_visit_occurrence vis
        ON  vis.visit_source_value = 
            CONCAT(CAST(src.subject_id AS TEXT), '|', CAST(src.hadm_id AS TEXT))
WHERE
    src.target_domain_id = 'Measurement'
;

RAISE NOTICE '%: Inserting in to etl_dataset_temp.measurement', now();

DROP TABLE IF EXISTS etl_dataset_temp.measurement;

CREATE TABLE etl_dataset_temp.measurement AS (SELECT * FROM etl_dataset_temp.cdm_measurement) WITH NO DATA;

CREATE OR REPLACE FUNCTION string_to_integer(s text) RETURNS INTEGER AS $$
BEGIN
    RETURN s::integer;
EXCEPTION WHEN OTHERS THEN
    RETURN 1000000000;
END; $$ LANGUAGE plpgsql STRICT;

-- CREATE TABLE
 CREATE TABLE etl_dataset_temp.measurement_1 ( CHECK ( string_to_integer(measurement_source_value) >= 0 AND string_to_integer(measurement_source_value) < 127 )) INHERITS (etl_dataset_temp.measurement);
 CREATE TABLE etl_dataset_temp.measurement_2 ( CHECK ( string_to_integer(measurement_source_value) >= 127 AND string_to_integer(measurement_source_value) < 210 )) INHERITS (etl_dataset_temp.measurement);
 CREATE TABLE etl_dataset_temp.measurement_3 ( CHECK ( string_to_integer(measurement_source_value) >= 210 AND string_to_integer(measurement_source_value) < 425 )) INHERITS (etl_dataset_temp.measurement);
 CREATE TABLE etl_dataset_temp.measurement_4 ( CHECK ( string_to_integer(measurement_source_value) >= 425 AND string_to_integer(measurement_source_value) < 549 )) INHERITS (etl_dataset_temp.measurement);
 CREATE TABLE etl_dataset_temp.measurement_5 ( CHECK ( string_to_integer(measurement_source_value) >= 549 AND string_to_integer(measurement_source_value) < 643 )) INHERITS (etl_dataset_temp.measurement);
 CREATE TABLE etl_dataset_temp.measurement_6 ( CHECK ( string_to_integer(measurement_source_value) >= 643 AND string_to_integer(measurement_source_value) < 741 )) INHERITS (etl_dataset_temp.measurement);
 CREATE TABLE etl_dataset_temp.measurement_7 ( CHECK ( string_to_integer(measurement_source_value) >= 741 AND string_to_integer(measurement_source_value) < 1483 )) INHERITS (etl_dataset_temp.measurement);
 CREATE TABLE etl_dataset_temp.measurement_8 ( CHECK ( string_to_integer(measurement_source_value) >= 1483 AND string_to_integer(measurement_source_value) < 3458 )) INHERITS (etl_dataset_temp.measurement);
 CREATE TABLE etl_dataset_temp.measurement_9 ( CHECK ( string_to_integer(measurement_source_value) >= 3458 AND string_to_integer(measurement_source_value) < 3695 )) INHERITS (etl_dataset_temp.measurement);
 CREATE TABLE etl_dataset_temp.measurement_10 ( CHECK ( string_to_integer(measurement_source_value) >= 3695 AND string_to_integer(measurement_source_value) < 8440 )) INHERITS (etl_dataset_temp.measurement);
 CREATE TABLE etl_dataset_temp.measurement_11 ( CHECK ( string_to_integer(measurement_source_value) >= 8440 AND string_to_integer(measurement_source_value) < 8553 )) INHERITS (etl_dataset_temp.measurement);
 CREATE TABLE etl_dataset_temp.measurement_12 ( CHECK ( string_to_integer(measurement_source_value) >= 8553 AND string_to_integer(measurement_source_value) < 220274 )) INHERITS (etl_dataset_temp.measurement);
 CREATE TABLE etl_dataset_temp.measurement_13 ( CHECK ( string_to_integer(measurement_source_value) >= 220274 AND string_to_integer(measurement_source_value) < 223921 )) INHERITS (etl_dataset_temp.measurement);
 CREATE TABLE etl_dataset_temp.measurement_14 ( CHECK ( string_to_integer(measurement_source_value) >= 223921 AND string_to_integer(measurement_source_value) < 224085 )) INHERITS (etl_dataset_temp.measurement);
 CREATE TABLE etl_dataset_temp.measurement_15 ( CHECK ( string_to_integer(measurement_source_value) >= 224085 AND string_to_integer(measurement_source_value) < 224859 )) INHERITS (etl_dataset_temp.measurement);
 CREATE TABLE etl_dataset_temp.measurement_16 ( CHECK ( string_to_integer(measurement_source_value) >= 224859 AND string_to_integer(measurement_source_value) < 227629 )) INHERITS (etl_dataset_temp.measurement);
 CREATE TABLE etl_dataset_temp.measurement_17 ( CHECK ( string_to_integer(measurement_source_value) >= 227629 AND string_to_integer(measurement_source_value) < 999999999 )) INHERITS (etl_dataset_temp.measurement);
 CREATE TABLE etl_dataset_temp.measurement_null ( CHECK ( string_to_integer(measurement_source_value) > 999999999 )) INHERITS (etl_dataset_temp.measurement);

-- CREATE TRIGGER
CREATE OR REPLACE FUNCTION measurement_insert_trigger()
RETURNS TRIGGER AS $$
BEGIN
IF ( string_to_integer(NEW.measurement_source_value) >= 0 AND string_to_integer(NEW.measurement_source_value) < 127 ) THEN INSERT INTO etl_dataset_temp.measurement_1 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 127 AND string_to_integer(NEW.measurement_source_value) < 210 ) THEN INSERT INTO etl_dataset_temp.measurement_2 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 210 AND string_to_integer(NEW.measurement_source_value) < 425 ) THEN INSERT INTO etl_dataset_temp.measurement_3 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 425 AND string_to_integer(NEW.measurement_source_value) < 549 ) THEN INSERT INTO etl_dataset_temp.measurement_4 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 549 AND string_to_integer(NEW.measurement_source_value) < 643 ) THEN INSERT INTO etl_dataset_temp.measurement_5 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 643 AND string_to_integer(NEW.measurement_source_value) < 741 ) THEN INSERT INTO etl_dataset_temp.measurement_6 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 741 AND string_to_integer(NEW.measurement_source_value) < 1483 ) THEN INSERT INTO etl_dataset_temp.measurement_7 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 1483 AND string_to_integer(NEW.measurement_source_value) < 3458 ) THEN INSERT INTO etl_dataset_temp.measurement_8 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 3458 AND string_to_integer(NEW.measurement_source_value) < 3695 ) THEN INSERT INTO etl_dataset_temp.measurement_9 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 3695 AND string_to_integer(NEW.measurement_source_value) < 8440 ) THEN INSERT INTO etl_dataset_temp.measurement_10 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 8440 AND string_to_integer(NEW.measurement_source_value) < 8553 ) THEN INSERT INTO etl_dataset_temp.measurement_11 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 8553 AND string_to_integer(NEW.measurement_source_value) < 220274 ) THEN INSERT INTO etl_dataset_temp.measurement_12 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 220274 AND string_to_integer(NEW.measurement_source_value) < 223921 ) THEN INSERT INTO etl_dataset_temp.measurement_13 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 223921 AND string_to_integer(NEW.measurement_source_value) < 224085 ) THEN INSERT INTO etl_dataset_temp.measurement_14 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 224085 AND string_to_integer(NEW.measurement_source_value) < 224859 ) THEN INSERT INTO etl_dataset_temp.measurement_15 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 224859 AND string_to_integer(NEW.measurement_source_value) < 227629 ) THEN INSERT INTO etl_dataset_temp.measurement_16 VALUES (NEW.*);
ELSIF ( string_to_integer(NEW.measurement_source_value) >= 227629 AND string_to_integer(NEW.measurement_source_value) < 999999999 ) THEN INSERT INTO etl_dataset_temp.measurement_17 VALUES (NEW.*);
ELSE
	INSERT INTO etl_dataset_temp.measurement_null VALUES (NEW.*);
END IF;
RETURN NULL;
END;
$$
LANGUAGE plpgsql;

CREATE TRIGGER insert_measurement_trigger
    BEFORE INSERT ON etl_dataset_temp.measurement
    FOR EACH ROW EXECUTE PROCEDURE measurement_insert_trigger();

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

RAISE NOTICE '%: End of the transcaction', now();

END
$$;
