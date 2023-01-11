import logging

log = logging.getLogger("Standardise")


def createObservationClean(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_observation_clean")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_observation_clean cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_observation_clean AS
        -- rule 1, insurance
        SELECT
            src.subject_id                  AS subject_id,
            src.hadm_id                     AS hadm_id,
            'Insurance'                     AS source_code,
            46235654                        AS target_concept_id, -- Primary insurance,
            src.admittime                   AS start_datetime,
            src.insurance                   AS value_as_string,
            'mimiciv_obs_insurance'         AS source_vocabulary_id,
            --
            'admissions.insurance'          AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.src_admissions src -- adm
        WHERE
            src.insurance IS NOT NULL

        UNION ALL
        -- rule 2, marital_status
        SELECT
            src.subject_id                  AS subject_id,
            src.hadm_id                     AS hadm_id,
            'Marital status'                AS source_code,
            40766231                        AS target_concept_id, -- Marital status,
            src.admittime                   AS start_datetime,
            src.marital_status              AS value_as_string,
            'mimiciv_obs_marital'           AS source_vocabulary_id,
            --
            'admissions.marital_status'     AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.src_admissions src -- adm
        WHERE
            src.marital_status IS NOT NULL

        UNION ALL
        -- rule 3, language
        SELECT
            src.subject_id                  AS subject_id,
            src.hadm_id                     AS hadm_id,
            'Language'                      AS source_code,
            40758030                        AS target_concept_id, -- Preferred language
            src.admittime                   AS start_datetime,
            src.language                    AS value_as_string,
            'mimiciv_obs_language'          AS source_vocabulary_id,
            --
            'admissions.language'           AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.src_admissions src -- adm
        WHERE
            src.language IS NOT NULL
        ;
        """
    insertQuery = """INSERT INTO """ + etlSchemaName + """.lk_observation_clean
        SELECT
            src.subject_id                  AS subject_id,
            src.hadm_id                     AS hadm_id,
            src.drg_code                    AS source_code,
            4296248                         AS target_concept_id, -- Cost containment
            COALESCE(adm.edregtime, adm.admittime)  AS start_datetime,
            src.description                 AS value_as_string,
            'mimiciv_obs_drgcodes'          AS source_vocabulary_id,
            'drgcodes.description'          AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.src_drgcodes src -- drg
        INNER JOIN
            """ + etlSchemaName + """.src_admissions adm
                ON src.hadm_id = adm.hadm_id
        WHERE
            src.description IS NOT NULL
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertQuery)


def createObsAdmissionsConcept(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_obs_admissions_concept")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_obs_admissions_concept cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_obs_admissions_concept AS
        SELECT DISTINCT
            src.value_as_string         AS source_code,
            src.source_vocabulary_id    AS source_vocabulary_id,
            vc.domain_id                AS source_domain_id,
            vc.concept_id               AS source_concept_id,
            vc2.domain_id               AS target_domain_id,
            vc2.concept_id              AS target_concept_id
        FROM
            """ + etlSchemaName + """.lk_observation_clean src
        LEFT JOIN
            voc_dataset.concept vc
                ON src.value_as_string = vc.concept_code
                AND src.source_vocabulary_id = vc.vocabulary_id
                -- valid period should be used to map drg_code, but due to the date shift it is not applicable
                -- AND src.start_datetime BETWEEN vc.valid_start_date AND vc.valid_end_date
        LEFT JOIN
            voc_dataset.concept_relationship vcr
                ON  vc.concept_id = vcr.concept_id_1
                AND vcr.relationship_id = 'Maps to'
        LEFT JOIN
            voc_dataset.concept vc2
                ON vc2.concept_id = vcr.concept_id_2
                AND vc2.standard_concept = 'S'
                AND vc2.invalid_reason IS NULL
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createObservationMapped(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_observation_mapped")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_observation_mapped cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_observation_mapped AS
        SELECT
            src.hadm_id                             AS hadm_id, -- to visit
            src.subject_id                          AS subject_id, -- to person
            COALESCE(src.target_concept_id, 0)      AS target_concept_id,
            src.start_datetime                      AS start_datetime,
            32817                                   AS type_concept_id, -- OMOP4976890 EHR, -- Rules 1-4
            src.source_code                         AS source_code,
            0                                       AS source_concept_id,
            src.value_as_string                     AS value_as_string,
            lc.target_concept_id                    AS value_as_concept_id,
            'Observation'                           AS target_domain_id, -- to join on src.target_concept_id?
            src.unit_id                     AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.lk_observation_clean src
        LEFT JOIN
            """ + etlSchemaName + """.lk_obs_admissions_concept lc
                ON src.value_as_string = lc.source_code
                AND src.source_vocabulary_id = lc.source_vocabulary_id
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createObservation(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".cdm_observation")
    dropQuery = """drop table if exists """ + etlSchemaName + """.cdm_observation cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.cdm_observation
        (
            observation_id                INTEGER     not null ,
            person_id                     INTEGER     not null ,
            observation_concept_id        INTEGER     not null ,
            observation_date              DATE      not null ,
            observation_datetime          TIMESTAMP           ,
            observation_type_concept_id   INTEGER     not null ,
            value_as_number               FLOAT        ,
            value_as_string               TEXT         ,
            value_as_concept_id           INTEGER          ,
            qualifier_concept_id          INTEGER          ,
            unit_concept_id               INTEGER          ,
            provider_id                   INTEGER          ,
            visit_occurrence_id           INTEGER          ,
            visit_detail_id               INTEGER          ,
            observation_source_value      TEXT         ,
            observation_source_concept_id INTEGER          ,
            unit_source_value             TEXT         ,
            qualifier_source_value        TEXT         ,
            -- 
            unit_id                       TEXT,
            load_table_id                 TEXT,
            load_row_id                   INTEGER,
            trace_id                      TEXT
        )
        ;
        """
    insertObservationQuery = """INSERT INTO """ + etlSchemaName + """.cdm_observation
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS observation_id,
            per.person_id                               AS person_id,
            src.target_concept_id                       AS observation_concept_id,
            CAST(src.start_datetime AS DATE)            AS observation_date,
            src.start_datetime                          AS observation_datetime,
            src.type_concept_id                         AS observation_type_concept_id,
            CAST(NULL AS FLOAT)                       AS value_as_number,
            src.value_as_string                         AS value_as_string,
            CASE
            WHEN src.value_as_string IS NOT NULL THEN COALESCE(src.value_as_concept_id, 0)
            ELSE NULL
            END                                   AS value_as_concept_id,
            CAST(NULL AS INTEGER)                         AS qualifier_concept_id,
            CAST(NULL AS INTEGER)                         AS unit_concept_id,
            CAST(NULL AS INTEGER)                         AS provider_id,
            vis.visit_occurrence_id                     AS visit_occurrence_id,
            CAST(NULL AS INTEGER)                         AS visit_detail_id,
            src.source_code                             AS observation_source_value,
            src.source_concept_id                       AS observation_source_concept_id,
            CAST(NULL AS TEXT)                        AS unit_source_value,
            CAST(NULL AS TEXT)                        AS qualifier_source_value,
            -- 
            CONCAT('observation.', src.unit_id)         AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.lk_observation_mapped src
        INNER JOIN
            """ + etlSchemaName + """.cdm_person per
                ON CAST(src.subject_id AS TEXT) = per.person_source_value
        INNER JOIN
            """ + etlSchemaName + """.cdm_visit_occurrence vis
                ON  vis.visit_source_value = 
                    CONCAT(CAST(src.subject_id AS TEXT), '|', CAST(src.hadm_id AS TEXT))
        WHERE
            src.target_domain_id = 'Observation'
        ;
        """
    insertCharteventsQuery = """INSERT INTO """ + etlSchemaName + """.cdm_observation
        SELECT
            src.measurement_id                          AS observation_id, -- id is generated already
            per.person_id                               AS person_id,
            src.target_concept_id                       AS observation_concept_id,
            CAST(src.start_datetime AS DATE)            AS observation_date,
            src.start_datetime                          AS observation_datetime,
            src.type_concept_id                         AS observation_type_concept_id,
            src.value_as_number                         AS value_as_number,
            src.value_source_value                      AS value_as_string,
            CASE
            WHEN src.value_source_value IS NOT NULL THEN COALESCE(src.value_as_concept_id, 0)
            ELSE NULL
            END                                  AS value_as_concept_id,
            CAST(NULL AS INTEGER)                         AS qualifier_concept_id,
            src.unit_concept_id                         AS unit_concept_id,
            CAST(NULL AS INTEGER)                         AS provider_id,
            vis.visit_occurrence_id                     AS visit_occurrence_id,
            CAST(NULL AS INTEGER)                         AS visit_detail_id,
            src.source_code                             AS observation_source_value,
            src.source_concept_id                       AS observation_source_concept_id,
            src.unit_source_value                       AS unit_source_value,
            CAST(NULL AS TEXT)                        AS qualifier_source_value,
            -- 
            CONCAT('observation.', src.unit_id)         AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.lk_chartevents_mapped src
        INNER JOIN
            """ + etlSchemaName + """.cdm_person per
                ON CAST(src.subject_id AS TEXT) = per.person_source_value
        INNER JOIN
            """ + etlSchemaName + """.cdm_visit_occurrence vis
                ON  vis.visit_source_value = 
                    CONCAT(CAST(src.subject_id AS TEXT), '|', CAST(src.hadm_id AS TEXT))
        WHERE
            src.target_domain_id = 'Observation'
        ;
        """
    insertProcedureQuery = """INSERT INTO """ + etlSchemaName + """.cdm_observation
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS observation_id,
            per.person_id                               AS person_id,
            src.target_concept_id                       AS observation_concept_id,
            CAST(src.start_datetime AS DATE)            AS observation_date,
            src.start_datetime                          AS observation_datetime,
            src.type_concept_id                         AS observation_type_concept_id,
            CAST(NULL AS FLOAT)                       AS value_as_number,
            CAST(NULL AS TEXT)                        AS value_as_string,
            CAST(NULL AS INTEGER)                         AS value_as_concept_id,
            CAST(NULL AS INTEGER)                         AS qualifier_concept_id,
            CAST(NULL AS INTEGER)                         AS unit_concept_id,
            CAST(NULL AS INTEGER)                         AS provider_id,
            vis.visit_occurrence_id                     AS visit_occurrence_id,
            CAST(NULL AS INTEGER)                         AS visit_detail_id,
            src.source_code                             AS observation_source_value,
            src.source_concept_id                       AS observation_source_concept_id,
            CAST(NULL AS TEXT)                        AS unit_source_value,
            CAST(NULL AS TEXT)                        AS qualifier_source_value,
            -- 
            CONCAT('observation.', src.unit_id)         AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.lk_procedure_mapped src
        INNER JOIN
            """ + etlSchemaName + """.cdm_person per
                ON CAST(src.subject_id AS TEXT) = per.person_source_value
        INNER JOIN
            """ + etlSchemaName + """.cdm_visit_occurrence vis
                ON  vis.visit_source_value = 
                    CONCAT(CAST(src.subject_id AS TEXT), '|', CAST(src.hadm_id AS TEXT))
        WHERE
            src.target_domain_id = 'Observation'
        ;
        """
    insertDiagnosisQuery = """INSERT INTO """ + etlSchemaName + """.cdm_observation
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS observation_id,
            per.person_id                               AS person_id,
            src.target_concept_id                       AS observation_concept_id, -- to rename fields in *_mapped
            CAST(src.start_datetime AS DATE)            AS observation_date,
            src.start_datetime                          AS observation_datetime,
            src.type_concept_id                         AS observation_type_concept_id,
            CAST(NULL AS FLOAT)                       AS value_as_number,
            CAST(NULL AS TEXT)                        AS value_as_string,
            CAST(NULL AS INTEGER)                         AS value_as_concept_id,
            CAST(NULL AS INTEGER)                         AS qualifier_concept_id,
            CAST(NULL AS INTEGER)                         AS unit_concept_id,
            CAST(NULL AS INTEGER)                         AS provider_id,
            vis.visit_occurrence_id                     AS visit_occurrence_id,
            CAST(NULL AS INTEGER)                         AS visit_detail_id,
            src.source_code                             AS observation_source_value,
            src.source_concept_id                       AS observation_source_concept_id,
            CAST(NULL AS TEXT)                        AS unit_source_value,
            CAST(NULL AS TEXT)                        AS qualifier_source_value,
            -- 
            CONCAT('observation.', src.unit_id)         AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.lk_diagnoses_icd_mapped src
        INNER JOIN
            """ + etlSchemaName + """.cdm_person per
                ON CAST(src.subject_id AS TEXT) = per.person_source_value
        INNER JOIN
            """ + etlSchemaName + """.cdm_visit_occurrence vis
                ON  vis.visit_source_value = 
                    CONCAT(CAST(src.subject_id AS TEXT), '|', CAST(src.hadm_id AS TEXT))
        WHERE
            src.target_domain_id = 'Observation'
        ;
        """
    insertSpecimenQuery = """INSERT INTO """ + etlSchemaName + """.cdm_observation
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS observation_id,
            per.person_id                               AS person_id,
            src.target_concept_id                       AS observation_concept_id,
            CAST(src.start_datetime AS DATE)            AS observation_date,
            src.start_datetime                          AS observation_datetime,
            src.type_concept_id                         AS observation_type_concept_id,
            CAST(NULL AS FLOAT)                       AS value_as_number,
            CAST(NULL AS TEXT)                        AS value_as_string,
            CAST(NULL AS INTEGER)                         AS value_as_concept_id,
            CAST(NULL AS INTEGER)                         AS qualifier_concept_id,
            CAST(NULL AS INTEGER)                         AS unit_concept_id,
            CAST(NULL AS INTEGER)                         AS provider_id,
            vis.visit_occurrence_id                     AS visit_occurrence_id,
            CAST(NULL AS INTEGER)                         AS visit_detail_id,
            src.source_code                             AS observation_source_value,
            src.source_concept_id                       AS observation_source_concept_id,
            CAST(NULL AS TEXT)                        AS unit_source_value,
            CAST(NULL AS TEXT)                        AS qualifier_source_value,
            -- 
            CONCAT('observation.', src.unit_id)         AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.lk_specimen_mapped src
        INNER JOIN
            """ + etlSchemaName + """.cdm_person per
                ON CAST(src.subject_id AS TEXT) = per.person_source_value
        INNER JOIN
            """ + etlSchemaName + """.cdm_visit_occurrence vis
                ON  vis.visit_source_value = 
                    CONCAT(CAST(src.subject_id AS TEXT), '|', 
                        COALESCE(CAST(src.hadm_id AS TEXT), CAST(src.date_id AS TEXT)))
        WHERE
            src.target_domain_id = 'Observation'
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertObservationQuery)
            cursor.execute(insertCharteventsQuery)
            cursor.execute(insertProcedureQuery)
            cursor.execute(insertDiagnosisQuery)
            cursor.execute(insertSpecimenQuery)


def createObservationPeriodClean(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_observation_period_clean")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_observation_period_clean cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_observation_period_clean AS
        SELECT
            src.person_id               AS person_id,
            MIN(src.visit_start_date)   AS start_date,
            MAX(src.visit_end_date)     AS end_date,
            src.unit_id                 AS unit_id
        FROM
            """ + etlSchemaName + """.cdm_visit_occurrence src
        GROUP BY
            src.person_id, src.unit_id
        ;
        """
    insertConditionQuery = """INSERT INTO """ + etlSchemaName + """.tmp_observation_period_clean
        SELECT
            src.person_id               AS person_id,
            MIN(src.condition_start_date)   AS start_date,
            MAX(src.condition_end_date)     AS end_date,
            src.unit_id                 AS unit_id
        FROM
            """ + etlSchemaName + """.cdm_condition_occurrence src
        GROUP BY
            src.person_id, src.unit_id
        ;
        """
    insertProcedureQuery = """INSERT INTO """ + etlSchemaName + """.tmp_observation_period_clean
        SELECT
            src.person_id               AS person_id,
            MIN(src.procedure_date)   AS start_date,
            MAX(src.procedure_date)     AS end_date,
            src.unit_id                 AS unit_id
        FROM
            """ + etlSchemaName + """.cdm_procedure_occurrence src
        GROUP BY
            src.person_id, src.unit_id
        ;
        """
    insertDrugExposureQuery = """INSERT INTO """ + etlSchemaName + """.tmp_observation_period_clean
        SELECT
            src.person_id               AS person_id,
            MIN(src.drug_exposure_start_date)   AS start_date,
            MAX(src.drug_exposure_end_date)     AS end_date,
            src.unit_id                 AS unit_id
        FROM
            """ + etlSchemaName + """.cdm_drug_exposure src
        GROUP BY
            src.person_id, src.unit_id
        ;
        """
    insertDeviceExposureQuery = """INSERT INTO """ + etlSchemaName + """.tmp_observation_period_clean
        SELECT
            src.person_id               AS person_id,
            MIN(src.device_exposure_start_date)   AS start_date,
            MAX(src.device_exposure_end_date)     AS end_date,
            src.unit_id                 AS unit_id
        FROM
            """ + etlSchemaName + """.cdm_device_exposure src
        GROUP BY
            src.person_id, src.unit_id
        ;
        """
    insertMeasurementQuery = """INSERT INTO """ + etlSchemaName + """.tmp_observation_period_clean
        SELECT
            src.person_id               AS person_id,
            MIN(src.measurement_date)   AS start_date,
            MAX(src.measurement_date)     AS end_date,
            src.unit_id                 AS unit_id
        FROM
            """ + etlSchemaName + """.cdm_measurement src
        GROUP BY
            src.person_id, src.unit_id
        ;
        """
    insertSpecimenQuery = """INSERT INTO """ + etlSchemaName + """.tmp_observation_period_clean
        SELECT
            src.person_id               AS person_id,
            MIN(src.specimen_date)   AS start_date,
            MAX(src.specimen_date)     AS end_date,
            src.unit_id                 AS unit_id
        FROM
            """ + etlSchemaName + """.cdm_specimen src
        GROUP BY
            src.person_id, src.unit_id
        ;
        """
    insertObservationQuery = """INSERT INTO """ + etlSchemaName + """.tmp_observation_period_clean
        SELECT
            src.person_id               AS person_id,
            MIN(src.observation_date)   AS start_date,
            MAX(src.observation_date)     AS end_date,
            src.unit_id                 AS unit_id
        FROM
            """ + etlSchemaName + """.cdm_observation src
        GROUP BY
            src.person_id, src.unit_id
        ;
        """
    insertDeathQuery = """INSERT INTO """ + etlSchemaName + """.tmp_observation_period_clean
        SELECT
            src.person_id               AS person_id,
            MIN(src.death_date)         AS start_date,
            MAX(src.death_date)         AS end_date,
            src.unit_id                 AS unit_id
        FROM
            """ + etlSchemaName + """.cdm_death src
        GROUP BY
            src.person_id, src.unit_id
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertConditionQuery)
            cursor.execute(insertProcedureQuery)
            cursor.execute(insertDrugExposureQuery)
            cursor.execute(insertDeviceExposureQuery)
            cursor.execute(insertMeasurementQuery)
            cursor.execute(insertSpecimenQuery)
            cursor.execute(insertObservationQuery)
            cursor.execute(insertDeathQuery)


def createTempObservationPeriod(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_observation_period")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_observation_period cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_observation_period AS
        SELECT
            src.person_id               AS person_id,
            MIN(src.start_date)   AS start_date,
            MAX(src.end_date)     AS end_date,
            src.unit_id                 AS unit_id
        FROM
            """ + etlSchemaName + """.tmp_observation_period_clean src
        GROUP BY
            src.person_id, src.unit_id
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createObservationPeriod(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".cdm_observation_period")
    dropQuery = """drop table if exists """ + etlSchemaName + """.cdm_observation_period cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.cdm_observation_period
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
        """
    insertQuery = """INSERT INTO """ + etlSchemaName + """.cdm_observation_period
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
            """ + etlSchemaName + """.tmp_observation_period src
        GROUP BY
            src.person_id
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertQuery)


def migrateLookup(con, etlSchemaName):
    createObservationClean(con = con, etlSchemaName = etlSchemaName)
    createObsAdmissionsConcept(con = con, etlSchemaName = etlSchemaName)
    createObservationMapped(con = con, etlSchemaName = etlSchemaName)


def migrate(con, etlSchemaName):
    createObservation(con = con, etlSchemaName = etlSchemaName)


def migratePeriod(con, etlSchemaName):
    createObservationPeriodClean(con = con, etlSchemaName = etlSchemaName)
    createTempObservationPeriod(con = con, etlSchemaName = etlSchemaName)
    createObservationPeriod(con = con, etlSchemaName = etlSchemaName)
