import logging

log = logging.getLogger("Standardise")


def createDeviceExposure(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".cdm_device_exposure")
    dropQuery = """drop table if exists """ + etlSchemaName + """.cdm_device_exposure cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.cdm_device_exposure
        (
            device_exposure_id              INTEGER       not null ,
            person_id                       INTEGER       not null ,
            device_concept_id               INTEGER       not null ,
            device_exposure_start_date      DATE        not null ,
            device_exposure_start_datetime  TIMESTAMP             ,
            device_exposure_end_date        DATE                 ,
            device_exposure_end_datetime    TIMESTAMP             ,
            device_type_concept_id          INTEGER       not null ,
            unique_device_id                TEXT               ,
            quantity                        DOUBLE PRECISION                ,
            provider_id                     INTEGER                ,
            visit_occurrence_id             INTEGER                ,
            visit_detail_id                 INTEGER                ,
            device_source_value             TEXT               ,
            device_source_concept_id        INTEGER                ,
            -- 
            unit_id                       TEXT,
            load_table_id                 TEXT,
            load_row_id                   INTEGER,
            trace_id                      TEXT
        )
        ;
        """
    insertDrugsQuery = """INSERT INTO """ + etlSchemaName + """.cdm_device_exposure
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS device_exposure_id,
            per.person_id                               AS person_id,
            src.target_concept_id                       AS device_concept_id,
            CAST(src.start_datetime AS DATE)            AS device_exposure_start_date,
            src.start_datetime                          AS device_exposure_start_datetime,
            CAST(src.end_datetime AS DATE)              AS device_exposure_end_date,
            src.end_datetime                            AS device_exposure_end_datetime,
            src.type_concept_id                         AS device_type_concept_id,
            CAST(NULL AS TEXT)                        AS unique_device_id,
            CAST(
            CASE
            WHEN ROUND(src.quantity) = src.quantity THEN src.quantity
            ELSE NULL
            END
                AS INTEGER)                               AS quantity,
            CAST(NULL AS INTEGER)                         AS provider_id,
            vis.visit_occurrence_id                     AS visit_occurrence_id,
            CAST(NULL AS INTEGER)                         AS visit_detail_id,
            src.source_code                             AS device_source_value,
            src.source_concept_id                       AS device_source_concept_id,
            -- 
            CONCAT('device.', src.unit_id)  AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.lk_drug_mapped src
        INNER JOIN
            """ + etlSchemaName + """.cdm_person per
                ON CAST(src.subject_id AS TEXT) = per.person_source_value
        INNER JOIN
            """ + etlSchemaName + """.cdm_visit_occurrence vis
                ON  vis.visit_source_value = 
                    CONCAT(CAST(src.subject_id AS TEXT), '|', CAST(src.hadm_id AS TEXT))
        WHERE
            src.target_domain_id = 'Device'
        ;
        """
    insertCharteventsQuery = """INSERT INTO """ + etlSchemaName + """.cdm_device_exposure
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS device_exposure_id,
            per.person_id                               AS person_id,
            src.target_concept_id                       AS device_concept_id,
            CAST(src.start_datetime AS DATE)            AS device_exposure_start_date,
            src.start_datetime                          AS device_exposure_start_datetime,
            CAST(src.start_datetime AS DATE)            AS device_exposure_end_date,
            src.start_datetime                          AS device_exposure_end_datetime,
            src.type_concept_id                         AS device_type_concept_id,
            CAST(NULL AS TEXT)                        AS unique_device_id,
            CAST(
            CASE 
            WHEN ROUND(src.value_as_number) = src.value_as_number THEN src.value_as_number
            ELSE NULL
            END
                AS DOUBLE PRECISION)                               AS quantity,
            CAST(NULL AS INTEGER)                         AS provider_id,
            vis.visit_occurrence_id                     AS visit_occurrence_id,
            CAST(NULL AS INTEGER)                         AS visit_detail_id,
            src.source_code                             AS device_source_value,
            src.source_concept_id                       AS device_source_concept_id,
            -- 
            CONCAT('device.', src.unit_id)  AS unit_id,
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
            src.target_domain_id = 'Device'
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertDrugsQuery)
            cursor.execute(insertCharteventsQuery)


def migrate(con, etlSchemaName):
    createDeviceExposure(con = con, etlSchemaName = etlSchemaName)
