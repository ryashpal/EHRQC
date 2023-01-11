import logging

log = logging.getLogger("Standardise")


def createConditionOccurrence(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".cdm_condition_occurrence")
    dropQuery = """drop table if exists """ + etlSchemaName + """.cdm_condition_occurrence cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.cdm_condition_occurrence
        (
            condition_occurrence_id       INTEGER     not null ,
            person_id                     INTEGER     not null ,
            condition_concept_id          INTEGER     not null ,
            condition_start_date          DATE      not null ,
            condition_start_datetime      TIMESTAMP           ,
            condition_end_date            DATE               ,
            condition_end_datetime        TIMESTAMP           ,
            condition_type_concept_id     INTEGER     not null ,
            stop_reason                   TEXT             ,
            provider_id                   INTEGER              ,
            visit_occurrence_id           INTEGER              ,
            visit_detail_id               INTEGER              ,
            condition_source_value        TEXT             ,
            condition_source_concept_id   INTEGER              ,
            condition_status_source_value TEXT             ,
            condition_status_concept_id   INTEGER              ,
            -- 
            unit_id                       TEXT,
            load_table_id                 TEXT,
            load_row_id                   INTEGER,
            trace_id                      TEXT
        )
        ;
        """
    insertDiagnosesQuery = """INSERT INTO """ + etlSchemaName + """.cdm_condition_occurrence
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int       AS condition_occurrence_id,
            per.person_id                           AS person_id,
            COALESCE(src.target_concept_id, 0)      AS condition_concept_id,
            CAST(src.start_datetime AS DATE)        AS condition_start_date,
            src.start_datetime                      AS condition_start_datetime,
            CAST(src.end_datetime AS DATE)          AS condition_end_date,
            src.end_datetime                        AS condition_end_datetime,
            src.type_concept_id                     AS condition_type_concept_id,
            CAST(NULL AS TEXT)                    AS stop_reason,
            CAST(NULL AS INTEGER)                     AS provider_id,
            vis.visit_occurrence_id                 AS visit_occurrence_id,
            CAST(NULL AS INTEGER)                     AS visit_detail_id,
            src.source_code                         AS condition_source_value,
            COALESCE(src.source_concept_id, 0)      AS condition_source_concept_id,
            CAST(NULL AS TEXT)                    AS condition_status_source_value,
            CAST(NULL AS INTEGER)                     AS condition_status_concept_id,
            --
            CONCAT('condition.', src.unit_id) AS unit_id,
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
            src.target_domain_id = 'Condition'
        ;
        """
    insertChartevents1Query = """INSERT INTO """ + etlSchemaName + """.cdm_condition_occurrence
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int       AS condition_occurrence_id,
            per.person_id                           AS person_id,
            COALESCE(src.target_concept_id, 0)      AS condition_concept_id,
            CAST(src.start_datetime AS DATE)        AS condition_start_date,
            src.start_datetime                      AS condition_start_datetime,
            CAST(src.start_datetime AS DATE)        AS condition_end_date,
            src.start_datetime                      AS condition_end_datetime,
            32817                                   AS condition_type_concept_id, -- EHR  Type Concept    Type Concept
            CAST(NULL AS TEXT)                    AS stop_reason,
            CAST(NULL AS INTEGER)                     AS provider_id,
            vis.visit_occurrence_id                 AS visit_occurrence_id,
            CAST(NULL AS INTEGER)                     AS visit_detail_id,
            src.source_code                         AS condition_source_value,
            COALESCE(src.source_concept_id, 0)      AS condition_source_concept_id,
            CAST(NULL AS TEXT)                    AS condition_status_source_value,
            CAST(NULL AS INTEGER)                     AS condition_status_concept_id,
            --
            CONCAT('condition.', src.unit_id) AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.lk_chartevents_condition_mapped src
        INNER JOIN
            """ + etlSchemaName + """.cdm_person per
                ON CAST(src.subject_id AS TEXT) = per.person_source_value
        INNER JOIN
            """ + etlSchemaName + """.cdm_visit_occurrence vis
                ON  vis.visit_source_value = 
                    CONCAT(CAST(src.subject_id AS TEXT), '|', CAST(src.hadm_id AS TEXT))
        WHERE
            src.target_domain_id = 'Condition'
        ;
        """
    insertChartevents2Query = """INSERT INTO """ + etlSchemaName + """.cdm_condition_occurrence
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int       AS condition_occurrence_id,
            per.person_id                           AS person_id,
            COALESCE(src.target_concept_id, 0)      AS condition_concept_id,
            CAST(src.start_datetime AS DATE)        AS condition_start_date,
            src.start_datetime                      AS condition_start_datetime,
            CAST(src.start_datetime AS DATE)        AS condition_end_date,
            src.start_datetime                      AS condition_end_datetime,
            src.type_concept_id                     AS condition_type_concept_id,
            CAST(NULL AS TEXT)                    AS stop_reason,
            CAST(NULL AS INTEGER)                     AS provider_id,
            vis.visit_occurrence_id                 AS visit_occurrence_id,
            CAST(NULL AS INTEGER)                     AS visit_detail_id,
            src.source_code                         AS condition_source_value,
            COALESCE(src.source_concept_id, 0)      AS condition_source_concept_id,
            CAST(NULL AS TEXT)                    AS condition_status_source_value,
            CAST(NULL AS INTEGER)                     AS condition_status_concept_id,
            --
            CONCAT('condition.', src.unit_id) AS unit_id,
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
            src.target_domain_id = 'Condition'
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertDiagnosesQuery)
            cursor.execute(insertChartevents1Query)
            cursor.execute(insertChartevents2Query)


def createTargetCondition(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_target_condition")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_target_condition cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_target_condition
        AS SELECT
            co.condition_occurrence_id                                              AS condition_occurrence_id,
            co.person_id                                                            AS person_id,
            co.condition_concept_id                                                 AS condition_concept_id,
            co.condition_start_date                                                 AS condition_start_date,
            COALESCE( co.condition_end_date,
                    co.condition_start_date + INTERVAL '1 DAY')           AS condition_end_date
            -- Depending on the needs of data, include more filters in cteConditionTarget
            -- For example
            -- - to exclude unmapped condition_concept_id's (i.e. condition_concept_id = 0)
                -- from being included in same era
            -- - to set condition_era_end_date to same condition_era_start_date
                -- or condition_era_start_date + INTERVAL '1 day', when condition_end_date IS NULL
        FROM
            """ + etlSchemaName + """.cdm_condition_occurrence co
        WHERE
            co.condition_concept_id != 0
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTempDatesUnCondition(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_dates_un_condition")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_dates_un_condition cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_dates_un_condition
            AS SELECT
                person_id                               AS person_id,
                condition_concept_id                    AS condition_concept_id,
                condition_start_date                    AS event_date,
                -1                                      AS event_type,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        person_id,
                        condition_concept_id
                    ORDER BY
                        condition_start_date)               AS start_ordinal
            FROM
                """ + etlSchemaName + """.tmp_target_condition
        UNION ALL
            SELECT
                person_id                                             AS person_id,
                condition_concept_id                                  AS condition_concept_id,
                condition_end_date + INTERVAL '30 DAY'        AS event_date,
                1                                                     AS event_type,
                NULL                                                  AS start_ordinal
            FROM
                """ + etlSchemaName + """.tmp_target_condition
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTempDatesRowsCondition(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_dates_rows_condition")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_dates_rows_condition cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_dates_rows_condition
        AS SELECT
            person_id                       AS person_id,
            condition_concept_id            AS condition_concept_id,
            event_date                      AS event_date,
            event_type                      AS event_type,
            MAX(start_ordinal) OVER (
                PARTITION BY
                    person_id,
                    condition_concept_id
                ORDER BY
                    event_date,
                    event_type
                ROWS UNBOUNDED PRECEDING)   AS start_ordinal,
                -- this pulls the current START down from the prior rows
                -- so that the NULLs from the END DATES will contain a value we can compare with
            ROW_NUMBER() OVER (
                PARTITION BY
                    person_id,
                    condition_concept_id
                ORDER BY
                    event_date,
                    event_type)             AS overall_ord
                -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
        FROM
            """ + etlSchemaName + """.tmp_dates_un_condition
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTempEnddatesCondition(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_enddates_condition")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_enddates_condition cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_enddates_condition
        AS SELECT
            person_id                                       AS person_id,
            condition_concept_id                            AS condition_concept_id,
            event_date - INTERVAL '30 DAY'          AS end_date  -- unpad the end date
        FROM
            """ + etlSchemaName + """.tmp_dates_rows_condition e
        WHERE
            (2 * e.start_ordinal) - e.overall_ord = 0
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTempCondition(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_conditionends")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_conditionends cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_conditionends
        AS SELECT
            c.person_id                             AS person_id,
            c.condition_concept_id                  AS condition_concept_id,
            c.condition_start_date                  AS condition_start_date,
            MIN(e.end_date)                         AS era_end_date
        FROM
            """ + etlSchemaName + """.tmp_target_condition c
        JOIN
            """ + etlSchemaName + """.tmp_enddates_condition e
                ON  c.person_id            = e.person_id
                AND c.condition_concept_id = e.condition_concept_id
                AND e.end_date             >= c.condition_start_date
        GROUP BY
            c.condition_occurrence_id,
            c.person_id,
            c.condition_concept_id,
            c.condition_start_date
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createCondition(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".cdm_condition_era")
    dropQuery = """drop table if exists """ + etlSchemaName + """.cdm_condition_era cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.cdm_condition_era
        (
            condition_era_id            INTEGER     not null ,
            person_id                   INTEGER     not null ,
            condition_concept_id        INTEGER     not null ,
            condition_era_start_date    DATE      not null ,
            condition_era_end_date      DATE      not null ,
            condition_occurrence_count  INTEGER              ,
            -- 
            unit_id                       TEXT,
            load_table_id                 TEXT,
            load_row_id                   INTEGER
        )
        ;
        """
    insertQuery = """INSERT INTO """ + etlSchemaName + """.cdm_condition_era
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int               AS condition_era_id,
            person_id                                       AS person_id,
            condition_concept_id                            AS condition_concept_id,
            MIN(condition_start_date)                       AS condition_era_start_date,
            era_end_date                                    AS condition_era_end_date,
            COUNT(*)                                        AS condition_occurrence_count,
        -- --
            'condition_era.condition_occurrence'            AS unit_id,
            CAST(NULL AS TEXT)                            AS load_table_id,
            CAST(NULL AS INTEGER)                             AS load_row_id
        FROM
            """ + etlSchemaName + """.tmp_conditionends
        GROUP BY
            person_id,
            condition_concept_id,
            era_end_date
        ORDER BY
            person_id,
            condition_concept_id
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertQuery)


def dropTempTables(con, etlSchemaName):
    log.info("Dropping temp tables")
    dropQuery2 = """drop table if exists """ + etlSchemaName + """.tmp_enddates_condition cascade"""
    dropQuery3 = """drop table if exists """ + etlSchemaName + """.tmp_dates_rows_condition cascade"""
    dropQuery4 = """drop table if exists """ + etlSchemaName + """.tmp_dates_un_condition cascade"""
    dropQuery5 = """drop table if exists """ + etlSchemaName + """.tmp_target_condition cascade"""
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery2)
            cursor.execute(dropQuery3)
            cursor.execute(dropQuery4)
            cursor.execute(dropQuery5)


def migrate(con, etlSchemaName):
    createConditionOccurrence(con = con, etlSchemaName = etlSchemaName)


def migrateConditionEra(con, etlSchemaName):
    createTargetCondition(con = con, etlSchemaName = etlSchemaName)
    createTempDatesUnCondition(con = con, etlSchemaName = etlSchemaName)
    createTempDatesRowsCondition(con = con, etlSchemaName = etlSchemaName)
    createTempEnddatesCondition(con = con, etlSchemaName = etlSchemaName)
    createTempCondition(con = con, etlSchemaName = etlSchemaName)
    createCondition(con = con, etlSchemaName = etlSchemaName)
    dropTempTables(con = con, etlSchemaName = etlSchemaName)
