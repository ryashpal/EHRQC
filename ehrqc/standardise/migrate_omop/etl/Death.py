import logging

log = logging.getLogger("Standardise")


def createDeathLookup(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_death_adm_mapped")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_death_adm_mapped cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_death_adm_mapped AS
        SELECT DISTINCT
            src.subject_id, 
            FIRST_VALUE(src.deathtime) OVER(
                PARTITION BY src.subject_id 
                ORDER BY src.admittime ASC
            )                                   AS deathtime, 
            FIRST_VALUE(src.dischtime) OVER(
                PARTITION BY src.subject_id 
                ORDER BY src.admittime ASC
            )                                   AS dischtime,
            32817                               AS type_concept_id, -- OMOP4976890 EHR
            --
            'admissions'                        AS unit_id,
            src.load_table_id                   AS load_table_id,
            FIRST_VALUE(src.load_row_id) OVER(
                PARTITION BY src.subject_id 
                ORDER BY src.admittime ASC
            )                                   AS load_row_id,
            FIRST_VALUE(src.trace_id) OVER(
                PARTITION BY src.subject_id 
                ORDER BY src.admittime ASC
            )                                   AS trace_id
        FROM 
            """ + etlSchemaName + """.src_admissions src -- adm
        WHERE 
            src.deathtime IS NOT NULL
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createDeathCdm(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".cdm_death")
    dropQuery = """drop table if exists """ + etlSchemaName + """.cdm_death cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.cdm_death
        (
            person_id               INTEGER     not null ,
            death_date              DATE      not null ,
            death_DATETIME          TIMESTAMP           ,
            death_type_concept_id   INTEGER     not null ,
            cause_concept_id        INTEGER              ,
            cause_source_value      TEXT             ,
            cause_source_concept_id INTEGER              ,
            unit_id                       TEXT,
            load_table_id                 TEXT,
            load_row_id                   INTEGER,
            trace_id                      TEXT
        )
        ;
        """
    insertQuery = """INSERT INTO """ + etlSchemaName + """.cdm_death
        SELECT
            per.person_id       AS person_id,
        CASE
                WHEN src.deathtime <= src.dischtime THEN CAST(src.deathtime AS DATE)
            ELSE CAST(src.dischtime AS DATE)
            END                           	    AS death_date,
        CASE
                WHEN src.deathtime <= src.dischtime THEN src.deathtime
            ELSE src.dischtime
            END                           	    AS death_DATETIME,

            src.type_concept_id                     AS death_type_concept_id,
            0                                       AS cause_concept_id,
            CAST(NULL AS TEXT)                    AS cause_source_value,
            0                                       AS cause_source_concept_id,
            CONCAT('death.', src.unit_id)           AS unit_id,
            src.load_table_id       AS load_table_id,
            src.load_row_id         AS load_row_id,
            src.trace_id            AS trace_id
        FROM
            """ + etlSchemaName + """.lk_death_adm_mapped src
        INNER JOIN
            """ + etlSchemaName + """.cdm_person per
                ON CAST(src.subject_id AS TEXT) = per.person_source_value
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertQuery)


def migrate(con, etlSchemaName):
    createDeathLookup(con = con, etlSchemaName = etlSchemaName)
    createDeathCdm(con = con, etlSchemaName = etlSchemaName)
