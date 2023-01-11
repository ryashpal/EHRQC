import logging

log = logging.getLogger("Standardise")


def createSubjectEthnicityTemp(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_subject_ethnicity")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_subject_ethnicity cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_subject_ethnicity AS
        SELECT DISTINCT
            src.subject_id                      AS subject_id,
            FIRST_VALUE(src.ethnicity) OVER (
                PARTITION BY src.subject_id
                ORDER BY src.admittime ASC)   AS ethnicity_first
        FROM
        """ + etlSchemaName + """.src_admissions src
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def dropSubjectEthnicityTemp(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_subject_ethnicity")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_subject_ethnicity cascade"""
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)


def createEthnicityConcept(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_pat_ethnicity_concept")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_pat_ethnicity_concept cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_pat_ethnicity_concept AS
        SELECT DISTINCT
            src.ethnicity_first   AS source_code,
            vc.concept_id         AS source_concept_id,
            vc.vocabulary_id      AS source_vocabulary_id,
            vc1.concept_id        AS target_concept_id,
            vc1.vocabulary_id     AS target_vocabulary_id -- look here to distinguish Race and Ethnicity
        FROM
            """ + etlSchemaName + """.tmp_subject_ethnicity src
        LEFT JOIN
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
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createPersonCdm(con, etlSchemaName):
    dropQuery = """drop table if exists """ + etlSchemaName + """.cdm_person cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.cdm_person
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
            unit_id                     TEXT,
            load_table_id               TEXT,
            load_row_id                 INTEGER,
            trace_id                    TEXT
        )
        ;
        """
    insertQuery = """INSERT INTO """ + etlSchemaName + """.cdm_person
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
            'person.patients'             AS unit_id,
            p.load_table_id               AS load_table_id,
            p.load_row_id                 AS load_row_id,
            p.trace_id                    AS trace_id
        FROM
        """ + etlSchemaName + """.src_patients p
        LEFT JOIN
            """ + etlSchemaName + """.tmp_subject_ethnicity eth
                ON  p.subject_id = eth.subject_id
        LEFT JOIN
            """ + etlSchemaName + """.lk_pat_ethnicity_concept map_eth
                ON  eth.ethnicity_first = map_eth.source_code
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertQuery)


def createPersonTemp(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_person")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_person cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_person AS
        SELECT per.*
        FROM 
            """ + etlSchemaName + """.cdm_person per
        INNER JOIN
            """ + etlSchemaName + """.cdm_observation_period op
                ON  per.person_id = op.person_id
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createPerson(con, etlSchemaName):
    log.info("Truncating table: " + etlSchemaName + ".cdm_person")
    truncateQuery = """TRUNCATE TABLE """ + etlSchemaName + """.cdm_person"""
    insertQuery = """INSERT INTO """ + etlSchemaName + """.cdm_person
        SELECT per.*
        FROM
            """ + etlSchemaName + """.tmp_person per
        ;
    """
    with con:
        with con.cursor() as cursor:
            cursor.execute(truncateQuery)
            cursor.execute(insertQuery)


def dropPersonTemp(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_person")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_person cascade"""
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)


def migrate(con, etlSchemaName):
    createSubjectEthnicityTemp(con = con, etlSchemaName = etlSchemaName)
    createEthnicityConcept(con = con, etlSchemaName = etlSchemaName)
    createPersonCdm(con = con, etlSchemaName = etlSchemaName)
    dropSubjectEthnicityTemp(con = con, etlSchemaName = etlSchemaName)


def migrateFinal(con, etlSchemaName):
    createPersonTemp(con = con, etlSchemaName = etlSchemaName)
    createPerson(con = con, etlSchemaName = etlSchemaName)
    dropPersonTemp(con = con, etlSchemaName = etlSchemaName)
