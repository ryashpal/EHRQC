import logging

log = logging.getLogger("Standardise")


def dropDatetimeeventsConcept(con, etlSchemaName):
    log.info("Dropping table: " + etlSchemaName + ".lk_datetimeevents_concept")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_datetimeevents_concept cascade"""
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)


def dropProcEventClean(con, etlSchemaName):
    log.info("Dropping table: " + etlSchemaName + ".lk_proc_event_clean")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_proc_event_clean cascade"""
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)


def dropDatetimeeventsClean(con, etlSchemaName):
    log.info("Dropping table: " + etlSchemaName + ".lk_datetimeevents_clean")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_datetimeevents_clean cascade"""
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)


def createHcpcsEventsClean(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_hcpcsevents_clean")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_hcpcsevents_clean cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_hcpcsevents_clean AS
        SELECT
            src.subject_id      AS subject_id,
            src.hadm_id         AS hadm_id,
            adm.dischtime       AS start_datetime,
            src.seq_num         AS seq_num, --- procedure_type as in condtion_occurrence
            src.hcpcs_cd                            AS hcpcs_cd,
            src.short_description                   AS short_description,
            --
            src.load_table_id                   AS load_table_id,
            src.load_row_id                     AS load_row_id,
            src.trace_id                        AS trace_id
        FROM
            """ + etlSchemaName + """.src_hcpcsevents src
        INNER JOIN
            """ + etlSchemaName + """.src_admissions adm
                ON src.hadm_id = adm.hadm_id
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createProceduresClean(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_procedures_icd_clean")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_procedures_icd_clean cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_procedures_icd_clean AS
        SELECT
            src.subject_id                              AS subject_id,
            src.hadm_id                                 AS hadm_id,
            adm.dischtime                               AS start_datetime,
            src.icd_code                                AS icd_code,
            src.icd_version                             AS icd_version,
            CASE
                WHEN src.icd_version = 9 THEN 'ICD9Proc'
                WHEN src.icd_version = 10 THEN 'ICD10PCS'
                ELSE 'Unknown'
            END                                         AS source_vocabulary_id,
            REPLACE(src.icd_code, '.', '')              AS source_code, -- to join lk_icd_proc_concept
            --
            src.load_table_id                   AS load_table_id,
            src.load_row_id                     AS load_row_id,
            src.trace_id                        AS trace_id
        FROM
            """ + etlSchemaName + """.src_procedures_icd src
        INNER JOIN
            """ + etlSchemaName + """.src_admissions adm
                ON src.hadm_id = adm.hadm_id
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createProcItemsClean(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_proc_d_items_clean")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_proc_d_items_clean cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_proc_d_items_clean AS
        SELECT
            src.subject_id                      AS subject_id,
            src.hadm_id                         AS hadm_id,
            src.starttime                       AS start_datetime,
            src.value                           AS quantity, 
            src.itemid                          AS itemid,
                                    -- THEN it stores the duration... this is a warkaround and may be inproved
            --
            'procedureevents'                   AS unit_id,
            src.load_table_id                   AS load_table_id,
            src.load_row_id                     AS load_row_id,
            src.trace_id                        AS trace_id
        FROM
            """ + etlSchemaName + """.src_procedureevents src
        WHERE
            src.cancelreason = 0 -- not cancelled
        ;
        """
    insertQuery = """INSERT INTO """ + etlSchemaName + """.lk_proc_d_items_clean
        SELECT
            src.subject_id                      AS subject_id,
            src.hadm_id                         AS hadm_id,
            src.value                           AS start_datetime,
            1                                   AS quantity,
            src.itemid                          AS itemid,
            --
            'datetimeevents'                    AS unit_id,
            src.load_table_id                   AS load_table_id,
            src.load_row_id                     AS load_row_id,
            src.trace_id                        AS trace_id    
        FROM
            """ + etlSchemaName + """.src_datetimeevents src -- de
        INNER JOIN
            """ + etlSchemaName + """.src_patients pat
                ON  pat.subject_id = src.subject_id
        WHERE
            EXTRACT(YEAR FROM src.value) >= pat.anchor_year - pat.anchor_age - 1
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertQuery)


def createHcpcsConcept(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_hcpcs_concept")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_hcpcs_concept cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_hcpcs_concept AS
        SELECT
            vc.concept_code         AS source_code,
            vc.vocabulary_id        AS source_vocabulary_id,    
            vc.domain_id            AS source_domain_id,
            vc.concept_id           AS source_concept_id,
            vc2.domain_id           AS target_domain_id,
            vc2.concept_id          AS target_concept_id
        FROM
            voc_dataset.concept vc
        LEFT JOIN
            voc_dataset.concept_relationship vcr
                ON  vc.concept_id = vcr.concept_id_1
                AND vcr.relationship_id = 'Maps to'
        LEFT JOIN
            voc_dataset.concept vc2
                ON vc2.concept_id = vcr.concept_id_2
                AND vc2.standard_concept = 'S'
                AND vc2.invalid_reason IS NULL
        WHERE
            vc.vocabulary_id IN ('HCPCS', 'CPT4')
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createIcdProcConcept(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_icd_proc_concept")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_icd_proc_concept cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_icd_proc_concept AS
        SELECT
            REPLACE(vc.concept_code, '.', '')       AS source_code,
            vc.vocabulary_id                        AS source_vocabulary_id,
            vc.domain_id                            AS source_domain_id,
            vc.concept_id                           AS source_concept_id,
            vc2.domain_id                           AS target_domain_id,
            vc2.concept_id                          AS target_concept_id
        FROM
            voc_dataset.concept vc
        LEFT JOIN
            voc_dataset.concept_relationship vcr
                ON  vc.concept_id = vcr.concept_id_1
                AND vcr.relationship_id = 'Maps to'
        LEFT JOIN
            voc_dataset.concept vc2
                ON vc2.concept_id = vcr.concept_id_2
                AND vc2.standard_concept = 'S'
                AND vc2.invalid_reason IS NULL
        WHERE
            vc.vocabulary_id IN ('ICD9Proc', 'ICD10PCS')
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createItemidConcept(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_itemid_concept")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_itemid_concept cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_itemid_concept AS
        SELECT
            d_items.itemid                      AS itemid,
            CAST(d_items.itemid AS TEXT)      AS source_code,
            d_items.label                       AS source_label,
            vc.vocabulary_id                    AS source_vocabulary_id,
            vc.domain_id                        AS source_domain_id,
            vc.concept_id                       AS source_concept_id,
            vc2.domain_id                       AS target_domain_id,
            vc2.concept_id                      AS target_concept_id
        FROM
            """ + etlSchemaName + """.src_d_items d_items
        LEFT JOIN
            voc_dataset.concept vc
                ON vc.concept_code = CAST(d_items.itemid AS TEXT)
                AND vc.vocabulary_id IN (
                    'mimiciv_proc_itemid',
                    'mimiciv_proc_datetimeevents'
                )
        LEFT JOIN
            voc_dataset.concept_relationship vcr
                ON  vc.concept_id = vcr.concept_id_1
                AND vcr.relationship_id = 'Maps to'
        LEFT JOIN
            voc_dataset.concept vc2
                ON vc2.concept_id = vcr.concept_id_2
                AND vc2.standard_concept = 'S'
                AND vc2.invalid_reason IS NULL
        WHERE
            d_items.linksto IN (
                'procedureevents',
                'datetimeevents'
            )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createProcedureMapped(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_procedure_mapped")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_procedure_mapped cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_procedure_mapped AS
        SELECT
            src.subject_id                          AS subject_id, -- to person
            src.hadm_id                             AS hadm_id, -- to visit
            src.start_datetime                      AS start_datetime,
            32821                                   AS type_concept_id, -- OMOP4976894 EHR billing record
            CAST(1 AS FLOAT)                      AS quantity,
            CAST(NULL AS INTEGER)                     AS itemid,
            CAST(src.hcpcs_cd AS TEXT)                AS source_code,
            CAST(NULL AS TEXT)                    AS source_label,
            lc.source_vocabulary_id                 AS source_vocabulary_id,
            lc.source_domain_id                     AS source_domain_id,
            COALESCE(lc.source_concept_id, 0)       AS source_concept_id,
            lc.target_domain_id                     AS target_domain_id,
            COALESCE(lc.target_concept_id, 0)       AS target_concept_id,
            'proc.hcpcsevents'              AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.lk_hcpcsevents_clean src
        LEFT JOIN
            """ + etlSchemaName + """.lk_hcpcs_concept lc
                ON src.hcpcs_cd = lc.source_code
        ;
        """
    insertIcdQuery = """INSERT INTO """ + etlSchemaName + """.lk_procedure_mapped
        SELECT
            src.subject_id                          AS subject_id, -- to person
            src.hadm_id                             AS hadm_id, -- to visit
            src.start_datetime                      AS start_datetime,
            32821                                   AS type_concept_id, -- OMOP4976894 EHR billing record
            1                                       AS quantity,
            CAST(NULL AS INTEGER)                     AS itemid,
            CAST(src.source_code AS TEXT)             AS source_code,
            CAST(NULL AS TEXT)                    AS source_label,
            src.source_vocabulary_id                AS source_vocabulary_id,
            lc.source_domain_id                     AS source_domain_id,
            COALESCE(lc.source_concept_id, 0)       AS source_concept_id,
            lc.target_domain_id                     AS target_domain_id,
            COALESCE(lc.target_concept_id, 0)       AS target_concept_id,
            'proc.procedures_icd'           AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.lk_procedures_icd_clean src
        LEFT JOIN
            """ + etlSchemaName + """.lk_icd_proc_concept lc
                ON  src.source_code = lc.source_code
                AND src.source_vocabulary_id = lc.source_vocabulary_id
        ;
        """
    insertProcItemsQuery = """INSERT INTO """ + etlSchemaName + """.lk_procedure_mapped
        SELECT
            src.subject_id                          AS subject_id, -- to person
            src.hadm_id                             AS hadm_id, -- to visit
            src.start_datetime                      AS start_datetime,
            32833                                   AS type_concept_id, -- OMOP4976906 EHR order
            src.quantity                            AS quantity,
            lc.itemid                               AS itemid,
            CAST(src.itemid AS TEXT)              AS source_code,
            lc.source_label                         AS source_label,
            lc.source_vocabulary_id                 AS source_vocabulary_id,
            lc.source_domain_id                     AS source_domain_id,
            COALESCE(lc.source_concept_id, 0)       AS source_concept_id,
            lc.target_domain_id                     AS target_domain_id,
            COALESCE(lc.target_concept_id, 0)       AS target_concept_id,
            --
            CONCAT('proc.', src.unit_id)    AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.lk_proc_d_items_clean src
        LEFT JOIN
            """ + etlSchemaName + """.lk_itemid_concept lc
                ON src.itemid = lc.itemid
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertIcdQuery)
            cursor.execute(insertProcItemsQuery)


def createProcedureOccurrence(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".cdm_procedure_occurrence")
    dropQuery = """drop table if exists """ + etlSchemaName + """.cdm_procedure_occurrence cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.cdm_procedure_occurrence
        (
            procedure_occurrence_id     INTEGER     not null ,
            person_id                   INTEGER     not null ,
            procedure_concept_id        INTEGER     not null ,
            procedure_date              DATE      not null ,
            procedure_datetime          TIMESTAMP           ,
            procedure_type_concept_id   INTEGER     not null ,
            modifier_concept_id         INTEGER              ,
            quantity                    INTEGER              ,
            provider_id                 INTEGER              ,
            visit_occurrence_id         INTEGER              ,
            visit_detail_id             INTEGER              ,
            procedure_source_value      TEXT             ,
            procedure_source_concept_id INTEGER              ,
            modifier_source_value      TEXT              ,
            -- 
            unit_id                       TEXT,
            load_table_id                 TEXT,
            load_row_id                   INTEGER,
            trace_id                      TEXT
        )
        ;
        """
    insertProcedureMappedQuery = """INSERT INTO """ + etlSchemaName + """.cdm_procedure_occurrence
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int          AS procedure_occurrence_id,
            per.person_id                               AS person_id,
            src.target_concept_id                       AS procedure_concept_id,
            CAST(src.start_datetime AS DATE)            AS procedure_date,
            src.start_datetime                          AS procedure_datetime,
            src.type_concept_id                         AS procedure_type_concept_id,
            0                                           AS modifier_concept_id,
            CAST(src.quantity AS INTEGER)                 AS quantity,
            CAST(NULL AS INTEGER)                         AS provider_id,
            vis.visit_occurrence_id                     AS visit_occurrence_id,
            CAST(NULL AS INTEGER)                         AS visit_detail_id,
            src.source_code                             AS procedure_source_value,
            src.source_concept_id                       AS procedure_source_concept_id,
            CAST(NULL AS TEXT)                        AS modifier_source_value,
            -- 
            CONCAT('procedure.', src.unit_id)           AS unit_id,
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
            src.target_domain_id = 'Procedure'
        ;
        """
    insertObservationMappedQuery = """INSERT INTO """ + etlSchemaName + """.cdm_procedure_occurrence
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int          AS procedure_occurrence_id,
            per.person_id                               AS person_id,
            src.target_concept_id                       AS procedure_concept_id,
            CAST(src.start_datetime AS DATE)            AS procedure_date,
            src.start_datetime                          AS procedure_datetime,
            src.type_concept_id                         AS procedure_type_concept_id,
            0                                           AS modifier_concept_id,
            CAST(NULL AS INTEGER)                         AS quantity,
            CAST(NULL AS INTEGER)                         AS provider_id,
            vis.visit_occurrence_id                     AS visit_occurrence_id,
            CAST(NULL AS INTEGER)                         AS visit_detail_id,
            src.source_code                             AS procedure_source_value,
            src.source_concept_id                       AS procedure_source_concept_id,
            CAST(NULL AS TEXT)                        AS modifier_source_value,
            -- 
            CONCAT('procedure.', src.unit_id)           AS unit_id,
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
            src.target_domain_id = 'Procedure'
        ;
        """
    insertSpecimenMappedQuery = """INSERT INTO """ + etlSchemaName + """.cdm_procedure_occurrence
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int          AS procedure_occurrence_id,
            per.person_id                               AS person_id,
            src.target_concept_id                       AS procedure_concept_id,
            CAST(src.start_datetime AS DATE)            AS procedure_date,
            src.start_datetime                          AS procedure_datetime,
            src.type_concept_id                         AS procedure_type_concept_id,
            0                                           AS modifier_concept_id,
            CAST(NULL AS INTEGER)                         AS quantity,
            CAST(NULL AS INTEGER)                         AS provider_id,
            vis.visit_occurrence_id                     AS visit_occurrence_id,
            CAST(NULL AS INTEGER)                         AS visit_detail_id,
            src.source_code                             AS procedure_source_value,
            src.source_concept_id                       AS procedure_source_concept_id,
            CAST(NULL AS TEXT)                        AS modifier_source_value,
            -- 
            CONCAT('procedure.', src.unit_id)           AS unit_id,
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
            src.target_domain_id = 'Procedure'
        ;
        """
    insertCharteventsMappedQuery = """INSERT INTO """ + etlSchemaName + """.cdm_procedure_occurrence
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS procedure_occurrence_id,
            per.person_id                               AS person_id,
            src.target_concept_id                       AS procedure_concept_id,
            CAST(src.start_datetime AS DATE)            AS procedure_date,
            src.start_datetime                          AS procedure_datetime,
            src.type_concept_id                         AS procedure_type_concept_id,
            0                                           AS modifier_concept_id,
            CAST(NULL AS INTEGER)                         AS quantity,
            CAST(NULL AS INTEGER)                         AS provider_id,
            vis.visit_occurrence_id                     AS visit_occurrence_id,
            CAST(NULL AS INTEGER)                         AS visit_detail_id,
            src.source_code                             AS procedure_source_value,
            src.source_concept_id                       AS procedure_source_concept_id,
            CAST(NULL AS TEXT)                        AS modifier_source_value,
            -- 
            CONCAT('procedure.', src.unit_id)           AS unit_id,
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
            src.target_domain_id = 'Procedure'
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertProcedureMappedQuery)
            cursor.execute(insertObservationMappedQuery)
            cursor.execute(insertSpecimenMappedQuery)
            cursor.execute(insertCharteventsMappedQuery)


def migrateLookup(con, etlSchemaName):
    dropDatetimeeventsConcept(con = con, etlSchemaName = etlSchemaName)
    dropProcEventClean(con = con, etlSchemaName = etlSchemaName)
    dropDatetimeeventsClean(con = con, etlSchemaName = etlSchemaName)
    createHcpcsEventsClean(con = con, etlSchemaName = etlSchemaName)
    createProceduresClean(con = con, etlSchemaName = etlSchemaName)
    createProcItemsClean(con = con, etlSchemaName = etlSchemaName)
    createHcpcsConcept(con = con, etlSchemaName = etlSchemaName)
    createIcdProcConcept(con = con, etlSchemaName = etlSchemaName)
    createItemidConcept(con = con, etlSchemaName = etlSchemaName)
    createProcedureMapped(con = con, etlSchemaName = etlSchemaName)

def migrate(con, etlSchemaName):
    createProcedureOccurrence(con = con, etlSchemaName = etlSchemaName)
