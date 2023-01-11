import logging

log = logging.getLogger("Standardise")


def createDiagnosesIcdClean(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_diagnoses_icd_clean")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_diagnoses_icd_clean cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_diagnoses_icd_clean AS
        SELECT
            src.subject_id                              AS subject_id,
            src.hadm_id                                 AS hadm_id,
            CASE 
                WHEN src.seq_num > 20 THEN 20
                ELSE src.seq_num
            END                                         AS seq_num, -- to fit "Inpatient detail %" concepts provided by OMOP
            COALESCE(adm.edregtime, adm.admittime)      AS start_datetime, -- always exists
            dischtime                                   AS end_datetime,
            src.icd_code                                AS source_code,
            CASE 
                WHEN src.icd_version = 9 THEN 'ICD9CM'
                WHEN src.icd_version = 10 THEN 'ICD10CM'
                ELSE NULL
            END                                         AS source_vocabulary_id,
            'diagnoses_icd'         AS unit_id,
            src.load_table_id       AS load_table_id,
            src.load_row_id         AS load_row_id,
            src.trace_id            AS trace_id
        FROM
            """ + etlSchemaName + """.src_diagnoses_icd src
        INNER JOIN
            """ + etlSchemaName + """.src_admissions adm
                ON  src.hadm_id = adm.hadm_id
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createDiagnosesIcdMapped(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_diagnoses_icd_mapped")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_diagnoses_icd_mapped cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_diagnoses_icd_mapped AS
        SELECT
            src.subject_id                      AS subject_id,
            src.hadm_id                         AS hadm_id,
            src.seq_num                         AS seq_num,
            src.start_datetime                  AS start_datetime,
            src.end_datetime                    AS end_datetime,
            32821                               AS type_concept_id, -- OMOP4976894 EHR billing record
            src.source_code                     AS source_code,
            src.source_vocabulary_id            AS source_vocabulary_id,
            vc.concept_id                       AS source_concept_id,
            vc.domain_id                        AS source_domain_id,
            vc2.concept_id                      AS target_concept_id,
            vc2.domain_id                       AS target_domain_id,
            CONCAT('cond.', src.unit_id) AS unit_id,
            src.load_table_id       AS load_table_id,
            src.load_row_id         AS load_row_id,
            src.trace_id            AS trace_id  
        FROM
            """ + etlSchemaName + """.lk_diagnoses_icd_clean src
        LEFT JOIN
            voc_dataset.concept vc
                ON REPLACE(vc.concept_code, '.', '') = REPLACE(TRIM(src.source_code), '.', '')
                AND vc.vocabulary_id = src.source_vocabulary_id
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


def dropDiagnosesIcdMapped(con, etlSchemaName):
    log.info("Dropping table: " + etlSchemaName + ".tmp_seq_num_to_concept")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_seq_num_to_concept cascade"""
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)


def migrate(con, etlSchemaName):
    createDiagnosesIcdClean(con = con, etlSchemaName = etlSchemaName)
    createDiagnosesIcdMapped(con = con, etlSchemaName = etlSchemaName)
    dropDiagnosesIcdMapped(con = con, etlSchemaName = etlSchemaName)
