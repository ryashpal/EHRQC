import logging

log = logging.getLogger("Standardise")


def createCareunitClean(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_trans_careunit_clean")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_trans_careunit_clean cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_trans_careunit_clean AS
        SELECT
            src.careunit                        AS source_code,
            src.load_table_id                   AS load_table_id,
            0                                   AS load_row_id,
            JSONB_AGG(src.trace_id)->> 0 as trace_id
        FROM 
            """ + etlSchemaName + """.src_transfers src
        WHERE
            src.careunit IS NOT NULL
        GROUP BY
            careunit,
            load_table_id	
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createCaresiteCdm(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".cdm_care_site")
    dropQuery = """drop table if exists """ + etlSchemaName + """.cdm_care_site cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.cdm_care_site
        (
            care_site_id                  INTEGER       not null ,
            care_site_name                TEXT               ,
            place_of_service_concept_id  INTEGER                ,
            location_id                   INTEGER               ,
            care_site_source_value        TEXT               ,
            place_of_service_source_value TEXT               ,
            unit_id                       TEXT,
            load_table_id                 TEXT,
            load_row_id                   INTEGER,
            trace_id                      TEXT
        )
        ;
        """
    insertQuery = """INSERT INTO """ + etlSchemaName + """.cdm_care_site
        SELECT
        ('x'||substr(md5(random():: text),1,8))::bit(32)::int    
        AS care_site_id,
            src.source_code                     AS care_site_name,
            vc2.concept_id                      AS place_of_service_concept_id,
            1                                   AS location_id,  -- hard-coded BIDMC
            src.source_code                     AS care_site_source_value,
            src.source_code                     AS place_of_service_source_value,
            'care_site.transfers'       AS unit_id,
            src.load_table_id           AS load_table_id,
            src.load_row_id             AS load_row_id,
        src.trace_id                AS trace_id
        FROM 
        """ + etlSchemaName + """.lk_trans_careunit_clean src
        LEFT JOIN
        voc_dataset.concept vc
                ON  vc.concept_code = src.source_code
                AND vc.vocabulary_id = 'mimiciv_cs_place_of_service' -- gcpt_care_site
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
            cursor.execute(insertQuery)


def migrate(con, etlSchemaName):
    createCareunitClean(con = con, etlSchemaName = etlSchemaName)
    createCaresiteCdm(con = con, etlSchemaName = etlSchemaName)
