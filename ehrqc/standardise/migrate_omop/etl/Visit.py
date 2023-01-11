import logging

log = logging.getLogger("Standardise")


def createAdmissionsClean(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_admissions_clean")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_admissions_clean cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_admissions_clean AS
        SELECT
            src.subject_id                                  AS subject_id,
            src.hadm_id                                     AS hadm_id,
            CASE 
                WHEN src.edregtime < src.admittime THEN src.edregtime
                ELSE src.admittime
                END 										AS start_datetime,
            src.dischtime                                   AS end_datetime,
            src.admission_type                              AS admission_type, -- current location
            src.admission_location                          AS admission_location, -- to hospital
            src.discharge_location                          AS discharge_location, -- from hospital
            CASE 
                WHEN src.edregtime IS NULL THEN FALSE
                ELSE TRUE
                END 										 AS is_er_admission, -- create visit_detail if TRUE
            'admissions'                    AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
        """ + etlSchemaName + """.src_admissions src -- adm
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTransfersClean(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_transfers_clean")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_transfers_clean cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_transfers_clean AS
        SELECT
            src.subject_id                                  AS subject_id,
            COALESCE(src.hadm_id, vis.hadm_id)              AS hadm_id,
            CAST(src.intime AS DATE)                        AS date_id,
            src.transfer_id                                 AS transfer_id,
            src.intime                                      AS start_datetime,
            src.outtime                                     AS end_datetime,
            src.careunit                                    AS current_location, -- find prev and next for adm and disch location
            'transfers'                     AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM 
            """ + etlSchemaName + """.src_transfers src
        LEFT JOIN
                """ + etlSchemaName + """.lk_admissions_clean vis -- associate transfers with admissions according to 
                ON vis.subject_id = src.subject_id
                AND src.intime BETWEEN vis.start_datetime AND vis.end_datetime
                AND src.hadm_id IS NULL
        WHERE 
            src.eventtype != 'discharge' -- these are not useful
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createServicesDuplicated(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_services_duplicated")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_services_duplicated cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_services_duplicated AS
        SELECT
            trace_id, COUNT(*) AS row_count
        FROM 
            """ + etlSchemaName + """.src_services src
        GROUP BY
            src.trace_id
        HAVING COUNT(*) > 1
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createServicesClean(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_services_clean")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_services_clean cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_services_clean AS
        SELECT
            src.subject_id                                  AS subject_id,
            src.hadm_id                                     AS hadm_id,
            src.transfertime                                AS start_datetime,
            LEAD(src.transfertime) OVER (
                PARTITION BY src.subject_id, src.hadm_id 
                ORDER BY src.transfertime
            )                                               AS end_datetime,
            src.curr_service                                AS curr_service,
            src.prev_service                                AS prev_service,
            LAG(src.curr_service) OVER (
                PARTITION BY src.subject_id, src.hadm_id 
                ORDER BY src.transfertime
            )                                               AS lag_service,
            'services'                      AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM 
        """ + etlSchemaName + """.src_services src
        LEFT JOIN
        """ + etlSchemaName + """.lk_services_duplicated sd
                ON src.trace_id = sd.trace_id
        WHERE
            sd.trace_id IS NULL -- remove duplicates with the exact same time of transferring
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createVisitWithoutId(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_visit_no_hadm_all")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_visit_no_hadm_all cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_visit_no_hadm_all AS
        -- labevents
        SELECT
            src.subject_id                                  AS subject_id,
            CAST(src.start_datetime AS DATE)                AS date_id,
            src.start_datetime                              AS start_datetime,
            -- 
            src.unit_id                     AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.lk_meas_labevents_mapped src
        WHERE
            src.hadm_id IS NULL
        UNION ALL
        -- specimen
        SELECT
            src.subject_id                                  AS subject_id,
            CAST(src.start_datetime AS DATE)                AS date_id,
            src.start_datetime                              AS start_datetime,
            -- 
            src.unit_id                     AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.lk_specimen_mapped src
        WHERE
            src.hadm_id IS NULL
        UNION ALL
        -- organism
        SELECT
            src.subject_id                                  AS subject_id,
            CAST(src.start_datetime AS DATE)                AS date_id,
            src.start_datetime                              AS start_datetime,
            -- 
            src.unit_id                     AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.lk_meas_organism_mapped src
        WHERE
            src.hadm_id IS NULL
        UNION ALL
        -- antibiotics
        SELECT
            src.subject_id                                  AS subject_id,
            CAST(src.start_datetime AS DATE)                AS date_id,
            src.start_datetime                              AS start_datetime,
            -- 
            src.unit_id                     AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.lk_meas_ab_mapped src
        WHERE
            src.hadm_id IS NULL
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createVisitWithoutIdDist(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_visit_no_hadm_dist")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_visit_no_hadm_dist cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_visit_no_hadm_dist AS
        SELECT
            src.subject_id                                  AS subject_id,
            src.date_id                                     AS date_id,
            MIN(src.start_datetime)                         AS start_datetime,
            MAX(src.start_datetime)                         AS end_datetime,
            'AMBULATORY OBSERVATION'                        AS admission_type, -- outpatient visit
            CAST(NULL AS TEXT)                            AS admission_location, -- to hospital
            CAST(NULL AS TEXT)                            AS discharge_location, -- from hospital
            -- 
            'no_hadm'                       AS unit_id,
            'lk_visit_no_hadm_all'          AS load_table_id,
            0                               AS load_row_id,
            jsonb_build_object('subject_id', src.subject_id, 'date_id', src.date_id )                                AS trace_id
        FROM
            """ + etlSchemaName + """.lk_visit_no_hadm_all src
        GROUP BY
            src.subject_id,
            src.date_id
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createVisitClean(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_visit_clean")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_visit_clean cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_visit_clean AS
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int               AS visit_occurrence_id,
            src.subject_id                                  AS subject_id,
            src.hadm_id                                     AS hadm_id,
            CAST(NULL AS DATE)                              AS date_id,
            src.start_datetime                              AS start_datetime,
            src.end_datetime                                AS end_datetime,
            src.admission_type                              AS admission_type, -- current location
            src.admission_location                          AS admission_location, -- to hospital
            src.discharge_location                          AS discharge_location, -- from hospital
            CONCAT(
                CAST(src.subject_id AS TEXT), '|',
                CAST(src.hadm_id AS TEXT)
            )                                               AS source_value,
            src.unit_id                     AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.lk_admissions_clean src -- adm
        UNION ALL
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int               AS visit_occurrence_id,
            src.subject_id                                  AS subject_id,
            CAST(NULL AS INTEGER)                             AS hadm_id,
            src.date_id                                     AS date_id,
            src.start_datetime                              AS start_datetime,
            src.end_datetime                                AS end_datetime,
            src.admission_type                              AS admission_type, -- current location
            src.admission_location                          AS admission_location, -- to hospital
            src.discharge_location                          AS discharge_location, -- from hospital
            CONCAT(
                CAST(src.subject_id AS TEXT), '|',
                CAST(src.date_id AS TEXT)
            )                                               AS source_value,
            src.unit_id                     AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.lk_visit_no_hadm_dist src -- adm
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createVisitDetailClean(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_visit_detail_clean")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_visit_detail_clean cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_visit_detail_clean AS
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int               AS visit_detail_id,
            src.subject_id                                  AS subject_id,
            src.hadm_id                                     AS hadm_id,
            src.date_id                                     AS date_id,
            src.start_datetime                              AS start_datetime,
            src.end_datetime                                AS end_datetime,  -- if null, populate with next start_datetime
            CONCAT(
                CAST(src.subject_id AS TEXT), '|',
                COALESCE(CAST(src.hadm_id AS TEXT), CAST(src.date_id AS TEXT)), '|',
                CAST(src.transfer_id AS TEXT)
            )                                               AS source_value,
            src.current_location                            AS current_location, -- find prev and next for adm and disch location
            src.unit_id                     AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM 
            """ + etlSchemaName + """.lk_transfers_clean src
        WHERE
            src.hadm_id IS NOT NULL -- some ER transfers are excluded because not all of them fit to additional single day visits
        ;
        """
    insertAdmissionsQuery = """INSERT INTO """ + etlSchemaName + """.lk_visit_detail_clean
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int               AS visit_detail_id,
            src.subject_id                                  AS subject_id,
            src.hadm_id                                     AS hadm_id,
            CAST(src.start_datetime AS DATE)                AS date_id,
            src.start_datetime                              AS start_datetime,
            CAST(NULL AS TIMESTAMP)                          AS end_datetime,  -- if null, populate with next start_datetime
            CONCAT(
                CAST(src.subject_id AS TEXT), '|',
                CAST(src.hadm_id AS TEXT)
            )                                               AS source_value,
            src.admission_type                              AS current_location, -- find prev and next for adm and disch location
            -- 
            src.unit_id                     AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM 
            """ + etlSchemaName + """.lk_admissions_clean src
        WHERE
            src.is_er_admission
        ;
        """
    insertServicesQuery = """INSERT INTO """ + etlSchemaName + """.lk_visit_detail_clean
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int               AS visit_detail_id,
            src.subject_id                                  AS subject_id,
            src.hadm_id                                     AS hadm_id,
            CAST(src.start_datetime AS DATE)                AS date_id,
            src.start_datetime                              AS start_datetime,
            src.end_datetime                                AS end_datetime,
            CONCAT(
                CAST(src.subject_id AS TEXT), '|',
                CAST(src.hadm_id AS TEXT), '|',
                CAST(src.start_datetime AS TEXT)
            )                                               AS source_value,
            src.curr_service                                AS current_location,
            -- 
            src.unit_id                     AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM 
            """ + etlSchemaName + """.lk_services_clean src
        WHERE
            src.prev_service = src.lag_service -- ensure that the services sequence is still consistent after removing duplicates
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertAdmissionsQuery)
            cursor.execute(insertServicesQuery)


def createVisitDetailPrevNext(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_visit_detail_prev_next")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_visit_detail_prev_next cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_visit_detail_prev_next AS
        SELECT 
            src.visit_detail_id                             AS visit_detail_id,
            src.subject_id                                  AS subject_id,
            src.hadm_id                                     AS hadm_id,
            src.date_id                                     AS date_id,
            src.start_datetime                              AS start_datetime,
            COALESCE(
                src.end_datetime,
                LEAD(src.start_datetime) OVER (
                    PARTITION BY src.subject_id, src.hadm_id, src.date_id
                    ORDER BY src.start_datetime ASC
                ),
                vis.end_datetime
            )                                               AS end_datetime,
            src.source_value                                AS source_value,
            src.current_location                            AS current_location,
            LAG(src.visit_detail_id) OVER (
                PARTITION BY src.subject_id, src.hadm_id, src.date_id, src.unit_id
                ORDER BY src.start_datetime ASC
            )                                                AS preceding_visit_detail_id,
            COALESCE(
                LAG(src.current_location) OVER (
                    PARTITION BY src.subject_id, src.hadm_id, src.date_id, src.unit_id -- double-check if chains follow each other or intercept
                    ORDER BY src.start_datetime ASC
                ),
                vis.admission_location
            )                                               AS admission_location,
            COALESCE(
                LEAD(src.current_location) OVER (
                    PARTITION BY src.subject_id, src.hadm_id, src.date_id, src.unit_id
                    ORDER BY src.start_datetime ASC
                ),
                vis.discharge_location
            )                                               AS discharge_location,
            src.unit_id                       AS unit_id,
            src.load_table_id                 AS load_table_id,
            src.load_row_id                   AS load_row_id,
            src.trace_id                      AS trace_id
        FROM 
            """ + etlSchemaName + """.lk_visit_detail_clean src
        LEFT JOIN 
            """ + etlSchemaName + """.lk_visit_clean vis
                ON  src.subject_id = vis.subject_id
                AND (
                    src.hadm_id = vis.hadm_id
                    OR src.hadm_id IS NULL AND src.date_id = vis.date_id
                )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createVisitConcept(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_visit_concept")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_visit_concept cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_visit_concept AS
        SELECT 
            vc.concept_code     AS source_code,
            vc.concept_id       AS source_concept_id,
            vc2.concept_id      AS target_concept_id,
            vc.vocabulary_id    AS source_vocabulary_id
        FROM 
            voc_dataset.concept vc
        LEFT JOIN
            voc_dataset.concept_relationship vcr
                ON  vc.concept_id = vcr.concept_id_1 
                and vcr.relationship_id = 'Maps to'
        LEFT JOIN
            voc_dataset.concept vc2
                ON vc2.concept_id = vcr.concept_id_2
                AND vc2.standard_concept = 'S'
                AND vc2.invalid_reason IS NULL
        WHERE
            vc.vocabulary_id IN (
                'mimiciv_vis_admission_location',   -- for admission_location_concept_id (visit and visit_detail)
                'mimiciv_vis_discharge_location',   -- for discharge_location_concept_id 
                'mimiciv_vis_service',              -- for admisstion_location_concept_id (visit_detail)
                'mimiciv_vis_admission_type',       -- for visit_concept_id
                'mimiciv_cs_place_of_service'       -- for visit_detail_concept_id
            )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createVisitOccurrenceCdm(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".cdm_visit_occurrence")
    dropQuery = """drop table if exists """ + etlSchemaName + """.cdm_visit_occurrence cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.cdm_visit_occurrence
        (
            visit_occurrence_id           INTEGER     not null ,
            person_id                     INTEGER     not null ,
            visit_concept_id              INTEGER     not null ,
            visit_start_date              DATE      not null ,
            visit_start_datetime          TIMESTAMP           ,
            visit_end_date                DATE      not null ,
            visit_end_datetime            TIMESTAMP           ,
            visit_type_concept_id         INTEGER     not null ,
            provider_id                   INTEGER              ,
            care_site_id                  INTEGER              ,
            visit_source_value            TEXT             ,
            visit_source_concept_id       INTEGER              ,
            admitting_source_concept_id   INTEGER              ,
            admitting_source_value        TEXT             ,
            discharge_to_concept_id       INTEGER              ,
            discharge_to_source_value     TEXT             ,
            preceding_visit_occurrence_id INTEGER              ,
            -- 
            unit_id                       TEXT,
            load_table_id                 TEXT,
            load_row_id                   INTEGER,
            trace_id                      TEXT
        )
        ;
        """
    insertQuery = """INSERT INTO """ + etlSchemaName + """.cdm_visit_occurrence
        SELECT
            src.visit_occurrence_id                 AS visit_occurrence_id,
            per.person_id                           AS person_id,
            COALESCE(lat.target_concept_id, 0)      AS visit_concept_id,
            CAST(src.start_datetime AS DATE)        AS visit_start_date,
            src.start_datetime                      AS visit_start_datetime,
            CAST(src.end_datetime AS DATE)          AS visit_end_date,
            src.end_datetime                        AS visit_end_datetime,
            32817                                   AS visit_type_concept_id,   -- EHR   Type Concept    Standard                          
            CAST(NULL AS INTEGER)                     AS provider_id,
            cs.care_site_id                         AS care_site_id,
            src.source_value                        AS visit_source_value, -- it should be an ID for visits
            COALESCE(lat.source_concept_id, 0)      AS visit_source_concept_id, -- it is where visit_concept_id comes from
        CASE
                WHEN src.admission_location IS NOT NULL THEN COALESCE(la.target_concept_id, 0)
                ELSE NULL 
            END 									AS admitting_source_concept_id,
            src.admission_location                  AS admitting_source_value,
            CASE
                WHEN src.discharge_location IS NOT NULL THEN COALESCE(ld.target_concept_id, 0)
                ELSE NULL
            END 									 AS discharge_to_concept_id,         
            src.discharge_location                  AS discharge_to_source_value,
            LAG(src.visit_occurrence_id) OVER ( 
                PARTITION BY subject_id, hadm_id 
                ORDER BY start_datetime
            )                                   AS preceding_visit_occurrence_id,
            --
            CONCAT('visit.', src.unit_id)   AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM 
            """ + etlSchemaName + """.lk_visit_clean src
        INNER JOIN
            """ + etlSchemaName + """.cdm_person per
                ON CAST(src.subject_id AS TEXT) = per.person_source_value
        LEFT JOIN 
            """ + etlSchemaName + """.lk_visit_concept lat
                ON lat.source_code = src.admission_type
        LEFT JOIN 
            """ + etlSchemaName + """.lk_visit_concept la 
                ON la.source_code = src.admission_location
        LEFT JOIN 
            """ + etlSchemaName + """.lk_visit_concept ld
                ON ld.source_code = src.discharge_location
        LEFT JOIN 
            """ + etlSchemaName + """.cdm_care_site cs
                ON care_site_name = 'BIDMC' -- Beth Israel hospital for all
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertQuery)


def createVisitDetailCdm(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".cdm_visit_detail")
    dropQuery = """drop table if exists """ + etlSchemaName + """.cdm_visit_detail cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.cdm_visit_detail
        (
            visit_detail_id                    INTEGER     not null ,
            person_id                          INTEGER     not null ,
            visit_detail_concept_id            INTEGER     not null ,
            visit_detail_start_date            DATE      not null ,
            visit_detail_start_datetime        TIMESTAMP           ,
            visit_detail_end_date              DATE      not null ,
            visit_detail_end_datetime          TIMESTAMP           ,
            visit_detail_type_concept_id       INTEGER     not null , -- detail! -- this typo still exists in v.5.3.1(???)
            provider_id                        INTEGER              ,
            care_site_id                       INTEGER              ,
            admitting_source_concept_id        INTEGER              ,
            discharge_to_concept_id            INTEGER              ,
            preceding_visit_detail_id          INTEGER              ,
            visit_detail_source_value          TEXT             ,
            visit_detail_source_concept_id     INTEGER              , -- detail! -- this typo still exists in v.5.3.1(???)
            admitting_source_value             TEXT             ,
            discharge_to_source_value          TEXT             ,
            visit_detail_parent_id             INTEGER              ,
            visit_occurrence_id                INTEGER     not null ,
            unit_id                       TEXT,
            load_table_id                 TEXT,
            load_row_id                   INTEGER,
            trace_id                      TEXT  
        )
        ;
        """
    insertQuery = """INSERT INTO """ + etlSchemaName + """.cdm_visit_detail
        SELECT
            src.visit_detail_id                     AS visit_detail_id,
            per.person_id                           AS person_id,
            COALESCE(vdc.target_concept_id, 0)      AS visit_detail_concept_id,
            CAST(src.start_datetime AS DATE)        AS visit_start_date,
            src.start_datetime                      AS visit_start_datetime,
            CAST(src.end_datetime AS DATE)          AS visit_end_date,
            src.end_datetime                        AS visit_end_datetime,
            32817                                   AS visit_detail_type_concept_id,   -- EHR   Type Concept    Standard                          
            CAST(NULL AS INTEGER)                     AS provider_id,
            cs.care_site_id                         AS care_site_id,
            CASE
                WHEN src.admission_location IS NOT NULL THEN COALESCE(la.target_concept_id, 0)
                ELSE NULL 
            END 									AS admitting_source_concept_id,
        CASE
                WHEN src.discharge_location IS NOT NULL THEN COALESCE(ld.target_concept_id, 0)
                ELSE NULL
            END 									 AS discharge_to_concept_id,
            src.preceding_visit_detail_id           AS preceding_visit_detail_id,
            src.source_value                        AS visit_detail_source_value,
            COALESCE(vdc.source_concept_id, 0)      AS visit_detail_source_concept_id,
            src.admission_location                  AS admitting_source_value,
            src.discharge_location                  AS discharge_to_source_value,
            CAST(NULL AS INTEGER)                     AS visit_detail_parent_id,
            vis.visit_occurrence_id                 AS visit_occurrence_id,
            CONCAT('visit_detail.', src.unit_id)    AS unit_id,
            src.load_table_id                 AS load_table_id,
            src.load_row_id                   AS load_row_id,
            src.trace_id                      AS trace_id
        FROM
            """ + etlSchemaName + """.lk_visit_detail_prev_next src
        INNER JOIN
            """ + etlSchemaName + """.cdm_person per 
                ON CAST(src.subject_id AS TEXT) = per.person_source_value
        INNER JOIN
            """ + etlSchemaName + """.cdm_visit_occurrence vis 
                ON  vis.visit_source_value = 
                    CONCAT(CAST(src.subject_id AS TEXT), '|', 
                        COALESCE(CAST(src.hadm_id AS TEXT), CAST(src.date_id AS TEXT)))
        LEFT JOIN
            """ + etlSchemaName + """.cdm_care_site cs
                ON cs.care_site_source_value = src.current_location
        LEFT JOIN
            """ + etlSchemaName + """.lk_visit_concept vdc
                ON vdc.source_code = src.current_location
        LEFT JOIN
            """ + etlSchemaName + """.lk_visit_concept la 
                ON la.source_code = src.admission_location
        LEFT JOIN
            """ + etlSchemaName + """.lk_visit_concept ld
                ON ld.source_code = src.discharge_location
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertQuery)


def migratePart1(con, etlSchemaName):
    createAdmissionsClean(con = con, etlSchemaName = etlSchemaName)
    createTransfersClean(con = con, etlSchemaName = etlSchemaName)
    createServicesDuplicated(con = con, etlSchemaName = etlSchemaName)
    createServicesClean(con = con, etlSchemaName = etlSchemaName)


def migratePart2(con, etlSchemaName):
    createVisitWithoutId(con = con, etlSchemaName = etlSchemaName)
    createVisitWithoutIdDist(con = con, etlSchemaName = etlSchemaName)
    createVisitClean(con = con, etlSchemaName = etlSchemaName)
    createVisitDetailClean(con = con, etlSchemaName = etlSchemaName)
    createVisitDetailPrevNext(con = con, etlSchemaName = etlSchemaName)
    createVisitConcept(con = con, etlSchemaName = etlSchemaName)


def migrateVisitOccurrence(con, etlSchemaName):
    createVisitOccurrenceCdm(con = con, etlSchemaName = etlSchemaName)

def migrateVisitDetail(con, etlSchemaName):
    createVisitDetailCdm(con = con, etlSchemaName = etlSchemaName)
