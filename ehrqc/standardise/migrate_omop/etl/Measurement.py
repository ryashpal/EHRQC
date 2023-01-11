import logging

log = logging.getLogger("Standardise")


def createMeasurementOperatorConcept(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_meas_operator_concept")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_meas_operator_concept cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_meas_operator_concept AS
        SELECT
            vc.concept_name     AS source_code, -- operator_name,
            vc.concept_id       AS target_concept_id -- operator_concept_id
        FROM
            voc_dataset.concept vc
        WHERE
            vc.domain_id = 'Meas Value Operator'
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMeasurementUnitTemp(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_meas_unit")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_meas_unit cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_meas_unit AS
        SELECT
            vc.concept_code                         AS concept_code,
            vc.vocabulary_id                        AS vocabulary_id,
            vc.domain_id                            AS domain_id,
            vc.concept_id                           AS concept_id,
            ROW_NUMBER() OVER (
                PARTITION BY vc.concept_code
                ORDER BY UPPER(vc.vocabulary_id)
            )                                       AS row_num -- for de-duplication
        FROM
            voc_dataset.concept vc
        WHERE
            vc.vocabulary_id IN ('UCUM', 'mimiciv_meas_unit', 'mimiciv_meas_wf_unit')
            AND vc.domain_id = 'Unit'
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMeasurementUnitConcept(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_meas_unit_concept")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_meas_unit_concept cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_meas_unit_concept AS
        SELECT
            vc.concept_code         AS source_code,
            vc.vocabulary_id        AS source_vocabulary_id,
            vc.domain_id            AS source_domain_id,
            vc.concept_id           AS source_concept_id,
            vc2.domain_id           AS target_domain_id,
            vc2.concept_id          AS target_concept_id
        FROM
            """ + etlSchemaName + """.tmp_meas_unit vc
        LEFT JOIN
            voc_dataset.concept_relationship vcr
                ON  vc.concept_id = vcr.concept_id_1
                AND vcr.relationship_id = 'Maps to'
        LEFT JOIN
            voc_dataset.concept vc2
                ON vc2.concept_id = vcr.concept_id_2
                AND vc2.invalid_reason IS NULL
        WHERE
            vc.row_num = 1
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def dropMeasurementUnitTemp(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_meas_unit")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_meas_unit cascade"""
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)


def createCharteventsClean(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_chartevents_clean")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_chartevents_clean cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_chartevents_clean AS
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
            """ + etlSchemaName + """.src_chartevents src -- ce
        INNER JOIN
            """ + etlSchemaName + """.src_d_items di
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
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createCharteventsCodeTemp(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_chartevents_code_dist")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_chartevents_code_dist cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_chartevents_code_dist AS
        SELECT
            itemid                      AS itemid,
            source_code                 AS source_code,
            source_label                AS source_label,
            'mimiciv_meas_chart'        AS source_vocabulary_id,
            COUNT(*)                    AS row_count
        FROM
            """ + etlSchemaName + """.lk_chartevents_clean
        GROUP BY
            itemid,
            source_code,
            source_label
        UNION ALL
        SELECT
            CAST(NULL AS INTEGER)                 AS itemid,
            value                               AS source_code,
            value                               AS source_label,
            'mimiciv_meas_chartevents_value'    AS source_vocabulary_id, -- both obs values and conditions
            COUNT(*)                            AS row_count
        FROM
            """ + etlSchemaName + """.lk_chartevents_clean
        GROUP BY
            value,
            source_code,
            source_label
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createCharteventsConcept(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_chartevents_concept")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_chartevents_concept cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_chartevents_concept AS
        SELECT
            src.itemid                  AS itemid,
            src.source_code             AS source_code,
            src.source_label            AS source_label,
            src.source_vocabulary_id    AS source_vocabulary_id,
            vc.domain_id                AS source_domain_id,
            vc.concept_id               AS source_concept_id,
            vc2.domain_id               AS target_domain_id,
            vc2.concept_id              AS target_concept_id,
            src.row_count               AS row_count
        FROM
            """ + etlSchemaName + """.tmp_chartevents_code_dist src
        LEFT JOIN
            voc_dataset.concept vc
                ON  vc.concept_code = src.source_code
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


def dropCharteventsCodeTemp(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_chartevents_code_dist")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_chartevents_code_dist cascade"""
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)


def createCharteventsMapped(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_chartevents_mapped")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_chartevents_mapped cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_chartevents_mapped AS
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
            CONCAT('meas.', src.unit_id)                AS unit_id,
            src.load_table_id       AS load_table_id,
            src.load_row_id         AS load_row_id,
            src.trace_id            AS trace_id
        FROM
            """ + etlSchemaName + """.lk_chartevents_clean src -- ce
        LEFT JOIN
            """ + etlSchemaName + """.lk_chartevents_concept c_main -- main
                ON c_main.source_code = src.source_code 
                AND c_main.source_vocabulary_id = 'mimiciv_meas_chart'
        LEFT JOIN
            """ + etlSchemaName + """.lk_chartevents_concept c_value -- values for main
                ON c_value.source_code = src.value
                AND c_value.source_vocabulary_id = 'mimiciv_meas_chartevents_value'
                AND c_value.target_domain_id = 'Meas Value'
        LEFT JOIN 
            """ + etlSchemaName + """.lk_meas_unit_concept uc
                ON uc.source_code = src.valueuom
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createCharteventsConditionMapped(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_chartevents_condition_mapped")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_chartevents_condition_mapped cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_chartevents_condition_mapped AS
        SELECT
            src.subject_id                              AS subject_id,
            src.hadm_id                                 AS hadm_id,
            src.stay_id                                 AS stay_id,
            src.start_datetime                          AS start_datetime,
            src.value                                   AS source_code,
            c_main.source_vocabulary_id                 AS source_vocabulary_id,
            c_main.source_concept_id                    AS source_concept_id,
            c_main.target_domain_id                     AS target_domain_id,
            c_main.target_concept_id                    AS target_concept_id,
            CONCAT('cond.', src.unit_id)                AS unit_id,
            src.load_table_id       AS load_table_id,
            src.load_row_id         AS load_row_id,
            src.trace_id            AS trace_id
        FROM
            """ + etlSchemaName + """.lk_chartevents_clean src -- ce
        INNER JOIN
            """ + etlSchemaName + """.lk_chartevents_concept c_main -- condition domain from values, mapped
                ON c_main.source_code = src.value
                AND c_main.source_vocabulary_id = 'mimiciv_meas_chartevents_value'
                AND c_main.target_domain_id = 'Condition'
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMeasurementsLookupLabeventsClean(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_meas_d_labitems_clean")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_meas_d_labitems_clean cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_meas_d_labitems_clean AS
        SELECT
            dlab.itemid                                                 AS itemid, -- for <cdm>.<source_value>
            COALESCE(dlab.loinc_code, 
                CAST(dlab.itemid AS TEXT))                            AS source_code, -- to join to vocabs
            dlab.loinc_code                                             AS loinc_code, -- for the crosswalk table
            CONCAT(dlab.label, '|', dlab.fluid, '|', dlab.category)     AS source_label, -- for the crosswalk table
            CASE 
            WHEN dlab.loinc_code IS NOT NULL THEN  'LOINC'
            ELSE 'mimiciv_meas_lab_loinc'
            END    
                                                        AS source_vocabulary_id
        FROM
            """ + etlSchemaName + """.src_d_labitems dlab
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMeasurementsLabeventsClean(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_meas_labevents_clean")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_meas_labevents_clean cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_meas_labevents_clean AS
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int       AS measurement_id,
            src.subject_id                          AS subject_id,
            src.charttime                           AS start_datetime, -- measurement_datetime,
            src.hadm_id                             AS hadm_id,
            src.itemid                              AS itemid,
            src.value                               AS value, -- value_source_value
            REGEXP_MATCHES(src.value, '^(\<=|\>=|\>|\<|=|)')   AS value_operator,
            REGEXP_MATCHES(src.value, '[-]?[\d]+[.]?[\d]*')    AS value_number, -- assume "-0.34 etc"
            CASE
            WHEN TRIM(src.valueuom) <> '' THEN src  .valueuom 
            ELSE NULL    
            END AS valueuom, -- unit_source_value,
            src.ref_range_lower                     AS ref_range_lower,
            src.ref_range_upper                     AS ref_range_upper,
            'labevents'                             AS unit_id,
            --
            src.load_table_id       AS load_table_id,
            src.load_row_id         AS load_row_id,
            src.trace_id            AS trace_id
        FROM
            """ + etlSchemaName + """.src_labevents src
        INNER JOIN
            """ + etlSchemaName + """.src_d_labitems dlab
                ON src.itemid = dlab.itemid
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMeasurementsLookupLabitemsConcept(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_meas_d_labitems_concept")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_meas_d_labitems_concept cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_meas_d_labitems_concept AS
        SELECT
            dlab.itemid                 AS itemid,
            dlab.source_code            AS source_code,
            dlab.loinc_code             AS loinc_code,
            dlab.source_label           AS source_label,
            dlab.source_vocabulary_id   AS source_vocabulary_id,
            -- source concept
            vc.domain_id                AS source_domain_id,
            vc.concept_id               AS source_concept_id,
            vc.concept_name             AS source_concept_name,
            -- target concept
            vc2.vocabulary_id           AS target_vocabulary_id,
            vc2.domain_id               AS target_domain_id,
            vc2.concept_id              AS target_concept_id,
            vc2.concept_name            AS target_concept_name,
            vc2.standard_concept        AS target_standard_concept
        FROM
            """ + etlSchemaName + """.lk_meas_d_labitems_clean dlab
        LEFT JOIN
            voc_dataset.concept vc
                ON  vc.concept_code = dlab.source_code -- join 
                AND vc.vocabulary_id = dlab.source_vocabulary_id
                -- AND vc.domain_id = 'Measurement'
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


def createMeasurementsLabeventsWithId(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_meas_labevents_hadm_id")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_meas_labevents_hadm_id cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_meas_labevents_hadm_id AS
        SELECT
            src.trace_id                        AS event_trace_id, 
            adm.hadm_id                         AS hadm_id,
            ROW_NUMBER() OVER (
                PARTITION BY src.trace_id
                ORDER BY adm.start_datetime
            )                                   AS row_num
        FROM  
            """ + etlSchemaName + """.lk_meas_labevents_clean src
        INNER JOIN 
            """ + etlSchemaName + """.lk_admissions_clean adm
                ON adm.subject_id = src.subject_id
                AND src.start_datetime BETWEEN adm.start_datetime AND adm.end_datetime
        WHERE
            src.hadm_id IS NULL
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMeasurementsLabeventsMapped(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_meas_labevents_mapped")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_meas_labevents_mapped cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_meas_labevents_mapped AS
        SELECT
            src.measurement_id                      AS measurement_id,
            src.subject_id                          AS subject_id,
            COALESCE(src.hadm_id, hadm.hadm_id)     AS hadm_id,
            CAST(src.start_datetime AS DATE)        AS date_id,
            src.start_datetime                      AS start_datetime,
            src.itemid                              AS itemid,
            CAST(src.itemid AS TEXT)              AS source_code, -- change working source code to the representation
            labc.source_vocabulary_id               AS source_vocabulary_id,
            labc.source_concept_id                  AS source_concept_id,
            COALESCE(labc.target_domain_id, 'Measurement')  AS target_domain_id,
            labc.target_concept_id                  AS target_concept_id,
            src.valueuom                            AS unit_source_value,
            CASE 
            WHEN src.valueuom IS NOT NULL THEN COALESCE(uc.target_concept_id, 0)
            ELSE NULL
            END    AS unit_concept_id,
            src.value_operator                      AS operator_source_value,
            opc.target_concept_id                   AS operator_concept_id,
            src.value                               AS value_source_value,
            src.value_number                        AS value_as_number,
            CAST(NULL AS INTEGER)                     AS value_as_concept_id,
            src.ref_range_lower                     AS range_low,
            src.ref_range_upper                     AS range_high,
            CONCAT('meas.', src.unit_id)    AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM  
            """ + etlSchemaName + """.lk_meas_labevents_clean src
        INNER JOIN 
            """ + etlSchemaName + """.lk_meas_d_labitems_concept labc
                ON labc.itemid = src.itemid
        LEFT JOIN 
            """ + etlSchemaName + """.lk_meas_operator_concept opc
                ON opc.source_code = src.value_operator[1]
        LEFT JOIN 
            """ + etlSchemaName + """.lk_meas_unit_concept uc
                ON uc.source_code = src.valueuom
        LEFT JOIN 
            """ + etlSchemaName + """.lk_meas_labevents_hadm_id hadm
                ON hadm.event_trace_id = src.trace_id
                AND hadm.row_num = 1
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMicroCrossReference(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_micro_cross_ref")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_micro_cross_ref cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_micro_cross_ref AS
        SELECT
            trace_id                                    AS trace_id_ab, -- for antibiotics
            FIRST_VALUE(src.trace_id) OVER (
                PARTITION BY
                    src.subject_id,
                    src.hadm_id,
                    COALESCE(src.charttime, src.chartdate),
                    src.spec_itemid,
                    src.test_itemid,
                    src.org_itemid
                ORDER BY src.trace_id
            )                                           AS trace_id_org, -- for test-organism pairs
            FIRST_VALUE(src.trace_id) OVER (
                PARTITION BY
                    src.subject_id,
                    src.hadm_id,
                    COALESCE(src.charttime, src.chartdate),
                    src.spec_itemid
                ORDER BY src.trace_id
            )                                           AS trace_id_spec, -- for specimen
            subject_id                                  AS subject_id,    -- to pick additional hadm_id from admissions
            hadm_id                                     AS hadm_id,
            COALESCE(src.charttime, src.chartdate)      AS start_datetime -- just to do coalesce once
        FROM
            """ + etlSchemaName + """.src_microbiologyevents src -- mbe
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMicroWithId(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_micro_hadm_id")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_micro_hadm_id cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_micro_hadm_id AS
        SELECT
            src.trace_id_ab                     AS event_trace_id,
            adm.hadm_id                         AS hadm_id,
            ROW_NUMBER() OVER (
                PARTITION BY src.trace_id_ab
                ORDER BY adm.start_datetime
            )                                   AS row_num
        FROM  
            """ + etlSchemaName + """.lk_micro_cross_ref src
        INNER JOIN 
            """ + etlSchemaName + """.lk_admissions_clean adm
                ON adm.subject_id = src.subject_id
                AND src.start_datetime BETWEEN adm.start_datetime AND adm.end_datetime
        WHERE
            src.hadm_id IS NULL
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMeasurementsOrganismClean(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_meas_organism_clean")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_meas_organism_clean cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_meas_organism_clean AS
        SELECT DISTINCT
            src.subject_id                              AS subject_id,
            src.hadm_id                                 AS hadm_id,
            cr.start_datetime                           AS start_datetime,
            src.spec_itemid                             AS spec_itemid, -- d_micro.itemid, type of specimen taken
            src.test_itemid                             AS test_itemid, -- d_micro.itemid, test taken from the specimen
            src.org_itemid                              AS org_itemid, -- d_micro.itemid, organism which has grown
            cr.trace_id_spec                            AS trace_id_spec, -- to link org and spec in fact_relationship
            'micro.organism'                AS unit_id,
            src.load_table_id               AS load_table_id,
            0                               AS load_row_id,
            cr.trace_id_org                 AS trace_id         -- trace_id for test-organism
        FROM
            """ + etlSchemaName + """.src_microbiologyevents src -- mbe
        INNER JOIN
            """ + etlSchemaName + """.lk_micro_cross_ref cr
                ON src.trace_id = cr.trace_id_org
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createSpecimenClean(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_specimen_clean")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_specimen_clean cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_specimen_clean AS
        SELECT DISTINCT
            src.subject_id                              AS subject_id,
            src.hadm_id                                 AS hadm_id,
            src.start_datetime                          AS start_datetime,
            src.spec_itemid                             AS spec_itemid, -- d_micro.itemid, type of specimen taken
            -- 
            'micro.specimen'                AS unit_id,
            src.load_table_id               AS load_table_id,
            0                               AS load_row_id,
            cr.trace_id_spec                AS trace_id         -- trace_id for specimen
        FROM
            """ + etlSchemaName + """.lk_meas_organism_clean src -- mbe
        INNER JOIN
            """ + etlSchemaName + """.lk_micro_cross_ref cr
                ON src.trace_id = cr.trace_id_spec
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMeasurementsAntibioticClean(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_meas_ab_clean")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_meas_ab_clean cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_meas_ab_clean AS
        SELECT
            src.subject_id                              AS subject_id,
            src.hadm_id                                 AS hadm_id,
            cr.start_datetime                           AS start_datetime,
            src.ab_itemid                               AS ab_itemid, -- antibiotic tested
            src.dilution_comparison                     AS dilution_comparison, -- operator sign
            src.dilution_value                          AS dilution_value, -- numeric dilution value
            src.interpretation                          AS interpretation, -- degree of resistance
            cr.trace_id_org                             AS trace_id_org, -- to link org to ab in fact_relationship
            -- 
            'micro.antibiotics'             AS unit_id,
            src.load_table_id               AS load_table_id,
            0                               AS load_row_id,
            src.trace_id                    AS trace_id         -- trace_id for antibiotics, no groupping is needed
        FROM
            """ + etlSchemaName + """.src_microbiologyevents src
        INNER JOIN
            """ + etlSchemaName + """.lk_micro_cross_ref cr
                ON src.trace_id = cr.trace_id_ab
        WHERE
            src.ab_itemid IS NOT NULL
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMicroLookupClean(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_d_micro_clean")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_d_micro_clean cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_d_micro_clean AS
        SELECT
            dm.itemid                                       AS itemid,
            CAST(dm.itemid AS TEXT)                       AS source_code,
            dm.label                                        AS source_label, -- for organism_mapped: test name plus specimen name
            CONCAT('mimiciv_micro_', LOWER(dm.category))    AS source_vocabulary_id
        FROM
            """ + etlSchemaName + """.src_d_micro dm
        UNION ALL
        SELECT DISTINCT
            CAST(NULL AS INTEGER)                             AS itemid,
            src.interpretation                              AS source_code,
            src.interpretation                              AS source_label,
            'mimiciv_micro_resistance'                      AS source_vocabulary_id
        FROM
            """ + etlSchemaName + """.lk_meas_ab_clean src
        WHERE
            src.interpretation IS NOT NULL
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMicroLookupConcept(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_d_micro_concept")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_d_micro_concept cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_d_micro_concept AS
        SELECT
            dm.itemid                   AS itemid,
            dm.source_code              AS source_code, -- itemid
            dm.source_label             AS source_label, -- symbolic information in case more mapping is required
            dm.source_vocabulary_id     AS source_vocabulary_id,
            -- source_concept
            vc.domain_id                AS source_domain_id,
            vc.concept_id               AS source_concept_id,
            vc.concept_name             AS source_concept_name,
            -- target concept
            vc2.vocabulary_id           AS target_vocabulary_id,
            vc2.domain_id               AS target_domain_id,
            vc2.concept_id              AS target_concept_id,
            vc2.concept_name            AS target_concept_name,
            vc2.standard_concept        AS target_standard_concept
        FROM
            """ + etlSchemaName + """.lk_d_micro_clean dm
        LEFT JOIN
            voc_dataset.concept vc
                ON  dm.source_code = vc.concept_code
                -- gcpt_microbiology_specimen_to_concept -> mimiciv_micro_specimen
                -- (gcpt) brand new vocab -> mimiciv_micro_test
                -- gcpt_org_name_to_concept -> mimiciv_micro_organism
                -- (gcpt) brand new vocab -> mimiciv_micro_resistance
                AND vc.vocabulary_id = dm.source_vocabulary_id
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


def createSpecimenMapped(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_specimen_mapped")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_specimen_mapped cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_specimen_mapped AS
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS specimen_id,
            src.subject_id                              AS subject_id,
            COALESCE(src.hadm_id, hadm.hadm_id)         AS hadm_id,
            CAST(src.start_datetime AS DATE)            AS date_id,
            32856                                       AS type_concept_id, -- Lab
            src.start_datetime                          AS start_datetime,
            src.spec_itemid                             AS spec_itemid,
            mc.source_code                              AS source_code,
            mc.source_vocabulary_id                     AS source_vocabulary_id,
            mc.source_concept_id                        AS source_concept_id,
            COALESCE(mc.target_domain_id, 'Specimen')   AS target_domain_id,
            mc.target_concept_id                        AS target_concept_id,
            src.unit_id                     AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.lk_specimen_clean src
        INNER JOIN
            """ + etlSchemaName + """.lk_d_micro_concept mc
                ON src.spec_itemid = mc.itemid
        LEFT JOIN
            """ + etlSchemaName + """.lk_micro_hadm_id hadm
                ON hadm.event_trace_id = src.trace_id
                AND hadm.row_num = 1
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMeasurementOrganismMapped(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_meas_organism_mapped")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_meas_organism_mapped cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_meas_organism_mapped AS
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS measurement_id,
            src.subject_id                              AS subject_id,
            COALESCE(src.hadm_id, hadm.hadm_id)         AS hadm_id,
            CAST(src.start_datetime AS DATE)            AS date_id,
            32856                                       AS type_concept_id, -- Lab
            src.start_datetime                          AS start_datetime,
            src.test_itemid                             AS test_itemid,
            src.spec_itemid                             AS spec_itemid,
            src.org_itemid                              AS org_itemid,
            CONCAT(tc.source_code, '|', sc.source_code)     AS source_code, -- test itemid plus specimen itemid
            tc.source_vocabulary_id                     AS source_vocabulary_id,
            tc.source_concept_id                        AS source_concept_id,
            COALESCE(tc.target_domain_id, 'Measurement')    AS target_domain_id,
            tc.target_concept_id                        AS target_concept_id,
            oc.source_code                              AS value_source_value,
            oc.target_concept_id                        AS value_as_concept_id,
            src.trace_id_spec                           AS trace_id_spec,
            src.unit_id                     AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.lk_meas_organism_clean src
        INNER JOIN
            """ + etlSchemaName + """.lk_d_micro_concept tc
                ON src.test_itemid = tc.itemid
        INNER JOIN
            """ + etlSchemaName + """.lk_d_micro_concept sc
                ON src.spec_itemid = sc.itemid
        LEFT JOIN
            """ + etlSchemaName + """.lk_d_micro_concept oc
                ON src.org_itemid = oc.itemid
        LEFT JOIN
            """ + etlSchemaName + """.lk_micro_hadm_id hadm
                ON hadm.event_trace_id = src.trace_id
                AND hadm.row_num = 1
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMeasurementAntibioticMapped(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_meas_ab_mapped")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_meas_ab_mapped cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_meas_ab_mapped AS
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS measurement_id,
            src.subject_id                              AS subject_id,
            COALESCE(src.hadm_id, hadm.hadm_id)         AS hadm_id,
            CAST(src.start_datetime AS DATE)            AS date_id,
            32856                                       AS type_concept_id, -- Lab
            src.start_datetime                          AS start_datetime,
            src.ab_itemid                               AS ab_itemid,
            ac.source_code                              AS source_code,
            COALESCE(ac.target_concept_id, 0)           AS target_concept_id,
            COALESCE(ac.source_concept_id, 0)           AS source_concept_id,
            rc.target_concept_id                        AS value_as_concept_id,
            src.interpretation                          AS value_source_value,
            src.dilution_value                          AS value_as_number,
            src.dilution_comparison                     AS operator_source_value,
            opc.target_concept_id                       AS operator_concept_id,
            COALESCE(ac.target_domain_id, 'Measurement')    AS target_domain_id,
            -- fields to link test-organism and antibiotics
            src.trace_id_org                            AS trace_id_org,
            -- 
            src.unit_id                     AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.lk_meas_ab_clean src
        INNER JOIN
            """ + etlSchemaName + """.lk_d_micro_concept ac
                ON src.ab_itemid = ac.itemid
        LEFT JOIN
            """ + etlSchemaName + """.lk_d_micro_concept rc
                ON src.interpretation = rc.source_code
                AND rc.source_vocabulary_id = 'mimiciv_micro_resistance' -- new vocab
        LEFT JOIN
            """ + etlSchemaName + """.lk_meas_operator_concept opc -- see lk_meas_labevents.sql
                ON src.dilution_comparison = opc.source_code
        LEFT JOIN
            """ + etlSchemaName + """.lk_micro_hadm_id hadm
                ON hadm.event_trace_id = src.trace_id
                AND hadm.row_num = 1
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMeasurements(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".cdm_measurement")
    dropQuery = """drop table if exists """ + etlSchemaName + """.cdm_measurement cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.cdm_measurement
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
        """
    insertLabeventsQuery = """INSERT INTO """ + etlSchemaName + """.cdm_measurement
        SELECT
            src.measurement_id                      AS measurement_id,
            per.person_id                           AS person_id,
            COALESCE(src.target_concept_id, 0)      AS measurement_concept_id,
            CAST(src.start_datetime AS DATE)        AS measurement_date,
            src.start_datetime                      AS measurement_datetime,
            CAST(NULL AS TEXT)                    AS measurement_time,
            32856                                   AS measurement_type_concept_id, -- OMOP4976929 Lab
            src.operator_concept_id                 AS operator_concept_id,
            CAST(src.value_as_number[1] AS FLOAT)    AS value_as_number,  -- to move CAST to mapped/clean
            CAST(NULL AS INTEGER)                     AS value_as_concept_id,
            src.unit_concept_id                     AS unit_concept_id,
            src.range_low                           AS range_low,
            src.range_high                          AS range_high,
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
            """ + etlSchemaName + """.lk_meas_labevents_mapped src -- 107,209 
        INNER JOIN
            """ + etlSchemaName + """.cdm_person per -- 110,849
                ON CAST(src.subject_id AS TEXT) = per.person_source_value
        INNER JOIN
            """ + etlSchemaName + """.cdm_visit_occurrence vis -- 116,559
                ON  vis.visit_source_value = 
                    CONCAT(CAST(src.subject_id AS TEXT), '|', 
                        COALESCE(CAST(src.hadm_id AS TEXT), CAST(src.date_id AS TEXT)))
        WHERE
            src.target_domain_id = 'Measurement' -- 115,272
        ;
        """
    insertCharteventsQuery = """INSERT INTO """ + etlSchemaName + """.cdm_measurement
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
            """ + etlSchemaName + """.lk_chartevents_mapped src
        INNER JOIN
            """ + etlSchemaName + """.cdm_person per
                ON CAST(src.subject_id AS TEXT) = per.person_source_value
        INNER JOIN
            """ + etlSchemaName + """.cdm_visit_occurrence vis
                ON  vis.visit_source_value = 
                    CONCAT(CAST(src.subject_id AS TEXT), '|', CAST(src.hadm_id AS TEXT))
        WHERE
            src.target_domain_id = 'Measurement'
        ;
        """
    insertOrganismQuery = """INSERT INTO """ + etlSchemaName + """.cdm_measurement
        SELECT
            src.measurement_id                      AS measurement_id,
            per.person_id                           AS person_id,
            COALESCE(src.target_concept_id, 0)      AS measurement_concept_id,
            CAST(src.start_datetime AS DATE)        AS measurement_date,
            src.start_datetime                      AS measurement_datetime,
            CAST(NULL AS TEXT)                    AS measurement_time,
            src.type_concept_id                     AS measurement_type_concept_id,
            CAST(NULL AS INTEGER)                     AS operator_concept_id,
            CAST(NULL AS FLOAT)                   AS value_as_number,
            COALESCE(src.value_as_concept_id, 0)    AS value_as_concept_id,
            CAST(NULL AS INTEGER)                     AS unit_concept_id,
            CAST(NULL AS INTEGER)                     AS range_low,
            CAST(NULL AS INTEGER)                     AS range_high,
            CAST(NULL AS INTEGER)                     AS provider_id,
            vis.visit_occurrence_id                 AS visit_occurrence_id,
            CAST(NULL AS INTEGER)                     AS visit_detail_id,
            src.source_code                         AS measurement_source_value,
            src.source_concept_id                   AS measurement_source_concept_id,
            CAST(NULL AS TEXT)                    AS unit_source_value,
            src.value_source_value                  AS value_source_value,
            --
            CONCAT('measurement.', src.unit_id)     AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM  
            """ + etlSchemaName + """.lk_meas_organism_mapped src
        INNER JOIN
            """ + etlSchemaName + """.cdm_person per
                ON CAST(src.subject_id AS TEXT) = per.person_source_value
        INNER JOIN
            """ + etlSchemaName + """.cdm_visit_occurrence vis -- 116,559
                ON  vis.visit_source_value = 
                    CONCAT(CAST(src.subject_id AS TEXT), '|', 
                        COALESCE(CAST(src.hadm_id AS TEXT), CAST(src.date_id AS TEXT)))
        WHERE
            src.target_domain_id = 'Measurement'
        ;
        """
    insertAntibioticsQuery = """INSERT INTO """ + etlSchemaName + """.cdm_measurement
        SELECT
            src.measurement_id                      AS measurement_id,
            per.person_id                           AS person_id,
            COALESCE(src.target_concept_id, 0)      AS measurement_concept_id,
            CAST(src.start_datetime AS DATE)        AS measurement_date,
            src.start_datetime                      AS measurement_datetime,
            CAST(NULL AS TEXT)                    AS measurement_time,
            src.type_concept_id                     AS measurement_type_concept_id,
            src.operator_concept_id                 AS operator_concept_id, -- dilution comparison
            src.value_as_number                     AS value_as_number, -- dilution value
            COALESCE(src.value_as_concept_id, 0)    AS value_as_concept_id, -- resistance (interpretation)
            CAST(NULL AS INTEGER)                     AS unit_concept_id,
            CAST(NULL AS INTEGER)                     AS range_low,
            CAST(NULL AS INTEGER)                     AS range_high,
            CAST(NULL AS INTEGER)                     AS provider_id,
            vis.visit_occurrence_id                 AS visit_occurrence_id,
            CAST(NULL AS INTEGER)                     AS visit_detail_id,
            src.source_code                         AS measurement_source_value, -- antibiotic name
            src.source_concept_id                   AS measurement_source_concept_id,
            CAST(NULL AS TEXT)                    AS unit_source_value,
            src.value_source_value                  AS value_source_value, -- resistance source value
            --
            CONCAT('measurement.', src.unit_id)     AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM  
            """ + etlSchemaName + """.lk_meas_ab_mapped src
        INNER JOIN
            """ + etlSchemaName + """.cdm_person per
                ON CAST(src.subject_id AS TEXT) = per.person_source_value
        INNER JOIN
            """ + etlSchemaName + """.cdm_visit_occurrence vis -- 116,559
                ON  vis.visit_source_value = 
                    CONCAT(CAST(src.subject_id AS TEXT), '|', 
                        COALESCE(CAST(src.hadm_id AS TEXT), CAST(src.date_id AS TEXT)))
        WHERE
            src.target_domain_id = 'Measurement'
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertLabeventsQuery)
            cursor.execute(insertCharteventsQuery)
            cursor.execute(insertOrganismQuery)
            cursor.execute(insertAntibioticsQuery)


def migrateUnits(con, etlSchemaName):
    createMeasurementOperatorConcept(con = con, etlSchemaName = etlSchemaName)
    createMeasurementUnitTemp(con = con, etlSchemaName = etlSchemaName)
    createMeasurementUnitConcept(con = con, etlSchemaName = etlSchemaName)
    dropMeasurementUnitTemp(con = con, etlSchemaName = etlSchemaName)


def migrateChartevents(con, etlSchemaName):
    createCharteventsClean(con = con, etlSchemaName = etlSchemaName)
    createCharteventsCodeTemp(con = con, etlSchemaName = etlSchemaName)
    createCharteventsConcept(con = con, etlSchemaName = etlSchemaName)
    dropCharteventsCodeTemp(con = con, etlSchemaName = etlSchemaName)
    createCharteventsMapped(con = con, etlSchemaName = etlSchemaName)
    createCharteventsConditionMapped(con = con, etlSchemaName = etlSchemaName)


def migrateLabevents(con, etlSchemaName):
    createMeasurementsLookupLabeventsClean(con = con, etlSchemaName = etlSchemaName)
    createMeasurementsLabeventsClean(con = con, etlSchemaName = etlSchemaName)
    createMeasurementsLookupLabitemsConcept(con = con, etlSchemaName = etlSchemaName)
    createMeasurementsLabeventsWithId(con = con, etlSchemaName = etlSchemaName)
    createMeasurementsLabeventsMapped(con = con, etlSchemaName = etlSchemaName)


def migrateSpecimen(con, etlSchemaName):
    createMicroCrossReference(con = con, etlSchemaName = etlSchemaName)
    createMicroWithId(con = con, etlSchemaName = etlSchemaName)
    createMeasurementsOrganismClean(con = con, etlSchemaName = etlSchemaName)
    createSpecimenClean(con = con, etlSchemaName = etlSchemaName)
    createMeasurementsAntibioticClean(con = con, etlSchemaName = etlSchemaName)
    createMicroLookupClean(con = con, etlSchemaName = etlSchemaName)
    createMicroLookupConcept(con = con, etlSchemaName = etlSchemaName)
    createSpecimenMapped(con = con, etlSchemaName = etlSchemaName)
    createMeasurementOrganismMapped(con = con, etlSchemaName = etlSchemaName)
    createMeasurementAntibioticMapped(con = con, etlSchemaName = etlSchemaName)


def migrate(con, etlSchemaName):
    createMeasurements(con = con, etlSchemaName = etlSchemaName)
