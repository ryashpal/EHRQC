import logging

log = logging.getLogger("Standardise")


def createPrescriptionsClean(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_prescriptions_clean")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_prescriptions_clean cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_prescriptions_clean AS
        SELECT
            -- -- 'drug:['                || COALESCE(drug, drug_name_poe, drug_name_generic,'') || ']'||
            -- 'drug:['                || COALESCE(drug,'') || ']'||
            -- 'prod_strength:['       || COALESCE(prod_strength,'') || ']'||
            -- 'drug_type:['           || COALESCE(drug_type,'') || ']'||
            -- -- 'formulary_drug_cd:['   || COALESCE(formulary_drug_cd,'') || ']' ||
            --  'dose_unit_rx:['       || COALESCE(dose_unit_rx,'') || ']' 
            --                                                                     AS concept_name,
            src.subject_id              AS subject_id,
            src.hadm_id                 AS hadm_id,
            src.dose_val_rx             AS dose_val_rx,
            src.starttime               AS start_datetime,
            COALESCE(src.stoptime, src.starttime) AS end_datetime,
            src.route                   AS route_source_code, --TODO: add route AS local concept,
            'mimiciv_drug_route'        AS route_source_vocabulary,
            src.form_unit_disp          AS dose_unit_source_code, --TODO: add unit AS local concept,
            CAST(src.ndc AS TEXT)     AS ndc_source_code, -- ndc was used for automatic/manual mapping,
            'NDC'                       AS ndc_source_vocabulary,
            src.form_val_disp           AS form_val_disp,
            CAST((REGEXP_MATCHES(src.form_val_disp, '[-]?[\d]+[.]?[\d]*'))[1] AS FLOAT)  AS quantity,
            -- COALESCE(
            --     -- src.drug, src.drug_name_poe, src.drug_name_generic,'')
            --     src.drug, '')
            --     || ' ' || COALESCE(src.prod_strength, '')               
        TRIM(COALESCE(
            CASE 
                WHEN src.drug IN ('Bag', 'Vial', 'Syringe', 'Syringe.', 
                                'Syringe (Neonatal)', 'Syringe (Chemo)', 'Soln', 'Soln.',
                                'Sodium Chloride 0.9%  Flush') then pharm.medication
                else src.drug
            END, '') || 
                    ' ' || COALESCE(src.prod_strength, ''))  AS gcpt_source_code, -- medication/drug + prod_strength
            'mimiciv_drug_ndc'                          AS gcpt_source_vocabulary, -- source_code = label
            src.pharmacy_id                             AS pharmacy_id,
            -- 
            'prescriptions'                 AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id

        FROM
            """ + etlSchemaName + """.src_prescriptions src -- pr
        LEFT JOIN 
            """ + etlSchemaName + """.src_pharmacy pharm
                ON src.pharmacy_id = pharm.pharmacy_id
        WHERE
            src.starttime IS NOT NULL
            AND src.drug IS NOT NULL
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createPrNdcConcept(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_pr_ndc_concept")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_pr_ndc_concept cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_pr_ndc_concept AS
        SELECT DISTINCT
            src.ndc_source_code     AS source_code,
            vc.domain_id            AS source_domain_id,
            vc.concept_id           AS source_concept_id,
            vc2.domain_id           AS target_domain_id,
            vc2.concept_id          AS target_concept_id
        FROM
            """ + etlSchemaName + """.lk_prescriptions_clean src -- pr
        LEFT JOIN
            voc_dataset.concept vc
                ON  vc.concept_code = src.ndc_source_code --this covers 85 percent of direct mapping but no standard
                AND vc.vocabulary_id = src.ndc_source_vocabulary -- NDC
        LEFT JOIN
            voc_dataset.concept_relationship vcr
                ON  vc.concept_id = vcr.concept_id_1 
                and vcr.relationship_id = 'Maps to'
        LEFT JOIN
            voc_dataset.concept vc2
                ON vc2.concept_id = vcr.concept_id_2
                AND vc2.standard_concept = 'S'
                AND vc2.invalid_reason IS NULL --covers 71 percent of rxnorm standards concepts
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createPrGcptConcept(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_pr_gcpt_concept")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_pr_gcpt_concept cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_pr_gcpt_concept AS
        SELECT DISTINCT
            src.gcpt_source_code    AS source_code,
            vc.domain_id            AS source_domain_id,
            vc.concept_id           AS source_concept_id,
            vc2.domain_id           AS target_domain_id,
            vc2.concept_id          AS target_concept_id
        FROM
            """ + etlSchemaName + """.lk_prescriptions_clean src -- pr
        LEFT JOIN
            voc_dataset.concept vc
                ON  vc.concept_code = src.gcpt_source_code
                AND vc.vocabulary_id = src.gcpt_source_vocabulary -- mimiciv_drug_ndc
        LEFT JOIN
            voc_dataset.concept_relationship vcr
                ON  vc.concept_id = vcr.concept_id_1 
                and vcr.relationship_id = 'Maps to'
        LEFT JOIN
            voc_dataset.concept vc2
                ON vc2.concept_id = vcr.concept_id_2
                AND vc2.standard_concept = 'S'
                AND vc2.invalid_reason IS NULL --covers 71 percent of rxnorm standards concepts
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createPrRouteConcept(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_pr_route_concept")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_pr_route_concept cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_pr_route_concept AS
        SELECT DISTINCT
            src.route_source_code   AS source_code,
            vc.domain_id            AS source_domain_id,
            vc.concept_id           AS source_concept_id,
            vc2.domain_id           AS target_domain_id,
            vc2.concept_id          AS target_concept_id
        FROM
            """ + etlSchemaName + """.lk_prescriptions_clean src -- pr
        LEFT JOIN
            voc_dataset.concept vc
                ON  vc.concept_code = src.route_source_code
                AND vc.vocabulary_id = src.route_source_vocabulary
        LEFT JOIN
            voc_dataset.concept_relationship vcr
                ON  vc.concept_id = vcr.concept_id_1 
                and vcr.relationship_id = 'Maps to'
        LEFT JOIN
            voc_dataset.concept vc2
                ON vc2.concept_id = vcr.concept_id_2
                AND vc2.standard_concept = 'S'
                AND vc2.invalid_reason IS NULL --covers 71 percent of rxnorm standards concepts
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createDrugMapped(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_drug_mapped")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_drug_mapped cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_drug_mapped AS
        SELECT
            src.hadm_id                                     AS hadm_id,
            src.subject_id                                  AS subject_id,
            COALESCE(vc_ndc.target_concept_id, vc_gcpt.target_concept_id, 0)    AS target_concept_id,
            COALESCE(vc_ndc.target_domain_id, vc_gcpt.target_domain_id, 'Drug') AS target_domain_id,
            src.start_datetime                                                  AS start_datetime,
            CASE
                WHEN src.end_datetime < src.start_datetime THEN src.start_datetime
                ELSE src.end_datetime
            END                                             AS end_datetime,
            32838                                           AS type_concept_id, -- OMOP4976911 EHR prescription
            src.quantity                                    AS quantity,
            COALESCE(vc_route.target_concept_id, 0)                             AS route_concept_id,
            COALESCE(vc_ndc.source_code, vc_gcpt.source_code, src.gcpt_source_code) AS source_code,
            COALESCE(vc_ndc.source_concept_id, vc_gcpt.source_concept_id, 0)    AS source_concept_id,
            src.route_source_code                                               AS route_source_code,
            src.dose_unit_source_code                       AS dose_unit_source_code,
            src.form_val_disp                               AS quantity_source_value,
            src.pharmacy_id                                 AS pharmacy_id, -- to investigate pharmacy.medication
            -- 
            CONCAT('drug.', src.unit_id)    AS unit_id,
            src.load_table_id               AS load_table_id,
            src.load_row_id                 AS load_row_id,
            src.trace_id                    AS trace_id
        FROM
            """ + etlSchemaName + """.lk_prescriptions_clean src
        LEFT JOIN
            """ + etlSchemaName + """.lk_pr_ndc_concept vc_ndc
                ON  src.ndc_source_code = vc_ndc.source_code
                AND vc_ndc.target_concept_id IS NOT NULL
        LEFT JOIN
            """ + etlSchemaName + """.lk_pr_gcpt_concept vc_gcpt
                ON  src.gcpt_source_code = vc_gcpt.source_code
                AND vc_gcpt.target_concept_id IS NOT NULL
        LEFT JOIN
            """ + etlSchemaName + """.lk_pr_route_concept vc_route
                ON src.route_source_code = vc_route.source_code
                AND vc_route.target_concept_id IS NOT NULL
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createDrugExposure(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".cdm_drug_exposure")
    dropQuery = """drop table if exists """ + etlSchemaName + """.cdm_drug_exposure cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.cdm_drug_exposure
        (
            drug_exposure_id              INTEGER       not null ,
            person_id                     INTEGER       not null ,
            drug_concept_id               INTEGER       not null ,
            drug_exposure_start_date      DATE        not null ,
            drug_exposure_start_datetime  TIMESTAMP             ,
            drug_exposure_end_date        DATE        not null ,
            drug_exposure_end_datetime    TIMESTAMP             ,
            verbatim_end_date             DATE                 ,
            drug_type_concept_id          INTEGER       not null ,
            stop_reason                   TEXT               ,
            refills                       INTEGER                ,
            quantity                      FLOAT              ,
            days_supply                   INTEGER                ,
            sig                           TEXT               ,
            route_concept_id              INTEGER                ,
            lot_number                    TEXT               ,
            provider_id                   INTEGER                ,
            visit_occurrence_id           INTEGER                ,
            visit_detail_id               INTEGER                ,
            drug_source_value             TEXT               ,
            drug_source_concept_id        INTEGER                ,
            route_source_value            TEXT               ,
            dose_unit_source_value        TEXT               ,
            -- 
            unit_id                       TEXT,
            load_table_id                 TEXT,
            load_row_id                   INTEGER,
            trace_id                      TEXT
        )
        ;
        """
    insertQuery = """INSERT INTO """ + etlSchemaName + """.cdm_drug_exposure
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int           AS drug_exposure_id,
            per.person_id                               AS person_id,
            src.target_concept_id                       AS drug_concept_id,
            CAST(src.start_datetime AS DATE)            AS drug_exposure_start_date,
            src.start_datetime                          AS drug_exposure_start_datetime,
            CAST(src.end_datetime AS DATE)              AS drug_exposure_end_date,
            src.end_datetime                            AS drug_exposure_end_datetime,
            CAST(NULL AS DATE)                          AS verbatim_end_date,
            src.type_concept_id                         AS drug_type_concept_id,
            CAST(NULL AS TEXT)                        AS stop_reason,
            CAST(NULL AS INTEGER)                         AS refills,
            src.quantity                                AS quantity,
            CAST(NULL AS INTEGER)                         AS days_supply,
            CAST(NULL AS TEXT)                        AS sig,
            src.route_concept_id                        AS route_concept_id,
            CAST(NULL AS TEXT)                        AS lot_number,
            CAST(NULL AS INTEGER)                         AS provider_id,
            vis.visit_occurrence_id                     AS visit_occurrence_id,
            CAST(NULL AS INTEGER)                         AS visit_detail_id,
            src.source_code                             AS drug_source_value,
            src.source_concept_id                       AS drug_source_concept_id,
            src.route_source_code                       AS route_source_value,
            src.dose_unit_source_code                   AS dose_unit_source_value,
            -- 
            CONCAT('drug.', src.unit_id)    AS unit_id,
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
            src.target_domain_id = 'Drug'
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertQuery)


def createJoinVocDrug(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".lk_join_voc_drug")
    dropQuery = """drop table if exists """ + etlSchemaName + """.lk_join_voc_drug cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.lk_join_voc_drug
        AS SELECT DISTINCT
            ca.descendant_concept_id    AS descendant_concept_id,
            ca.ancestor_concept_id      AS ancestor_concept_id,
            c.concept_id                AS concept_id
        FROM
            voc_dataset.concept_ancestor ca
        JOIN
            voc_dataset.concept c
                ON  ca.ancestor_concept_id = c.concept_id
                AND c.vocabulary_id        IN ('RxNorm', 'RxNorm Extension')    -- selects RxNorm, RxNorm Extension vocabulary_id
                AND c.concept_class_id     = 'Ingredient'                       -- selects the Ingredients only.
                                                                                -- There are other concept_classes in RxNorm that
                                                                                    -- we are not interested in.
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTempPretargetDrug(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_pretarget_drug")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_pretarget_drug cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_pretarget_drug
        AS SELECT
            d.drug_exposure_id          AS drug_exposure_id,
            d.person_id                 AS person_id,
            v.concept_id                AS ingredient_concept_id,
            d.drug_exposure_start_date  AS drug_exposure_start_date,
            d.days_supply               AS days_supply,
            d.drug_exposure_end_date    AS drug_exposure_end_date
        FROM
            """ + etlSchemaName + """.cdm_drug_exposure d
        JOIN
            """ + etlSchemaName + """.lk_join_voc_drug v
                ON v.descendant_concept_id = d.drug_concept_id
        WHERE
            d.drug_concept_id != 0
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTempSubenddatesUnDrug(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_subenddates_un_drug")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_subenddates_un_drug cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_subenddates_un_drug
            AS SELECT
                person_id                           AS person_id,
                ingredient_concept_id               AS ingredient_concept_id,
                drug_exposure_start_date            AS event_date,
                -1                                  AS event_type,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        person_id,
                        ingredient_concept_id
                    ORDER BY
                        drug_exposure_start_date)   AS start_ordinal
            FROM
                """ + etlSchemaName + """.tmp_pretarget_drug
        UNION ALL
            SELECT
                person_id                   AS person_id,
                ingredient_concept_id       AS ingredient_concept_id,
                drug_exposure_end_date      AS event_date,
                1                           AS event_type,
                NULL                        AS start_ordinal
            FROM
                """ + etlSchemaName + """.tmp_pretarget_drug
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTempSubenddatesRowsDrug(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_subenddates_rows_drug")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_subenddates_rows_drug cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_subenddates_rows_drug
        AS SELECT
            person_id                       AS person_id,
            ingredient_concept_id           AS ingredient_concept_id,
            event_date                      AS event_date,
            event_type                      AS event_type,
            MAX(start_ordinal) OVER (
                PARTITION BY
                    person_id,
                    ingredient_concept_id
                ORDER BY
                    event_date,
                    event_type
                ROWS UNBOUNDED PRECEDING)   AS start_ordinal,
                    -- this pulls the current START down from the prior rows so that the NULLs
                    -- from the END DATES will contain a value we can compare with
            ROW_NUMBER() OVER (
                PARTITION BY
                    person_id,
                    ingredient_concept_id
                ORDER BY
                    event_date,
                    event_type)             AS overall_ord
                    -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
        FROM
            """ + etlSchemaName + """.tmp_subenddates_un_drug
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTempSubenddatesDrug(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_subenddates_drug")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_subenddates_drug cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_subenddates_drug
        AS SELECT
            person_id               AS person_id,
            ingredient_concept_id   AS ingredient_concept_id,
            event_date              AS end_date
        FROM
            """ + etlSchemaName + """.tmp_subenddates_rows_drug e
        WHERE
            (2 * e.start_ordinal) - e.overall_ord = 0
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTempEndsDrug(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".temp_ends_drug")
    dropQuery = """drop table if exists """ + etlSchemaName + """.temp_ends_drug cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.temp_ends_drug
        AS SELECT
            dt.person_id                    AS person_id,
            dt.ingredient_concept_id        AS drug_concept_id,
            dt.drug_exposure_start_date     AS drug_exposure_start_date,
            MIN(e.end_date)                 AS drug_sub_exposure_end_date
        FROM
            """ + etlSchemaName + """.tmp_pretarget_drug dt
        JOIN
            """ + etlSchemaName + """.tmp_subenddates_drug e
                ON  dt.person_id             = e.person_id
                AND dt.ingredient_concept_id = e.ingredient_concept_id
                AND e.end_date               >= dt.drug_exposure_start_date
        GROUP BY
            dt.drug_exposure_id,
            dt.person_id,
            dt.ingredient_concept_id,
            dt.drug_exposure_start_date
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTempSubDrug(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_sub_drug")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_sub_drug cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_sub_drug
        AS SELECT
            ROW_NUMBER() OVER (
                PARTITION BY
                    person_id,
                    drug_concept_id,
                    drug_sub_exposure_end_date
                ORDER BY
                    person_id,
                    drug_concept_id)        AS row_number,
            person_id                       AS person_id,
            drug_concept_id                 AS drug_concept_id,
            MIN(drug_exposure_start_date)   AS drug_sub_exposure_start_date,
            drug_sub_exposure_end_date      AS drug_sub_exposure_end_date,
            COUNT(*)                        AS drug_exposure_count
        FROM
            """ + etlSchemaName + """.temp_ends_drug
        GROUP BY
            person_id,
            drug_concept_id,
            drug_sub_exposure_end_date
        ORDER BY
            person_id,
            drug_concept_id
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTempFinaltargetDrug(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_finaltarget_drug")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_finaltarget_drug cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_finaltarget_drug
        AS SELECT
            row_number                              AS row_number,
            person_id                               AS person_id,
            drug_concept_id                         AS ingredient_concept_id,
            drug_sub_exposure_start_date            AS drug_sub_exposure_start_date,
            drug_sub_exposure_end_date              AS drug_sub_exposure_end_date,
            drug_exposure_count                     AS drug_exposure_count,
            DATE_PART('day', drug_sub_exposure_end_date::timestamp - drug_sub_exposure_start_date::timestamp ) AS days_exposed
        FROM
            """ + etlSchemaName + """.tmp_sub_drug
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTempEnddatesUnDrug(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_enddates_un_drug")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_enddates_un_drug cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_enddates_un_drug
            AS SELECT
                person_id                                       AS person_id,
                ingredient_concept_id                           AS ingredient_concept_id,
                drug_sub_exposure_start_date                    AS event_date,
                -1                                              AS event_type,
                ROW_NUMBER() OVER (
                    PARTITION BY
                        person_id,
                        ingredient_concept_id
                    ORDER BY
                        drug_sub_exposure_start_date)           AS start_ordinal
            FROM
                """ + etlSchemaName + """.tmp_finaltarget_drug
        UNION ALL
        -- pad the end dates by 30 to allow a grace period for overlapping ranges.
            SELECT
                person_id                                                       AS person_id,
                ingredient_concept_id                                           AS ingredient_concept_id,
                DATE(drug_sub_exposure_end_date+ INTERVAL '30 DAY' )         AS event_date,
                1                                                               AS event_type,
                NULL                                                            AS start_ordinal
            FROM
                """ + etlSchemaName + """.tmp_finaltarget_drug
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTempEnddatesRowsDrug(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_enddates_rows_drug")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_enddates_rows_drug cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_enddates_rows_drug
        AS SELECT
            person_id                       AS person_id,
            ingredient_concept_id           AS ingredient_concept_id,
            event_date                      AS event_date,
            event_type                      AS event_type,
            MAX(start_ordinal) OVER (
                PARTITION BY
                    person_id,
                    ingredient_concept_id
                ORDER BY
                    event_date,
                    event_type
                ROWS UNBOUNDED PRECEDING)   AS start_ordinal,
            -- this pulls the current START down from the prior rows so that the NULLs
            -- from the END DATES will contain a value we can compare with
            ROW_NUMBER() OVER (
                PARTITION BY
                    person_id,
                    ingredient_concept_id
                ORDER BY
                    event_date,
                    event_type)             AS overall_ord
            -- this re-numbers the inner UNION so all rows are numbered ordered by the event date
        FROM
            """ + etlSchemaName + """.tmp_enddates_un_drug
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTempEnddatesDrug(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_enddates_drug")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_enddates_drug cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_enddates_drug
        AS SELECT
            person_id                                       AS person_id,
            ingredient_concept_id                           AS ingredient_concept_id,
            DATE(event_date - INTERVAL '30 DAY')          AS end_date  -- unpad the end date
        FROM
            """ + etlSchemaName + """.tmp_enddates_rows_drug e
        WHERE
            (2 * e.start_ordinal) - e.overall_ord = 0
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTempDrugeraEndsDrug(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_drugera_ends_drug")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_drugera_ends_drug cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_drugera_ends_drug
        AS SELECT
            ft.person_id                        AS person_id,
            ft.ingredient_concept_id            AS ingredient_concept_id,
            ft.drug_sub_exposure_start_date     AS drug_sub_exposure_start_date,
            MIN(e.end_date)                     AS drug_era_end_date,
            ft.drug_exposure_count              AS drug_exposure_count,
            ft.days_exposed                     AS days_exposed
        FROM
            """ + etlSchemaName + """.tmp_finaltarget_drug ft
        JOIN
            """ + etlSchemaName + """.tmp_enddates_drug e
                ON ft.person_id              = e.person_id
                AND e.end_date               >= ft.drug_sub_exposure_start_date
                AND ft.ingredient_concept_id = e.ingredient_concept_id
        GROUP BY
            ft.person_id,
            ft.days_exposed,
            ft.drug_exposure_count,
            ft.ingredient_concept_id,
            ft.drug_sub_exposure_start_date
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createDrugEra(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".cdm_drug_era")
    dropQuery = """drop table if exists """ + etlSchemaName + """.cdm_drug_era cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.cdm_drug_era
        (
            drug_era_id         INTEGER     not null ,
            person_id           INTEGER     not null ,
            drug_concept_id     INTEGER     not null ,
            drug_era_start_date DATE      not null ,
            drug_era_end_date   DATE      not null ,
            drug_exposure_count INTEGER              ,
            gap_days            INTEGER              ,
            -- 
            unit_id                       TEXT,
            load_table_id                 TEXT,
            load_row_id                   INTEGER
        )
        ;
        """
    insertQuery = """INSERT INTO """ + etlSchemaName + """.cdm_drug_era
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int                                   AS drug_era_id,
            person_id                                                           AS person_id,
            ingredient_concept_id                                               AS drug_concept_id,
            MIN (drug_sub_exposure_start_date)                                  AS drug_era_start_date,
            drug_era_end_date                                                   AS drug_era_end_date,
            SUM(drug_exposure_count)                                            AS drug_exposure_count,
        DATE_PART('day', drug_era_end_date::timestamp - MIN(drug_sub_exposure_start_date)::timestamp )- SUM(days_exposed)   AS gap_days,
        -- --
            'drug_era.drug_exposure'                                            AS unit_id,
            CAST(NULL AS TEXT)                                                AS load_table_id,
            CAST(NULL AS INTEGER)                                                 AS load_row_id
        FROM
            """ + etlSchemaName + """.tmp_drugera_ends_drug
        GROUP BY
            person_id,
            drug_era_end_date,
            ingredient_concept_id
        ORDER BY
            person_id,
            ingredient_concept_id
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertQuery)


def dropTempTables1(con, etlSchemaName):
    log.info("Dropping temporary tables")
    dropQuery1 = """drop table if exists """ + etlSchemaName + """.tmp_drugera_ends_drug cascade"""
    dropQuery2 = """drop table if exists """ + etlSchemaName + """.tmp_enddates_drug cascade"""
    dropQuery3 = """drop table if exists """ + etlSchemaName + """.tmp_finaltarget_drug cascade"""
    dropQuery4 = """drop table if exists """ + etlSchemaName + """.tmp_enddates_un_drug cascade"""
    dropQuery5 = """drop table if exists """ + etlSchemaName + """.tmp_sub_drug cascade"""
    dropQuery6 = """drop table if exists """ + etlSchemaName + """.temp_ends_drug cascade"""
    dropQuery7 = """drop table if exists """ + etlSchemaName + """.tmp_pretarget_drug cascade"""
    dropQuery8 = """drop table if exists """ + etlSchemaName + """.tmp_subenddates_un_drug cascade"""
    dropQuery9 = """drop table if exists """ + etlSchemaName + """.tmp_subenddates_rows_drug cascade"""
    dropQuery10 = """drop table if exists """ + etlSchemaName + """.tmp_subenddates_drug cascade"""
    dropQuery11 = """drop table if exists """ + etlSchemaName + """.tmp_enddates_rows_drug cascade"""

    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery1)
            cursor.execute(dropQuery2)
            cursor.execute(dropQuery3)
            cursor.execute(dropQuery4)
            cursor.execute(dropQuery5)
            cursor.execute(dropQuery6)
            cursor.execute(dropQuery7)
            cursor.execute(dropQuery8)
            cursor.execute(dropQuery9)
            cursor.execute(dropQuery10)
            cursor.execute(dropQuery11)


def createDoseEra(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".cdm_dose_era")
    dropQuery = """drop table if exists """ + etlSchemaName + """.cdm_dose_era cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.cdm_dose_era
        (
            dose_era_id           INTEGER     not null ,
            person_id             INTEGER     not null ,
            drug_concept_id       INTEGER     not null ,
            unit_concept_id       INTEGER     not null ,
            dose_value            FLOAT   not null ,
            dose_era_start_date   DATE      not null ,
            dose_era_end_date     DATE      not null ,
            -- 
            unit_id                       TEXT,
            load_table_id                 TEXT,
            load_row_id                   INTEGER
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTempDrugIngredientExp(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_drugIngredientExp")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_drugIngredientExp cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_drugIngredientExp AS
        SELECT
            de.drug_exposure_id                     AS drug_exposure_id,
            de.person_id                            AS person_id,
            de.drug_exposure_start_date             AS drug_exposure_start_date,
            de.drug_exposure_end_date               AS drug_exposure_end_date,
            de.drug_concept_id                      AS drug_concept_id,
            ds.ingredient_concept_id                AS ingredient_concept_id,
            de.refills                              AS refills,
            CASE
                WHEN DE.days_supply = 0 THEN 1
                ELSE DE.days_supply
            END                                     AS days_supply,
            de.quantity                             AS quantity,
            ds.box_size                             AS box_size,
            ds.amount_value                         AS amount_value,
            ds.amount_unit_concept_id               AS amount_unit_concept_id,
            ds.numerator_value                      AS numerator_value,
            ds.numerator_unit_concept_id            AS numerator_unit_concept_id,
            ds.denominator_value                    AS denominator_value,
            ds.denominator_unit_concept_id          AS denominator_unit_concept_id,
            c.concept_class_id                      AS concept_class_id
        FROM """ + etlSchemaName + """.cdm_drug_exposure de
        INNER JOIN voc_dataset.drug_strength ds
            ON de.drug_concept_id = ds.drug_concept_id
        INNER JOIN voc_dataset.concept_ancestor ca
            ON  de.drug_concept_id = ca.descendant_concept_id
            AND ds.ingredient_concept_id = ca.ancestor_concept_id
        LEFT JOIN voc_dataset.concept c
            ON  de.drug_concept_id = concept_id
            AND c.vocabulary_id IN ('RxNorm', 'RxNorm Extension')
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTempDrugWithDose(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_drugWithDose")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_drugWithDose cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_drugWithDose AS
        SELECT
            drug_exposure_id                        AS drug_exposure_id,
            person_id                               AS person_id,
            drug_exposure_start_date                AS drug_exposure_start_date,
            drug_exposure_end_date                  AS drug_exposure_end_date,
            ingredient_concept_id                   AS drug_concept_id,
            refills                                 AS refills,
            days_supply                             AS days_supply,
            quantity                                AS quantity,
            -- CASE 1
            CASE
                WHEN amount_value IS NOT NULL
                    AND denominator_unit_concept_id IS NULL
                THEN
                    CASE
                        WHEN quantity > 0
                            AND box_size IS NOT NULL
                            AND concept_class_id IN ('Branded Drug Box', 'Clinical Drug Box', 'Marketed Product',
                                                    'Quant Branded Box', 'Quant Clinical Box')
                        THEN amount_value * quantity * box_size / days_supply
                        WHEN quantity > 0
                            AND concept_class_id NOT IN ('Branded Drug Box', 'Clinical Drug Box', 'Marketed Product',
                                                        'Quant Branded Box', 'Quant Clinical Box')
                        THEN amount_value * quantity / days_supply
                        WHEN quantity = 0 AND box_size IS NOT NULL
                        THEN amount_value * box_size / days_supply
                        WHEN quantity = 0 AND box_size IS NULL
                        THEN -1
                    END
                -- CASE 2, 3
                WHEN numerator_value IS NOT NULL
                    AND concept_class_id != 'Ingredient'
                    AND denominator_unit_concept_id != 8505     --hour
                THEN
                    CASE
                        WHEN denominator_value IS NOT NULL
                        THEN numerator_value / days_supply
                        WHEN denominator_value IS NULL AND quantity != 0
                        THEN numerator_value * quantity / days_supply
                        WHEN denominator_value IS NULL AND quantity = 0
                        THEN -1
                    END
                -- CASE 4
                WHEN numerator_value IS NOT NULL
                    AND concept_class_id = 'Ingredient'
                    AND denominator_unit_concept_id != 8505
                THEN
                    CASE
                        WHEN quantity > 0
                        THEN quantity / days_supply
                        WHEN quantity = 0
                        THEN -1
                    END
                -- CASE 6
                WHEN numerator_value IS NOT NULL
                    AND denominator_unit_concept_id = 8505
                THEN
                    CASE
                        WHEN denominator_value IS NOT NULL
                        THEN numerator_value * 24 / denominator_value
                        WHEN denominator_value IS NULL AND quantity != 0
                        THEN numerator_value * 24 / quantity
                        WHEN denominator_value IS NULL AND quantity = 0
                        THEN -1
                    END
            END                                     AS dose_value,
            -- CASE 1
            CASE
                WHEN amount_value IS NOT NULL
                    AND denominator_unit_concept_id IS NULL
                THEN
                    CASE
                        WHEN quantity = 0 AND box_size IS NULL
                        THEN -1
                        ELSE amount_unit_concept_id
                    END
                -- CASE 2, 3
                WHEN numerator_value IS NOT NULL
                    AND concept_class_id != 'Ingredient'
                    AND denominator_unit_concept_id != 8505     --hour
                THEN
                    CASE
                        WHEN denominator_value IS NULL AND quantity = 0
                        THEN -1
                        ELSE numerator_unit_concept_id
                    END
                -- CASE 4
                WHEN numerator_value IS NOT NULL
                    AND concept_class_id = 'Ingredient'
                    AND denominator_unit_concept_id != 8505
                THEN
                    CASE
                        WHEN quantity > 0
                        THEN 0
                        WHEN quantity = 0
                        THEN -1
                    END
                -- CASE 6
                WHEN numerator_value IS NOT NULL
                    AND denominator_unit_concept_id = 8505
                THEN
                    CASE
                    WHEN denominator_value IS NULL AND quantity = 0
                    THEN -1
                    ELSE numerator_unit_concept_id
                END
            END                                     AS unit_concept_id
        FROM """ + etlSchemaName + """.tmp_drugIngredientExp
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTempCteDoseTarget(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_cteDoseTarget")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_cteDoseTarget cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_cteDoseTarget AS
        SELECT
            dwd.drug_exposure_id                                                AS drug_exposure_id,
            dwd.person_id                                                       AS person_id,
            dwd.drug_concept_id                                                 AS drug_concept_id,
            dwd.unit_concept_id                                                 AS unit_concept_id,
            dwd.dose_value                                                      AS dose_value,
            dwd.drug_exposure_start_date                                        AS drug_exposure_start_date,
            dwd.days_supply                                                     AS days_supply,
        COALESCE(drug_exposure_end_date,
                -- If drug_exposure_end_date != NULL,
                -- return drug_exposure_end_date, otherwise go to next case
                NULLIF(drug_exposure_start_date + (1*days_supply * (COALESCE(refills, 0) + 1))*INTERVAL '1 day', drug_exposure_start_date),
                
                --If days_supply != NULL or 0, return drug_exposure_start_date + days_supply,
                -- otherwise go to next case
                drug_exposure_start_date + INTERVAL '1 day')       AS drug_exposure_end_date
                -- Add 1 day to the drug_exposure_start_date since
                -- there is no end_date or INTERVAL for the days_supply
        FROM """ + etlSchemaName + """.tmp_drugWithDose dwd
        WHERE
            dose_value <> -1
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTempCteDoseEndDatesRawdata(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_cteDoseEndDates_rawdata")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_cteDoseEndDates_rawdata cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_cteDoseEndDates_rawdata AS
        SELECT
            person_id                                       AS person_id,
            drug_concept_id                                 AS drug_concept_id,
            unit_concept_id                                 AS unit_concept_id,
            dose_value                                      AS dose_value,
            drug_exposure_start_date                        AS event_date,
            -1                                              AS event_type,
            ROW_NUMBER() OVER (
                PARTITION BY person_id, drug_concept_id, unit_concept_id, CAST(dose_value AS INTEGER)
                ORDER BY drug_exposure_start_date)          AS start_ordinal
        FROM """ + etlSchemaName + """.tmp_cteDoseTarget
        UNION ALL
        -- pad the end dates by 30 to allow a grace period for overlapping ranges.
        SELECT
            person_id                                                   AS person_id,
            drug_concept_id                                             AS drug_concept_id,
            unit_concept_id                                             AS unit_concept_id,
            dose_value                                                  AS dose_value,
            Drug_exposure_end_date + 30*INTERVAL '1 day'           AS event_date,
            1                                                           AS event_type,
            NULL                                                        AS start_ordinal
        FROM """ + etlSchemaName + """.tmp_cteDoseTarget
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTempCteDoseEndDatesE(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_cteDoseEndDates_e")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_cteDoseEndDates_e cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_cteDoseEndDates_e AS
        SELECT
            person_id                                                                   AS person_id,
            drug_concept_id                                                             AS drug_concept_id,
            unit_concept_id                                                             AS unit_concept_id,
            dose_value                                                                  AS dose_value,
            event_date                                                                  AS event_date,
            event_type                                                                  AS event_type,
            MAX(start_ordinal) OVER (
                PARTITION BY person_id, drug_concept_id, unit_concept_id, CAST(dose_value AS INTEGER) -- double-check if it is a valid cast
                ORDER BY event_date, event_type ROWS unbounded preceding)               AS start_ordinal,
            ROW_NUMBER() OVER (
                PARTITION BY person_id, drug_concept_id, unit_concept_id, CAST(dose_value AS INTEGER)
                ORDER BY event_date, event_type)                                        AS overall_ord
                -- order by above pulls the current START down from the prior
                -- rows so that the NULLs from the END DATES will contain a value we can compare with
        FROM """ + etlSchemaName + """.tmp_cteDoseEndDates_rawdata
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTempCteDoseEndDates(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_cteDoseEndDates")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_cteDoseEndDates cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_cteDoseEndDates AS
        SELECT
            person_id                                       AS person_id,
            drug_concept_id                                 AS drug_concept_id,
            unit_concept_id                                 AS unit_concept_id,
            dose_value                                      AS dose_value,
            Event_date - 30 * INTERVAL '1 day'           AS end_date   -- unpad the end date
        FROM """ + etlSchemaName + """.tmp_cteDoseEndDates_e
        WHERE
            (2 * start_ordinal) - overall_ord = 0
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTempCteDoseFinalEnds(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_cteDoseFinalEnds")
    dropQuery = """drop table if exists """ + etlSchemaName + """.tmp_cteDoseFinalEnds cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.tmp_cteDoseFinalEnds AS
        SELECT
            dt.person_id                    AS person_id,
            dt.drug_concept_id              AS drug_concept_id,
            dt.unit_concept_id              AS unit_concept_id,
            dt.dose_value                   AS dose_value,
            dt.drug_exposure_start_date     AS drug_exposure_start_date,
            MIN(e.end_date)                 AS drug_era_end_date
        FROM """ + etlSchemaName + """.tmp_cteDoseTarget dt
        INNER JOIN """ + etlSchemaName + """.tmp_cteDoseEndDates e
            ON  dt.person_id = e.person_id
            AND dt.drug_concept_id = e.drug_concept_id
            AND dt.unit_concept_id = e.unit_concept_id
            AND dt.drug_concept_id = e.drug_concept_id
            AND dt.dose_value = e.dose_value
            AND e.end_date >= dt.drug_exposure_start_date
        GROUP BY
            dt.drug_exposure_id,
            dt.person_id,
            dt.drug_concept_id,
            dt.drug_exposure_start_date,
            dt.unit_concept_id,
            dt.dose_value
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def insertDoseEra(con, etlSchemaName):
    log.info("Inserting table: " + etlSchemaName + ".cdm_dose_era")
    insertQuery = """INSERT INTO """ + etlSchemaName + """.cdm_dose_era
        SELECT
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int   AS dose_era_id,
            person_id                           AS person_id,
            drug_concept_id                     AS drug_concept_id,
            unit_concept_id                     AS unit_concept_id,
            dose_value                          AS dose_value,
            MIN(drug_exposure_start_date)       AS dose_era_start_date,
            drug_era_end_date                   AS dose_era_end_date,
            'dose_era.drug_exposure'            AS unit_id,
            CAST(NULL AS TEXT)                AS load_table_id,
            CAST(NULL AS INTEGER)                 AS load_row_id
        FROM """ + etlSchemaName + """.tmp_cteDoseFinalEnds
        GROUP BY
            person_id,
            drug_concept_id,
            unit_concept_id,
            dose_value,
            drug_era_end_date
        ORDER BY
            person_id,
            drug_concept_id
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(insertQuery)


def dropTempTables2(con, etlSchemaName):
    log.info("Dropping temporary tables")
    dropQuery1 = """drop table if exists """ + etlSchemaName + """.tmp_drugIngredientExp cascade"""
    dropQuery2 = """drop table if exists """ + etlSchemaName + """.tmp_drugWithDose cascade"""
    dropQuery3 = """drop table if exists """ + etlSchemaName + """.tmp_cteDoseTarget cascade"""
    dropQuery4 = """drop table if exists """ + etlSchemaName + """.tmp_cteDoseEndDates_rawdata cascade"""
    dropQuery5 = """drop table if exists """ + etlSchemaName + """.tmp_cteDoseEndDates_e cascade"""
    dropQuery6 = """drop table if exists """ + etlSchemaName + """.tmp_cteDoseEndDates cascade"""
    dropQuery7 = """drop table if exists """ + etlSchemaName + """.tmp_cteDoseFinalEnds cascade"""

    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery1)
            cursor.execute(dropQuery2)
            cursor.execute(dropQuery3)
            cursor.execute(dropQuery4)
            cursor.execute(dropQuery5)
            cursor.execute(dropQuery6)
            cursor.execute(dropQuery7)


def migrateLookup(con, etlSchemaName):
    createPrescriptionsClean(con = con, etlSchemaName = etlSchemaName)
    createPrNdcConcept(con = con, etlSchemaName = etlSchemaName)
    createPrGcptConcept(con = con, etlSchemaName = etlSchemaName)
    createPrRouteConcept(con = con, etlSchemaName = etlSchemaName)
    createDrugMapped(con = con, etlSchemaName = etlSchemaName)


def migrate(con, etlSchemaName):
    createDrugExposure(con = con, etlSchemaName = etlSchemaName)


def migrateDrugEra(con, etlSchemaName):
    createJoinVocDrug(con = con, etlSchemaName = etlSchemaName)
    createTempPretargetDrug(con = con, etlSchemaName = etlSchemaName)
    createTempSubenddatesUnDrug(con = con, etlSchemaName = etlSchemaName)
    createTempSubenddatesRowsDrug(con = con, etlSchemaName = etlSchemaName)
    createTempSubenddatesDrug(con = con, etlSchemaName = etlSchemaName)
    createTempEndsDrug(con = con, etlSchemaName = etlSchemaName)
    createTempSubDrug(con = con, etlSchemaName = etlSchemaName)
    createTempFinaltargetDrug(con = con, etlSchemaName = etlSchemaName)
    createTempEnddatesUnDrug(con = con, etlSchemaName = etlSchemaName)
    createTempEnddatesRowsDrug(con = con, etlSchemaName = etlSchemaName)
    createTempEnddatesDrug(con = con, etlSchemaName = etlSchemaName)
    createTempDrugeraEndsDrug(con = con, etlSchemaName = etlSchemaName)
    createDrugEra(con = con, etlSchemaName = etlSchemaName)
    dropTempTables1(con = con, etlSchemaName = etlSchemaName)


def migrateDoseEra(con, etlSchemaName):
    createDoseEra(con = con, etlSchemaName = etlSchemaName)
    createTempDrugIngredientExp(con = con, etlSchemaName = etlSchemaName)
    createTempDrugWithDose(con = con, etlSchemaName = etlSchemaName)
    createTempCteDoseTarget(con = con, etlSchemaName = etlSchemaName)
    createTempCteDoseEndDatesRawdata(con = con, etlSchemaName = etlSchemaName)
    createTempCteDoseEndDatesE(con = con, etlSchemaName = etlSchemaName)
    createTempCteDoseEndDates(con = con, etlSchemaName = etlSchemaName)
    createTempCteDoseFinalEnds(con = con, etlSchemaName = etlSchemaName)
    insertDoseEra(con = con, etlSchemaName = etlSchemaName)
    dropTempTables2(con = con, etlSchemaName = etlSchemaName)
