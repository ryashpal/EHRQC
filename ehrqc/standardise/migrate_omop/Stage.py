import logging

log = logging.getLogger("Standardise")


def createPatientsStaging(con, sourceSchemaName, destinationSchemaName):
    log.info("Creating staging table: " + destinationSchemaName + ".src_patients")
    dropQuery = """drop table if exists """ + destinationSchemaName + """.src_patients cascade"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.src_patients AS
        SELECT
            subject_id                          AS subject_id,
            anchor_year                         AS anchor_year,
            anchor_age                          AS anchor_age,
            anchor_year_group                   AS anchor_year_group,
            gender                              AS gender,
            'patients'                          AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
            jsonb_build_object('subject_id',subject_id)                                  AS trace_id
        FROM
            """ + sourceSchemaName + """.patients
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createAdmissionsStaging(con, sourceSchemaName, destinationSchemaName):
    log.info("Creating staging table: " + destinationSchemaName + ".src_admissions")
    dropQuery = """drop table if exists """ + destinationSchemaName + """.src_admissions cascade"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.src_admissions AS
        SELECT
            hadm_id                             AS hadm_id, -- PK
            subject_id                          AS subject_id,
            admittime                           AS admittime,
            dischtime                           AS dischtime,
            deathtime                           AS deathtime,
            admission_type                      AS admission_type,
            admission_location                  AS admission_location,
            discharge_location                  AS discharge_location,
            ethnicity                           AS ethnicity,
            edregtime                           AS edregtime,
            insurance                           AS insurance,
            marital_status                      AS marital_status,
            language                            AS language,
            'admissions'                        AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
            jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id)                                  AS trace_id
        FROM
            """ + sourceSchemaName + """.admissions
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTransfersStaging(con, sourceSchemaName, destinationSchemaName):
    log.info("Creating staging table: " + destinationSchemaName + ".src_transfers")
    dropQuery = """drop table if exists """ + destinationSchemaName + """.src_transfers cascade"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.src_transfers AS
        SELECT
            transfer_id                         AS transfer_id,
            hadm_id                             AS hadm_id,
            subject_id                          AS subject_id,
            careunit                            AS careunit,
            intime                              AS intime,
            outtime                             AS outtime,
            eventtype                           AS eventtype,
            'transfers'                         AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
            jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id, 'transfer_id',  transfer_id)                                 AS trace_id
        FROM
            """ + sourceSchemaName + """.transfers
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createDiagnosesStaging(con, sourceSchemaName, destinationSchemaName):
    log.info("Creating staging table: " + destinationSchemaName + ".src_diagnoses_icd")
    dropQuery = """drop table if exists """ + destinationSchemaName + """.src_diagnoses_icd cascade"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.src_diagnoses_icd AS
        SELECT
            subject_id      AS subject_id,
            hadm_id         AS hadm_id,
            seq_num         AS seq_num,
            icd_code        AS icd_code,
            icd_version     AS icd_version,
            'diagnoses_icd'                     AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
            jsonb_build_object('hadm_id',hadm_id,'seq_num',seq_num)                                 AS trace_id
        FROM
            """ + sourceSchemaName + """.diagnoses_icd
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createServicesStaging(con, sourceSchemaName, destinationSchemaName):
    log.info("Creating staging table: " + destinationSchemaName + ".src_services")
    dropQuery = """drop table if exists """ + destinationSchemaName + """.src_services cascade"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.src_services AS
        SELECT
            subject_id                          AS subject_id,
            hadm_id                             AS hadm_id,
            transfertime                        AS transfertime,
            prev_service                        AS prev_service,
            curr_service                        AS curr_service,
            'services'                          AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
            jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id, 'transfertime',  transfertime)                                 AS trace_id
        FROM
            """ + sourceSchemaName + """.services
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createLabEventsStaging(con, sourceSchemaName, destinationSchemaName):
    log.info("Creating staging table: " + destinationSchemaName + ".src_labevents")
    dropQuery = """drop table if exists """ + destinationSchemaName + """.src_labevents cascade"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.src_labevents AS
        SELECT
            labevent_id                         AS labevent_id,
            subject_id                          AS subject_id,
            charttime                           AS charttime,
            hadm_id                             AS hadm_id,
            itemid                              AS itemid,
            valueuom                            AS valueuom,
            value                               AS value,
            flag                                AS flag,
            ref_range_lower                     AS ref_range_lower,
            ref_range_upper                     AS ref_range_upper,
            'labevents'                         AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
            jsonb_build_object('labevent_id',labevent_id)                                 AS trace_id
        FROM
            """ + sourceSchemaName + """.labevents
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createLabItemsStaging(con, sourceSchemaName, destinationSchemaName):
    log.info("Creating staging table: " + destinationSchemaName + ".src_d_labitems")
    dropQuery = """drop table if exists """ + destinationSchemaName + """.src_d_labitems cascade"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.src_d_labitems AS
        SELECT
            itemid                              AS itemid,
            label                               AS label,
            fluid                               AS fluid,
            category                            AS category,
            loinc_code                          AS loinc_code,
            'd_labitems'                        AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
            jsonb_build_object('itemid',itemid)                                 AS trace_id
        FROM
            """ + sourceSchemaName + """.d_labitems
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createProceduresStaging(con, sourceSchemaName, destinationSchemaName):
    log.info("Creating staging table: " + destinationSchemaName + ".src_procedures_icd")
    dropQuery = """drop table if exists """ + destinationSchemaName + """.src_procedures_icd cascade"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.src_procedures_icd AS
        SELECT
            subject_id                          AS subject_id,
            hadm_id                             AS hadm_id,
            icd_code        AS icd_code,
            icd_version     AS icd_version,
            'procedures_icd'                    AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
            jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id, 'icd_code',  icd_code, 'icd_version',icd_version )                                 AS trace_id -- this set of fields is not unique. To set quantity?
        FROM
            """ + sourceSchemaName + """.procedures_icd
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createHcpcsEventsStaging(con, sourceSchemaName, destinationSchemaName):
    log.info("Creating staging table: " + destinationSchemaName + ".src_hcpcsevents")
    dropQuery = """drop table if exists """ + destinationSchemaName + """.src_hcpcsevents cascade"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.src_hcpcsevents AS
        SELECT
            hadm_id                             AS hadm_id,
            subject_id                          AS subject_id,
            hcpcs_cd                            AS hcpcs_cd,
            seq_num                             AS seq_num,
            short_description                   AS short_description,
            'hcpcsevents'                       AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
            jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id, 'hcpcs_cd',  hcpcs_cd, 'seq_num',seq_num)                                 AS trace_id -- this set of fields is not unique. To set quantity?
        FROM
            """ + sourceSchemaName + """.hcpcsevents
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createDrugCodesStaging(con, sourceSchemaName, destinationSchemaName):
    log.info("Creating staging table: " + destinationSchemaName + ".src_drgcodes")
    dropQuery = """drop table if exists """ + destinationSchemaName + """.src_drgcodes cascade"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.src_drgcodes AS
        SELECT
            hadm_id                             AS hadm_id,
            subject_id                          AS subject_id,
            drg_code                            AS drg_code,
            description                         AS description,
            'drgcodes'                       AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
            jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id, 'drg_code',  COALESCE(drg_code, null))  AS trace_id                             
        FROM
            """ + sourceSchemaName + """.drgcodes
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createPrescriptionsStaging(con, sourceSchemaName, destinationSchemaName):
    log.info("Creating staging table: " + destinationSchemaName + ".src_prescriptions")
    dropQuery = """drop table if exists """ + destinationSchemaName + """.src_prescriptions cascade"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.src_prescriptions AS
        SELECT
            hadm_id                             AS hadm_id,
            subject_id                          AS subject_id,
            pharmacy_id                         AS pharmacy_id,
            starttime                           AS starttime,
            stoptime                            AS stoptime,
            drug_type                           AS drug_type,
            drug                                AS drug,
            gsn                                 AS gsn,
            ndc                                 AS ndc,
            prod_strength                       AS prod_strength,
            form_rx                             AS form_rx,
            dose_val_rx                         AS dose_val_rx,
            dose_unit_rx                        AS dose_unit_rx,
            form_val_disp                       AS form_val_disp,
            form_unit_disp                      AS form_unit_disp,
            doses_per_24_hrs                    AS doses_per_24_hrs,
            route                               AS route,
            'prescriptions'                     AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
            jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id, 'pharmacy_id',  pharmacy_id, 'starttime', starttime)                                 AS trace_id
        FROM
            """ + sourceSchemaName + """.prescriptions
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMicrobiologyEventsStaging(con, sourceSchemaName, destinationSchemaName):
    log.info("Creating staging table: " + destinationSchemaName + ".src_microbiologyevents")
    dropQuery = """drop table if exists """ + destinationSchemaName + """.src_microbiologyevents cascade"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.src_microbiologyevents AS
        SELECT
            microevent_id               AS microevent_id,
            subject_id                  AS subject_id,
            hadm_id                     AS hadm_id,
            chartdate                   AS chartdate,
            charttime                   AS charttime, -- usage: COALESCE(charttime, chartdate)
            spec_itemid                 AS spec_itemid, -- d_micro, type of specimen taken. If no grouth, then all other fields is null
            spec_type_desc              AS spec_type_desc, -- for reference
            test_itemid                 AS test_itemid, -- d_micro, what test is taken, goes to measurement
            test_name                   AS test_name, -- for reference
            org_itemid                  AS org_itemid, -- d_micro, what bacteria have grown
            org_name                    AS org_name, -- for reference
            ab_itemid                   AS ab_itemid, -- d_micro, antibiotic tested on the bacteria
            ab_name                     AS ab_name, -- for reference
            dilution_comparison         AS dilution_comparison, -- operator sign
            dilution_value              AS dilution_value, -- numeric value
            interpretation              AS interpretation, -- bacteria's degree of resistance to the antibiotic
            'microbiologyevents'                AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
            jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id, 'microevent_id', microevent_id)                                  AS trace_id
        FROM
            """ + sourceSchemaName + """.microbiologyevents
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createMicrobiologyItemsStaging(con, sourceSchemaName, destinationSchemaName):
    log.info("Creating staging table: " + destinationSchemaName + ".src_d_micro")
    dropQuery = """drop table if exists """ + destinationSchemaName + """.src_d_micro cascade"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.src_d_micro(
            itemid						INTEGER,
            label						CHARACTER VARYING(100),
            category					CHARACTER VARYING(50),
            load_table_id				TEXT,
            load_row_id					INTEGER,
            trace_id					TEXT
        )
        ;
        """
    insertSpecimenQuery = """INSERT INTO """ + destinationSchemaName + """.src_d_micro
        SELECT 
            spec_itemid as itemid,
            spec_type_desc as label,
            'SPECIMEN' as category,
            'd_micro'                   AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int  AS load_row_id,
            jsonb_build_object('itemid', spec_itemid)                   AS trace_id

        FROM 
            """ + destinationSchemaName + """.src_microbiologyevents
        GROUP BY
        itemid,
        label
        ;
        """
    insertOrganismQuery = """INSERT INTO """ + destinationSchemaName + """.src_d_micro
        SELECT 
            org_itemid as itemid,
            org_name as label,
            'ORGANISM' as category,
            'd_micro'                   AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int  AS load_row_id,
            jsonb_build_object('itemid', org_itemid)                   AS trace_id

        FROM 
            """ + destinationSchemaName + """.src_microbiologyevents
        GROUP BY
        itemid,
        label
        ;
        """
    insertAntibioticQuery = """INSERT INTO """ + destinationSchemaName + """.src_d_micro
        SELECT 
            ab_itemid as itemid,
            ab_name as label,
            'ANTIBIOTIC' as category,
            'd_micro'                   AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int  AS load_row_id,
            jsonb_build_object('itemid', ab_itemid)                   AS trace_id

        FROM 
            """ + destinationSchemaName + """.src_microbiologyevents
        GROUP BY
        itemid,
        label
        ;
        """
    deleteQuery = """delete from """ + destinationSchemaName + """.src_d_micro
        where not (itemid is not null);
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertSpecimenQuery)
            cursor.execute(insertOrganismQuery)
            cursor.execute(insertAntibioticQuery)
            cursor.execute(deleteQuery)


def createPharmacyStaging(con, sourceSchemaName, destinationSchemaName):
    log.info("Creating staging table: " + destinationSchemaName + ".src_pharmacy")
    dropQuery = """drop table if exists """ + destinationSchemaName + """.src_pharmacy cascade"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.src_pharmacy AS
        SELECT
            pharmacy_id                         AS pharmacy_id,
            medication                          AS medication,
            'pharmacy'                          AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
            jsonb_build_object('pharmacy_id',pharmacy_id)                                 AS trace_id
        FROM
            """ + sourceSchemaName + """.pharmacy
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createProcedureEventsStaging(con, sourceSchemaName, destinationSchemaName):
    log.info("Creating staging table: " + destinationSchemaName + ".src_procedureevents")
    dropQuery = """drop table if exists """ + destinationSchemaName + """.src_procedureevents cascade"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.src_procedureevents AS
        SELECT
            hadm_id                             AS hadm_id,
            subject_id                          AS subject_id,
            stay_id                             AS stay_id,
            itemid                              AS itemid,
            starttime                           AS starttime,
            value                               AS value,
            cancelreason                        AS cancelreason,
            'procedureevents'                   AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
            jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id, 'starttime', starttime)                                 AS trace_id
        FROM
            """ + sourceSchemaName + """.procedureevents
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createItemsStaging(con, sourceSchemaName, destinationSchemaName):
    log.info("Creating staging table: " + destinationSchemaName + ".src_d_items")
    dropQuery = """drop table if exists """ + destinationSchemaName + """.src_d_items cascade"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.src_d_items AS
        SELECT
            itemid                              AS itemid,
            label                               AS label,
            linksto                             AS linksto,
            'd_items'                           AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
            jsonb_build_object('itemid', itemid, 'linksto', linksto)                                 AS trace_id
        FROM
            """ + sourceSchemaName + """.d_items
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createDatetimeEventsStaging(con, sourceSchemaName, destinationSchemaName):
    log.info("Creating staging table: " + destinationSchemaName + ".src_datetimeevents")
    dropQuery = """drop table if exists """ + destinationSchemaName + """.src_datetimeevents cascade"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.src_datetimeevents AS
        SELECT
            subject_id  AS subject_id,
            hadm_id     AS hadm_id,
            stay_id     AS stay_id,
            itemid      AS itemid,
            charttime   AS charttime,
            value       AS value,
            'datetimeevents'                    AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
            jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id, 'stay_id',  stay_id, 'charttime', charttime)                                 AS trace_id
        FROM
            """ + sourceSchemaName + """.datetimeevents
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createChartEventsStaging(con, sourceSchemaName, destinationSchemaName):
    log.info("Creating staging table: " + destinationSchemaName + ".src_chartevents")
    dropQuery = """drop table if exists """ + destinationSchemaName + """.src_chartevents cascade"""
    createQuery = """CREATE TABLE """ + destinationSchemaName + """.src_chartevents AS
        SELECT
            subject_id  AS subject_id,
            hadm_id     AS hadm_id,
            stay_id     AS stay_id,
            itemid      AS itemid,
            charttime   AS charttime,
            value       AS value,
            valuenum    AS valuenum,
            valueuom    AS valueuom,
            'chartevents'                       AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int     AS load_row_id,
            jsonb_build_object('subject_id',subject_id, 'hadm_id',hadm_id, 'stay_id',  stay_id, 'charttime', charttime)                                 AS trace_id
        FROM
            """ + sourceSchemaName + """.chartevents
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def migrate(con, sourceSchemaName, destinationSchemaName):
    createPatientsStaging(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    createAdmissionsStaging(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    createTransfersStaging(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    createDiagnosesStaging(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    createServicesStaging(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    createLabEventsStaging(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    createLabItemsStaging(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    createProceduresStaging(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    createHcpcsEventsStaging(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    createDrugCodesStaging(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    createPrescriptionsStaging(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    createMicrobiologyEventsStaging(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    createMicrobiologyItemsStaging(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    createPharmacyStaging(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    createProcedureEventsStaging(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    createItemsStaging(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    createDatetimeEventsStaging(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
    createChartEventsStaging(con=con, sourceSchemaName=sourceSchemaName, destinationSchemaName=destinationSchemaName)
