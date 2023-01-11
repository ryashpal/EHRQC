import logging

log = logging.getLogger("Standardise")

from ehrqc import Config


def __saveDataframe(con, destinationSchemaName, destinationTableName, df, dfColumns):

    import numpy as np
    import psycopg2.extras
    import psycopg2.extensions

    psycopg2.extensions.register_adapter(np.int64, psycopg2._psycopg.AsIs)

    log.info("Importing data to table: " + destinationSchemaName + '.' + destinationTableName)

    if len(df) > 0:
        table = destinationSchemaName + '.' + destinationTableName
        columns = '"' + '", "'.join(dfColumns) + '"'
        values = "VALUES({})".format(",".join(["%s" for _ in dfColumns]))
        insert_stmt = "INSERT INTO {} ({}) {}".format(table, columns, values)
        try:
            cur = con.cursor()
            psycopg2.extras.execute_batch(cur, insert_stmt, df[dfColumns].values)
            con.commit()
        finally:
            cur.close()


def importPatients(con, sourceSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + sourceSchemaName + ".PATIENTS")

    dropQuery = """DROP TABLE IF EXISTS """ + sourceSchemaName + """.PATIENTS CASCADE"""
    createQuery = """CREATE TABLE """ + sourceSchemaName + """.PATIENTS
        (
            SUBJECT_ID INT NOT NULL,
            GENDER VARCHAR(5) NOT NULL,
            ANCHOR_AGE INT NOT NULL,
            ANCHOR_YEAR INT NOT NULL,
            ANCHOR_YEAR_GROUP VARCHAR(12) NOT NULL,
            DOD TIMESTAMP(0), -- This is a NaN column

            CONSTRAINT pat_subid_unique UNIQUE (SUBJECT_ID),
            CONSTRAINT pat_subid_pk PRIMARY KEY (SUBJECT_ID)
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    df.dod.replace({np.nan: None}, inplace=True)
    dfColumns = [
        Config.patients['column_mapping']['subject_id'],
        Config.patients['column_mapping']['gender'],
        Config.patients['column_mapping']['anchor_age'],
        Config.patients['column_mapping']['anchor_year'],
        Config.patients['column_mapping']['anchor_year_group'],
        Config.patients['column_mapping']['dod'],
        ]
    __saveDataframe(con=con, destinationSchemaName=sourceSchemaName, destinationTableName='PATIENTS', df=df, dfColumns=dfColumns)


def importAdmissions(con, sourceSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + sourceSchemaName + ".ADMISSIONS")

    dropQuery = """DROP TABLE IF EXISTS """ + sourceSchemaName + """.ADMISSIONS CASCADE"""
    createQuery = """CREATE TABLE """ + sourceSchemaName + """.ADMISSIONS
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT NOT NULL,
            ADMITTIME TIMESTAMP(0) NOT NULL,
            DISCHTIME TIMESTAMP(0) NOT NULL,
            DEATHTIME TIMESTAMP(0),
            ADMISSION_TYPE VARCHAR(50) NOT NULL,
            ADMISSION_LOCATION VARCHAR(50), -- There is NULL in this version
            DISCHARGE_LOCATION VARCHAR(50), -- There is NULL in this version
            INSURANCE VARCHAR(255) NOT NULL,
            LANGUAGE VARCHAR(10),
            MARITAL_STATUS VARCHAR(50),
            ETHNICITY VARCHAR(200) NOT NULL,
            EDREGTIME TIMESTAMP(0),
            EDOUTTIME TIMESTAMP(0),
            HOSPITAL_EXPIRE_FLAG SMALLINT,
            CONSTRAINT adm_hadm_pk PRIMARY KEY (HADM_ID),
            CONSTRAINT adm_hadm_unique UNIQUE (HADM_ID)

        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    df.deathtime.replace({np.nan: None}, inplace=True)
    df.edregtime.replace({np.nan: None}, inplace=True)
    df.edouttime.replace({np.nan: None}, inplace=True)
    dfColumns = [
        Config.admissions['column_mapping']['subject_id'],
        Config.admissions['column_mapping']['hadm_id'],
        Config.admissions['column_mapping']['admittime'],
        Config.admissions['column_mapping']['dischtime'],
        Config.admissions['column_mapping']['deathtime'],
        Config.admissions['column_mapping']['admission_type'],
        Config.admissions['column_mapping']['admission_location'],
        Config.admissions['column_mapping']['discharge_location'],
        Config.admissions['column_mapping']['insurance'],
        Config.admissions['column_mapping']['language'],
        Config.admissions['column_mapping']['marital_status'],
        Config.admissions['column_mapping']['ethnicity'],
        Config.admissions['column_mapping']['edregtime'],
        Config.admissions['column_mapping']['edouttime'],
        Config.admissions['column_mapping']['hospital_expire_flag'],
        ]
    __saveDataframe(con=con, destinationSchemaName=sourceSchemaName, destinationTableName='ADMISSIONS', df=df, dfColumns=dfColumns)


def importTransfers(con, sourceSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + sourceSchemaName + ".TRANSFERS")

    dropQuery = """DROP TABLE IF EXISTS """ + sourceSchemaName + """.TRANSFERS CASCADE"""
    createQuery = """CREATE TABLE """ + sourceSchemaName + """.TRANSFERS
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT,
            TRANSFER_ID INT NOT NULL,
            EVENTTYPE VARCHAR(20) NOT NULL,
            CAREUNIT VARCHAR(50),
            INTIME TIMESTAMP(0),
            OUTTIME TIMESTAMP(0),
            CONSTRAINT transfers_subid_transid_pk PRIMARY KEY (SUBJECT_ID, TRANSFER_ID)
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    df.intime.replace({np.nan: None}, inplace=True)
    df.outtime.replace({np.nan: None}, inplace=True)
    dfColumns = [
        Config.transfers['column_mapping']['subject_id'],
        Config.transfers['column_mapping']['hadm_id'],
        Config.transfers['column_mapping']['transfer_id'],
        Config.transfers['column_mapping']['eventtype'],
        Config.transfers['column_mapping']['careunit'],
        Config.transfers['column_mapping']['intime'],
        Config.transfers['column_mapping']['outtime'],
        ]
    df['hadm_id'] = df['hadm_id'].astype('Int64').fillna(0).astype('int').replace({0: None})
    __saveDataframe(con=con, destinationSchemaName=sourceSchemaName, destinationTableName='TRANSFERS', df=df, dfColumns=dfColumns)


def importDiagnosesIcd(con, sourceSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + sourceSchemaName + ".DIAGNOSES_ICD")

    dropQuery = """DROP TABLE IF EXISTS """ + sourceSchemaName + """.DIAGNOSES_ICD CASCADE"""
    createQuery = """CREATE TABLE """ + sourceSchemaName + """.DIAGNOSES_ICD
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT NOT NULL,
            SEQ_NUM INT NOT NULL,
            ICD_CODE VARCHAR(10) NOT NULL,
            ICD_VERSION INT NOT NULL
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    dfColumns = [
        Config.diagnoses_icd['column_mapping']['subject_id'],
        Config.diagnoses_icd['column_mapping']['hadm_id'],
        Config.diagnoses_icd['column_mapping']['seq_num'],
        Config.diagnoses_icd['column_mapping']['icd_code'],
        Config.diagnoses_icd['column_mapping']['icd_version'],
        ]
    __saveDataframe(con=con, destinationSchemaName=sourceSchemaName, destinationTableName='DIAGNOSES_ICD', df=df, dfColumns=dfColumns)


def importServices(con, sourceSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + sourceSchemaName + ".SERVICES")

    dropQuery = """DROP TABLE IF EXISTS """ + sourceSchemaName + """.SERVICES CASCADE"""
    createQuery = """CREATE TABLE """ + sourceSchemaName + """.SERVICES
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT NOT NULL,
            TRANSFERTIME TIMESTAMP(0) NOT NULL,
            PREV_SERVICE VARCHAR(20) ,
            CURR_SERVICE VARCHAR(20) NOT NULL
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    df.transfertime.replace({np.nan: None}, inplace=True)
    dfColumns = [
        Config.services['column_mapping']['subject_id'],
        Config.services['column_mapping']['hadm_id'],
        Config.services['column_mapping']['transfertime'],
        Config.services['column_mapping']['prev_service'],
        Config.services['column_mapping']['curr_service'],
        ]
    __saveDataframe(con=con, destinationSchemaName=sourceSchemaName, destinationTableName='SERVICES', df=df, dfColumns=dfColumns)


def importLabEvents(con, sourceSchemaName, filePath, fileSeparator, createSchema=True):

    log.info("Creating table: " + sourceSchemaName + ".LABEVENTS")

    dropQuery = """DROP TABLE IF EXISTS """ + sourceSchemaName + """.LABEVENTS CASCADE"""
    createQuery = """CREATE TABLE """ + sourceSchemaName + """.LABEVENTS
        (
            LABEVENT_ID INT NOT NULL,
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT ,
            SPECIMEN_ID INT NOT NULL,
            ITEMID INT NOT NULL,
            CHARTTIME TIMESTAMP NOT NULL,
            STORETIME TIMESTAMP ,
            VALUE VARCHAR(200) ,
            VALUENUM DOUBLE PRECISION ,
            VALUEUOM VARCHAR(20) ,
            REF_RANGE_LOWER DOUBLE PRECISION ,
            REF_RANGE_UPPER  DOUBLE PRECISION,
            FLAG VARCHAR(10) ,
            PRIORITY VARCHAR(7) ,
            COMMENTS VARCHAR(620) ,
            CONSTRAINT labevents_labeventid_pk PRIMARY KEY (LABEVENT_ID)
        )
        ;
        """
    if createSchema:
        with con:
            with con.cursor() as cursor:
                cursor.execute(dropQuery)
                cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    df['hadm_id'] = df['hadm_id'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['specimen_id'] = df['specimen_id'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['itemid'] = df['itemid'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df.charttime.replace({np.nan: None}, inplace=True)
    df.storetime.replace({np.nan: None}, inplace=True)

    dfColumns = [
        Config.labevents['column_mapping']['labevent_id'],
        Config.labevents['column_mapping']['subject_id'],
        Config.labevents['column_mapping']['hadm_id'],
        Config.labevents['column_mapping']['specimen_id'],
        Config.labevents['column_mapping']['itemid'],
        Config.labevents['column_mapping']['charttime'],
        Config.labevents['column_mapping']['storetime'],
        Config.labevents['column_mapping']['value'],
        Config.labevents['column_mapping']['valuenum'],
        Config.labevents['column_mapping']['valueuom'],
        Config.labevents['column_mapping']['ref_range_lower'],
        Config.labevents['column_mapping']['ref_range_upper'],
        Config.labevents['column_mapping']['flag'],
        Config.labevents['column_mapping']['priority'],
        Config.labevents['column_mapping']['comments'],
        ]
    __saveDataframe(con=con, destinationSchemaName=sourceSchemaName, destinationTableName='LABEVENTS', df=df, dfColumns=dfColumns)


def importLabItems(con, sourceSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + sourceSchemaName + ".D_LABITEMS")

    dropQuery = """DROP TABLE IF EXISTS """ + sourceSchemaName + """.D_LABITEMS CASCADE"""
    createQuery = """CREATE TABLE """ + sourceSchemaName + """.D_LABITEMS
        (
            ITEMID INT NOT NULL,
            LABEL VARCHAR(50),
            FLUID VARCHAR(50) NOT NULL,
            CATEGORY VARCHAR(50) NOT NULL,
            LOINC_CODE VARCHAR(50),
            CONSTRAINT d_labitems_itemid_pk PRIMARY KEY (ITEMID)
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep=fileSeparator)
    dfColumns = [
        Config.d_labitems['column_mapping']['itemid'],
        Config.d_labitems['column_mapping']['label'],
        Config.d_labitems['column_mapping']['fluid'],
        Config.d_labitems['column_mapping']['category'],
        Config.d_labitems['column_mapping']['loinc_code'],
        ]
    __saveDataframe(con=con, destinationSchemaName=sourceSchemaName, destinationTableName='D_LABITEMS', df=df, dfColumns=dfColumns)


def importProcedures(con, sourceSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + sourceSchemaName + ".PROCEDURES_ICD")

    dropQuery = """DROP TABLE IF EXISTS """ + sourceSchemaName + """.PROCEDURES_ICD CASCADE"""
    createQuery = """CREATE TABLE """ + sourceSchemaName + """.PROCEDURES_ICD
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT NOT NULL,
            SEQ_NUM INT NOT NULL,
            CHARTDATE TIMESTAMP(0) NOT NULL,
            ICD_CODE VARCHAR(10) NOT NULL,
            ICD_VERSION INT NOT NULL
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep=fileSeparator)
    dfColumns = [
        Config.procedures_icd['column_mapping']['subject_id'],
        Config.procedures_icd['column_mapping']['hadm_id'],
        Config.procedures_icd['column_mapping']['seq_num'],
        Config.procedures_icd['column_mapping']['chartdate'],
        Config.procedures_icd['column_mapping']['icd_code'],
        Config.procedures_icd['column_mapping']['icd_version'],
        ]
    __saveDataframe(con=con, destinationSchemaName=sourceSchemaName, destinationTableName='PROCEDURES_ICD', df=df, dfColumns=dfColumns)


def importHcpcsEvents(con, sourceSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + sourceSchemaName + ".HCPCSEVENTS")

    dropQuery = """DROP TABLE IF EXISTS """ + sourceSchemaName + """.HCPCSEVENTS CASCADE"""
    createQuery = """CREATE TABLE """ + sourceSchemaName + """.HCPCSEVENTS
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT NOT NULL,
            CHARTDATE TIMESTAMP(0) NOT NULL, -- new for 1.0
            HCPCS_CD VARCHAR(5) NOT NULL,
            SEQ_NUM INT NOT NULL,
            SHORT_DESCRIPTION VARCHAR(170) NOT NULL
            -- longest is 165
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep=fileSeparator)
    dfColumns = [
        Config.hcpcsevents['column_mapping']['subject_id'],
        Config.hcpcsevents['column_mapping']['hadm_id'],
        Config.hcpcsevents['column_mapping']['chartdate'],
        Config.hcpcsevents['column_mapping']['hcpcs_cd'],
        Config.hcpcsevents['column_mapping']['seq_num'],
        Config.hcpcsevents['column_mapping']['short_description'],
        ]
    __saveDataframe(con=con, destinationSchemaName=sourceSchemaName, destinationTableName='HCPCSEVENTS', df=df, dfColumns=dfColumns)


def importDrugCodes(con, sourceSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + sourceSchemaName + ".DRGCODES")

    dropQuery = """DROP TABLE IF EXISTS """ + sourceSchemaName + """.DRGCODES CASCADE"""
    createQuery = """CREATE TABLE  """ + sourceSchemaName + """.DRGCODES
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT NOT NULL,
            DRG_TYPE VARCHAR(4) NOT NULL,
            DRG_CODE VARCHAR(10) NOT NULL,
            DESCRIPTION VARCHAR(195) ,
            DRG_SEVERITY SMALLINT,
            DRG_MORTALITY SMALLINT
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep=fileSeparator)
    df['drg_severity'] = df['drg_severity'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['drg_mortality'] = df['drg_mortality'].astype('Int64').fillna(0).astype('int').replace({0: None})
    dfColumns = [
        Config.drgcodes['column_mapping']['subject_id'],
        Config.drgcodes['column_mapping']['hadm_id'],
        Config.drgcodes['column_mapping']['drg_type'],
        Config.drgcodes['column_mapping']['drg_code'],
        Config.drgcodes['column_mapping']['description'],
        Config.drgcodes['column_mapping']['drg_severity'],
        Config.drgcodes['column_mapping']['drg_mortality'],
        ]
    __saveDataframe(con=con, destinationSchemaName=sourceSchemaName, destinationTableName='DRGCODES', df=df, dfColumns=dfColumns)


def importPrescriptions(con, sourceSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + sourceSchemaName + ".PRESCRIPTIONS")

    dropQuery = """DROP TABLE IF EXISTS """ + sourceSchemaName + """.PRESCRIPTIONS CASCADE"""
    createQuery = """CREATE TABLE """ + sourceSchemaName + """.PRESCRIPTIONS
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT NOT NULL,
            PHARMACY_ID INT NOT NULL,
            STARTTIME TIMESTAMP(0) ,
            STOPTIME TIMESTAMP(0) ,
            DRUG_TYPE VARCHAR(10) NOT NULL,
            DRUG VARCHAR(100) ,
            GSN VARCHAR(250) , -- exceeds 10
            NDC VARCHAR(20) ,
            PROD_STRENGTH VARCHAR(120) ,
            FORM_RX  VARCHAR(10),
            DOSE_VAL_RX VARCHAR(50) ,
            DOSE_UNIT_RX VARCHAR(50) ,
            FORM_VAL_DISP VARCHAR(30) ,
            FORM_UNIT_DISP VARCHAR(30) ,
            DOSES_PER_24_HRS DOUBLE PRECISION ,
            ROUTE VARCHAR(30)

        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    df.stoptime.replace({np.nan: None}, inplace=True)
    df.starttime.replace({np.nan: None}, inplace=True)
    dfColumns = [
        Config.prescriptions['column_mapping']['subject_id'],
        Config.prescriptions['column_mapping']['hadm_id'],
        Config.prescriptions['column_mapping']['pharmacy_id'],
        Config.prescriptions['column_mapping']['starttime'],
        Config.prescriptions['column_mapping']['stoptime'],
        Config.prescriptions['column_mapping']['drug_type'],
        Config.prescriptions['column_mapping']['drug'],
        Config.prescriptions['column_mapping']['gsn'],
        Config.prescriptions['column_mapping']['ndc'],
        Config.prescriptions['column_mapping']['prod_strength'],
        Config.prescriptions['column_mapping']['form_rx'],
        Config.prescriptions['column_mapping']['dose_val_rx'],
        Config.prescriptions['column_mapping']['dose_unit_rx'],
        Config.prescriptions['column_mapping']['form_val_disp'],
        Config.prescriptions['column_mapping']['form_unit_disp'],
        Config.prescriptions['column_mapping']['doses_per_24_hrs'],
        Config.prescriptions['column_mapping']['route'],
        ]
    __saveDataframe(con=con, destinationSchemaName=sourceSchemaName, destinationTableName='PRESCRIPTIONS', df=df, dfColumns=dfColumns)


def importMicrobiologyEvents(con, sourceSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + sourceSchemaName + ".MICROBIOLOGYEVENTS")

    dropQuery = """DROP TABLE IF EXISTS """ + sourceSchemaName + """.MICROBIOLOGYEVENTS CASCADE"""
    createQuery = """CREATE TABLE """ + sourceSchemaName + """.MICROBIOLOGYEVENTS
        (
            MICROEVENT_ID INT NOT NULL,
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT ,
            MICRO_SPECIMEN_ID INT NOT NULL,
            CHARTDATE TIMESTAMP(0) NOT NULL,
            CHARTTIME TIMESTAMP(0) ,
            SPEC_ITEMID INT NOT NULL,
            SPEC_TYPE_DESC VARCHAR(100) NOT NULL,
            TEST_SEQ INT NOT NULL,
            STOREDATE TIMESTAMP(0) ,
            STORETIME TIMESTAMP(0) ,
            TEST_ITEMID INT NOT NULL,
            TEST_NAME VARCHAR(100) NOT NULL,
            ORG_ITEMID INT ,
            ORG_NAME VARCHAR(100) ,
            ISOLATE_NUM SMALLINT ,
            QUANTITY VARCHAR(50) ,
            AB_ITEMID INT ,
            AB_NAME VARCHAR(30) ,
            DILUTION_TEXT VARCHAR(10) ,
            DILUTION_COMPARISON VARCHAR(20) ,
            DILUTION_VALUE DOUBLE PRECISION ,
            INTERPRETATION VARCHAR(5) ,
            COMMENTS VARCHAR(750),
            CONSTRAINT mbe_microevent_id_pk PRIMARY KEY (MICROEVENT_ID)
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    df['hadm_id'] = df['hadm_id'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['org_itemid'] = df['org_itemid'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['ab_itemid'] = df['ab_itemid'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['isolate_num'] = df['isolate_num'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df.chartdate.replace({np.nan: None}, inplace=True)
    df.charttime.replace({np.nan: None}, inplace=True)
    df.storedate.replace({np.nan: None}, inplace=True)
    df.storetime.replace({np.nan: None}, inplace=True)
    dfColumns = [
        Config.microbiologyevents['column_mapping']['microevent_id'],
        Config.microbiologyevents['column_mapping']['subject_id'],
        Config.microbiologyevents['column_mapping']['hadm_id'],
        Config.microbiologyevents['column_mapping']['micro_specimen_id'],
        Config.microbiologyevents['column_mapping']['chartdate'],
        Config.microbiologyevents['column_mapping']['charttime'],
        Config.microbiologyevents['column_mapping']['spec_itemid'],
        Config.microbiologyevents['column_mapping']['spec_type_desc'],
        Config.microbiologyevents['column_mapping']['test_seq'],
        Config.microbiologyevents['column_mapping']['storedate'],
        Config.microbiologyevents['column_mapping']['storetime'],
        Config.microbiologyevents['column_mapping']['test_itemid'],
        Config.microbiologyevents['column_mapping']['test_name'],
        Config.microbiologyevents['column_mapping']['org_itemid'],
        Config.microbiologyevents['column_mapping']['org_name'],
        Config.microbiologyevents['column_mapping']['isolate_num'],
        Config.microbiologyevents['column_mapping']['quantity'],
        Config.microbiologyevents['column_mapping']['ab_itemid'],
        Config.microbiologyevents['column_mapping']['ab_name'],
        Config.microbiologyevents['column_mapping']['dilution_text'],
        Config.microbiologyevents['column_mapping']['dilution_comparison'],
        Config.microbiologyevents['column_mapping']['dilution_value'],
        Config.microbiologyevents['column_mapping']['interpretation'],
        Config.microbiologyevents['column_mapping']['comments'],
        ]
    __saveDataframe(con=con, destinationSchemaName=sourceSchemaName, destinationTableName='MICROBIOLOGYEVENTS', df=df, dfColumns=dfColumns)


def importMicrobiologyItems(con, destinationSchemaName, filePath, fileSeparator):
    pass


def importPharmacy(con, sourceSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + sourceSchemaName + ".PHARMACY")

    dropQuery = """DROP TABLE IF EXISTS """ + sourceSchemaName + """.PHARMACY CASCADE"""
    createQuery = """CREATE TABLE """ + sourceSchemaName + """.PHARMACY
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT NOT NULL,
            PHARMACY_ID INT NOT NULL,
            POE_ID VARCHAR(25) ,
            STARTTIME TIMESTAMP(0) ,
            STOPTIME TIMESTAMP(0) ,
            MEDICATION VARCHAR(100) ,
            PROC_TYPE VARCHAR(50) NOT NULL,
            STATUS VARCHAR(50) NOT NULL,
            ENTERTIME TIMESTAMP(0) NOT NULL,
            VERIFIEDTIME TIMESTAMP(0) ,
            ROUTE VARCHAR(30),
            FREQUENCY VARCHAR(30) ,
            DISP_SCHED VARCHAR(100) ,
            INFUSION_TYPE VARCHAR(15) ,
            SLIDING_SCALE VARCHAR(5) ,
            LOCKOUT_INTERVAL VARCHAR(50) ,
            BASAL_RATE  DOUBLE PRECISION,
            ONE_HR_MAX  VARCHAR(30), 
            DOSES_PER_24_HRS DOUBLE PRECISION ,
            DURATION DOUBLE PRECISION,
            DURATION_INTERVAL VARCHAR(50) ,
            EXPIRATION_VALUE INT ,
            EXPIRATION_UNIT VARCHAR(50) ,
            EXPIRATIONDATE TIMESTAMP(0) ,
            DISPENSATION VARCHAR(50) ,
            FILL_QUANTITY VARCHAR(30),
            CONSTRAINT pharmacy_pharmacy_pk PRIMARY KEY (PHARMACY_ID)
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    df['subject_id'] = df['subject_id'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['hadm_id'] = df['hadm_id'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['pharmacy_id'] = df['pharmacy_id'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['expiration_value'] = df['expiration_value'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df.starttime.replace({np.nan: None}, inplace=True)
    df.stoptime.replace({np.nan: None}, inplace=True)
    df.entertime.replace({np.nan: None}, inplace=True)
    df.verifiedtime.replace({np.nan: None}, inplace=True)
    df.expirationdate.replace({np.nan: None}, inplace=True)
    dfColumns = [
        Config.pharmacy['column_mapping']['subject_id'],
        Config.pharmacy['column_mapping']['hadm_id'],
        Config.pharmacy['column_mapping']['pharmacy_id'],
        Config.pharmacy['column_mapping']['poe_id'],
        Config.pharmacy['column_mapping']['starttime'],
        Config.pharmacy['column_mapping']['stoptime'],
        Config.pharmacy['column_mapping']['medication'],
        Config.pharmacy['column_mapping']['proc_type'],
        Config.pharmacy['column_mapping']['status'],
        Config.pharmacy['column_mapping']['entertime'],
        Config.pharmacy['column_mapping']['verifiedtime'],
        Config.pharmacy['column_mapping']['route'],
        Config.pharmacy['column_mapping']['frequency'],
        Config.pharmacy['column_mapping']['disp_sched'],
        Config.pharmacy['column_mapping']['infusion_type'],
        Config.pharmacy['column_mapping']['sliding_scale'],
        Config.pharmacy['column_mapping']['lockout_interval'],
        Config.pharmacy['column_mapping']['basal_rate'],
        Config.pharmacy['column_mapping']['one_hr_max'],
        Config.pharmacy['column_mapping']['doses_per_24_hrs'],
        Config.pharmacy['column_mapping']['duration'],
        Config.pharmacy['column_mapping']['duration_interval'],
        Config.pharmacy['column_mapping']['expiration_value'],
        Config.pharmacy['column_mapping']['expiration_unit'],
        Config.pharmacy['column_mapping']['expirationdate'],
        Config.pharmacy['column_mapping']['dispensation'],
        Config.pharmacy['column_mapping']['fill_quantity'],
        ]
    __saveDataframe(con=con, destinationSchemaName=sourceSchemaName, destinationTableName='PHARMACY', df=df, dfColumns=dfColumns)


def importProcedureEvents(con, sourceSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + sourceSchemaName + ".PROCEDUREEVENTS")

    dropQuery = """DROP TABLE IF EXISTS """ + sourceSchemaName + """.PROCEDUREEVENTS CASCADE"""
    createQuery = """CREATE TABLE """ + sourceSchemaName + """.PROCEDUREEVENTS
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT NOT NULL,
            STAY_ID INT NOT NULL,
            STARTTIME TIMESTAMP(0) NOT NULL,
            ENDTIME TIMESTAMP(0) NOT NULL,
            STORETIME TIMESTAMP(0) NOT NULL,
            ITEMID INT NOT NULL,
            VALUE DOUBLE PRECISION NOT NULL,
            VALUEUOM VARCHAR(30) NOT NULL,
            LOCATION VARCHAR(30),
            LOCATIONCATEGORY VARCHAR(30),
            ORDERID INT NOT NULL,
            LINKORDERID INT NOT NULL,
            ORDERCATEGORYNAME VARCHAR(100) NOT NULL,
            SECONDARYORDERCATEGORYNAME VARCHAR(100),
            ORDERCATEGORYDESCRIPTION VARCHAR(50) NOT NULL,
            PATIENTWEIGHT DOUBLE PRECISION NOT NULL,
            TOTALAMOUNT DOUBLE PRECISION,
            TOTALAMOUNTUOM VARCHAR(50),
            ISOPENBAG SMALLINT NOT NULL,
            CONTINUEINNEXTDEPT SMALLINT NOT NULL,
            CANCELREASON SMALLINT NOT NULL,
            STATUSDESCRIPTION VARCHAR(30) NOT NULL,
            COMMENTS_DATE TIMESTAMP(0),
            ORIGINALAMOUNT DOUBLE PRECISION NOT NULL,
            ORIGINALRATE DOUBLE PRECISION NOT NULL
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    df['hadm_id'] = df['hadm_id'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['stay_id'] = df['stay_id'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['itemid'] = df['itemid'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['orderid'] = df['orderid'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df['linkorderid'] = df['linkorderid'].astype('Int64').fillna(0).astype('int').replace({0: None})
    df.starttime.replace({np.nan: None}, inplace=True)
    df.endtime.replace({np.nan: None}, inplace=True)
    df.storetime.replace({np.nan: None}, inplace=True)
    df.comments_date.replace({np.nan: None}, inplace=True)
    dfColumns = [
        Config.procedureevents['column_mapping']['subject_id'],
        Config.procedureevents['column_mapping']['hadm_id'],
        Config.procedureevents['column_mapping']['stay_id'],
        Config.procedureevents['column_mapping']['starttime'],
        Config.procedureevents['column_mapping']['endtime'],
        Config.procedureevents['column_mapping']['storetime'],
        Config.procedureevents['column_mapping']['itemid'],
        Config.procedureevents['column_mapping']['value'],
        Config.procedureevents['column_mapping']['valueuom'],
        Config.procedureevents['column_mapping']['location'],
        Config.procedureevents['column_mapping']['locationcategory'],
        Config.procedureevents['column_mapping']['orderid'],
        Config.procedureevents['column_mapping']['linkorderid'],
        Config.procedureevents['column_mapping']['ordercategoryname'],
        Config.procedureevents['column_mapping']['secondaryordercategoryname'],
        Config.procedureevents['column_mapping']['ordercategorydescription'],
        Config.procedureevents['column_mapping']['patientweight'],
        Config.procedureevents['column_mapping']['totalamount'],
        Config.procedureevents['column_mapping']['totalamountuom'],
        Config.procedureevents['column_mapping']['isopenbag'],
        Config.procedureevents['column_mapping']['continueinnextdept'],
        Config.procedureevents['column_mapping']['cancelreason'],
        Config.procedureevents['column_mapping']['statusdescription'],
        Config.procedureevents['column_mapping']['comments_date'],
        Config.procedureevents['column_mapping']['originalamount'],
        Config.procedureevents['column_mapping']['originalrate'],
        ]
    __saveDataframe(con=con, destinationSchemaName=sourceSchemaName, destinationTableName='PROCEDUREEVENTS', df=df, dfColumns=dfColumns)


def importItems(con, sourceSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + sourceSchemaName + ".D_ITEMS")

    dropQuery = """DROP TABLE IF EXISTS """ + sourceSchemaName + """.D_ITEMS CASCADE"""
    createQuery = """CREATE TABLE """ + sourceSchemaName + """.D_ITEMS
        (
            ITEMID INT NOT NULL,
            LABEL VARCHAR(200) NOT NULL,
            ABBREVIATION VARCHAR(100) NOT NULL,
            LINKSTO VARCHAR(50) NOT NULL,
            CATEGORY VARCHAR(100) NOT NULL,
            UNITNAME VARCHAR(100),
            PARAM_TYPE VARCHAR(30) NOT NULL,
            LOWNORMALVALUE DOUBLE PRECISION,
            HIGHNORMALVALUE DOUBLE PRECISION,
            CONSTRAINT ditems_itemid_unique UNIQUE (ITEMID),
            CONSTRAINT ditems_itemid_pk PRIMARY KEY (ITEMID)
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd
    import numpy as np

    df = pd.read_csv(filePath, sep=fileSeparator)
    dfColumns = [
        Config.d_items['column_mapping']['itemid'],
        Config.d_items['column_mapping']['label'],
        Config.d_items['column_mapping']['abbreviation'],
        Config.d_items['column_mapping']['linksto'],
        Config.d_items['column_mapping']['category'],
        Config.d_items['column_mapping']['unitname'],
        Config.d_items['column_mapping']['param_type'],
        Config.d_items['column_mapping']['lownormalvalue'],
        Config.d_items['column_mapping']['highnormalvalue'],
        ]
    __saveDataframe(con=con, destinationSchemaName=sourceSchemaName, destinationTableName='D_ITEMS', df=df, dfColumns=dfColumns)


def importDatetimeEvents(con, sourceSchemaName, filePath, fileSeparator):

    log.info("Creating table: " + sourceSchemaName + ".DATETIMEEVENTS")

    dropQuery = """DROP TABLE IF EXISTS """ + sourceSchemaName + """.DATETIMEEVENTS CASCADE"""
    createQuery = """CREATE TABLE """ + sourceSchemaName + """.DATETIMEEVENTS
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT,
            STAY_ID INT,
            CHARTTIME TIMESTAMP(0) NOT NULL,
            STORETIME TIMESTAMP(0) NOT NULL,
            ITEMID INT NOT NULL,
            VALUE TIMESTAMP(0) NOT NULL,
            VALUEUOM VARCHAR(50) NOT NULL,
            WARNING SMALLINT NOT NULL
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep=fileSeparator)
    dfColumns = [
        Config.datetimeevents['column_mapping']['subject_id'],
        Config.datetimeevents['column_mapping']['hadm_id'],
        Config.datetimeevents['column_mapping']['stay_id'],
        Config.datetimeevents['column_mapping']['charttime'],
        Config.datetimeevents['column_mapping']['storetime'],
        Config.datetimeevents['column_mapping']['itemid'],
        Config.datetimeevents['column_mapping']['value'],
        Config.datetimeevents['column_mapping']['valueuom'],
        Config.datetimeevents['column_mapping']['warning'],
        ]
    __saveDataframe(con=con, destinationSchemaName=sourceSchemaName, destinationTableName='DATETIMEEVENTS', df=df, dfColumns=dfColumns)


def importChartEvents(con, sourceSchemaName, filePath, fileSeparator, createSchema=True):

    log.info("Creating table: " + sourceSchemaName + ".CHARTEVENTS")

    dropQuery = """DROP TABLE IF EXISTS """ + sourceSchemaName + """.CHARTEVENTS CASCADE"""
    createQuery = """CREATE TABLE """ + sourceSchemaName + """.CHARTEVENTS
        (
            SUBJECT_ID INT NOT NULL,
            HADM_ID INT NOT NULL,
            STAY_ID INT NOT NULL,
            CHARTTIME TIMESTAMP(0) NOT NULL,
            STORETIME TIMESTAMP(0) ,
            ITEMID INT NOT NULL,
            VALUE VARCHAR(160) ,
            VALUENUM DOUBLE PRECISION,
            VALUEUOM VARCHAR(20),
            WARNING SMALLINT NOT NULL
        )
        ;
        """
    createChildQuery1 = """CREATE TABLE """ + sourceSchemaName + """.CHARTEVENTS_1 ( CHECK ( itemid >= 220000 AND itemid < 221000 )) INHERITS (""" + sourceSchemaName + """.CHARTEVENTS);"""
    createChildQuery2 = """CREATE TABLE """ + sourceSchemaName + """.CHARTEVENTS_2 ( CHECK ( itemid >= 221000 AND itemid < 222000 )) INHERITS (""" + sourceSchemaName + """.CHARTEVENTS);"""
    createChildQuery3 = """CREATE TABLE """ + sourceSchemaName + """.CHARTEVENTS_3 ( CHECK ( itemid >= 222000 AND itemid < 223000 )) INHERITS (""" + sourceSchemaName + """.CHARTEVENTS);"""
    createChildQuery4 = """CREATE TABLE """ + sourceSchemaName + """.CHARTEVENTS_4 ( CHECK ( itemid >= 223000 AND itemid < 224000 )) INHERITS (""" + sourceSchemaName + """.CHARTEVENTS);"""
    createChildQuery5 = """CREATE TABLE """ + sourceSchemaName + """.CHARTEVENTS_5 ( CHECK ( itemid >= 224000 AND itemid < 225000 )) INHERITS (""" + sourceSchemaName + """.CHARTEVENTS);"""
    createChildQuery6 = """CREATE TABLE """ + sourceSchemaName + """.CHARTEVENTS_6 ( CHECK ( itemid >= 225000 AND itemid < 226000 )) INHERITS (""" + sourceSchemaName + """.CHARTEVENTS);"""
    createChildQuery7 = """CREATE TABLE """ + sourceSchemaName + """.CHARTEVENTS_7 ( CHECK ( itemid >= 226000 AND itemid < 227000 )) INHERITS (""" + sourceSchemaName + """.CHARTEVENTS);"""
    createChildQuery8 = """CREATE TABLE """ + sourceSchemaName + """.CHARTEVENTS_8 ( CHECK ( itemid >= 227000 AND itemid < 228000 )) INHERITS (""" + sourceSchemaName + """.CHARTEVENTS);"""
    createChildQuery9 = """CREATE TABLE """ + sourceSchemaName + """.CHARTEVENTS_9 ( CHECK ( itemid >= 228000 AND itemid < 229000 )) INHERITS (""" + sourceSchemaName + """.CHARTEVENTS);"""
    createChildQuery10 = """CREATE TABLE """ + sourceSchemaName + """.CHARTEVENTS_10 ( CHECK ( itemid >= 229000 AND itemid < 230000 )) INHERITS (""" + sourceSchemaName + """.CHARTEVENTS);"""
    createFunctionQuery = """CREATE OR REPLACE FUNCTION """ + sourceSchemaName + """.chartevents_insert_trigger()
        RETURNS TRIGGER AS $$
        BEGIN
        IF ( NEW.itemid >= 220000 AND NEW.itemid < 221000 ) THEN INSERT INTO """ + sourceSchemaName + """.CHARTEVENTS_1 VALUES (NEW.*);
        ELSIF ( NEW.itemid >= 221000 AND NEW.itemid < 222000 ) THEN INSERT INTO """ + sourceSchemaName + """.CHARTEVENTS_2 VALUES (NEW.*);
        ELSIF ( NEW.itemid >= 222000 AND NEW.itemid < 223000 ) THEN INSERT INTO """ + sourceSchemaName + """.CHARTEVENTS_3 VALUES (NEW.*);
        ELSIF ( NEW.itemid >= 223000 AND NEW.itemid < 224000 ) THEN INSERT INTO """ + sourceSchemaName + """.CHARTEVENTS_4 VALUES (NEW.*);
        ELSIF ( NEW.itemid >= 224000 AND NEW.itemid < 225000 ) THEN INSERT INTO """ + sourceSchemaName + """.CHARTEVENTS_5 VALUES (NEW.*);
        ELSIF ( NEW.itemid >= 225000 AND NEW.itemid < 226000 ) THEN INSERT INTO """ + sourceSchemaName + """.CHARTEVENTS_6 VALUES (NEW.*);
        ELSIF ( NEW.itemid >= 226000 AND NEW.itemid < 227000 ) THEN INSERT INTO """ + sourceSchemaName + """.CHARTEVENTS_7 VALUES (NEW.*);
        ELSIF ( NEW.itemid >= 227000 AND NEW.itemid < 228000 ) THEN INSERT INTO """ + sourceSchemaName + """.CHARTEVENTS_8 VALUES (NEW.*);
        ELSIF ( NEW.itemid >= 228000 AND NEW.itemid < 229000 ) THEN INSERT INTO """ + sourceSchemaName + """.CHARTEVENTS_9 VALUES (NEW.*);
        ELSIF ( NEW.itemid >= 229000 AND NEW.itemid < 230000 ) THEN INSERT INTO """ + sourceSchemaName + """.CHARTEVENTS_10 VALUES (NEW.*);
        ELSE
            INSERT INTO """ + sourceSchemaName + """.chartevents_null VALUES (NEW.*);
        END IF;
        RETURN NULL;
        END;
        $$
        LANGUAGE plpgsql
        ;
        """
    dropTriggerQuery = """DROP TRIGGER IF EXISTS insert_chartevents_trigger
    ON """ + sourceSchemaName + """.CHARTEVENTS
    ;
    """
    createTriggerQuery = """CREATE TRIGGER insert_chartevents_trigger
    BEFORE INSERT ON """ + sourceSchemaName + """.CHARTEVENTS
    FOR EACH ROW EXECUTE PROCEDURE """ + sourceSchemaName + """.chartevents_insert_trigger()
    ;
    """
    if createSchema:
        with con:
            with con.cursor() as cursor:
                cursor.execute(dropQuery)
                cursor.execute(createQuery)
                log.info("Creating child table: " + sourceSchemaName + ".CHARTEVENTS_1")
                cursor.execute(createChildQuery1)
                log.info("Creating child table: " + sourceSchemaName + ".CHARTEVENTS_2")
                cursor.execute(createChildQuery2)
                log.info("Creating child table: " + sourceSchemaName + ".CHARTEVENTS_3")
                cursor.execute(createChildQuery3)
                log.info("Creating child table: " + sourceSchemaName + ".CHARTEVENTS_4")
                cursor.execute(createChildQuery4)
                log.info("Creating child table: " + sourceSchemaName + ".CHARTEVENTS_5")
                cursor.execute(createChildQuery5)
                log.info("Creating child table: " + sourceSchemaName + ".CHARTEVENTS_6")
                cursor.execute(createChildQuery6)
                log.info("Creating child table: " + sourceSchemaName + ".CHARTEVENTS_7")
                cursor.execute(createChildQuery7)
                log.info("Creating child table: " + sourceSchemaName + ".CHARTEVENTS_8")
                cursor.execute(createChildQuery8)
                log.info("Creating child table: " + sourceSchemaName + ".CHARTEVENTS_9")
                cursor.execute(createChildQuery9)
                log.info("Creating child table: " + sourceSchemaName + ".CHARTEVENTS_10")
                cursor.execute(createChildQuery10)
                log.info("Creating function: " + sourceSchemaName + ".CHARTEVENTS.chartevents_insert_trigger()")
                cursor.execute(createFunctionQuery)
                log.info("Dropping trigger: " + sourceSchemaName + ".CHARTEVENTS.insert_chartevents_trigger")
                cursor.execute(dropTriggerQuery)
                log.info("Creating trigger: " + sourceSchemaName + ".CHARTEVENTS.insert_chartevents_trigger")
                cursor.execute(createTriggerQuery)

    import pandas as pd

    log.info("Reading file: " + str(filePath))
    df = pd.read_csv(filePath, sep=fileSeparator)
    dfColumns = [
        Config.chartevents['column_mapping']['subject_id'],
        Config.chartevents['column_mapping']['hadm_id'],
        Config.chartevents['column_mapping']['stay_id'],
        Config.chartevents['column_mapping']['charttime'],
        Config.chartevents['column_mapping']['storetime'],
        Config.chartevents['column_mapping']['itemid'],
        Config.chartevents['column_mapping']['value'],
        Config.chartevents['column_mapping']['valuenum'],
        Config.chartevents['column_mapping']['valueuom'],
        Config.chartevents['column_mapping']['warning'],
        ]
    __saveDataframe(con=con, destinationSchemaName=sourceSchemaName, destinationTableName='CHARTEVENTS', df=df, dfColumns=dfColumns)


def importDataCsv(con, sourceSchemaName):
    importPatients(
        con=con,
        sourceSchemaName=sourceSchemaName,
        filePath = Config.patients['file_name'],
        fileSeparator=','
        )
    importAdmissions(
        con=con,
        sourceSchemaName=sourceSchemaName,
        filePath = Config.admissions['file_name'],
        fileSeparator=','
        )
    importTransfers(
        con=con,
        sourceSchemaName=sourceSchemaName,
        filePath = Config.transfers['file_name'],
        fileSeparator=','
        )
    importDiagnosesIcd(
        con=con,
        sourceSchemaName=sourceSchemaName,
        filePath = Config.diagnoses_icd['file_name'],
        fileSeparator=','
        )
    importServices(
        con=con,
        sourceSchemaName=sourceSchemaName,
        filePath = Config.services['file_name'],
        fileSeparator=','
        )
    importLabEvents(
        con=con,
        sourceSchemaName=sourceSchemaName,
        filePath = Config.labevents['file_name'],
        fileSeparator=','
        )
    # i = 'a'
    # filePath = '/superbugai-data/mimiciv/1.0/hosp/xa'
    # importLabEvents(
    #     con=con,
    #     sourceSchemaName=sourceSchemaName,
    #     filePath = filePath + i,
    #     fileSeparator=','
    #     )
    # for i in ['b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm']:
    #     filePath = '/superbugai-data/mimiciv/1.0/hosp/xa'
    #     importLabEvents(
    #         con=con,
    #         sourceSchemaName=sourceSchemaName,
    #         filePath = filePath + i,
    #         fileSeparator=',',
    #         createSchema=False
    #         )
    importLabItems(
        con=con,
        sourceSchemaName=sourceSchemaName,
        filePath = Config.d_labitems['file_name'],
        fileSeparator=','
        )
    importProcedures(
        con=con,
        sourceSchemaName=sourceSchemaName,
        filePath = Config.procedures_icd['file_name'],
        fileSeparator=','
        )
    importHcpcsEvents(
        con=con,
        sourceSchemaName=sourceSchemaName,
        filePath = Config.hcpcsevents['file_name'],
        fileSeparator=','
        )
    importDrugCodes(
        con=con,
        sourceSchemaName=sourceSchemaName,
        filePath = Config.drgcodes['file_name'],
        fileSeparator=','
        )
    importPrescriptions(
        con=con,
        sourceSchemaName=sourceSchemaName,
        filePath = Config.prescriptions['file_name'],
        fileSeparator=','
        )
    importMicrobiologyEvents(
        con=con,
        sourceSchemaName=sourceSchemaName,
        filePath = Config.microbiologyevents['file_name'],
        fileSeparator=','
        )
    importPharmacy(
        con=con,
        sourceSchemaName=sourceSchemaName,
        filePath = Config.pharmacy['file_name'],
        fileSeparator=','
        )
    importProcedureEvents(
        con=con,
        sourceSchemaName=sourceSchemaName,
        filePath = Config.procedureevents['file_name'],
        fileSeparator=','
        )
    importItems(
        con=con,
        sourceSchemaName=sourceSchemaName,
        filePath = Config.d_items['file_name'],
        fileSeparator=','
        )
    importDatetimeEvents(
        con=con,
        sourceSchemaName=sourceSchemaName,
        filePath = Config.datetimeevents['file_name'],
        fileSeparator=','
        )
    importChartEvents(
        con=con,
        sourceSchemaName=sourceSchemaName,
        filePath = Config.chartevents['file_name'],
        fileSeparator=','
        )
    # filePath = '/superbugai-data/mimiciv/1.0/icu/xaa'
    # importChartEvents(
    #     con=con,
    #     sourceSchemaName=sourceSchemaName,
    #     filePath = filePath,
    #     fileSeparator=','
    #     )
    # for i in ['b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u']:
    #     filePath = '/superbugai-data/mimiciv/1.0/icu/xa'
    #     importChartEvents(
    #         con=con,
    #         sourceSchemaName=sourceSchemaName,
    #         filePath = filePath + i,
    #         fileSeparator=',',
    #         createSchema=False
    #         )
