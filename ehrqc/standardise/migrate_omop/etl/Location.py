import logging

log = logging.getLogger("Standardise")


def createLocationCdm(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".cdm_location")
    dropQuery = """drop table if exists """ + etlSchemaName + """.cdm_location cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.cdm_location
        (
            location_id           INTEGER     not null ,
            address_1             TEXT             ,
            address_2             TEXT             ,
            city                  TEXT             ,
            state                 TEXT             ,
            zip                   TEXT             ,
            county                TEXT             ,
            location_source_value TEXT             ,
            -- 
            unit_id                       TEXT,
            load_table_id                 TEXT,
            load_row_id                   INTEGER,
            trace_id                      TEXT
        )
        ;
        """
    insertQuery = """INSERT INTO """ + etlSchemaName + """.cdm_location
        SELECT
            1                           AS location_id,
            CAST(NULL AS TEXT)        AS address_1,
            CAST(NULL AS TEXT)        AS address_2,
            CAST(NULL AS TEXT)        AS city,
            'MA'                        AS state,
            CAST(NULL AS TEXT)        AS zip,
            CAST(NULL AS TEXT)        AS county,
            'Beth Israel Hospital'      AS location_source_value,
            -- 
            'location.null'             AS unit_id,
            'null'                      AS load_table_id,
            0                           AS load_row_id,
            CAST(NULL AS TEXT)        AS trace_id
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertQuery)


def migrate(con, etlSchemaName):
    createLocationCdm(con = con, etlSchemaName = etlSchemaName)
