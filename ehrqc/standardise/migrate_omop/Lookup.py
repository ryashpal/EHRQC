import logging

from ehrqc import Config

log = logging.getLogger("Standardise")


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


def createConcept(con, etlSchemaName, filePath):
    log.info("Creating table: " + etlSchemaName + ".voc_concept")
    dropQuery = """drop table if exists """ + etlSchemaName + """.voc_concept cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.voc_concept (
        concept_id          INTEGER       not null ,
        concept_name        TEXT      ,
        domain_id           TEXT      not null ,
        vocabulary_id       TEXT      not null ,
        concept_class_id    TEXT      not null ,
        standard_concept    TEXT               ,
        concept_code        TEXT      not null ,
        valid_start_date    DATE        not null ,
        valid_end_date      DATE        not null ,
        invalid_reason      TEXT
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep='\t')
    df['valid_start_date'] = pd.to_datetime(df['valid_start_date'], unit='s')
    df['valid_end_date'] = pd.to_datetime(df['valid_end_date'], unit='s')
    dfColumns = ['concept_id', 'concept_name', 'domain_id', 'vocabulary_id', 'concept_class_id', 'standard_concept', 'concept_code', 'valid_start_date', 'valid_end_date', 'invalid_reason']
    __saveDataframe(con=con, destinationSchemaName=etlSchemaName, destinationTableName='voc_concept', df=df, dfColumns=dfColumns)


def createVocabulary(con, etlSchemaName, filePath):
    log.info("Creating table: " + etlSchemaName + ".voc_vocabulary")
    dropQuery = """drop table if exists """ + etlSchemaName + """.voc_vocabulary cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.voc_vocabulary (
        vocabulary_id         TEXT      not null,
        vocabulary_name       TEXT      not null,
        vocabulary_reference  TEXT      not null,
        vocabulary_version    TEXT              ,
        vocabulary_concept_id INTEGER       not null
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep='\t')
    dfColumns = ['vocabulary_id', 'vocabulary_name', 'vocabulary_reference', 'vocabulary_version', 'vocabulary_concept_id']
    __saveDataframe(con=con, destinationSchemaName=etlSchemaName, destinationTableName='voc_vocabulary', df=df, dfColumns=dfColumns)


def createDomain(con, etlSchemaName, filePath):
    log.info("Creating table: " + etlSchemaName + ".voc_domain")
    dropQuery = """drop table if exists """ + etlSchemaName + """.voc_domain cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.voc_domain (
        domain_id         TEXT      not null,
        domain_name       TEXT      not null,
        domain_concept_id INTEGER       not null
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep='\t')
    dfColumns = ['domain_id', 'domain_name', 'domain_concept_id']
    __saveDataframe(con=con, destinationSchemaName=etlSchemaName, destinationTableName='voc_domain', df=df, dfColumns=dfColumns)


def createConceptClass(con, etlSchemaName, filePath):
    log.info("Creating table: " + etlSchemaName + ".voc_concept_class")
    dropQuery = """drop table if exists """ + etlSchemaName + """.voc_concept_class cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.voc_concept_class (
        concept_class_id          TEXT      not null,
        concept_class_name        TEXT      not null,
        concept_class_concept_id  INTEGER       not null
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep='\t')
    dfColumns = ['concept_class_id', 'concept_class_name', 'concept_class_concept_id']
    __saveDataframe(con=con, destinationSchemaName=etlSchemaName, destinationTableName='voc_concept_class', df=df, dfColumns=dfColumns)


def createConceptRelationship(con, etlSchemaName, filePath):
    log.info("Creating table: " + etlSchemaName + ".voc_concept_relationship")
    dropQuery = """drop table if exists """ + etlSchemaName + """.voc_concept_relationship cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.voc_concept_relationship (
        concept_id_1      INTEGER     not null,
        concept_id_2      INTEGER     not null,
        relationship_id   TEXT    not null,
        valid_start_DATE  DATE      not null,
        valid_end_DATE    DATE      not null,
        invalid_reason    TEXT
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep='\t')
    df['valid_start_date'] = pd.to_datetime(df['valid_start_date'], unit='s')
    df['valid_end_date'] = pd.to_datetime(df['valid_end_date'], unit='s')
    dfColumns = ['concept_id_1', 'concept_id_2', 'relationship_id', 'valid_start_date', 'valid_end_date', 'invalid_reason']
    __saveDataframe(con=con, destinationSchemaName=etlSchemaName, destinationTableName='voc_concept_relationship', df=df, dfColumns=dfColumns)


def createRelationship(con, etlSchemaName, filePath):
    log.info("Creating table: " + etlSchemaName + ".voc_relationship")
    dropQuery = """drop table if exists """ + etlSchemaName + """.voc_relationship cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.voc_relationship (
        relationship_id         TEXT      not null,
        relationship_name       TEXT      not null,
        is_hierarchical         TEXT      not null,
        defines_ancestry        TEXT      not null,
        reverse_relationship_id TEXT      not null,
        relationship_concept_id INTEGER       not null
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep='\t')
    dfColumns = ['relationship_id', 'relationship_name', 'is_hierarchical', 'defines_ancestry', 'reverse_relationship_id', 'relationship_concept_id']
    __saveDataframe(con=con, destinationSchemaName=etlSchemaName, destinationTableName='voc_relationship', df=df, dfColumns=dfColumns)


def createConceptSynonym(con, etlSchemaName, filePath):
    log.info("Creating table: " + etlSchemaName + ".voc_concept_synonym")
    dropQuery = """drop table if exists """ + etlSchemaName + """.voc_concept_synonym cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.voc_concept_synonym (
        concept_id            INTEGER       not null,
        concept_synonym_name  TEXT      not null,
        language_concept_id   INTEGER       not null
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep='\t')
    dfColumns = ['concept_id', 'concept_synonym_name', 'language_concept_id']
    __saveDataframe(con=con, destinationSchemaName=etlSchemaName, destinationTableName='voc_concept_synonym', df=df, dfColumns=dfColumns)


def createConceptAncestor(con, etlSchemaName, filePath):
    log.info("Creating table: " + etlSchemaName + ".voc_concept_ancestor")
    dropQuery = """drop table if exists """ + etlSchemaName + """.voc_concept_ancestor cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.voc_concept_ancestor (
        ancestor_concept_id       INTEGER   not null,
        descendant_concept_id     INTEGER   not null,
        min_levels_of_separation  INTEGER   not null,
        max_levels_of_separation  INTEGER   not null
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)

    import pandas as pd

    df = pd.read_csv(filePath, sep='\t')
    dfColumns = ['ancestor_concept_id', 'descendant_concept_id', 'min_levels_of_separation', 'max_levels_of_separation']
    __saveDataframe(con=con, destinationSchemaName=etlSchemaName, destinationTableName='voc_concept_ancestor', df=df, dfColumns=dfColumns)


def createLookupConcept(con, etlSchemaName, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".concept")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.concept cascade"""
    createQuery = """CREATE TABLE """ + lookupSchemaName + """.concept AS
        SELECT
            *,
            'concept' AS load_table_id,
            ('x'||substr(md5(random():: text),1,8))::bit(32)::int AS load_row_id
        FROM
            """ + etlSchemaName + """.voc_concept
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createLookupConceptRelationship(con, etlSchemaName, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".concept_relationship")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.concept_relationship cascade"""
    createQuery = """CREATE TABLE """ + lookupSchemaName + """.concept_relationship AS
        SELECT
            *,
            'concept_relationship' AS load_table_id,
            0 AS load_row_id
        FROM
            """ + etlSchemaName + """.voc_concept_relationship
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createLookupVocabulary(con, etlSchemaName, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".vocabulary")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.vocabulary cascade"""
    createQuery = """CREATE  TABLE """ + lookupSchemaName + """.vocabulary AS
        SELECT
            *,
            'vocabulary' AS load_table_id,
            0 AS load_row_id
        FROM
            """ + etlSchemaName + """.voc_vocabulary
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createLookupConceptClass(con, etlSchemaName, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".concept_class")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.concept_class cascade"""
    createQuery = """CREATE TABLE """ + lookupSchemaName + """.concept_class AS
        SELECT
            *,
            'concept_class' AS load_table_id,
            0 AS load_row_id
        FROM
            """ + etlSchemaName + """.voc_concept_class
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createLookupConceptAncestor(con, etlSchemaName, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".concept_ancestor")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.concept_ancestor cascade"""
    createQuery = """CREATE TABLE """ + lookupSchemaName + """.concept_ancestor AS
        SELECT
            *,
            'concept_ancestor' AS load_table_id,
            0 AS load_row_id
        FROM
            """ + etlSchemaName + """.voc_concept_ancestor
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createLookupConceptSynonym(con, etlSchemaName, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".concept_synonym")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.concept_synonym cascade"""
    createQuery = """CREATE TABLE """ + lookupSchemaName + """.concept_synonym AS
        SELECT
            *,
            'concept_synonym' AS load_table_id,
            0 AS load_row_id
        FROM
            """ + etlSchemaName + """.voc_concept_synonym
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createLookupDomain(con, etlSchemaName, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".domain")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.domain cascade"""
    createQuery = """CREATE  TABLE """ + lookupSchemaName + """.domain AS
        SELECT
            *,
            'domain' AS load_table_id,
            0 AS load_row_id
        FROM
            """ + etlSchemaName + """.voc_domain
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createLookupRelationship(con, etlSchemaName, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".relationship")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.relationship cascade"""
    createQuery = """CREATE TABLE """ + lookupSchemaName + """.relationship AS
        SELECT
            *,
            'relationship' AS load_table_id,
            0 AS load_row_id
        FROM
            """ + etlSchemaName + """.voc_relationship
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTmpCustomMapping(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".tmp_custom_mapping")
    createQuery = """CREATE TABLE IF NOT EXISTS """ + etlSchemaName + """.tmp_custom_mapping (
        concept_name			        TEXT		not null,
        source_concept_id		        INTEGER		not null,
        source_vocabulary_id		    TEXT		not null,
        source_domain_id		        TEXT		not null,
        source_concept_class_id		    TEXT		not null,
        standard_concept	            TEXT,
        concept_code    		        TEXT		not null,
        valid_start_date                DATE        not null,
        valid_end_date                  DATE        not null,
        invalid_reason			        TEXT,
        target_concept_id		        INTEGER		not null,
        relationship_id			        TEXT		not null,
        reverese_relationship_id	    TEXT		not null,
        relationship_valid_start_date   DATE        not null,
        relationship_end_date           DATE        not null,
        invalid_reason_cr		    TEXT
        )
        ;"""
    with con:
        with con.cursor() as cursor:
            cursor.execute(createQuery)


def generateTmpCustomMapping(con, etlSchemaName):

    log.info("Generating table: " + etlSchemaName + ".tmp_custom_mapping")

    from ehrqc.standardise.migrate_omop import ConceptMapper

    for conceptType in Config.customMapping:
        df = ConceptMapper.createCustomMapping(
            con
            , etlSchemaName = etlSchemaName
            , fieldName = Config.customMapping[conceptType]['source_attributes']['field_name']
            , tableName = Config.customMapping[conceptType]['source_attributes']['table_name']
            , whereCondition = Config.customMapping[conceptType]['source_attributes']['where_condition']
            , sourceVocabularyId = Config.customMapping[conceptType]['source_attributes']['vocabulary_id']
            , domainId = Config.customMapping[conceptType]['standard_attributes']['domain_id']
            , vocabularyId = Config.customMapping[conceptType]['standard_attributes']['vocabulary_id']
            , conceptClassId = Config.customMapping[conceptType]['standard_attributes']['concept_class_id']
            , keyPhrase = Config.customMapping[conceptType]['standard_attributes']['key_phrase']
            , algorithm='semantic'
            )
        dfColumns = ['concept_name', 'source_concept_id', 'source_vocabulary_id', 'source_domain_id', 'source_concept_class_id', 'standard_concept', 'concept_code', 'valid_start_date', 'valid_end_date', 'invalid_reason', 'target_concept_id', 'relationship_id', 'reverese_relationship_id', 'relationship_valid_start_date', 'relationship_end_date', 'invalid_reason_cr']
        __saveDataframe(con=con, destinationSchemaName=etlSchemaName, destinationTableName='tmp_custom_mapping', df=df, dfColumns=dfColumns)


def loadTmpCustomMapping(con, etlSchemaName, filePath):

    log.info("Loading table: " + etlSchemaName + ".tmp_custom_mapping")

    import pandas as pd

    df = pd.read_csv(filePath)

    dfColumns = ['concept_name', 'source_concept_id', 'source_vocabulary_id', 'source_domain_id', 'source_concept_class_id', 'standard_concept', 'concept_code', 'valid_start_date', 'valid_end_date', 'invalid_reason', 'target_concept_id', 'relationship_id', 'reverese_relationship_id', 'relationship_valid_start_date', 'relationship_end_date', 'invalid_reason_cr']
    __saveDataframe(con=con, destinationSchemaName=etlSchemaName, destinationTableName='tmp_custom_mapping', df=df, dfColumns=dfColumns)


def createTmpCustomConcept(con, etlSchemaName, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".tmp_custom_concept")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.tmp_custom_concept cascade"""
    createQuery = """CREATE TABLE """ + lookupSchemaName + """.tmp_custom_concept AS
        SELECT
            voc.source_concept_id           AS concept_id,
            voc.concept_name                AS concept_name,
            voc.source_domain_id            AS domain_id,
            voc.source_vocabulary_id        AS vocabulary_id,
            voc.source_concept_class_id     AS concept_class_id,
            CASE
                WHEN voc.target_concept_id = 0 THEN 'S'
                ELSE voc.standard_concept 
            END                             AS standard_concept,
            voc.concept_code                AS concept_code,
            voc.valid_start_date    AS valid_start_date,
            voc.valid_end_date      AS valid_end_date,
            voc.invalid_reason              AS invalid_reason,

            'tmp_custom_mapping'            AS load_table_id,
            CAST(NULL AS INTEGER)             AS load_row_id
            -- voc.load_table_id               AS load_table_id,
            -- MIN(voc.load_row_id)            AS load_row_id
        FROM
            """ + etlSchemaName + """.tmp_custom_mapping voc
        GROUP BY
            voc.source_concept_id,
            voc.concept_name,
            voc.source_domain_id,
            voc.source_vocabulary_id,
            voc.source_concept_class_id,
            CASE
                WHEN voc.target_concept_id = 0 THEN 'S'
                ELSE voc.standard_concept 
            END,
            voc.concept_code,
            voc.valid_start_date,
            voc.valid_end_date,
            voc.invalid_reason
            -- voc.load_table_id
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTmpCustomConceptRelationship(con, etlSchemaName, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".tmp_custom_concept_relationship")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.tmp_custom_concept_relationship cascade"""
    createQuery = """CREATE TABLE """ + lookupSchemaName + """.tmp_custom_concept_relationship AS
        SELECT
            tcr.source_concept_id               AS concept_id_1,
            CASE
                WHEN tcr.target_concept_id = 0 THEN tcr.source_concept_id
                ELSE tcr.target_concept_id
            END                                 AS concept_id_2,
            tcr.relationship_id                 AS relationship_id,
            tcr.relationship_valid_start_date   AS valid_start_date,
            tcr.relationship_end_date           AS valid_end_date,
            tcr.invalid_reason_cr               AS invalid_reason,

            'tmp_custom_mapping'            AS load_table_id,
            CAST(NULL AS INTEGER)             AS load_row_id
            -- tcr.load_table_id                   AS load_table_id,
            -- tcr.load_row_id                     AS load_row_id
        FROM
            """ + etlSchemaName + """.tmp_custom_mapping tcr
        WHERE
            tcr.target_concept_id IS NOT NULL

        UNION ALL

        SELECT
            CASE
                WHEN tcr.target_concept_id = 0 THEN tcr.source_concept_id
                ELSE tcr.target_concept_id
            END                                 AS concept_id_1,
            tcr.source_concept_id               AS concept_id_2,
            tcr.reverese_relationship_id        AS relationship_id,
            tcr.relationship_valid_start_date   AS valid_start_date,
            tcr.relationship_end_date           AS valid_end_date,
            tcr.invalid_reason_cr               AS invalid_reason,

            'tmp_custom_mapping'            AS load_table_id,
            CAST(NULL AS INTEGER)             AS load_row_id
            -- tcr.load_table_id                   AS load_table_id,
            -- tcr.load_row_id                     AS load_row_id
        FROM
            """ + etlSchemaName + """.tmp_custom_mapping tcr
        WHERE
            tcr.target_concept_id IS NOT NULL
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTmpCustomVocabularyDist(con, etlSchemaName, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".tmp_custom_vocabulary_dist")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.tmp_custom_vocabulary_dist cascade"""
    createQuery = """CREATE TABLE """ + lookupSchemaName + """.tmp_custom_vocabulary_dist AS
        SELECT
            voc.source_vocabulary_id        AS source_vocabulary_id,

            'tmp_custom_mapping'            AS load_table_id,
            CAST(NULL AS INTEGER)             AS load_row_id
            -- voc.load_table_id               AS load_table_id,
            -- MIN(voc.load_row_id)            AS load_row_id
        FROM
            """ + etlSchemaName + """.tmp_custom_mapping voc
        GROUP BY
            voc.source_vocabulary_id
            -- voc.load_table_id
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTmpCustomVocabulary(con, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".tmp_custom_vocabulary")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.tmp_custom_vocabulary cascade"""
    createQuery = """CREATE TABLE """ + lookupSchemaName + """.tmp_custom_vocabulary AS
        SELECT
            voc.source_vocabulary_id        AS vocabulary_id,
            voc.source_vocabulary_id        AS vocabulary_name,
            'Odysseus generated'            AS vocabulary_reference,
            CAST(NULL AS TEXT)            AS vocabulary_version,
            2110000001 + 
                ROW_NUMBER() OVER (
                    ORDER BY voc.source_vocabulary_id
                )                           AS vocabulary_concept_id,

            voc.load_table_id               AS load_table_id,
            voc.load_row_id                 AS load_row_id
        FROM
            """ + lookupSchemaName + """.tmp_custom_vocabulary_dist voc
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def dropTmpCustomVocabularyDist(con, lookupSchemaName):
    log.info("Dropping table: " + lookupSchemaName + ".tmp_custom_vocabulary_dist")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.tmp_custom_vocabulary_dist cascade"""
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)


def createTmpVocConcept(con, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".tmp_voc_concept")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.tmp_voc_concept cascade"""
    createQuery = """CREATE TABLE """ + lookupSchemaName + """.tmp_voc_concept AS
        SELECT *
        FROM
            """ + lookupSchemaName + """.concept
        WHERE
            concept_id < 2000000000
        ;
        """
    dropQueryRelationship = """drop table if exists """ + lookupSchemaName + """.tmp_voc_concept_relationship cascade"""
    createQueryRelationship = """CREATE TABLE """ + lookupSchemaName + """.tmp_voc_concept_relationship AS
        SELECT vr.*
        FROM
            """ + lookupSchemaName + """.concept_relationship vr
        INNER JOIN
            """ + lookupSchemaName + """.tmp_voc_concept vc1
                ON  vc1.concept_id = vr.concept_id_1
        INNER JOIN
            """ + lookupSchemaName + """.tmp_voc_concept vc2
                ON  vc2.concept_id = vr.concept_id_2
        ;
        """
    insertQuery = """INSERT INTO """ + lookupSchemaName + """.tmp_voc_concept
        SELECT
            voc.concept_id              AS concept_id,
            voc.concept_name            AS concept_name,
            voc.domain_id               AS domain_id,
            voc.vocabulary_id           AS vocabulary_id,
            voc.concept_class_id        AS concept_class_id,
            voc.standard_concept        AS standard_concept,
            voc.concept_code            AS concept_code,
            voc.valid_start_date        AS valid_start_date,
            voc.valid_end_date          AS valid_end_date,
            voc.invalid_reason          AS invalid_reason,

            voc.load_table_id           AS load_table_id,
            voc.load_row_id              AS load_row_id
        FROM 
            """ + lookupSchemaName + """.tmp_custom_concept voc
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(dropQueryRelationship)
            cursor.execute(createQueryRelationship)
            cursor.execute(insertQuery)


def createConceptFinal(con, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".concept")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.concept cascade"""
    createQuery = """CREATE TABLE """ + lookupSchemaName + """.concept AS
        SELECT * 
        FROM """ + lookupSchemaName + """.tmp_voc_concept
        ;
        """
    insertQuery = """INSERT INTO """ + lookupSchemaName + """.tmp_voc_concept_relationship
        SELECT
            tcr.concept_id_1             AS concept_id_1,
            tcr.concept_id_2             AS concept_id_2,
            tcr.relationship_id          AS relationship_id,
            tcr.valid_start_date         AS valid_start_date,
            tcr.valid_end_date           AS valid_end_date,
            tcr.invalid_reason           AS invalid_reason,
            tcr.load_table_id            AS load_table_id,
            tcr.load_row_id              AS load_row_id
        FROM
            """ + lookupSchemaName + """.tmp_custom_concept_relationship tcr
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertQuery)


def createConceptRelationshipFinal(con, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".concept_relationship")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.concept_relationship cascade"""
    createQuery = """CREATE TABLE """ + lookupSchemaName + """.concept_relationship AS
        SELECT *
        FROM """ + lookupSchemaName + """.tmp_voc_concept_relationship
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def createTmpVocVocabulary(con, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".tmp_voc_vocabulary")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.tmp_voc_vocabulary cascade"""
    createQuery = """CREATE TABLE """ + lookupSchemaName + """.tmp_voc_vocabulary AS
        SELECT *
        FROM
            """ + lookupSchemaName + """.vocabulary
        WHERE
            vocabulary_concept_id < 2000000000
        ;
        """
    insertQuery = """INSERT INTO """ + lookupSchemaName + """.tmp_voc_vocabulary
        SELECT
            voc.vocabulary_id         AS vocabulary_id,
            voc.vocabulary_name       AS vocabulary_name,
            voc.vocabulary_reference  AS vocabulary_reference,
            voc.vocabulary_version    AS vocabulary_version,
            voc.vocabulary_concept_id AS vocabulary_concept_id,

            voc.load_table_id           AS load_table_id,
            voc.load_row_id             AS load_row_id
        FROM 
            """ + lookupSchemaName + """.tmp_custom_vocabulary voc
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertQuery)


def createVocabularyFinal(con, lookupSchemaName):
    log.info("Creating table: " + lookupSchemaName + ".vocabulary")
    dropQuery = """drop table if exists """ + lookupSchemaName + """.vocabulary cascade"""
    createQuery = """CREATE TABLE """ + lookupSchemaName + """.vocabulary AS
        SELECT *
        FROM """ + lookupSchemaName + """.tmp_voc_vocabulary
        ;
        """
    insertQuery = """INSERT INTO """ + lookupSchemaName + """.concept
        SELECT
            vcv.vocabulary_concept_id   AS concept_id,
            vcv.vocabulary_name         AS concept_name,
            'Metadata'                  AS domain_id,
            'Vocabulary'                AS vocabulary_id,
            'Vocabulary'                AS concept_class_id,
            'S'                         AS standard_concept,
            vcv.vocabulary_reference    AS concept_code,
            CAST('1970-01-01' AS DATE)  AS valid_start_date,
            CAST('2099-12-31' AS DATE)  AS valid_end_date,
            NULL                        AS invalid_reason,

            NULL                        AS load_table_id,
            NULL                        AS load_row_id
        FROM 
            """ + lookupSchemaName + """.tmp_custom_vocabulary vcv 
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertQuery)


def dropTmpTables(con, lookupSchemaName):
    log.info("Dropping temporary tables")
    dropQuery1 = """drop table if exists """ + lookupSchemaName + """.tmp_custom_concept cascade"""
    dropQuery2 = """drop table if exists """ + lookupSchemaName + """.tmp_custom_concept_relationship cascade"""
    dropQuery3 = """drop table if exists """ + lookupSchemaName + """.tmp_custom_vocabulary cascade"""
    dropQuery4 = """drop table if exists """ + lookupSchemaName + """.tmp_voc_concept cascade"""
    dropQuery5 = """drop table if exists """ + lookupSchemaName + """.tmp_voc_concept_relationship cascade"""
    dropQuery6 = """drop table if exists """ + lookupSchemaName + """.tmp_voc_vocabulary cascade"""
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery1)
            cursor.execute(dropQuery2)
            cursor.execute(dropQuery3)
            cursor.execute(dropQuery4)
            cursor.execute(dropQuery5)
            cursor.execute(dropQuery6)


def migrateStandardVocabulary(con):
    importAthenaVocabulary(con=con)
    stageAthenaVocabulary(con=con)


def migrateCustomMapping(con, isFromFile):
    importCustomVocabulary(con=con, isFromFile=isFromFile)


def importAthenaVocabulary(con):
    createConcept(con=con, etlSchemaName=Config.etl_schema_name, filePath = Config.vocabulary['concept'])
    createVocabulary(con=con, etlSchemaName=Config.etl_schema_name, filePath = Config.vocabulary['vocabulary'])
    createDomain(con=con, etlSchemaName=Config.etl_schema_name, filePath = Config.vocabulary['domain'])
    createConceptClass(con=con, etlSchemaName=Config.etl_schema_name, filePath = Config.vocabulary['concept_class'])
    createConceptRelationship(con=con, etlSchemaName=Config.etl_schema_name, filePath = Config.vocabulary['concept_relationship'])
    createRelationship(con=con, etlSchemaName=Config.etl_schema_name, filePath = Config.vocabulary['relationship'])
    createConceptSynonym(con=con, etlSchemaName=Config.etl_schema_name, filePath = Config.vocabulary['concept_synonym'])
    createConceptAncestor(con=con, etlSchemaName=Config.etl_schema_name, filePath = Config.vocabulary['concept_ancestor'])


def stageAthenaVocabulary(con):
    createLookupConcept(con=con, etlSchemaName=Config.etl_schema_name, lookupSchemaName=Config.lookup_schema_name)
    createLookupConceptRelationship(con=con, etlSchemaName=Config.etl_schema_name, lookupSchemaName=Config.lookup_schema_name)
    createLookupVocabulary(con=con, etlSchemaName=Config.etl_schema_name, lookupSchemaName=Config.lookup_schema_name)
    createLookupConceptClass(con=con, etlSchemaName=Config.etl_schema_name, lookupSchemaName=Config.lookup_schema_name)
    createLookupConceptAncestor(con=con, etlSchemaName=Config.etl_schema_name, lookupSchemaName=Config.lookup_schema_name)
    createLookupConceptSynonym(con=con, etlSchemaName=Config.etl_schema_name, lookupSchemaName=Config.lookup_schema_name)
    createLookupDomain(con=con, etlSchemaName=Config.etl_schema_name, lookupSchemaName=Config.lookup_schema_name)
    createLookupRelationship(con=con, etlSchemaName=Config.etl_schema_name, lookupSchemaName=Config.lookup_schema_name)


def importCustomVocabulary(con, isFromFile):
    createTmpCustomMapping(con=con, etlSchemaName=Config.etl_schema_name)
    if isFromFile:
        loadTmpCustomMapping(con=con, etlSchemaName=Config.etl_schema_name, filePath=Config.vocabulary['tmp_custom_mapping'])
    else:
        generateTmpCustomMapping(con=con, etlSchemaName=Config.etl_schema_name)


def processCustomVocabulary(con):
    createTmpCustomConcept(con=con, etlSchemaName=Config.etl_schema_name, lookupSchemaName=Config.lookup_schema_name)
    createTmpCustomConceptRelationship(con=con, etlSchemaName=Config.etl_schema_name, lookupSchemaName=Config.lookup_schema_name)
    createTmpCustomVocabularyDist(con=con, etlSchemaName=Config.etl_schema_name, lookupSchemaName=Config.lookup_schema_name)
    createTmpCustomVocabulary(con=con, lookupSchemaName=Config.lookup_schema_name)
    dropTmpCustomVocabularyDist(con=con, lookupSchemaName=Config.lookup_schema_name)
    createTmpVocConcept(con=con, lookupSchemaName=Config.lookup_schema_name)
    createConceptFinal(con=con, lookupSchemaName=Config.lookup_schema_name)
    createConceptRelationshipFinal(con=con, lookupSchemaName=Config.lookup_schema_name)
    createTmpVocVocabulary(con=con, lookupSchemaName=Config.lookup_schema_name)
    createVocabularyFinal(con=con, lookupSchemaName=Config.lookup_schema_name)
    dropTmpTables(con=con, lookupSchemaName=Config.lookup_schema_name)


if __name__ == "__main__":


    def getConnnection():

        log.info("Establishing DB connection")
        import psycopg2

        # Connect to postgres with a copy of the MIMIC-III database
        con = psycopg2.connect(
            dbname=Config.sql_db_name,
            user=Config.sql_user_name,
            host=Config.sql_host_name,
            port=Config.sql_port_number,
            password=Config.sql_password
            )

        log.info("DB connection obtained successfully")

        return con


    con = getConnnection()
    generateTmpCustomMapping(con=con, etlSchemaName=Config.etl_schema_name)
