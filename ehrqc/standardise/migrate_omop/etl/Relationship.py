import logging

log = logging.getLogger("Standardise")


def createFactRelationship(con, etlSchemaName):
    log.info("Creating table: " + etlSchemaName + ".cdm_fact_relationship")
    dropQuery = """drop table if exists """ + etlSchemaName + """.cdm_fact_relationship cascade"""
    createQuery = """CREATE TABLE """ + etlSchemaName + """.cdm_fact_relationship
        (
            domain_concept_id_1     INTEGER     not null ,
            fact_id_1               INTEGER     not null ,
            domain_concept_id_2     INTEGER     not null ,
            fact_id_2               INTEGER     not null ,
            relationship_concept_id INTEGER     not null ,
            -- 
            unit_id                       TEXT  
        )
        ;
        """
    insertS2MQuery = """INSERT INTO """ + etlSchemaName + """.cdm_fact_relationship
        SELECT
            36                      AS domain_concept_id_1, -- Specimen
            spec.specimen_id        AS fact_id_1,
            21                      AS domain_concept_id_2, -- Measurement
            org.measurement_id      AS fact_id_2,
            32669                   AS relationship_concept_id, -- Specimen to Measurement   Standard
            'fact.spec.test'        AS unit_id
        FROM
            """ + etlSchemaName + """.lk_specimen_mapped spec
        INNER JOIN
            """ + etlSchemaName + """.lk_meas_organism_mapped org
                ON org.trace_id_spec = spec.trace_id
        ;
        """
    insertM2SQuery = """INSERT INTO """ + etlSchemaName + """.cdm_fact_relationship
        SELECT
            21                      AS domain_concept_id_1, -- Measurement
            org.measurement_id      AS fact_id_1,
            36                      AS domain_concept_id_2, -- Specimen
            spec.specimen_id        AS fact_id_2,
            32668                   AS relationship_concept_id, -- Measurement to Specimen   Standard
            'fact.test.spec'        AS unit_id
        FROM
            """ + etlSchemaName + """.lk_specimen_mapped spec
        INNER JOIN
            """ + etlSchemaName + """.lk_meas_organism_mapped org
                ON org.trace_id_spec = spec.trace_id
        ;
        """
    insertP2CQuery = """INSERT INTO """ + etlSchemaName + """.cdm_fact_relationship
        SELECT
            21                      AS domain_concept_id_1, -- Measurement
            org.measurement_id      AS fact_id_1,
            21                      AS domain_concept_id_2, -- Measurement
            ab.measurement_id       AS fact_id_2,
            581436                  AS relationship_concept_id, -- Parent to Child Measurement   Standard
            'fact.test.ab'          AS unit_id
        FROM
            """ + etlSchemaName + """.lk_meas_organism_mapped org
        INNER JOIN
            """ + etlSchemaName + """.lk_meas_ab_mapped ab
                ON ab.trace_id_org = org.trace_id
        ;
        """
    insertC2PQuery = """INSERT INTO """ + etlSchemaName + """.cdm_fact_relationship
        SELECT
            21                      AS domain_concept_id_1, -- Measurement
            ab.measurement_id       AS fact_id_1,
            21                      AS domain_concept_id_2, -- Measurement
            org.measurement_id      AS fact_id_2,
            581437                  AS relationship_concept_id, -- Child to Parent Measurement   Standard
            'fact.ab.test'          AS unit_id
        FROM
            """ + etlSchemaName + """.lk_meas_organism_mapped org
        INNER JOIN
            """ + etlSchemaName + """.lk_meas_ab_mapped ab
                ON ab.trace_id_org = org.trace_id
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
            cursor.execute(insertS2MQuery)
            cursor.execute(insertM2SQuery)
            cursor.execute(insertP2CQuery)
            cursor.execute(insertC2PQuery)


def migrate(con, etlSchemaName):
    createFactRelationship(con = con, etlSchemaName = etlSchemaName)
