import logging

log = logging.getLogger("Standardise")

def unloadConcept(con, vocSchemaName, cdmSchemaName):
    log.info("Unloading table: " + vocSchemaName + ".concept")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.concept cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.concept AS 
        SELECT
            concept_id,
            concept_name,
            domain_id,
            vocabulary_id,
            concept_class_id,
            standard_concept,
            concept_code,
            valid_start_DATE,
            valid_end_DATE,
            invalid_reason
        FROM """ + vocSchemaName + """.concept
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadVocabulary(con, vocSchemaName, cdmSchemaName):
    log.info("Unloading table: " + vocSchemaName + ".vocabulary")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.vocabulary cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.vocabulary AS 
        SELECT
            vocabulary_id,
            vocabulary_name,
            vocabulary_reference,
            vocabulary_version,
            vocabulary_concept_id
        FROM """ + vocSchemaName+ """.vocabulary
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadDomain(con, vocSchemaName, cdmSchemaName):
    log.info("Unloading table: " + vocSchemaName + ".domain")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.domain cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.domain AS 
        SELECT
            domain_id,
            domain_name,
            domain_concept_id
        FROM """ + vocSchemaName+ """.domain
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadConceptClass(con, vocSchemaName, cdmSchemaName):
    log.info("Unloading table: " + vocSchemaName + ".concept_class")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.concept_class cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.concept_class AS 
        SELECT
            concept_class_id,
            concept_class_name,
            concept_class_concept_id
        FROM """ + vocSchemaName+ """.concept_class
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadConceptRelationship(con, vocSchemaName, cdmSchemaName):
    log.info("Unloading table: " + vocSchemaName + ".concept_relationship")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.concept_relationship cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.concept_relationship AS 
        SELECT
            concept_id_1,
            concept_id_2,
            relationship_id,
            valid_start_DATE,
            valid_end_DATE,
            invalid_reason
        FROM """ + vocSchemaName+ """.concept_relationship
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadRelationship(con, vocSchemaName, cdmSchemaName):
    log.info("Unloading table: " + vocSchemaName + ".relationship")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.relationship cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.relationship AS 
        SELECT
            relationship_id,
            relationship_name,
            is_hierarchical,
            defines_ancestry,
            reverse_relationship_id,
            relationship_concept_id
        FROM """ + vocSchemaName+ """.relationship
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadConceptSynonym(con, vocSchemaName, cdmSchemaName):
    log.info("Unloading table: " + vocSchemaName + ".concept_synonym")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.concept_synonym cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.concept_synonym AS 
        SELECT
            concept_id,
            concept_synonym_name,
            language_concept_id
        FROM """ + vocSchemaName+ """.concept_synonym
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadConceptAncestor(con, vocSchemaName, cdmSchemaName):
    log.info("Unloading table: " + vocSchemaName + ".concept_ancestor")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.concept_ancestor cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.concept_ancestor AS 
        SELECT
            ancestor_concept_id,
            descendant_concept_id,
            min_levels_of_separation,
            max_levels_of_separation
        FROM """ + vocSchemaName+ """.concept_ancestor
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadSource(con, etlSchemaName, cdmSchemaName):
    log.info("Unloading table: " + cdmSchemaName + ".cdm_source")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.cdm_source cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.cdm_source AS 
        SELECT
            cdm_source_name,
            cdm_source_abbreviation,
            cdm_holder,
            source_description,
            source_documentation_reference,
            cdm_etl_reference,
            source_release_date,
            cdm_release_date,
            cdm_version,
            vocabulary_version
        FROM """ + etlSchemaName+ """.cdm_cdm_source
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadPerson(con, etlSchemaName, cdmSchemaName):
    log.info("Unloading table: " + cdmSchemaName + ".person")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.person cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.person AS 
        SELECT
            person_id,
            gender_concept_id,
            year_of_birth,
            month_of_birth,
            day_of_birth,
            birth_datetime,
            race_concept_id,
            ethnicity_concept_id,
            location_id,
            provider_id,
            care_site_id,
            person_source_value,
            gender_source_value,
            gender_source_concept_id,
            race_source_value,
            race_source_concept_id,
            ethnicity_source_value,
            ethnicity_source_concept_id
        FROM """ + etlSchemaName+ """.cdm_person
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadObservationPeriod(con, etlSchemaName, cdmSchemaName):
    log.info("Unloading table: " + cdmSchemaName + ".observation_period")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.observation_period cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.observation_period AS 
        SELECT
            observation_period_id,
            person_id,
            observation_period_start_date,
            observation_period_end_date,
            period_type_concept_id
        FROM """ + etlSchemaName+ """.cdm_observation_period
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadSpecimen(con, etlSchemaName, cdmSchemaName):
    log.info("Unloading table: " + cdmSchemaName + ".specimen")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.specimen cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.specimen AS 
        SELECT
            specimen_id,
            person_id,
            specimen_concept_id,
            specimen_type_concept_id,
            specimen_date,
            specimen_datetime,
            quantity,
            unit_concept_id,
            anatomic_site_concept_id,
            disease_status_concept_id,
            specimen_source_id,
            specimen_source_value,
            unit_source_value,
            anatomic_site_source_value,
            disease_status_source_value
        FROM """ + etlSchemaName+ """.cdm_specimen
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadDeath(con, etlSchemaName, cdmSchemaName):
    log.info("Unloading table: " + cdmSchemaName + ".death")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.death cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.death AS 
        SELECT
            person_id,
            death_date,
            death_datetime,
            death_type_concept_id,
            cause_concept_id,
            cause_source_value,
            cause_source_concept_id
        FROM """ + etlSchemaName+ """.cdm_death
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadVisitOccurrence(con, etlSchemaName, cdmSchemaName):
    log.info("Unloading table: " + cdmSchemaName + ".visit_occurrence")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.visit_occurrence cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.visit_occurrence AS 
        SELECT
            visit_occurrence_id,
            person_id,
            visit_concept_id,
            visit_start_date,
            visit_start_datetime,
            visit_end_date,
            visit_end_datetime,
            visit_type_concept_id,
            provider_id,
            care_site_id,
            visit_source_value,
            visit_source_concept_id,
            admitting_source_concept_id,
            admitting_source_value,
            discharge_to_concept_id,
            discharge_to_source_value,
            preceding_visit_occurrence_id
        FROM """ + etlSchemaName+ """.cdm_visit_occurrence
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadVisitDetail(con, etlSchemaName, cdmSchemaName):
    log.info("Unloading table: " + cdmSchemaName + ".visit_detail")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.visit_detail cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.visit_detail AS 
        SELECT
            visit_detail_id,
            person_id,
            visit_detail_concept_id,
            visit_detail_start_date,
            visit_detail_start_datetime,
            visit_detail_end_date,
            visit_detail_end_datetime,
            visit_detail_type_concept_id,
            provider_id,
            care_site_id,
            admitting_source_concept_id,
            discharge_to_concept_id,
            preceding_visit_detail_id,
            visit_detail_source_value,
            visit_detail_source_concept_id,
            admitting_source_value,
            discharge_to_source_value,
            visit_detail_parent_id,
            visit_occurrence_id
        FROM """ + etlSchemaName+ """.cdm_visit_detail
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadProcedureOccurrence(con, etlSchemaName, cdmSchemaName):
    log.info("Unloading table: " + cdmSchemaName + ".procedure_occurrence")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.procedure_occurrence cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.procedure_occurrence AS 
        SELECT
            procedure_occurrence_id,
            person_id,
            procedure_concept_id,
            procedure_date,
            procedure_datetime,
            procedure_type_concept_id,
            modifier_concept_id,
            quantity,
            provider_id,
            visit_occurrence_id,
            visit_detail_id,
            procedure_source_value,
            procedure_source_concept_id,
            modifier_source_value
        FROM """ + etlSchemaName+ """.cdm_procedure_occurrence
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadDrugExposure(con, etlSchemaName, cdmSchemaName):
    log.info("Unloading table: " + cdmSchemaName + ".drug_exposure")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.drug_exposure cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.drug_exposure AS 
        SELECT
            drug_exposure_id,
            person_id,
            drug_concept_id,
            drug_exposure_start_date,
            drug_exposure_start_datetime,
            drug_exposure_end_date,
            drug_exposure_end_datetime,
            verbatim_end_date,
            drug_type_concept_id,
            stop_reason,
            refills,
            quantity,
            days_supply,
            sig,
            route_concept_id,
            lot_number,
            provider_id,
            visit_occurrence_id,
            visit_detail_id,
            drug_source_value,
            drug_source_concept_id,
            route_source_value,
            dose_unit_source_value
        FROM """ + etlSchemaName+ """.cdm_drug_exposure
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadDeviceExposure(con, etlSchemaName, cdmSchemaName):
    log.info("Unloading table: " + cdmSchemaName + ".device_exposure")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.device_exposure cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.device_exposure AS 
        SELECT
            device_exposure_id,
            person_id,
            device_concept_id,
            device_exposure_start_date,
            device_exposure_start_datetime,
            device_exposure_end_date,
            device_exposure_end_datetime,
            device_type_concept_id,
            unique_device_id,
            quantity,
            provider_id,
            visit_occurrence_id,
            visit_detail_id,
            device_source_value,
            device_source_concept_id
        FROM """ + etlSchemaName+ """.cdm_device_exposure
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadConditionOccurrence(con, etlSchemaName, cdmSchemaName):
    log.info("Unloading table: " + cdmSchemaName + ".condition_occurrence")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.condition_occurrence cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.condition_occurrence AS 
        SELECT
            condition_occurrence_id,
            person_id,
            condition_concept_id,
            condition_start_date,
            condition_start_datetime,
            condition_end_date,
            condition_end_datetime,
            condition_type_concept_id,
            stop_reason,
            provider_id,
            visit_occurrence_id,
            visit_detail_id,
            condition_source_value,
            condition_source_concept_id,
            condition_status_source_value,
            condition_status_concept_id
        FROM """ + etlSchemaName+ """.cdm_condition_occurrence
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadMeasurement(con, etlSchemaName, cdmSchemaName):
    log.info("Unloading table: " + cdmSchemaName + ".measurement")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.measurement cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.measurement AS 
        SELECT
            measurement_id,
            person_id,
            measurement_concept_id,
            measurement_date,
            measurement_datetime,
            measurement_time,
            measurement_type_concept_id,
            operator_concept_id,
            value_as_number,
            value_as_concept_id,
            unit_concept_id,
            range_low,
            range_high,
            provider_id,
            visit_occurrence_id,
            visit_detail_id,
            measurement_source_value,
            measurement_source_concept_id,
            unit_source_value,
            value_source_value
        FROM """ + etlSchemaName+ """.cdm_measurement
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadObservation(con, etlSchemaName, cdmSchemaName):
    log.info("Unloading table: " + cdmSchemaName + ".observation")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.observation cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.observation AS 
        SELECT
            observation_id,
            person_id,
            observation_concept_id,
            observation_date,
            observation_datetime,
            observation_type_concept_id,
            value_as_number,
            value_as_string,
            value_as_concept_id,
            qualifier_concept_id,
            unit_concept_id,
            provider_id,
            visit_occurrence_id,
            visit_detail_id,
            observation_source_value,
            observation_source_concept_id,
            unit_source_value,
            qualifier_source_value
        FROM """ + etlSchemaName+ """.cdm_observation
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadFactRelationship(con, etlSchemaName, cdmSchemaName):
    log.info("Unloading table: " + cdmSchemaName + ".fact_relationship")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.fact_relationship cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.fact_relationship AS 
        SELECT
            domain_concept_id_1,
            fact_id_1,
            domain_concept_id_2,
            fact_id_2,
            relationship_concept_id
        FROM """ + etlSchemaName+ """.cdm_fact_relationship
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadLocation(con, etlSchemaName, cdmSchemaName):
    log.info("Unloading table: " + cdmSchemaName + ".fact_relationship")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.location cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.location AS 
        SELECT
            location_id,
            address_1,
            address_2,
            city,
            state,
            zip,
            county,
            location_source_value
        FROM """ + etlSchemaName+ """.cdm_location
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadCareSite(con, etlSchemaName, cdmSchemaName):
    log.info("Unloading table: " + cdmSchemaName + ".care_site")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.care_site cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.care_site AS 
        SELECT
            care_site_id,
            care_site_name,
            place_of_service_concept_id,
            location_id,
            care_site_source_value,
            place_of_service_source_value
        FROM """ + etlSchemaName+ """.cdm_care_site
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadDrugEra(con, etlSchemaName, cdmSchemaName):
    log.info("Unloading table: " + cdmSchemaName + ".drug_era")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.drug_era cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.drug_era AS 
        SELECT
            drug_era_id,
            person_id,
            drug_concept_id,
            drug_era_start_date,
            drug_era_end_date,
            drug_exposure_count,
            gap_days
        FROM """ + etlSchemaName+ """.cdm_drug_era
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


def unloadDoseEra(con, etlSchemaName, cdmSchemaName):
    log.info("Unloading table: " + cdmSchemaName + ".dose_era")
    dropQuery = """drop table if exists """ + cdmSchemaName + """.dose_era cascade"""
    createQuery = """CREATE TABLE """ + cdmSchemaName + """.dose_era AS 
        SELECT
            dose_era_id,
            person_id,
            drug_concept_id,
            unit_concept_id,
            dose_value,
            dose_era_start_date,
            dose_era_end_date
        FROM """ + etlSchemaName+ """.cdm_dose_era;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)


# def unloadConditionEra(con, etlSchemaName, cdmSchemaName):
#     log.info("Unloading table: " + etlSchemaName + ".condition_era")
#     dropQuery = """drop table if exists """ + cdmSchemaName + """.condition_era cascade"""
#     createQuery = """CREATE TABLE """ + cdmSchemaName + """.condition_era AS 
#         SELECT
#             condition_era_id,
#             person_id,
#             condition_concept_id,
#             condition_era_start_date,
#             condition_era_end_date,
#             condition_occurrence_count
#         FROM """ + etlSchemaName+ """.cdm_condition_era
#         ;
#         """
#     with con:
#         with con.cursor() as cursor:
#             cursor.execute(dropQuery)
#             cursor.execute(createQuery)


def unloadVoc(con, vocSchemaName, cdmSchemaName):
    unloadConcept(con=con, vocSchemaName=vocSchemaName, cdmSchemaName=cdmSchemaName)
    unloadVocabulary(con=con, vocSchemaName=vocSchemaName, cdmSchemaName=cdmSchemaName)
    unloadDomain(con=con, vocSchemaName=vocSchemaName, cdmSchemaName=cdmSchemaName)
    unloadConceptClass(con=con, vocSchemaName=vocSchemaName, cdmSchemaName=cdmSchemaName)
    unloadConceptRelationship(con=con, vocSchemaName=vocSchemaName, cdmSchemaName=cdmSchemaName)
    unloadRelationship(con=con, vocSchemaName=vocSchemaName, cdmSchemaName=cdmSchemaName)
    unloadConceptSynonym(con=con, vocSchemaName=vocSchemaName, cdmSchemaName=cdmSchemaName)
    unloadConceptAncestor(con=con, vocSchemaName=vocSchemaName, cdmSchemaName=cdmSchemaName)


def unloadData(con, etlSchemaName, cdmSchemaName):
    unloadSource(con=con, etlSchemaName=etlSchemaName, cdmSchemaName=cdmSchemaName)
    unloadPerson(con=con, etlSchemaName=etlSchemaName, cdmSchemaName=cdmSchemaName)
    unloadObservationPeriod(con=con, etlSchemaName=etlSchemaName, cdmSchemaName=cdmSchemaName)
    unloadSpecimen(con=con, etlSchemaName=etlSchemaName, cdmSchemaName=cdmSchemaName)
    unloadDeath(con=con, etlSchemaName=etlSchemaName, cdmSchemaName=cdmSchemaName)
    unloadVisitOccurrence(con=con, etlSchemaName=etlSchemaName, cdmSchemaName=cdmSchemaName)
    unloadVisitDetail(con=con, etlSchemaName=etlSchemaName, cdmSchemaName=cdmSchemaName)
    unloadProcedureOccurrence(con=con, etlSchemaName=etlSchemaName, cdmSchemaName=cdmSchemaName)
    unloadDrugExposure(con=con, etlSchemaName=etlSchemaName, cdmSchemaName=cdmSchemaName)
    unloadDeviceExposure(con=con, etlSchemaName=etlSchemaName, cdmSchemaName=cdmSchemaName)
    unloadConditionOccurrence(con=con, etlSchemaName=etlSchemaName, cdmSchemaName=cdmSchemaName)
    unloadMeasurement(con=con, etlSchemaName=etlSchemaName, cdmSchemaName=cdmSchemaName)
    unloadObservation(con=con, etlSchemaName=etlSchemaName, cdmSchemaName=cdmSchemaName)
    unloadFactRelationship(con=con, etlSchemaName=etlSchemaName, cdmSchemaName=cdmSchemaName)
    unloadLocation(con=con, etlSchemaName=etlSchemaName, cdmSchemaName=cdmSchemaName)
    unloadCareSite(con=con, etlSchemaName=etlSchemaName, cdmSchemaName=cdmSchemaName)
    unloadDrugEra(con=con, etlSchemaName=etlSchemaName, cdmSchemaName=cdmSchemaName)
    unloadDoseEra(con=con, etlSchemaName=etlSchemaName, cdmSchemaName=cdmSchemaName)
    # unloadConditionEra(con=con, etlSchemaName=etlSchemaName, cdmSchemaName=cdmSchemaName)
