import logging
import sys

import argparse

log = logging.getLogger("Standardise")
log.setLevel(logging.INFO)
format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(format)
log.addHandler(ch)

from ehrqc.standardise.migrate_omop import Lookup
from ehrqc.standardise.migrate_omop import Import
from ehrqc.standardise.migrate_omop import Stage
from ehrqc.standardise.migrate_omop.etl import Location
from ehrqc.standardise.migrate_omop.etl import Person
from ehrqc.standardise.migrate_omop.etl import Death
from ehrqc.standardise.migrate_omop.etl import CareSite
from ehrqc.standardise.migrate_omop.etl import Visit
from ehrqc.standardise.migrate_omop.etl import Measurement
from ehrqc.standardise.migrate_omop.etl import Diagnoses
from ehrqc.standardise.migrate_omop.etl import Procedure
from ehrqc.standardise.migrate_omop.etl import Observation
from ehrqc.standardise.migrate_omop.etl import ConditionOccurrence
from ehrqc.standardise.migrate_omop.etl import Specimen
from ehrqc.standardise.migrate_omop.etl import Drug
from ehrqc.standardise.migrate_omop.etl import DeviceExposure
from ehrqc.standardise.migrate_omop.etl import Relationship
from ehrqc.standardise.migrate_omop.etl import Source
from ehrqc.standardise.migrate_omop import Unload

from ehrqc import Config

from ehrqc.Utils import getConnection


def createSchema(con, schemaName):
    log.info("Creating schema: " + schemaName)
    createSchemaQuery = """create schema if not exists """ + schemaName
    with con:
        with con.cursor() as cursor:
            cursor.execute(createSchemaQuery)


def createLookup(con):
    log.info("Creating Lookups")
    Lookup.migrateStandardVocabulary(con=con)


def importCsv(con):
    log.info("Importing EHR data from CSV files")
    Import.importDataCsv(con=con, sourceSchemaName=Config.source_schema_name)


def generateCustomLookup(con):
    log.info("Generate Custom Lookups")
    Lookup.migrateCustomMapping(con=con, isFromFile=False)


def importCustomLookup(con):
    log.info("Import Custom Lookups")
    Lookup.migrateCustomMapping(con=con, isFromFile=True)


def processCustomLookup(con):
    log.info("Process Custom Lookups")
    Lookup.processCustomVocabulary(con=con)


def stageData(con):
    log.info("Staging EHR data")
    Stage.migrate(con=con, sourceSchemaName=Config.source_schema_name, destinationSchemaName=Config.etl_schema_name)


def performETL(con):
    log.info("Performing ETL")
    # Run the following code in the given order

    # 01. Location
    Location.migrate(con=con, etlSchemaName=Config.etl_schema_name)

    # 02. Person
    Person.migrate(con=con, etlSchemaName=Config.etl_schema_name)

    # 03. Death
    Death.migrate(con=con, etlSchemaName=Config.etl_schema_name)

    # 04. Care Site
    CareSite.migrate(con=con, etlSchemaName=Config.etl_schema_name)

    # 05. Visit Occurrence Part 1
    Visit.migratePart1(con=con, etlSchemaName=Config.etl_schema_name)

    # 06. Measurement Units
    Measurement.migrateUnits(con=con, etlSchemaName=Config.etl_schema_name)

    # 07. Measurement Chartevents
    Measurement.migrateChartevents(con=con, etlSchemaName=Config.etl_schema_name)

    # 08. Measurement Labevents
    Measurement.migrateLabevents(con=con, etlSchemaName=Config.etl_schema_name)

    # 09. Measurement Specimen
    Measurement.migrateSpecimen(con=con, etlSchemaName=Config.etl_schema_name)

    # 10. Visit Occurrence Part 2
    Visit.migratePart2(con=con, etlSchemaName=Config.etl_schema_name)

    # 11. Visit Occurrence
    Visit.migrateVisitOccurrence(con=con, etlSchemaName=Config.etl_schema_name)

    # 12. Visit Detail
    Visit.migrateVisitDetail(con=con, etlSchemaName=Config.etl_schema_name)

    # 13. Diagnosis
    Diagnoses.migrate(con=con, etlSchemaName=Config.etl_schema_name)

    # 14. Procedure Lookup
    Procedure.migrateLookup(con=con, etlSchemaName=Config.etl_schema_name)

    # 15. Observation Lookup
    Observation.migrateLookup(con=con, etlSchemaName=Config.etl_schema_name)

    # 16. Condition Occurrence
    ConditionOccurrence.migrate(con=con, etlSchemaName=Config.etl_schema_name)

    # 17. Procedure occurrence
    Procedure.migrate(con=con, etlSchemaName=Config.etl_schema_name)

    # 18. Specimen
    Specimen.migrate(con=con, etlSchemaName=Config.etl_schema_name)

    # 19. Measurement
    Measurement.migrate(con=con, etlSchemaName=Config.etl_schema_name)

    # 20. Drug Lookup
    Drug.migrateLookup(con=con, etlSchemaName=Config.etl_schema_name)

    # 21. Drug Exposure
    Drug.migrate(con=con, etlSchemaName=Config.etl_schema_name)

    # 22. Device Exposure
    DeviceExposure.migrate(con=con, etlSchemaName=Config.etl_schema_name)

    # 23. Observation
    Observation.migrate(con=con, etlSchemaName=Config.etl_schema_name)

    # 24. Observation Period
    Observation.migratePeriod(con=con, etlSchemaName=Config.etl_schema_name)

    # 25. Person Final
    Person.migrateFinal(con=con, etlSchemaName=Config.etl_schema_name)

    # 26. Fact Relationship
    Relationship.migrate(con=con, etlSchemaName=Config.etl_schema_name)

    # 27. Condition Era
    ConditionOccurrence.migrateConditionEra(con=con, etlSchemaName=Config.etl_schema_name)

    # 28. Drug Era
    Drug.migrateDrugEra(con=con, etlSchemaName=Config.etl_schema_name)

    # 29. Dose Era
    Drug.migrateDoseEra(con=con, etlSchemaName=Config.etl_schema_name)

    # 30. 
    # Not migrated

    # 31. Source
    Source.migrate(con=con, etlSchemaName=Config.etl_schema_name)


def unloadData(con):
    log.info("Unloading migrated data to CDM schema")
    # 98. Unload Vocabulary
    Unload.unloadVoc(con=con, vocSchemaName=Config.lookup_schema_name, cdmSchemaName=Config.cdm_schema_name)

    # 99. Unload data
    Unload.unloadData(con=con, etlSchemaName=Config.etl_schema_name, cdmSchemaName=Config.cdm_schema_name)


if __name__ == "__main__":

    log.info("Parsing command line arguments")

    parser = argparse.ArgumentParser(description='Migrate EHR to OMOP-CDM')
    parser.add_argument('-l', '--create_lookup', action='store_true',
                        help='Create lookup by importing Athena vocabulary and custom mapping')
    parser.add_argument('-f', '--import_file', action='store_true',
                        help='Import EHR from a csv files')
    parser.add_argument('-s', '--stage', action='store_true',
                        help='Stage the data on the ETL schema')
    parser.add_argument('-m', '--generate_mapping', action='store_true',
                        help='Generate custom mapping of concepts from the data')
    parser.add_argument('-c', '--import_custom_mapping', action='store_true',
                        help='Import custom mapping file')
    parser.add_argument('-e', '--perform_etl', action='store_true',
                        help='Perform migration Extract-Transform-Load (ETL) operations')
    parser.add_argument('-u', '--unload', action='store_true',
                        help='Unload data to CDM schema')

    args = parser.parse_args()

    log.info("Start!!")

    con = getConnection()

    if args.create_lookup:
        createSchema(con=con, schemaName=Config.lookup_schema_name)

    if args.import_file:
        createSchema(con=con, schemaName=Config.source_schema_name)

    if args.create_lookup or args.import_file or args.stage or args.generate_mapping or args.import_custom_mapping or args.perform_etl:
        createSchema(con=con, schemaName=Config.etl_schema_name)

    if args.unload:
        createSchema(con=con, schemaName=Config.cdm_schema_name)

    if args.create_lookup:
        createLookup(con=con)

    if args.import_file:
        importCsv(con=con)

    if args.stage:
        stageData(con=con)

    if args.generate_mapping:
        generateCustomLookup(con=con)

    if args.import_custom_mapping:
        importCustomLookup(con=con)

    if args.generate_mapping or args.import_custom_mapping:
        processCustomLookup(con=con)

    if args.perform_etl:
        performETL(con=con)

    if args.unload:
        unloadData(con=con)

    log.info("End!!")
