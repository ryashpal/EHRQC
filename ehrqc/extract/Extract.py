import os
import pandas as pd


def extractMimicDemographics(con, schemaName):
    curDir = os.path.dirname(__file__)
    mimicDemographicsPath = os.path.join(curDir, 'sql/mimic_demographics.sql')
    mimicDemographicsFile = open(mimicDemographicsPath)
    mimicDemographicsQuery = mimicDemographicsFile.read()
    mimicDemographicsQuery = mimicDemographicsQuery.replace('__schema_name__', schemaName)
    mimicDemographicsDf = pd.read_sql_query(mimicDemographicsQuery, con)
    return mimicDemographicsDf


def extractOmopDemographics(con, schemaName):
    curDir = os.path.dirname(__file__)
    mimicOmopDemographicsPath = os.path.join(curDir, 'sql/mimic_omop_demographics.sql')
    mimicOmopDemographicsFile = open(mimicOmopDemographicsPath)
    mimicOmopDemographicsQuery = mimicOmopDemographicsFile.read()
    mimicOmopDemographicsQuery = mimicOmopDemographicsQuery.replace('__schema_name__', schemaName)
    mimicOmopDemographicsDf = pd.read_sql_query(mimicOmopDemographicsQuery, con)
    return mimicOmopDemographicsDf


def extractMimicVitals(con, schemaName):
    curDir = os.path.dirname(__file__)
    mimicSelectedVitalsPath = os.path.join(curDir, 'sql/mimic_selected_vitals.sql')
    mimicSelectedVitalsFile = open(mimicSelectedVitalsPath)
    mimicSelectedVitalsQuery = mimicSelectedVitalsFile.read()
    mimicSelectedVitalsQuery = mimicSelectedVitalsQuery.replace('__schema_name__', schemaName)
    mimicSelectedVitalsDf = pd.read_sql_query(mimicSelectedVitalsQuery, con)
    return mimicSelectedVitalsDf


def extractOmopVitals(con, schemaName):
    curDir = os.path.dirname(__file__)
    mimicOmopSelectedVitalsPath = os.path.join(curDir, 'sql/mimic_omop_selected_vitals.sql')
    mimicOmopSelectedVitalsFile = open(mimicOmopSelectedVitalsPath)
    mimicOmopSelectedVitalsQuery = mimicOmopSelectedVitalsFile.read()
    mimicOmopSelectedVitalsQuery = mimicOmopSelectedVitalsQuery.replace('__schema_name__', schemaName)
    mimicOmopSelectedVitalsDf = pd.read_sql_query(mimicOmopSelectedVitalsQuery, con)
    return mimicOmopSelectedVitalsDf


def extractMimicLabMeasurements(con, schemaName):
    curDir = os.path.dirname(__file__)
    mimicLabMeasurementsPath = os.path.join(curDir, 'sql/mimic_lab_measurements.sql')
    mimicLabMeasurementsFile = open(mimicLabMeasurementsPath)
    mimicLabMeasurementsQuery = mimicLabMeasurementsFile.read()
    mimicLabMeasurementsQuery = mimicLabMeasurementsQuery.replace('__schema_name__', schemaName)
    mimicLabMeasurementsDf = pd.read_sql_query(mimicLabMeasurementsQuery, con)
    return mimicLabMeasurementsDf


def extractOmopLabMeasurements(con, schemaName):
    curDir = os.path.dirname(__file__)
    mimicOmopSelectedLabMeasurementsPath = os.path.join(curDir, 'sql/mimic_omop_selected_lab_measurements.sql')
    mimicOmopSelectedLabMeasurementsFile = open(mimicOmopSelectedLabMeasurementsPath)
    mimicOmopSelectedLabMeasurementsQuery = mimicOmopSelectedLabMeasurementsFile.read()
    mimicOmopSelectedLabMeasurementsQuery = mimicOmopSelectedLabMeasurementsQuery.replace('__schema_name__', schemaName)
    mimicOmopSelectedLabMeasurementsDf = pd.read_sql_query(mimicOmopSelectedLabMeasurementsQuery, con)
    return mimicOmopSelectedLabMeasurementsDf


def extract(con, savePath='temp', source='mimic', schemaName = 'mimiciv', type='demographics'):
    log.info('extracting data')
    data = None
    if (source == 'mimic') and (type == 'demographics'):
        data = extractMimicDemographics(con, schemaName = schemaName)
    elif (source == 'omop') and (type == 'demographics'):
        data = extractOmopDemographics(con, schemaName = schemaName)
    if (source == 'mimic') and (type == 'vitals'):
        data = extractMimicVitals(con, schemaName = schemaName)
    elif (source == 'omop') and (type == 'vitals'):
        data = extractOmopVitals(con, schemaName = schemaName)
    if (source == 'mimic') and (type == 'lab_measurements'):
        data = extractMimicLabMeasurements(con, schemaName = schemaName)
    elif (source == 'omop') and (type == 'lab_measurements'):
        data = extractOmopLabMeasurements(con, schemaName = schemaName)

    if data is not None:
        log.info('Saving raw data to file')
        data.to_csv(savePath)
    else:
        log.error('Unable to extract data, please check the parametrs and try again!')

if __name__ == '__main__':

    import logging
    import sys
    import argparse

    log = logging.getLogger("EHRQC")
    log.setLevel(logging.INFO)
    format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(format)
    log.addHandler(ch)

    log.info("Parsing command line arguments")

    parser = argparse.ArgumentParser(description='EHRQC')

    parser.add_argument('save_path', nargs=1, default='temp/data.csv',
                        help='Path of the file to store the outputs')

    parser.add_argument('source_db', nargs=1, default='mimic4',
                        help='Source name [mimic, omop]')

    parser.add_argument('data_type', nargs=1, default='demographics',
                        help='Data type name [demographics, vitals, lab_measurements]')

    parser.add_argument('schema_name', nargs=1, default='mimiciv',
                        help='Source schema name')

    args = parser.parse_args()

    log.info('args.save_path: ' + str(args.save_path[0]))
    log.info('args.source_db: ' + str(args.source_db[0]))
    log.info('args.data_type: ' + str(args.data_type[0]))
    log.info('args.schema_name: ' + str(args.schema_name[0]))

    from ehrqc.Utils import getConnection

    con = getConnection()

    extract(con=con, savePath=args.save_path[0], source=args.source_db[0], schemaName = args.schema_name[0], type=args.data_type[0])
