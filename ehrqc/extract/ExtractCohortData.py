import os

import pandas as pd
import numpy as np

from ehrqc.Utils import saveDataframe


def extractDemographics(con, schemaName):
    curDir = os.path.dirname(__file__)
    demographicsPath = os.path.join(curDir, 'sql/cohort/mimic_omop_demographics.sql')
    demographicsFile = open(demographicsPath)
    demographicsQuery = demographicsFile.read()
    demographicsQuery = demographicsQuery.replace('__schema_name__', schemaName)
    demographicsDf = pd.read_sql_query(demographicsQuery, con)
    return demographicsDf


def extractVitals(con, schemaName, aggFunction='min', before=24, after=24):
    curDir = os.path.dirname(__file__)
    vitalsQuery = None
    if(aggFunction  in ['min', 'max', 'avg', 'stddev']):
        vitalsPath = os.path.join(curDir, 'sql/cohort/mimic_omop_selected_vitals_1.sql')
        vitalsFile = open(vitalsPath)
        vitalsQuery = vitalsFile.read()
        vitalsQuery = vitalsQuery.replace('__agg_function__', aggFunction)
    elif(aggFunction in ['first', 'last']):
        vitalsPath = os.path.join(curDir, 'sql/cohort/mimic_omop_selected_vitals_2.sql')
        vitalsFile = open(vitalsPath)
        vitalsQuery = vitalsFile.read()
        if(aggFunction == 'first'):
            vitalsQuery = vitalsQuery.replace('__order__', 'asc')
        else:
            vitalsQuery = vitalsQuery.replace('__order__', 'desc')
    vitalsDf = None
    if vitalsQuery:
        vitalsQuery = vitalsQuery.replace('__schema_name__', schemaName)
        vitalsQuery = vitalsQuery.replace('__before_time__', before)
        vitalsQuery = vitalsQuery.replace('__after_time__', after)
        vitalsDf = pd.read_sql_query(vitalsQuery, con)
    return vitalsDf


def extractLabMeasurements(con, schemaName, aggFunction='min', before=24, after=24):
    curDir = os.path.dirname(__file__)
    labMeasurementsQuery = None
    if(aggFunction  in ['min', 'max', 'avg', 'stddev']):
        labMeasurementsPath = os.path.join(curDir, 'sql/cohort/mimic_omop_selected_lab_measurements_1.sql')
        labMeasurementsFile = open(labMeasurementsPath)
        labMeasurementsQuery = labMeasurementsFile.read()
        labMeasurementsQuery = labMeasurementsQuery.replace('__agg_function__', aggFunction)
    elif(aggFunction in ['first', 'last']):
        labMeasurementsPath = os.path.join(curDir, 'sql/cohort/mimic_omop_selected_lab_measurements_2.sql')
        labMeasurementsFile = open(labMeasurementsPath)
        labMeasurementsQuery = labMeasurementsFile.read()
        if(aggFunction == 'first'):
            labMeasurementsQuery = labMeasurementsQuery.replace('__order__', 'asc')
        else:
            labMeasurementsQuery = labMeasurementsQuery.replace('__order__', 'desc')
    labMeasurementsDf = None
    if labMeasurementsQuery:
        labMeasurementsQuery = labMeasurementsQuery.replace('__schema_name__', schemaName)
        labMeasurementsQuery = labMeasurementsQuery.replace('__before_time__', before)
        labMeasurementsQuery = labMeasurementsQuery.replace('__after_time__', after)
        labMeasurementsDf = pd.read_sql_query(labMeasurementsQuery, con)
    return labMeasurementsDf


def extract(con, savePath='data/data.csv', cohortPath = 'data/cohort.csv', schemaName = 'mimiciv', type='demographics', aggFunction='min', before=24, after=24):
    log.info("Creating table: " + schemaName + ".COHORT")
    dropQuery = """DROP TABLE IF EXISTS """ + schemaName + """.COHORT CASCADE"""
    createQuery = """CREATE TABLE """ + schemaName + """.COHORT
        (
            PERSON_ID INT NOT NULL,
            ANCHOR_TIME TIMESTAMP(0)
        )
        ;
        """
    with con:
        with con.cursor() as cursor:
            cursor.execute(dropQuery)
            cursor.execute(createQuery)
    log.info("Table: " + schemaName + ".COHORT created successfully!!")

    log.info('Saving cohort to table: ' + schemaName + '.COHORT')
    df = pd.read_csv(cohortPath)
    df.replace({np.nan: None}, inplace=True)
    saveDataframe(con=con, destinationSchemaName=schemaName, destinationTableName='COHORT', df=df, dfColumns=df.columns)
    log.info('Cohort data saved successfully!!')
    log.info('extracting data')
    data = None
    if(type == 'demographics'):
        data = extractDemographics(con, schemaName = schemaName)
    if(type == 'vitals'):
        data = extractVitals(con, schemaName = schemaName, aggFunction=aggFunction, before=before, after=after)
    elif(type == 'lab_measurements'):
        data = extractLabMeasurements(con, schemaName = schemaName, aggFunction=aggFunction, before=before, after=after)
    if data is not None:
        log.info('Saving raw data to file')
        data.to_csv(savePath, index=False)
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

    parser.add_argument('save_path', nargs=1, default='data/data.csv',
                        help='Path of the file to store the outputs')

    parser.add_argument('cohort_path', nargs=1, default='data/cohort.csv',
                        help='Path of the file containing cohort data (Person IDs and the anchor timestamp)')

    parser.add_argument('schema_name', nargs=1, default='mimiciv',
                        help='Source schema name')

    parser.add_argument('data_type', nargs=1, default='demographics',
                        help='Data type name [demographics, vitals, lab_measurements]')

    parser.add_argument('agg_function', nargs=1, default='min',
                        help='Aggregation Function to use [min, max, avg, stddev, first, last]')

    parser.add_argument('-b', '--before', default=24,
                        help='Define the amount of time window before anchor point (in hours)')

    parser.add_argument('-a', '--after', default=24,
                        help='Define the amount of time window after anchor point (in hours)')

    args = parser.parse_args()

    log.info('args.save_path: ' + str(args.save_path[0]))
    log.info('args.cohort_path: ' + str(args.cohort_path[0]))
    log.info('args.schema_name: ' + str(args.schema_name[0]))
    log.info('args.data_type: ' + str(args.data_type[0]))
    log.info('args.agg_function: ' + str(args.agg_function[0]))
    log.info('args.before: ' + str(args.before))
    log.info('args.after: ' + str(args.after))

    from ehrqc.Utils import getConnection

    con = getConnection()

    extract(con=con, savePath=args.save_path[0], cohortPath=args.cohort_path[0], schemaName = args.schema_name[0], type=args.data_type[0], aggFunction=args.agg_function[0], before=args.before, after=args.after)
