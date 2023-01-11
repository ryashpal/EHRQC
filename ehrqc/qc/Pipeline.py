from ehrqc.extract.Extract import extractMimicDemographics
from ehrqc.extract.Extract import extractOmopDemographics
from ehrqc.extract.Extract import extractMimicVitals
from ehrqc.extract.Extract import extractOmopVitals
from ehrqc.extract.Extract import extractMimicLabMeasurements
from ehrqc.extract.Extract import extractOmopLabMeasurements

from ehrqc.qc.demographicsGraphs import plot as plotDemographicsGraphs
from ehrqc.qc.vitalsGraphs import plot as plotVitalsGraphs
from ehrqc.qc.labMeasurementsGraphs import plot as plotLabMeasurementsGraphs

from ehrqc.qc.Impute import compare
from ehrqc.qc.Impute import impute

from ehrqc.Utils import getConnection


def run(savePath, source='mimic', schemaName = 'mimiciv', type='demographics', graph=False, imputeMissing=False):

    con = getConnection()

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
        filePath = savePath + '/' + str(source) + '_' + str(type) + '_raw_data.csv'
        data.to_csv(filePath)
    else:
        log.error('Unable to extract data, please check the parametrs and try again!')

    if (data is not None) and graph:
        log.info('generating graphs')
        filePath = savePath + '/' + str(source) + '_' + str(type) + '_plots.html'
        if (type == 'demographics'):
            plotDemographicsGraphs(
                df=data
                , outputFile = filePath
                )
        elif (type == 'vitals'):
            plotVitalsGraphs(
                df = data
                , outputFile = filePath
                )
        elif (type == 'lab_measurements'):
            plotLabMeasurementsGraphs(
                df = data
                , outputFile = filePath
                )

    if (data is not None) and imputeMissing:
        log.info('imputing missing data')
        fullData = data.dropna()
        meanR2, medianR2, knnR2, mfR2, emR2, miR2 = compare(fullData)

        log.info('mean: ', meanR2, 'median: ', medianR2, 'knn: ', knnR2, 'mf: ', mfR2, 'em: ', emR2, 'mi: ', miR2)

        if (meanR2 == max([meanR2, medianR2, knnR2, mfR2, emR2, miR2])):
            log.info('Using mean imputation')
            data = impute(data, 'mean')
        elif (medianR2 == max([meanR2, medianR2, knnR2, mfR2, emR2, miR2])):
            log.info('Using median imputation')
            data = impute(data, 'median')
        elif (knnR2 == max([meanR2, medianR2, knnR2, mfR2, emR2, miR2])):
            log.info('Using knn imputation')
            data = impute(data, 'knn')
        elif (mfR2 == max([meanR2, medianR2, knnR2, mfR2, emR2, miR2])):
            log.info('Using mf imputation')
            data = impute(data, 'miss_forest')
        elif (emR2 == max([meanR2, medianR2, knnR2, mfR2, emR2, miR2])):
            log.info('Using em imputation')
            data = impute(data, 'expectation_maximization')
        elif (miR2 == max([meanR2, medianR2, knnR2, mfR2, emR2, miR2])):
            log.info('Using mi imputation')
            data = impute(data, 'multiple_imputation')

        log.info('Saving imputed data to file')
        filePath = savePath + '/' + str(source) + '_' + str(type) + '_imputed_data.csv'
        data.to_csv(filePath)


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

    parser.add_argument('save_path', nargs=1, default='data.csv',
                        help='Path of the folder to store the outputs')

    parser.add_argument('source_db', nargs=1, default='mimic4',
                        help='Source name [mimic, omop]')

    parser.add_argument('data_type', nargs=1, default='demographics',
                        help='Data type name [demographics, vitals, lab_measurements]')

    parser.add_argument('schema_name', nargs=1, default='mimiciv',
                        help='Source schema name')

    parser.add_argument('-d', '--draw_graphs', action='store_true',
                        help='Draw graphs to visualise EHR data quality')

    parser.add_argument('-i', '--impute_missing', action='store_true',
                        help='Impute missing values by automatically selecting the best imputation strategy for this data')

    args = parser.parse_args()

    log.info('args.save_path: ' + str(args.save_path[0]))
    log.info('args.source_db: ' + str(args.source_db[0]))
    log.info('args.data_type: ' + str(args.data_type[0]))
    log.info('args.schema_name: ' + str(args.schema_name[0]))
    log.info('args.draw_graphs: ' + str(args.draw_graphs))
    log.info('args.impute_missing: ' + str(args.impute_missing))

    run(savePath=args.save_path[0], source=args.source_db[0], schemaName = args.schema_name[0], type=args.data_type[0], graph=args.draw_graphs, imputeMissing=args.impute_missing)

    log.info('Done!!')
