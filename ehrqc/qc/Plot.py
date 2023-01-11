import pandas as pd

from ehrqc.qc.demographicsGraphs import plot as plotDemographicsGraphs
from ehrqc.qc.vitalsGraphs import plot as plotVitalsGraphs
from ehrqc.qc.labMeasurementsGraphs import plot as plotLabMeasurementsGraphs
from ehrqc.qc.vitalsAnomalies import plot as plotVitalsAnomalies
from ehrqc.qc.labMeasurementsAnomalies import plot as plotLabMeasurementsAnomalies


def run(plotType = 'demographics', sourcePath = 'data.csv', savePath = 'plot.html'):

    dataDf = pd.read_csv(sourcePath)
    if (dataDf is not None):
        log.info('generating graphs')
        if (plotType == 'demographics_explore'):
            plotDemographicsGraphs(
                df=dataDf[['age', 'weight', 'height', 'gender', 'ethnicity', 'dob', 'dod']]
                , outputFile = savePath
                )
        elif (plotType == 'vitals_explore'):
            plotVitalsGraphs(
                df = dataDf
                , outputFile = savePath
                )
        elif (plotType == 'lab_measurements_explore'):
            plotLabMeasurementsGraphs(
                df = dataDf
                , outputFile = savePath
                )
        elif (plotType == 'vitals_anomalies'):
            plotVitalsAnomalies(
                df = dataDf
                , outputFile = savePath
                )
        elif (plotType == 'lab_measurements_anomalies'):
            plotLabMeasurementsAnomalies(
                df = dataDf
                , outputFile = savePath
                )


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

    parser.add_argument('plot_type', nargs=1, default='demographics',
                        help='Type of plot to generate [demographics_explore, vitals_explore, lab_measurements_explore, vitals_anomalies, lab_measurements_anomalies]')

    parser.add_argument('source_path', nargs=1, default='data.csv',
                        help='Source data path')

    parser.add_argument('save_path', nargs=1, default='plot.html',
                        help='Path of the file to store the output')

    args = parser.parse_args()

    log.info('args.plot_type: ' + str(args.plot_type[0]))
    log.info('args.source_path: ' + str(args.source_path[0]))
    log.info('args.save_path: ' + str(args.save_path[0]))

    run(plotType=args.plot_type[0], sourcePath = args.source_path[0], savePath = args.save_path[0])

    log.info('Done!!')
