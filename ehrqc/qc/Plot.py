import sys

import pandas as pd

from ehrqc.qc.demographicsGraphs import plot as plotDemographicsGraphs
from ehrqc.qc.vitalsGraphs import plot as plotVitalsGraphs
from ehrqc.qc.labMeasurementsGraphs import plot as plotLabMeasurementsGraphs

from ehrqc import Settings


def run(plotType = 'demographics', sourcePath = 'data.csv', savePath = 'plot.html', column_mapping = {}):

    dataDf = pd.read_csv(sourcePath)

    if dataDf.shape[1] > int(Settings.col_limit):
        log.info('Too many variables!! Please select only the ones to be plotted.')
        return
    elif (dataDf.shape[0] * dataDf.shape[1]) > int(Settings.cell_limit):
        log.info('This file has ' + str(dataDf.shape[0] * dataDf.shape[1]) + ' cells.')
        log.info('The maximum number of cell that can be passed to this pipeline is ' + str(Settings.cell_limit))
        log.info('File too big to handle!! Please remove the columns with low coverage and try again.')
        log.info('Refer to this link: https://ehr-qc-tutorials.readthedocs.io/en/latest/process.html#large-file-handling')
        return

    if (dataDf is not None):
        log.info('generating graphs')
        if (plotType == 'demographics_explore'):
            plotDemographicsGraphs(
                df=dataDf
                , outputFile = savePath
                , column_mapping = column_mapping
                )
        elif (plotType == 'vitals_explore'):
            plotVitalsGraphs(
                df = dataDf
                , outputFile = savePath
                , column_mapping = column_mapping
                )
        elif (plotType == 'lab_measurements_explore'):
            plotLabMeasurementsGraphs(
                df = dataDf
                , outputFile = savePath
                , column_mapping = column_mapping
                )


if __name__ == '__main__':

    import logging
    import sys
    import argparse
    import json

    log = logging.getLogger("EHRQC")
    log.setLevel(logging.INFO)
    format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

    ch = logging.StreamHandler(sys.stdout)
    ch.setFormatter(format)
    log.addHandler(ch)

    log.info("Parsing command line arguments")

    parser = argparse.ArgumentParser(description='EHRQC')

    parser.add_argument('plot_type', nargs=1, default='demographics',
                        help='Type of plot to generate [demographics_explore, vitals_explore, lab_measurements_explore]')

    parser.add_argument('source_path', nargs=1, default='data.csv',
                        help='Source data path')

    parser.add_argument('save_path', nargs=1, default='plot.html',
                        help='Path of the file to store the output')

    parser.add_argument('-c', '--column_mapping', type=str)

    args = parser.parse_args()

    log.info('args.plot_type: ' + str(args.plot_type[0]))
    log.info('args.source_path: ' + str(args.source_path[0]))
    log.info('args.save_path: ' + str(args.save_path[0]))
    log.info('args.column_mapping: ' + str(args.column_mapping))

    column_mapping_raw = args.column_mapping
    if column_mapping_raw:
        column_mapping = json.loads(column_mapping_raw)
    else:
        column_mapping = {}

    log.info('args.column_mapping: ' + str(column_mapping))

    run(plotType=args.plot_type[0], sourcePath = args.source_path[0], savePath = args.save_path[0], column_mapping=column_mapping)

    log.info('Done!!')
