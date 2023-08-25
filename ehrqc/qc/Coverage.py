import logging
import sys

import argparse

log = logging.getLogger("Anomalies")
log.setLevel(logging.INFO)
format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(format)
log.addHandler(ch)


import pandas as pd
import numpy as np

from pathlib import Path

def calculateMissingness(source_file, chunksize):
    chunksize=chunksize
    source_df_list = []
    missing_df_list = []
    for df in pd.read_csv(source_file,  chunksize=chunksize):
        source_df_list.append(df)
        df.replace('', np.nan, inplace=True)
        missing_counts = df.isnull().sum()
        missing_df = pd.DataFrame({'column_name': df.columns, 'missing_count': missing_counts, 'total_count': len(df.columns)*[len(df)]})
        missing_df_list.append(missing_df)
    final_df = pd.concat(missing_df_list, ignore_index=True)
    final_agg_df = final_df.groupby(['column_name']).sum().reset_index()
    final_agg_df['percentage_missing'] = round(final_agg_df['missing_count']/final_agg_df['total_count']*100, 2)
    return final_agg_df, source_df_list


def run(source_file, chunksize, drop, percentage, save_path):

    log.info("Calculating Missingnes")
    missing_df, source_df_list = calculateMissingness(source_file=source_file, chunksize=chunksize)

    log.info("Missingness Report")
    log.info('\n\n' + str(missing_df) + '\n')

    log.info('''Dropping columns with below ''' + str(percentage) + ''' % coverage''')
    if drop:
        source_df = pd.concat(source_df_list, ignore_index=True)
        source_df.drop(missing_df[missing_df.percentage_missing > percentage].column_name, axis=1, inplace=True)
        save_file_name = args.source_file[0].split('/')[-1].split('.')[0] + '_dropped.' + args.source_file[0].split('/')[-1].split('.')[1]
        log.info('''Saving data to ''' + save_file_name)
        source_df.to_csv(Path(save_path, save_file_name), index=None)


if __name__ == "__main__":

    log.info("Parsing command line arguments")

    parser = argparse.ArgumentParser(description='Perform Coverage Analysis')
    parser.add_argument('source_file', nargs=1, default='data.csv',
                        help='Source data file path')
    parser.add_argument('chunksize', nargs=1, type=int, default=100,
                        help='Number of chunks the input file should be fragmented into')
    parser.add_argument('-d', '--drop', action='store_true',
                        help='Drop the columns')
    parser.add_argument('-p', '--percentage', nargs=1, type=int, default=[90],
                        help='Specify the cutoff percentage to drop the columns (required only for drop=True)')
    parser.add_argument('-sp', '--save_path', nargs=1, default='./',
                        help='Path of the file to store the outputs (required only for drop=True)')

    args = parser.parse_args()

    log.info("Start!!")
    log.info('args.source_file: ' + str(args.source_file[0]))
    log.info('args.chunksize: ' + str(args.chunksize[0]))
    log.info('args.drop: ' + str(args.drop))
    log.info('args.percentage: ' + str(args.percentage[0]))
    log.info('args.save_path: ' + str(args.save_path[0]))

    run(
        source_file=args.source_file[0],
        chunksize=args.chunksize[0],
        drop=args.drop,
        percentage=args.percentage[0],
        save_path=args.save_path[0],
    )

    log.info("Done!!")
