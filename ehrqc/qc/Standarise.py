import joblib
import pandas as pd

from sklearn.preprocessing import StandardScaler


def standardise(source_path, columns, save_path, scaler_save_path):
    dataDf = pd.read_csv(source_path, index=False)
    scaler = StandardScaler()
    cols = []
    if columns:
        cols = list(map(lambda s:s.strip(), columns.split(',')))
    else:
        cols = dataDf.columns
    scaler.fit(dataDf[cols])
    standardisedData = scaler.transform(dataDf[cols])
    standardisedDf = pd.concat([dataDf[dataDf.columns[~dataDf.columns.isin(cols)]], pd.DataFrame(standardisedData, columns=cols)], axis=1)
    standardisedDf.to_csv(save_path, index=None)
    if scaler_save_path:
        joblib.dump(scaler, scaler_save_path)


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

    parser = argparse.ArgumentParser(description='EHRQC-Standardise')

    parser.add_argument('source_path', help='Source data path (csv file)')

    parser.add_argument('columns', help='Names of the columns to be scaled, enclosed in double quotes and seperated by comma')

    parser.add_argument('save_path', help='Path of a file to store the standardised output')

    parser.add_argument('-ssp', '--scaler_save_path', help='Path of the scaler to save')

    args = parser.parse_args()

    log.info('args.source_path: ' + str(args.source_path))
    log.info('args.columns: ' + str(args.columns))
    log.info('args.save_path: ' + str(args.save_path))
    log.info('args.scaler_save_path: ' + str(args.scaler_save_path))

    standardise(source_path=args.source_path, columns=args.columns, save_path=args.save_path, scaler_save_path=args.scaler_save_path)

    log.info('Done!!')
