from unidip import UniDip

import time

import logging
import sys

import argparse

log = logging.getLogger("Anomalies")
log.setLevel(logging.INFO)
format = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

ch = logging.StreamHandler(sys.stdout)
ch.setFormatter(format)
log.addHandler(ch)

import base64
import itertools
from io import BytesIO
from matplotlib import pyplot as plt
import seaborn as sns
from yattag import Doc

import pandas as pd
import numpy as np
from scipy import stats

from ehrqc.Utils import drawMissingDataPlot

from ehrqc.qc.Outliers import irt_ensemble
from ehrqc.qc.Impute import compare, impute

from ehrqc import ErrorConfig


doc, tag, text = Doc().tagtext()


def detect(
    df,
    missing = False,
    outliers = False,
    errors = False,
    inconsistencies = False,
    outputFile = 'anomalies.html',
    ):
    
    start = time.time()

    doc.asis('<!DOCTYPE html>')
    with tag('html'):
        doc.asis('<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.1/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-F3w7mX95PdgyTmZZMECAngseQB83DfGTowi0iMjiWaeVhAn4FJkqJByhZMI3AhiU" crossorigin="anonymous">')
        with tag('body'):
            doc.asis('<div style="clear:both;"></div>')
            with tag('div'):
                with tag('h1'):
                    doc.asis('<svg xmlns="http://www.w3.org/2000/svg" width="25" height="25" fill="currentColor" class="bi bi-check-circle" viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/></svg>')
                    with tag('span', klass='fs-4', style="margin: 10px;"):
                        text('Vitals Summary')
                __drawSummary(df, df.columns)
            doc.asis('<div style="clear:both;"></div>')
            if missing:
                with tag('div'):
                    with tag('h1'):
                        doc.asis('<svg xmlns="http://www.w3.org/2000/svg" width="25" height="25" fill="currentColor" class="bi bi-check-circle" viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/></svg>')
                        with tag('span', klass='fs-4', style="margin: 10px;"):
                            text('Missing Data Plot')
                    doc.asis('<img src=\'data:image/png;base64,{}\'>'.format(drawMissingDataPlot(df)))
                doc.asis('<div style="clear:both;"></div>')
                with tag('div'):
                    with tag('h1'):
                        doc.asis('<svg xmlns="http://www.w3.org/2000/svg" width="25" height="25" fill="currentColor" class="bi bi-check-circle" viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/></svg>')
                        with tag('span', klass='fs-4', style="margin: 10px;"):
                            text('Missing Data Analysis')
                    drawMissingAnalysis(df, df.columns, tag, text)
                doc.asis('<div style="clear:both;"></div>')
            if outliers:
                for (x, y) in itertools.combinations(df.columns, 2):
                    if x in df.columns and y in df.columns:
                        with tag('div'):
                            with tag('h1'):
                                doc.asis('<svg xmlns="http://www.w3.org/2000/svg" width="25" height="25" fill="currentColor" class="bi bi-check-circle" viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/></svg>')
                                with tag('span', klass='fs-4', style="margin: 10px;"):
                                    text('Outliers plot - ' + x + ' - ' + y)
                            fig = __drawOutliers(df, x, y)
                            if fig:
                                with tag('div', style="float: left;"):
                                    doc.asis('<img src=\'data:image/png;base64,{}\'>'.format(fig))
                    doc.asis('<div style="clear:both;"></div>')
                doc.asis('<div style="clear:both;"></div>')
                with tag('div'):
                    with tag('h1'):
                        doc.asis('<svg xmlns="http://www.w3.org/2000/svg" width="25" height="25" fill="currentColor" class="bi bi-check-circle" viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/></svg>')
                        with tag('span', klass='fs-4', style="margin: 10px;"):
                            text('Outlier Analysis')
                    drawOutlierAnalysis(df, df.columns, tag, text)
                doc.asis('<div style="clear:both;"></div>')
            if errors:
                with tag('div'):
                    with tag('h1'):
                        doc.asis('<svg xmlns="http://www.w3.org/2000/svg" width="25" height="25" fill="currentColor" class="bi bi-check-circle" viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/></svg>')
                        with tag('Error', klass='fs-4', style="margin: 10px;"):
                            text('Error Analysis')
                    drawErrorAnalysis(df, df.columns, tag, text)
                doc.asis('<div style="clear:both;"></div>')
            if inconsistencies:
                with tag('div'):
                    with tag('h1'):
                        doc.asis('<svg xmlns="http://www.w3.org/2000/svg" width="25" height="25" fill="currentColor" class="bi bi-check-circle" viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/></svg>')
                        with tag('span', klass='fs-4', style="margin: 10px;"):
                            text('Inconsistencies Analysis')
                    drawInconsistenciesAnalysis(df, tag, text)
                doc.asis('<div style="clear:both;"></div>')
            with tag('div'):
                with tag('span', klass='description', style="margin: 10px; color:grey"):
                    with tag('small'):
                        text('Time taken to generate this report: ' + str(round(time.time() - start, 2)) + ' Sec')
            doc.asis('<div style="clear:both;"></div>')
    with open(outputFile, 'w') as output:
        output.write(doc.getvalue())


def __drawSummary(df, colNames):

    with tag('table table-dark', style='border: 1px solid black; border-collapse: collapse'):
        with tag('tr'):
            with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                text('Column')
            with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                text('DataType')
            with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                text('Count')
        for col in colNames:
            with tag('tr'):
                with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                    text(col)
                with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                    text(str(df[col].dtypes))
                with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                    text(str(df[col].count()))


def __drawOutliers(df, x, y):

    # log.info('Plotting' + str(x), str(y))

    outliersDf = irt_ensemble(df[[x, y]].dropna())

    if outliersDf is not None and len(outliersDf) > 0:
        fig, ax = plt.subplots()
        sns.scatterplot(data=outliersDf, x=x, y=y, hue='ensemble_scores')

        ax.set_title('Scatter plot')
        ax.set_xlabel(x)
        ax.set_ylabel(y)

        tempFile = BytesIO()
        fig.savefig(tempFile, format='png', bbox_inches='tight')
        encoded = base64.b64encode(tempFile.getvalue()).decode('utf-8')
    else:
        encoded = None

    return encoded


def correct(
    df,
    missing = False,
    outliers = False,
    outputFile = 'corrected.csv',
    ):

    imputedDf = None
    if missing:
        meanR2, medianR2, knnR2, mfR2, emR2, miR2 = compare(df)
        r2List = [meanR2, medianR2, knnR2, mfR2, emR2, miR2]
        log.info('meanR2, medianR2, knnR2, mfR2, emR2, miR2: ' + str(r2List))
        if meanR2 != 0 and meanR2 == max(r2List):
            log.info('Choosing mean imputation')
            imputedDf = impute(df, algorithm='mean')
        elif medianR2 != 0 and medianR2 == max(r2List):
            log.info('Choosing median imputation')
            imputedDf = impute(df, algorithm='median')
        elif knnR2 != 0 and knnR2 == max(r2List):
            log.info('Choosing KNN imputation')
            imputedDf = impute(df, algorithm='knn')
        elif mfR2 != 0 and mfR2 == max(r2List):
            log.info('Choosing MissForest imputation')
            imputedDf = impute(df, algorithm='miss_forest')
        elif emR2 != 0 and emR2 == max(r2List):
            log.info('Choosing Expectation Maximisation imputation')
            imputedDf = impute(df, algorithm='expectation_maximization')
        elif miR2 != 0 and miR2 == max(r2List):
            log.info('Choosing Multiple Imputation imputation')
            imputedDf = impute(df, algorithm='multiple_imputation')
        else:
            imputedDf = df.dropna()
    else:
        imputedDf = df.dropna()

    outliersDf = imputedDf
    if outliers:
        if imputedDf is not None:
            if not imputedDf.empty:
                outliersDf = irt_ensemble(imputedDf)
                outliersDf = outliersDf[(np.abs(stats.zscore(outliersDf.ensemble_scores)) < 2)]

    outliersDf.to_csv(outputFile, index=False)


def drawMissingAnalysis(df, colNames, tag, text):

    with tag('table table-dark', style='border: 1px solid black; border-collapse: collapse'):
        with tag('tr'):
            with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                text('Column')
            with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                text('Count')
            with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                text('Percentage')
        isMissing = False
        for col in colNames:
            if col in df.columns:
                with tag('tr'):
                    with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                        text(col)
                    missingCount = df[col].isna().sum()
                    missingProportion = round(df[col].isna().sum()/df[col].count()*100, 2)
                    if missingCount > 0:
                        isMissing = True
                    with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                        text(str(missingCount))
                    with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                        text(str(missingProportion))

    if isMissing:
        with tag('div'):
            with tag('span', klass='description', style="margin: 10px; color:red"):
                text('There are missing values in the data, to fix them please refer ')
                with tag('a', href='https://ehr-qc-tutorials.readthedocs.io/en/latest/process.html#anomalies'):
                    text('Anomalies Section')


def drawOutlierAnalysis(df, colNames, tag, text):

    with tag('table table-dark', style='border: 1px solid black; border-collapse: collapse'):
        with tag('tr'):
            with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                text('Column')
            with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                text('Count')
            with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                text('Percentage')
        isOutlier = False
        for col in colNames:
            if col in df.columns:
                with tag('tr'):
                    with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                        text(col)
                    Q1 = df[col].quantile(0.25)
                    Q3 = df[col].quantile(0.75)
                    IQR = Q3 - Q1
                    outlierCount = df[((df[col] < (Q1 - 1.5 * IQR)) | (df[col] > (Q3 + 1.5 * IQR)))].shape[0]
                    outlierProportion = round(outlierCount/df[col].count()*100, 2)
                    if outlierCount > 0:
                        isOutlier = True
                    with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                        text(str(outlierCount))
                    with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                        text(str(outlierProportion))

    if isOutlier:
        with tag('div'):
            with tag('span', klass='description', style="margin: 10px; color:red"):
                text('There are Outliers in the data, to fix them please refer ')
                with tag('a', href='https://ehr-qc-tutorials.readthedocs.io/en/latest/process.html#anomalies'):
                    text('Anomalies Section')


def drawErrorAnalysis(df, colNames, tag, text):

    with tag('table table-dark', style='border: 1px solid black; border-collapse: collapse'):
        with tag('tr'):
            with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                text('Column')
            with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                text('Modality')
        isMultimodal = False
        for col in colNames:
            if col in df.columns:
                with tag('tr'):
                    with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                        text(col)
                    try:
                        dat = np.sort(df[col], axis=0)
                        intervals = UniDip(dat).run()
                    except:
                        intervals = []
                    with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                        if len(intervals) > 1:
                            isMultimodal = True
                            text('Multimodal')
                        else:
                            text('Unimodal')

    if isMultimodal:
        with tag('div'):
            with tag('span', klass='description', style="margin: 10px; color:red"):
                text('The data has columns with multimodal distributions, it might be due to mixing data from multiple sources or measurement units')


def drawInconsistenciesAnalysis(df, tag, text):

    for col in df.columns:
        if col in ErrorConfig.boundaries:
            with tag('table table-dark', style='border: 1px solid black; border-collapse: collapse'):
                with tag('tr'):
                    with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                        text('Column')
                    with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                        text('Type')
                    with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                        text('Range')
                    with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                        text('Count')
                    with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                        text('Percentage')
                errorDict = ErrorConfig.boundaries[col]
                for key in errorDict.keys():
                    (lower, upper) = errorDict[key]
                    filteredCol = []
                    if (lower) and (not upper):
                        filteredCol = df[df[col] > lower]
                    elif (not lower) and (upper):
                        filteredCol = df[df[col] < upper]
                    elif lower and upper:
                        filteredCol = df[(df[col] > lower) & (df[col] < upper)]
                    percent = round((filteredCol.shape[0]/df.shape[0]) * 100, 2)
                    with tag('tr'):
                        with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                            text(col)
                        with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                            text(key)
                        with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                            text('[' + str(lower) + ' , ' + str(upper) + ']')
                        with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                            text(filteredCol.shape[0])
                        with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                            text(percent)
            doc.asis('<div style="clear:both;"></div>')
            with tag('div', klass='col-5', style="float: left;"):
                doc.asis('<img src=\'data:image/png;base64,{}\'>'.format(__drawLinePlot(df, col)))
            doc.asis('<div style="clear:both;"></div>')


def __drawLinePlot(df, col):

    fig, ax = plt.subplots()
    sns.kdeplot(
        data = df,
        x = col,
        ax = ax,
    )

    ax.set_title('Line Plot - ' + col)
    ax.set_xlabel(col)
    ax.set_ylabel('Value')

    tempFile = BytesIO()
    fig.savefig(tempFile, format='png', bbox_inches='tight')
    encoded = base64.b64encode(tempFile.getvalue()).decode('utf-8')

    return encoded


if __name__ == "__main__":

    log.info("Parsing command line arguments")

    parser = argparse.ArgumentParser(description='Detect and Correct Anomalies')
    parser.add_argument('source_path', nargs=1, default='data.csv',
                        help='Source data path')
    parser.add_argument('save_path', nargs=1, default='temp',
                        help='Path to save the data')
    parser.add_argument('save_prefix', nargs=1, default='ehrqc',
                        help='Path to save the data')
    parser.add_argument('-dm', '--detect_missing', action='store_true',
                        help='Detect Missing Values in the dataframe')
    parser.add_argument('-do', '--detect_outliers', action='store_true',
                        help='Detect Outliers in the dataframe')
    parser.add_argument('-de', '--detect_errors', action='store_true',
                        help='Detect Errors in the dataframe')
    parser.add_argument('-di', '--detect_inconsistencies', action='store_true',
                        help='Detect Inconsistencies in the dataframe')
    parser.add_argument('-cm', '--correct_missing', action='store_true',
                        help='Correct Missing Values in the dataframe')
    parser.add_argument('-co', '--correct_outliers', action='store_true',
                        help='Correct Outliers in the dataframe')

    args = parser.parse_args()

    log.info("Start!!")
    log.info('args.source_path: ' + str(args.source_path[0]))
    log.info('args.save_path: ' + str(args.save_path[0]))
    log.info('args.save_prefix: ' + str(args.save_prefix[0]))
    log.info('args.detect_missing: ' + str(args.detect_missing))
    log.info('args.detect_outliers: ' + str(args.detect_outliers))
    log.info('args.detect_errors: ' + str(args.detect_errors))
    log.info('args.detect_inconsistencies: ' + str(args.detect_inconsistencies))
    log.info('args.correct_missing: ' + str(args.correct_missing))
    log.info('args.correct_outliers: ' + str(args.correct_outliers))

    if args.detect_missing or args.detect_outliers or args.detect_errors or args.detect_inconsistencies:
        dataDf = pd.read_csv(args.source_path[0])
        detect(
            df=dataDf,
            missing=args.detect_missing,
            outliers=args.detect_outliers,
            errors=args.detect_errors,
            inconsistencies=args.detect_inconsistencies,
            outputFile=args.save_path[0] + '/' + args.save_prefix[0] + '_anomalies.html'
            )

    if args.correct_missing or args.correct_outliers:
        dataDf = pd.read_csv(args.source_path[0])
        correct(
            df=dataDf,
            missing=args.correct_missing,
            outliers=args.correct_outliers,
            outputFile=args.save_path[0] + '/' + args.save_prefix[0] + '_corrected.csv'
        )
