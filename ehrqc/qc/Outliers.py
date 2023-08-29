import os, tempfile, subprocess
import time
import base64
import itertools

import pandas as pd

from pathlib import Path

from yattag import Doc

doc, tag, text = Doc().tagtext()

from ehrqc import Settings


def run(source_file, save_path, combinations):

    outputFileName = 'outlier_report.html'
    outputFile = Path(save_path, outputFileName)
    dataDf = pd.read_csv(source_file)
    log.info('Validating the input arguments.')

    if(not combinations and (dataDf.shape[1] > int(Settings.col_limit_for_outlier_plot))):
        log.info('Too many variables to plot!! Please sleect the vaiables to plot using combinations argument.')
        return
    elif(combinations and (len(combinations) > Settings.combinations_limit_for_outlier_plot)):
        log.info('Too many variables to plot!! The number of combination should not be more than 6.')
        return
    elif (dataDf.shape[0] * dataDf.shape[1]) > int(Settings.cell_limit):
        log.info('This file has ' + str(dataDf.shape[0] * dataDf.shape[1]) + ' cells.')
        log.info('The maximum number of cell that can be passed to this pipeline is ' + str(Settings.cell_limit))
        log.info('File too big to handle!! Please remove the columns with low coverage and try again.')
        log.info('Refer to this link: https://ehr-qc-tutorials.readthedocs.io/en/latest/process.html#large-file-handling')
        return
    log.info('Validating complete!!')

    start = time.time()

    log.info('Generating outlier report')
    doc.asis('<!DOCTYPE html>')
    with tag('html'):
        doc.asis('<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.1/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-F3w7mX95PdgyTmZZMECAngseQB83DfGTowi0iMjiWaeVhAn4FJkqJByhZMI3AhiU" crossorigin="anonymous">')
        with tag('body'):
            if not combinations:
                log.info('Calculating combinations for the input file')
                columnPairs = list(itertools.combinations(dataDf.columns, 2))
            else:
                columnPairs = combinations
            for (x, y) in columnPairs:
                if x in dataDf.columns and y in dataDf.columns:
                    with tag('div'):
                        with tag('h1'):
                            doc.asis('<svg xmlns="http://www.w3.org/2000/svg" width="25" height="25" fill="currentColor" class="bi bi-check-circle" viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/></svg>')
                            with tag('span', klass='fs-4', style="margin: 10px;"):
                                text('Outliers plot - ' + x + ' - ' + y)
                                log.info('Generating outlier plot for columns: ' + x + ' - ' + y)
                        fig = __drawOutliers(dataDf, x, y, outputFile)
                        if fig:
                            with tag('div', style="float: left;"):
                                doc.asis('<img src=\'data:image/png;base64,{}\'>'.format(fig))
                doc.asis('<div style="clear:both;"></div>')
            doc.asis('<div style="clear:both;"></div>')
            with tag('div'):
                with tag('span', klass='description', style="margin: 10px; color:grey"):
                    with tag('small'):
                        text('Time taken to generate this report: ' + str(round(time.time() - start, 2)) + ' Sec')
            doc.asis('<div style="clear:both;"></div>')
    log.info('Report generated!!')

    log.info('Writing output file')
    with open(outputFile, 'w') as output:
        output.write(doc.getvalue())
    log.info('Completed writing output file!!')


def __drawOutliers(df, x, y, outputFile):

    from matplotlib import pyplot as plt
    import seaborn as sns


    outliersDf = irt_ensemble(df[[x, y]])

    if outliersDf is not None and len(outliersDf) > 0:
        fig, ax = plt.subplots()
        sns.scatterplot(data=outliersDf, x=x, y=y, hue='ensemble_scores')

        ax.set_title('Scatter plot')
        ax.set_xlabel(x)
        ax.set_ylabel(y)

        encoded = None
        outPath = Path(Path(outputFile).parent, 'outlier_plot_' + x + '_' + y + '.png')
        fig.savefig(outPath, format='png', bbox_inches='tight')
        with open(outPath, "rb") as outFile:
            encoded = base64.b64encode(outFile.read()).decode('utf-8')
    else:
        encoded = None

    return encoded


def irt_ensemble(data):

    inFile = tempfile.NamedTemporaryFile(delete=False)
    outFile = tempfile.NamedTemporaryFile(delete=False)
    try:
        data.to_csv(inFile, index=False)
        subprocess.call (["/usr/bin/Rscript", "--vanilla", "ehrqc/qc/script.r", inFile.name, outFile.name])

        if outFile:
            try:
                out = pd.read_csv(outFile)
            except:
                out = None
        else:
            out = None

    finally:
        inFile.close()
        outFile.close()
        os.unlink(inFile.name)
        os.unlink(outFile.name)

    return out


if __name__ == '__main__':
    # import pandas as pd
    # data = pd.read_csv('/home/yram0006/phd/chapter_1/workspace/EHRQC/data/case_study/vitals_corrected.csv')
    # # data.dropna(inplace=True)
    # # df = pd.DataFrame(data, columns=['heartrate', 'sysbp', 'diabp', 'meanbp']) -- heartrate,sysbp,diabp,meanbp,resprate,tempc,spo2,gcseye,gcsverbal,gcsmotor
    # out = irt_ensemble(data[['heartrate', 'sysbp', 'diabp', 'meanbp', 'resprate', 'tempc', 'spo2', 'gcseye', 'gcsverbal', 'gcsmotor']])
    # out.to_csv('/tmp/vitals_irt.csv')

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

    parser.add_argument('source_file', nargs=1, default='./data.csv',
                        help='Source data file path')

    parser.add_argument('save_path', nargs=1, default='./',
                        help='Path of the directory to store the output')

    parser.add_argument('-c', '--combinations', nargs='*', action='append',
                        help='Combinations to plot')

    args = parser.parse_args()

    log.info('args.source_file: ' + str(args.source_file[0]))
    log.info('args.save_path: ' + str(args.save_path[0]))
    log.info('args.combinations: ' + str(args.combinations))

    run(source_file=args.source_file[0], save_path=args.save_path[0], combinations=args.combinations)

    log.info('Done!!')
