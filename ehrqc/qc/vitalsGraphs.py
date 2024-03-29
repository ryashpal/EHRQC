import time
import base64
from io import BytesIO
from matplotlib import pyplot as plt
import seaborn as sns
from yattag import Doc
from pathlib import Path

from ehrqc.Utils import drawMissingDataPlot, drawSummaryTable


doc, tag, text = Doc().tagtext()


def plot(
    df,
    outputFile = 'vitals.html',
    heartrateCol = 'heartrate',
    sysbpCol = 'sysbp',
    diabpCol = 'diabp',
    meanbpCol = 'meanbp',
    resprateCol = 'resprate',
    tempcCol = 'tempc',
    spo2Col = 'spo2',
    gcseyeCol = 'gcseye',
    gcsverbalCol = 'gcsverbal',
    gcsmotorCol = 'gcsmotor',
    column_mapping = {}
    ):

    if 'heartrate' in column_mapping:
        heartrateCol = column_mapping['heartrate']
    if 'sysbp' in column_mapping:
        sysbpCol = column_mapping['sysbp']
    if 'diabp' in column_mapping:
        diabpCol = column_mapping['diabp']
    if 'meanbp' in column_mapping:
        meanbpCol = column_mapping['meanbp']
    if 'resprate' in column_mapping:
        resprateCol = column_mapping['resprate']
    if 'tempc' in column_mapping:
        tempcCol = column_mapping['tempc']
    if 'spo2' in column_mapping:
        spo2Col = column_mapping['spo2']
    if 'gcseye' in column_mapping:
        gcseyeCol = column_mapping['gcseye']
    if 'gcsverbal' in column_mapping:
        gcsverbalCol = column_mapping['gcsverbal']
    if 'gcsmotor' in column_mapping:
        gcsmotorCol = column_mapping['gcsmotor']

    colNames = [heartrateCol, sysbpCol, diabpCol, meanbpCol, resprateCol, tempcCol, spo2Col, gcseyeCol, gcsverbalCol, gcsmotorCol]
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
                __drawVitalsSummary(df, colNames)
            doc.asis('<div style="clear:both;"></div>')
            for col in colNames:
                if col in df.columns:
                    with tag('div'):
                        with tag('h1'):
                            doc.asis('<svg xmlns="http://www.w3.org/2000/svg" width="25" height="25" fill="currentColor" class="bi bi-check-circle" viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/></svg>')
                            with tag('span', klass='fs-4', style="margin: 10px;"):
                                text('Distribution - ' + col)
                        with tag('div', klass='col-5', style="float: left;"):
                            doc.asis('<img src=\'data:image/png;base64,{}\'>'.format(__drawVitalsViolinPlot(df, col, outputFile)))
                        with tag('div', klass='col-2', style="float: left;"):
                            drawSummaryTable(df, tag, text, col)
                doc.asis('<div style="clear:both;"></div>')
            with tag('div'):
                with tag('h1'):
                    doc.asis('<svg xmlns="http://www.w3.org/2000/svg" width="25" height="25" fill="currentColor" class="bi bi-check-circle" viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/></svg>')
                    with tag('span', klass='fs-4', style="margin: 10px;"):
                        text('Missing Data Plot')
                doc.asis('<img src=\'data:image/png;base64,{}\'>'.format(drawMissingDataPlot(df, outputFile)))
            doc.asis('<div style="clear:both;"></div>')
            with tag('div'):
                with tag('span', klass='description', style="margin: 10px; color:grey"):
                    with tag('small'):
                        text('Time taken to generate this report: ' + str(round(time.time() - start, 2)) + ' Sec')
            doc.asis('<div style="clear:both;"></div>')
    with open(outputFile, 'w') as output:
        output.write(doc.getvalue())


def __drawVitalsSummary(df, colNames):

    with tag('table table-dark', style='border: 1px solid black; border-collapse: collapse'):
        with tag('tr'):
            with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                text('Column')
            with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                text('DataType')
            with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                text('Count')
        for col in colNames:
            if col in df.columns:
                with tag('tr'):
                    with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                        text(col)
                    with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                        text(str(df[col].dtypes))
                    with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                        text(str(df[col].count()))


def __drawVitalsViolinPlot(df, col, outputFile):

    fig, ax = plt.subplots()
    sns.violinplot(
        y = df[col]
        , ax=ax
    )

    ax.set_title('Violin Plot - ' + col)
    ax.set_xlabel(col)
    ax.set_ylabel('Value')

    encoded = None
    outPath = Path(Path(outputFile).parent, 'vitals_' + col + '.png')
    fig.savefig(outPath, format='png', bbox_inches='tight')
    with open(outPath, "rb") as outFile:
        encoded = base64.b64encode(outFile.read()).decode('utf-8')

    return encoded

