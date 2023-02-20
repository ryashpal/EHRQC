import base64
from io import BytesIO
from matplotlib import pyplot as plt
import seaborn as sns
from yattag import Doc

from ehrqc.Utils import drawMissingDataPlot


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
    elif 'sysbp' in column_mapping:
        sysbpCol = column_mapping['sysbp']
    elif 'diabp' in column_mapping:
        diabpCol = column_mapping['diabp']
    elif 'meanbp' in column_mapping:
        meanbpCol = column_mapping['meanbp']
    elif 'resprate' in column_mapping:
        resprateCol = column_mapping['resprate']
    elif 'tempc' in column_mapping:
        tempcCol = column_mapping['tempc']
    elif 'spo2' in column_mapping:
        spo2Col = column_mapping['spo2']
    elif 'gcseye' in column_mapping:
        gcseyeCol = column_mapping['gcseye']
    elif 'gcsverbal' in column_mapping:
        gcsverbalCol = column_mapping['gcsverbal']
    elif 'gcsmotor' in column_mapping:
        gcsmotorCol = column_mapping['gcsmotor']

    colNames = [heartrateCol, sysbpCol, diabpCol, meanbpCol, resprateCol, tempcCol, spo2Col, gcseyeCol, gcsverbalCol, gcsmotorCol]

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
                                text('Histogram - ' + col)
                        with tag('div', klass='col-5', style="float: left;"):
                            doc.asis('<img src=\'data:image/png;base64,{}\'>'.format(__drawVitalsViolinPlot(df, col)))
                        with tag('div', klass='col-2', style="float: left;"):
                            __drawVitalsTable(df, col)
                doc.asis('<div style="clear:both;"></div>')
            with tag('div'):
                with tag('h1'):
                    doc.asis('<svg xmlns="http://www.w3.org/2000/svg" width="25" height="25" fill="currentColor" class="bi bi-check-circle" viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/></svg>')
                    with tag('span', klass='fs-4', style="margin: 10px;"):
                        text('Missing Data Plot')
                doc.asis('<img src=\'data:image/png;base64,{}\'>'.format(drawMissingDataPlot(df)))
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


def __drawVitalsViolinPlot(df, col):

    fig, ax = plt.subplots()
    sns.violinplot(
        y = df[col]
        , ax=ax
    )

    ax.set_title('Violin Plot - ' + col)
    ax.set_xlabel(col)
    ax.set_ylabel('Value')

    tempFile = BytesIO()
    fig.savefig(tempFile, format='png', bbox_inches='tight')
    encoded = base64.b64encode(tempFile.getvalue()).decode('utf-8')

    return encoded


def __drawVitalsTable(df, col):

    with tag('table table-dark', style='border: 1px solid black; border-collapse: collapse'):
        with tag('tr'):
            with tag('th', style='border: 1px solid black; border-collapse: collapse'):
                text('Statistics - ' + col)
        with tag('tr'):
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text('First Quartile')
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text(str(round(df[col].quantile(0.25), 2)))
        with tag('tr'):
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text('Mean')
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text(str(round(df[col].mean(), 2)))
        with tag('tr'):
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text('Median')
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text(str(round(df[col].median(), 2)))
        with tag('tr'):
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text('Mode')
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text(str(round(df[col].mode()[0], 2)))
        with tag('tr'):
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text('Third Quartile')
            with tag('td', style='border: 1px solid black; border-collapse: collapse'):
                text(str(round(df[col].quantile(0.75), 2)))
