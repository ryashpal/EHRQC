import time
import base64
import numpy as np
from io import BytesIO
from matplotlib import pyplot as plt
import seaborn as sns
from yattag import Doc
from pathlib import Path

from ehrqc.Utils import drawMissingDataPlot, drawSummaryTable


doc, tag, text = Doc().tagtext()


def plot(
    df,
    outputFile = 'lab_measurements.html',
    glucoseCol = 'glucose',
    hemoglobinCol = 'hemoglobin',
    anion_gapCol = 'anion_gap',
    bicarbonateCol = 'bicarbonate',
    calcium_totalCol = 'calcium_total',
    chlorideCol = 'chloride',
    creatinineCol = 'creatinine',
    magnesiumCol = 'magnesium',
    phosphateCol = 'phosphate',
    potassiumCol = 'potassium',
    sodiumCol = 'sodium',
    urea_nitrogenCol = 'urea_nitrogen',
    hematocritCol = 'hematocrit',
    mchCol = 'mch',
    mchcCol = 'mchc',
    mcvCol = 'mcv',
    platelet_countCol = 'platelet_count',
    rdwCol = 'rdw',
    red_blood_cellsCol = 'red_blood_cells',
    white_blood_cellsCol = 'white_blood_cells',
    column_mapping = {}
    ):

    if 'glucose' in column_mapping:
        glucoseCol = column_mapping['glucose']
    if 'hemoglobin' in column_mapping:
        hemoglobinCol = column_mapping['hemoglobin']
    if 'anion_gap' in column_mapping:
        anion_gapCol = column_mapping['anion_gap']
    if 'bicarbonate' in column_mapping:
        bicarbonateCol = column_mapping['bicarbonate']
    if 'calcium_total' in column_mapping:
        calcium_totalCol = column_mapping['calcium_total']
    if 'chloride' in column_mapping:
        chlorideCol = column_mapping['chloride']
    if 'creatinine' in column_mapping:
        creatinineCol = column_mapping['creatinine']
    if 'magnesium' in column_mapping:
        magnesiumCol = column_mapping['magnesium']
    if 'phosphate' in column_mapping:
        phosphateCol = column_mapping['phosphate']
    if 'potassium' in column_mapping:
        potassiumCol = column_mapping['potassium']
    if 'sodium' in column_mapping:
        sodiumCol = column_mapping['sodium']
    if 'urea_nitrogen' in column_mapping:
        urea_nitrogenCol = column_mapping['urea_nitrogen']
    if 'hematocrit' in column_mapping:
        hematocritCol = column_mapping['hematocrit']
    if 'mch' in column_mapping:
        mchCol = column_mapping['mch']
    if 'mchc' in column_mapping:
        mchcCol = column_mapping['mchc']
    if 'mcv' in column_mapping:
        mcvCol = column_mapping['mcv']
    if 'platelet_count' in column_mapping:
        platelet_countCol = column_mapping['platelet_count']
    if 'rdw' in column_mapping:
        rdwCol = column_mapping['rdw']
    if 'red_blood_cells' in column_mapping:
        red_blood_cellsCol = column_mapping['red_blood_cells']
    if 'white_blood_cells' in column_mapping:
        white_blood_cellsCol = column_mapping['white_blood_cells']

    colNames = [glucoseCol, hemoglobinCol, anion_gapCol, bicarbonateCol, calcium_totalCol, chlorideCol, creatinineCol, magnesiumCol, phosphateCol, potassiumCol, sodiumCol, urea_nitrogenCol, hematocritCol, mchCol, mchcCol, mcvCol, platelet_countCol, rdwCol, red_blood_cellsCol, white_blood_cellsCol]
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
                        text('Lab Values Summary')
                __drawLabMeasurementsSummary(df, colNames)
            doc.asis('<div style="clear:both;"></div>')
            for col in colNames:
                if col in df.columns:
                    with tag('div'):
                        with tag('h1'):
                            doc.asis('<svg xmlns="http://www.w3.org/2000/svg" width="25" height="25" fill="currentColor" class="bi bi-check-circle" viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/></svg>')
                            with tag('span', klass='fs-4', style="margin: 10px;"):
                                text('Distribution - ' + col)
                        with tag('div', klass='col-5', style="float: left;"):
                            doc.asis('<img src=\'data:image/png;base64,{}\'>'.format(__drawLabMeasurementsViolinPlot(df, col, outputFile)))
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


def __drawLabMeasurementsSummary(df, colNames):

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


def __drawLabMeasurementsViolinPlot(df, col, outputFile):

    fig, ax = plt.subplots()
    sns.violinplot(
        y = df[col]
        , ax=ax
    )

    ax.set_title('Violin Plot - ' + col)
    ax.set_xlabel(col)
    ax.set_ylabel('Value')

    encoded = None
    outPath = Path(Path(outputFile).parent, 'labs_' + col + '.png')
    fig.savefig(outPath, format='png', bbox_inches='tight')
    with open(outPath, "rb") as outFile:
        encoded = base64.b64encode(outFile.read()).decode('utf-8')

    return encoded
