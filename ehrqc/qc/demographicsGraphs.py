import time
import base64
from io import BytesIO
from matplotlib import pyplot as plt
from yattag import Doc

from ehrqc.Utils import drawMissingDataPlot


doc, tag, text = Doc().tagtext()


def plot(
    df, 
    outputFile = 'demographics.html', 
    ageCol = 'age', 
    weightCol = 'weight', 
    heightCol = 'height', 
    genderCol = 'gender', 
    ethnicityCol = 'ethnicity',
    column_mapping = {}
    ):

    if 'age' in column_mapping:
        ageCol = column_mapping['age']
    elif 'weight' in column_mapping:
        weightCol = column_mapping['weight']
    elif 'height' in column_mapping:
        heightCol = column_mapping['height']
    elif 'gender' in column_mapping:
        genderCol = column_mapping['gender']
    elif 'ethnicity' in column_mapping:
        ethnicityCol = column_mapping['ethnicity']

    start = time.time()

    doc.asis('<!DOCTYPE html>')
    with tag('html'):
        with tag('head'):
            doc.asis('<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.1/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-F3w7mX95PdgyTmZZMECAngseQB83DfGTowi0iMjiWaeVhAn4FJkqJByhZMI3AhiU" crossorigin="anonymous">')
        with tag('body'):
            doc.asis('<div style="clear:both;"></div>')
            with tag('div'):
                with tag('h1'):
                    doc.asis('<svg xmlns="http://www.w3.org/2000/svg" width="25" height="25" fill="currentColor" class="bi bi-check-circle" viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/></svg>')
                    with tag('span', klass='fs-4', style="margin: 10px;"):
                        text('Missing Data Plot')
                doc.asis('<img src=\'data:image/png;base64,{}\'>'.format(drawMissingDataPlot(df)))
            doc.asis('<div style="clear:both;"></div>')
            if ageCol in df.columns:
                with tag('div'):
                    with tag('h1'):
                        doc.asis('<svg xmlns="http://www.w3.org/2000/svg" width="25" height="25" fill="currentColor" class="bi bi-check-circle" viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/></svg>')
                        with tag('span', klass='fs-4', style="margin: 10px;"):
                            text('Age Distribution')
                    doc.asis('<img src=\'data:image/png;base64,{}\'>'.format(__drawAgeHistogram(df, ageCol)))
                doc.asis('<div style="clear:both;"></div>')
            if weightCol in df.columns:
                with tag('div'):
                    with tag('h1'):
                        doc.asis('<svg xmlns="http://www.w3.org/2000/svg" width="25" height="25" fill="currentColor" class="bi bi-check-circle" viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/></svg>')
                        with tag('span', klass='fs-4', style="margin: 10px;"):
                            text('Weight Distribution')
                    doc.asis('<img src=\'data:image/png;base64,{}\'>'.format(__drawWeightHistogram(df, weightCol)))
                doc.asis('<div style="clear:both;"></div>')
            if heightCol in df.columns:
                with tag('div'):
                    with tag('h1'):
                        doc.asis('<svg xmlns="http://www.w3.org/2000/svg" width="25" height="25" fill="currentColor" class="bi bi-check-circle" viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/></svg>')
                        with tag('span', klass='fs-4', style="margin: 10px;"):
                            text('Height Distribution')
                    doc.asis('<img src=\'data:image/png;base64,{}\'>'.format(__drawHeightHistogram(df, heightCol)))
                doc.asis('<div style="clear:both;"></div>')
            if genderCol in df.columns:
                with tag('div'):
                    with tag('h1'):
                        doc.asis('<svg xmlns="http://www.w3.org/2000/svg" width="25" height="25" fill="currentColor" class="bi bi-check-circle" viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/></svg>')
                        with tag('span', klass='fs-4', style="margin: 10px;"):
                            text('Gender Value Counts')
                    doc.asis('<img src=\'data:image/png;base64,{}\'>'.format(__drawGenderBarplot(df, genderCol)))
                doc.asis('<div style="clear:both;"></div>')
            if ethnicityCol in df.columns:
                with tag('div'):
                    with tag('h1'):
                        doc.asis('<svg xmlns="http://www.w3.org/2000/svg" width="25" height="25" fill="currentColor" class="bi bi-check-circle" viewBox="0 0 16 16"><path d="M8 15A7 7 0 1 1 8 1a7 7 0 0 1 0 14zm0 1A8 8 0 1 0 8 0a8 8 0 0 0 0 16z"/><path d="M10.97 4.97a.235.235 0 0 0-.02.022L7.477 9.417 5.384 7.323a.75.75 0 0 0-1.06 1.06L6.97 11.03a.75.75 0 0 0 1.079-.02l3.992-4.99a.75.75 0 0 0-1.071-1.05z"/></svg>')
                        with tag('span', klass='fs-4', style="margin: 10px;"):
                            text('Ethnicity Value Counts')
                    doc.asis('<img src=\'data:image/png;base64,{}\'>'.format(__drawEthnicityBarplot(df, ethnicityCol)))
                doc.asis('<div style="clear:both;"></div>')
            with tag('div'):
                with tag('span', klass='description', style="margin: 10px; color:grey"):
                    with tag('small'):
                        text('Time taken to generate this report: ' + str(round(time.time() - start, 2)) + ' Sec')
            doc.asis('<div style="clear:both;"></div>')
    with open(outputFile, 'w') as output:
        output.write(doc.getvalue())


def __drawAgeHistogram(df, ageCol):

    fig, ax = plt.subplots()
    plt.xticks(rotation = 45)
    plt.hist(df[ageCol])

    ax.set_title('Age Histogram')
    ax.set_xlabel('Age')
    ax.set_ylabel('Count')

    tempFile = BytesIO()
    fig.savefig(tempFile, format='png', bbox_inches='tight')
    encoded = base64.b64encode(tempFile.getvalue()).decode('utf-8')

    return encoded


def __drawWeightHistogram(df, weightCol):

    fig, ax = plt.subplots()
    plt.xticks(rotation = 45)
    plt.hist(df[weightCol])

    ax.set_title('Weight Distribution')
    ax.set_xlabel('Weight')
    ax.set_ylabel('Count')

    tempFile = BytesIO()
    fig.savefig(tempFile, format='png', bbox_inches='tight')
    encoded = base64.b64encode(tempFile.getvalue()).decode('utf-8')

    return encoded


def __drawHeightHistogram(df, heightCol):

    fig, ax = plt.subplots()
    plt.xticks(rotation = 45)
    plt.hist(df[heightCol])

    ax.set_title('Height Distribution')
    ax.set_xlabel('Height')
    ax.set_ylabel('Count')

    tempFile = BytesIO()
    fig.savefig(tempFile, format='png', bbox_inches='tight')
    encoded = base64.b64encode(tempFile.getvalue()).decode('utf-8')

    return encoded


def __drawGenderBarplot(df, genderCol):

    fig, ax = plt.subplots()
    plt.xticks(rotation = 45)
    df[genderCol].value_counts().plot(kind='barh')

    ax.set_title('Gender Barplot')
    ax.set_xlabel('Count')
    ax.set_ylabel('Gender')

    tempFile = BytesIO()
    fig.savefig(tempFile, format='png', bbox_inches='tight')
    encoded = base64.b64encode(tempFile.getvalue()).decode('utf-8')

    return encoded


def __drawEthnicityBarplot(df, ethnicityCol):

    fig, ax = plt.subplots()
    plt.xticks(rotation = 45)
    df[ethnicityCol].value_counts().plot(kind='barh')

    ax.set_title('Ethnicity Distribution')
    ax.set_xlabel('Count')
    ax.set_ylabel('Ethnicity')

    tempFile = BytesIO()
    fig.savefig(tempFile, format='png', bbox_inches='tight')
    encoded = base64.b64encode(tempFile.getvalue()).decode('utf-8')

    return encoded

