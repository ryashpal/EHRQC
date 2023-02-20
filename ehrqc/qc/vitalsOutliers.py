import time
import base64
import itertools

import numpy as np
from io import BytesIO
from matplotlib import pyplot as plt
import seaborn as sns
from yattag import Doc

from ehrqc.qc.Outliers import irt_ensemble


doc, tag, text = Doc().tagtext()


def plot(
    df,
    outputFile = 'vitals_outliers.html',
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
    start = time.time()

    doc.asis('<!DOCTYPE html>')
    with tag('html'):
        doc.asis('<link href="https://cdn.jsdelivr.net/npm/bootstrap@5.1.1/dist/css/bootstrap.min.css" rel="stylesheet" integrity="sha384-F3w7mX95PdgyTmZZMECAngseQB83DfGTowi0iMjiWaeVhAn4FJkqJByhZMI3AhiU" crossorigin="anonymous">')
        with tag('body'):
            for (x, y) in itertools.combinations(colNames, 2):
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
                with tag('span', klass='description', style="margin: 10px; color:grey"):
                    with tag('small'):
                        text('Time taken to generate this report: ' + str(round(time.time() - start, 2)) + ' Sec')
            doc.asis('<div style="clear:both;"></div>')
    with open(outputFile, 'w') as output:
        output.write(doc.getvalue())


def __drawOutliers(df, x, y):

    outliersDf = irt_ensemble(df[[x, y]])

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


if __name__ == '__main__':
    import pandas as pd
    import psycopg2

    # data = np.random.randn(100, 5)
    # df = pd.DataFrame(data, columns=['heartrate', 'sysbp', 'diabp', 'meanbp', 'resprateCol'])
    # plot(df)


    # information used to create a database connection
    sqluser = 'postgres'
    dbname = 'mimic4'
    hostname = 'localhost'
    port_number = 5434
    schema_name = 'mimiciv'

    # Connect to postgres with a copy of the MIMIC-III database
    con = psycopg2.connect(dbname=dbname, user=sqluser, host=hostname, port=port_number, password='mysecretpassword')

    # the below statement is prepended to queries to ensure they select from the right schema
    query_schema = 'set search_path to ' + schema_name + ';'

    query = query_schema + \
    """
    WITH vitals_stg_1 AS
    (
        SELECT icu.stay_id, cev.charttime
        , CASE
                WHEN itemid = 223761 THEN (cev.valuenum-32)/1.8
            ELSE cev.valuenum
        END AS valuenum
        , CASE
                WHEN itemid = 220045 THEN 'HEARTRATE'
                WHEN itemid = 220050 THEN 'SYSBP'
                WHEN itemid = 220179 THEN 'SYSBP'
                WHEN itemid = 220051 THEN 'DIASBP'
                WHEN itemid = 220180 THEN 'DIASBP'
                WHEN itemid = 220052 THEN 'MEANBP'
                WHEN itemid = 220181 THEN 'MEANBP'
                WHEN itemid = 225312 THEN 'MEANBP'
                WHEN itemid = 220210 THEN 'RESPRATE'
                WHEN itemid = 224688 THEN 'RESPRATE'
                WHEN itemid = 224689 THEN 'RESPRATE'
                WHEN itemid = 224690 THEN 'RESPRATE'
                WHEN itemid = 223761 THEN 'TEMPC'
                WHEN itemid = 223762 THEN 'TEMPC'
                WHEN itemid = 220277 THEN 'SPO2'
                WHEN itemid = 220739 THEN 'GCSEYE'
                WHEN itemid = 223900 THEN 'GCSVERBAL'
                WHEN itemid = 223901 THEN 'GCSMOTOR'
            ELSE null
            END AS label
        FROM mimiciv.icustays icu
        INNER JOIN mimiciv.chartevents cev
        ON cev.stay_id = icu.stay_id
        AND cev.charttime >= icu.intime
        AND cev.charttime <= icu.intime + interval '24 hour'
        WHERE cev.itemid IN
        (
        220045 -- heartrate
        , 220050, 220179 -- sysbp
        , 220051, 220180 -- diasbp
        , 220052, 220181, 225312 -- meanbp
        , 220210, 224688, 224689, 224690 -- resprate
        , 223761, 223762 -- tempc
        , 220277 -- SpO2
        , 220739 -- gcseye
        , 223900 -- gcsverbal
        , 223901 -- gscmotor
        )
        AND valuenum IS NOT null
    )
    , vitals_stg_2 AS
    (
    SELECT
        stay_id, valuenum, label
        , ROW_NUMBER() OVER (PARTITION BY stay_id, label ORDER BY charttime) AS rn
    FROM vitals_stg_1
    )
    , vitals_stg_3 AS
    (
    SELECT
        stay_id
        , rn
        , COALESCE(MAX(CASE WHEN label = 'HEARTRATE' THEN valuenum ELSE null END)) AS heartrate
        , COALESCE(MAX(CASE WHEN label = 'SYSBP' THEN valuenum ELSE null END)) AS sysbp
        , COALESCE(MAX(CASE WHEN label = 'DIASBP' THEN valuenum ELSE null END)) AS diabp
        , COALESCE(MAX(CASE WHEN label = 'MEANBP' THEN valuenum ELSE null END)) AS meanbp
        , COALESCE(MAX(CASE WHEN label = 'RESPRATE' THEN valuenum ELSE null END)) AS resprate
        , COALESCE(MAX(CASE WHEN label = 'TEMPC' THEN valuenum ELSE null END)) AS tempc
        , COALESCE(MAX(CASE WHEN label = 'SPO2' THEN valuenum ELSE null END)) AS spo2
        , COALESCE(MAX(CASE WHEN label = 'GCSEYE' THEN valuenum ELSE null END)) AS gcseye
        , COALESCE(MAX(CASE WHEN label = 'GCSVERBAL' THEN valuenum ELSE null END)) AS gcsverbal
        , COALESCE(MAX(CASE WHEN label = 'GCSMOTOR' THEN valuenum ELSE null END)) AS gcsmotor
    FROM vitals_stg_2
    GROUP BY stay_id, rn
    )
    , vitals_stg_4 AS
    (
        SELECT
        stay_id,
        AVG(heartrate) AS heartrate
        , AVG(sysbp) AS sysbp
        , AVG(diabp) AS diabp
        , AVG(meanbp) AS meanbp
        , AVG(resprate) AS resprate
        , AVG(tempc) AS tempc
        , AVG(spo2) AS spo2
        , AVG(gcseye) AS gcseye
        , AVG(gcsverbal) AS gcsverbal
        , AVG(gcsmotor) AS gcsmotor
        FROM vitals_stg_3
        GROUP BY stay_id
    )
    SELECT * FROM vitals_stg_4
    """

    vitals = pd.read_sql_query(query, con)

    vitals.fillna(0)

    plot(vitals)
