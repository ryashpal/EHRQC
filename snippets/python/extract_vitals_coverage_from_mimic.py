import psycopg2
import pandas as pd


if __name__ == '__main__':
    import re
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

    itemidQuery = query_schema + \
    """
    SELECT
    itm.itemid,
    itm.label
    FROM mimiciv.chartevents cev
    INNER JOIN mimiciv.d_items itm
    ON itm.itemid = cev.itemid
    GROUP BY itm.itemid
    """

    itemidsDf = pd.read_sql_query(itemidQuery, con)

    allIcustaysQuery = query_schema + \
    """
    SELECT
    pat.subject_id,
    adm.hadm_id,
    icu.stay_id
    FROM mimiciv.patients pat
    INNER JOIN mimiciv.admissions adm
    ON adm.subject_id = pat.subject_id
    INNER JOIN mimiciv.icustays icu
    ON icu.hadm_id = adm.hadm_id
    GROUP BY pat.subject_id, adm.hadm_id, icu.stay_id
    """

    allIcustaysDf = pd.read_sql_query(allIcustaysQuery, con)

    i = 0

    for itemid in itemidsDf['itemid']:
        lbl = re.sub("[^a-zA-Z]+", "_", str(itemidsDf.loc[itemidsDf['itemid'] == itemid, 'label'].values[0])).lower()
        i = i + 1
        print('iteration #' + str(i))
        coverageQuery = query_schema + \
        """
            SELECT
            pat.subject_id,
            adm.hadm_id,
            icu.stay_id,
            COUNT(cev.stay_id) as """ + lbl + """
            FROM mimiciv.patients pat
            INNER JOIN mimiciv.admissions adm
            ON adm.subject_id = pat.subject_id
            INNER JOIN mimiciv.icustays icu
            ON icu.hadm_id = adm.hadm_id
            LEFT JOIN mimiciv.chartevents cev
            ON cev.stay_id = icu.stay_id AND cev.itemid = '""" + str(itemid) + """'
            GROUP BY pat.subject_id, adm.hadm_id, icu.stay_id, cev.itemid
        """

        coverageDf = pd.read_sql_query(coverageQuery, con)

        allIcustaysDf = pd.merge(allIcustaysDf, coverageDf, on=['subject_id', 'hadm_id', 'stay_id'], how='inner')

    allIcustaysDf.to_csv('data/coverage.csv')
