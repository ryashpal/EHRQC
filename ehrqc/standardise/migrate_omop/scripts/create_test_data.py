# This Python code is for quickly creating a test EHR data (1% sampled) from the MIMIC IV data
# Python code is better than the shell script as it handles the data enclosed in double quotes with commas inside better
# Because of this usage of shell script might result in a corrupted file sometimes

import pandas as pd


# For Chartevents and Labevents follow the below template
patientsDf = pd.read_csv('test_data/patients.csv', sep=',', quotechar='"')
charteventsDf = pd.read_csv('1.0/icu/xaa', sep=',', quotechar='"')
outDfs = []
for i in ['a', 'b', 'c', 'd', 'e', 'f', 'g', 'h', 'i', 'j', 'k', 'l', 'm', 'n', 'o', 'p', 'q', 'r', 's', 't', 'u']:
    charteventsDf = pd.read_csv('1.0/icu/xa' + i, sep=',', quotechar='"')
    outDfs.append(pd.merge(patientsDf, charteventsDf, on='subject_id'))
outDf = pd.concat(outDfs)
outDf.to_csv('test_data/chartevents.csv', index=False)


# For all others follow the below template

patientsDf = pd.read_csv('test_data/patients.csv', sep=',', quotechar='"')
pharmacyDf = pd.read_csv('1.0/hosp/pharmacy.csv', sep=',', quotechar='"')
outDf = pd.merge(patientsDf, pharmacyDf, on='subject_id')
outDf.to_csv('test_data/pharmacy.csv', index=False)
