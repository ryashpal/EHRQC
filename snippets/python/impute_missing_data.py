import os
import pandas as pd

from statsmodels.multivariate.pca import PCA


# path = os.getcwd() + '/../../data/mimic_top_numeric_vitals.csv'
inPath = os.getcwd() + '/data/mimic_top_numeric_vitals.csv'
dataDf = pd.read_csv(inPath)

dataDf = dataDf.iloc[: , 1:]
dataDf.set_index('stay_id', inplace=True)

pc = PCA(data=dataDf, ncomp=1, missing='fill-em', method='nipals')

imputedDf = pd.DataFrame(pc._adjusted_data)

outPath = os.getcwd() + '/data/mimic_top_numeric_vitals_nipals_imputed.csv'
imputedDf.to_csv(outPath)
