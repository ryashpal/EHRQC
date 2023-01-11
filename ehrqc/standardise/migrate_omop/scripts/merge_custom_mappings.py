import pandas as pd
import os

dfList = []
print(len(dfList))


for file in os.listdir():
    if file.endswith('.csv') and file != 'merged.csv':
        dfList.append(pd.read_csv(file, sep=',', doublequote=True))

print(len(dfList))

mergedDf = pd.DataFrame(columns=dfList[0].columns)
mergedDf

for df in dfList:
    mergedDf = mergedDf.append(df)

mergedDf.shape

mergedDf.head()

mergedDf.to_csv('tmp_custom_mapping.csv', index=False)
