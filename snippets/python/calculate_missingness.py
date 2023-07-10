def calculate_missingness_for_large_file():
    import pandas as pd
    import numpy as np

    chunksize=100
    missing_df_list = []
    for df in pd.read_csv("all_obs.csv",  chunksize=chunksize):
        df.replace('', np.nan, inplace=True)
        df = df.dropna(subset=df.columns[~df.columns.isin(['PATIENT_ID', 'EPISODE_ID', 'RESULT_UPDT_DATETIME'])], how='any')
        missing_counts = df.isnull().sum()
        missing_df = pd.DataFrame({'column_name': df.columns, 'missing_count': missing_counts, 'total_count': len(df.columns)*[len(df)]})
        missing_df_list.append(missing_df)
    final_df = pd.concat(missing_df_list, ignore_index=True)
    final_agg_df = final_df.groupby(['column_name']).sum().reset_index()
    final_agg_df['percentage_missing'] = round(final_agg_df['missing_count']/final_agg_df['total_count']*100, 2)
    final_agg_df.to_csv("all-obs-missingness.csv", index=None)
