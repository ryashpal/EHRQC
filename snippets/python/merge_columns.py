import pandas as pd


def merge_columns(*merge_columns, time_stamp_column, input_file_name, output_column_name='merged', output_file_name='merged.csv'):
    df = pd.read_csv(input_file_name)
    columns = list(merge_columns)
    columns.append(time_stamp_column)
    df = df[columns]
    df = df.groupby(time_stamp_column).agg('mean').reset_index()
    out_df = pd.DataFrame(df[time_stamp_column])
    out_df[output_column_name] = df[list(merge_columns)].mean(axis=1)
    out_df = out_df.dropna()
    out_df.to_csv(output_file_name, index=False)


if __name__ == '__main__':
    merge_columns('col2', 'col3', time_stamp_column = 'col4', input_file_name='snippets/python/data/testing.csv', output_column_name='merged_col', output_file_name='snippets/python/data/merged.csv')
