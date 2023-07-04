import pandas as pd


def _merge_columns(*merge_columns, input_df, output_column_name='merged'):
    columns = list(merge_columns)
    input_df[output_column_name] = input_df[columns].mean(axis=1)
    input_df.drop(columns, axis=1, inplace=True)


def merge_columns(column_mapping, input_file_name, output_file_name='merged.csv'):
    df = pd.read_csv(input_file_name)
    for key in column_mapping.keys():
        _merge_columns(*column_mapping[key], input_df=df, output_column_name=key)
    df.to_csv(output_file_name, index=False)


if __name__ == '__main__':
    merge_columns({'merged_col_1' : ['col1', 'col2'], 'merged_col_2' : ['col3', 'col4']}, input_file_name='snippets/python/data/testing.csv', output_file_name='snippets/python/data/merged.csv')
