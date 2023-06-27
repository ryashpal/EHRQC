import pandas as pd


def merge_columns(*merge_columns, input_file_name, output_column_name='merged', output_file_name='merged.csv'):
    df = pd.read_csv(input_file_name)
    columns = list(merge_columns)
    df[output_column_name] = df[columns].mean(axis=1)
    df.drop(columns, axis=1, inplace=True)
    df.to_csv(output_file_name, index=False)


if __name__ == '__main__':
    merge_columns('col2', 'col3', 'col4', input_file_name='snippets/python/data/testing.csv', output_column_name='merged_col', output_file_name='snippets/python/data/merged.csv')
