import pandas as pd
import numpy as np


group_by_cols_list = ['legal_entity', 'counter_party', 'tier']


def generate_pandas_output_file(dataset1_path: str, dataset2_path: str):
    try:
        merged_df = merge_dataframes(dataset1_path, dataset2_path)
        groupby_df = generate_group_by_cals(merged_df)

        result_df = pd.concat([pd.DataFrame({}), groupby_df], ignore_index=True)
        for col_nm in group_by_cols_list:
            total_df = caluclate_totals(groupby_df, col_nm)
            result_df = pd.concat([result_df, total_df], ignore_index=True)
        result_df.fillna('Total', inplace=True)
        create_output_file(result_df)
    except Exception as e:
        print(e)
        print('Pandas File failed to be created') 


def merge_dataframes(dataset1_path: str, dataset2_path: str) -> pd.DataFrame:
    # reading csv files
    df1 = pd.read_csv(dataset1_path)
    df2 = pd.read_csv(dataset2_path)
    
    merged_df = pd.merge(df1, df2, 
                   on='counter_party', 
                   how='outer')

    return merged_df


def generate_group_by_cals(merged_df: pd.DataFrame  ) -> pd.DataFrame:
    # Group by 'legal_entity', 'counter_party', and 'tier' and calculate the required aggregates
    grouped_df = merged_df.groupby(group_by_cols_list).agg(
        max_rating=('rating', 'max'),
        sum_ARAP=('value', lambda x: x[merged_df['status'] == 'ARAP'].sum()),
        sum_ACCR=('value', lambda x: x[merged_df['status'] == 'ACCR'].sum())
    ).reset_index()
    return grouped_df


def caluclate_totals(df: pd.DataFrame, group_by_col_nm: str) -> pd.DataFrame:
    #calculate totals by given col name
    total_df = df.groupby([group_by_col_nm]).agg(
        max_rating=('max_rating', 'sum'),
        sum_ARAP=('sum_ARAP', 'sum'),
        sum_ACCR=('sum_ACCR', 'sum'),
    ).reset_index()
    return total_df


def create_output_file(df: pd.DataFrame):
    df.to_csv('output_pandas.csv', index=False)
    print("Pandas File Generated Successfully")