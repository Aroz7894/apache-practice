from generate_file_pandas import generate_pandas_output_file
from generate_file_apache import generate_apache_file



if __name__ == "__main__":
    dataset1_path = 'resources/dataset1.csv'
    dataset2_path = 'resources/dataset2.csv'
    generate_pandas_output_file(dataset1_path, dataset2_path)
    generate_apache_file(dataset1_path, dataset2_path)
