from generate_file_pandas import generate_output_file
#from generate_file_apache import generate_output_w_beam



if __name__ == "__main__":
    dataset1_path = 'resources/dataset1.csv'
    dataset2_path = 'resources/dataset2.csv'
    generate_output_file(dataset1_path, dataset2_path)
    # generate_output_w_beam(dataset1_path, dataset2_path)
