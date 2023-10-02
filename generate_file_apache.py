# import apache_beam as beam
# import pandas as pd
# from apache_beam.io import WriteToText
# from apache_beam.options.pipeline_options import PipelineOptions


# # Function to parse CSV lines for dataset1
# def parse_dataset1(element):
#     return {
#         'invoice_id': int(element['invoice_id']),
#         'legal_entity': element['legal_entity'],
#         'counter_party': element['counter_party'],
#         'rating': int(element['rating']),
#         'status': element['status'],
#         'value': float(element['value'])
#     }


# # Function to parse CSV lines for dataset2
# def parse_dataset2(element):
#     return {
#         'counter_party': element['counter_party'],
#         'tier': int(element['tier'])
#     }

# def join_datasets(element):
#     key = element[0]
#     dataset1_values, dataset2_values = element[1]
#     for data1 in dataset1_values:
#         for data2 in dataset2_values:
#             if data1['counter_party'] == data2['counter_party']:
#                 yield {
#                     'legal_entity': data1['legal_entity'],
#                     'counter_party': data1['counter_party'],
#                     'tier': data2['tier'],
#                     'rating': data1['rating'],
#                     'status': data1['status'],
#                     'value': data1['value']
#                 }

# # Function to format the output
# def format_output(element):
#     return f"{element['legal_entity']}, {element['counter_party']}, {element['tier']}, {element['max_rating']}, {element['sum_value_ARAP']}, {element['sum_value_ACCR']}"

# # Pipeline options
# options = PipelineOptions()

# # Read the CSV files using pandas
# dataset1 = pd.read_csv('resources/dataset1.csv')
# dataset2 = pd.read_csv('resources/dataset2.csv')

# # Create a pipeline
# with beam.Pipeline(options=options) as p:
#     # Parse the datasets and convert to dictionaries
#     dataset1 = (
#         p | 'Parse Dataset1' >> beam.Create(dataset1.to_dict('records'))
#           | 'Map Dataset1' >> beam.Map(parse_dataset1)
#     )

#     dataset2 = (
#         p | 'Parse Dataset2' >> beam.Create(dataset2.to_dict('records'))
#           | 'Map Dataset2' >> beam.Map(parse_dataset2)
#     )

#     # # Join dataset1 with dataset2 on 'counterparty'
#     joined_data = (
#         {'dataset1': dataset1, 'dataset2': dataset2}
#         | 'Group By Key' >> beam.CoGroupByKey()
#         | 'Join Datasets' >> beam.ParDo(join_datasets)
#     )

#     # # Calculate required aggregates and format the output
#     # formatted_output = (
#     #     joined_data
#     #     | 'Calculate Aggregates' >> beam.Map(
#     #         lambda element: {
#     #             'legal_entity': element[1]['dataset1'][0]['legal_entity'],
#     #             'counterparty': element[1]['dataset1'][0]['counterparty'],
#     #             'tier': element[1]['dataset2'][0]['tier'],
#     #             'max_rating': element[1]['dataset2'][0]['max_rating'],
#     #             'sum_value_ARAP': sum(item['value'] for item in element[1]['dataset1'] if item['status'] == 'ARAP'),
#     #             'sum_value_ACCR': sum(item['value'] for item in element[1]['dataset1'] if item['status'] == 'ACCR')
#     #         }
#     #     )
#     #     | 'Format Output' >> beam.Map(format_output)
#     # )

#     # # Add records for total aggregation at the end of each legal_entity, counterparty, and tier
#     # total_aggregates = (
#     #     formatted_output
#     #     | 'Add Total Aggregates' >> beam.Map(
#     #         lambda line: f"Total,{line.split(',')[1]},{line.split(',')[2]},, "
#     #                      f"{sum(float(value.split(',')[4]) for value in formatted_output if value.split(',')[1] == line.split(',')[1])}, "
#     #                      f"{sum(float(value.split(',')[5]) for value in formatted_output if value.split(',')[1] == line.split(',')[1])}"
#     #     )
#     # )

#     # Write the output to a file
#     joined_data | 'Write Output' >> WriteToText('apache_output.csv', file_name_suffix='.csv')

import apache_beam as beam
from apache_beam.io import ReadFromText, WriteToText
from apache_beam.options.pipeline_options import PipelineOptions
import pandas as pd

class ParseDataset1(beam.DoFn):
    def process(self, element):
        return {
            'invoice_id': int(element['invoice_id']),
            'legal_entity': element['legal_entity'],
            'counter_party': element['counter_party'],
            'rating': int(element['rating']),
            'status': element['status'],
            'value': float(element['value'])
        }

class ParseDataset2(beam.DoFn):
    def process(self, element):
        return {
            'counter_party': element['counter_party'],
            'tier': int(element['tier'])
        }


def join_datasets(element):
    key, values = element
    dataset1_values = values['dataset1']
    dataset2_values = values['dataset2']

    for data1 in dataset1_values:
        for data2 in dataset2_values:
            if data1['counter_party'] == data2['counter_party']:
                yield {
                    'legal_entity': data1['legal_entity'],
                    'counter_party': data1['counter_party'],
                    'tier': data2['tier'],
                    'rating': data1['rating'],
                    'status': data1['status'],
                    'value': data1['value']
                }

options = PipelineOptions()

with beam.Pipeline(options=options) as p:
    dataset1 = pd.read_csv('resources/dataset1.csv')
    dataset2 = pd.read_csv('resources/dataset2.csv')
    
    dataset1 = (
        p | 'Create Dataset1' >> beam.Create(dataset1.to_dict('records'))
          | 'Parse Dataset1' >> beam.ParDo(ParseDataset1())
          | 'Key by counter_party' >> beam.Map(lambda x: (x['counter_party'], x))
          | 'Group by counter_party' >> beam.GroupByKey()
    )
    dataset2 = (
        p | 'Create Dataset2' >> beam.Create(dataset2.to_dict('records'))
          | 'Parse Dataset2' >> beam.ParDo(ParseDataset2())
    )

    joined_data = (
        {'dataset1': dataset1, 'dataset2': dataset2}
        | 'CoGroupByKey' >> beam.CoGroupByKey()
        | 'Join Datasets' >> beam.ParDo(join_datasets)
    )

    # Write the output to a file
    joined_data | 'Write Output' >> WriteToText('apache_output.csv', file_name_suffix='.csv')
