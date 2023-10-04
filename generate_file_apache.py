import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.io import WriteToText

options = PipelineOptions()

# Define schema for dataset1 and dataset2
dataset1_schema = {
    'invoice_id': int,
    'legal_entity': str,
    'counter_party': str,
    'rating': int,
    'status': str,
    'value': float
}

dataset2_schema = {
    'counter_party': str,
    'tier': int
}

class ParseDataset1(beam.DoFn):
    def process(self, element):
        invoice_id, legal_entity, counter_party, rating, status, value = element.split(',')
        return [{
            'invoice_id': int(invoice_id),
            'legal_entity': legal_entity,
            'counter_party': counter_party,
            'rating': int(rating),
            'status': status,
            'value': float(value)
        }]

class ParseDataset2(beam.DoFn):
    def process(self, element):
        counter_party, tier = element.split(',')
        return [{
            'counter_party': counter_party,
            'tier': int(tier)
        }]

class JoinData(beam.DoFn):
    def process(self, element):
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
         
class CalculateAggregates(beam.DoFn):
    def process(self, element):
        (counter_party, legal_entity, tier), records = element
        max_rating = max(record['rating'] for record in records)
        sum_value_arap = sum(record['value'] for record in records if record['status'] == 'ARAP')
        sum_value_accr = sum(record['value'] for record in records if record['status'] == 'ACCR')

        yield {
            'counter_party': counter_party,
            'legal_entity': legal_entity,
            'tier': tier,
            'max_rating': max_rating,
            'sum_arap': sum_value_arap,
            'sum_accr': sum_value_accr
        }

class CalculateTotals(beam.DoFn):
    def process(self, element):
        (key), records = element
        max_rating = max(record['rating'] for record in records)
        sum_value_arap = sum(record['value'] for record in records if record['status'] == 'ARAP')
        sum_value_accr = sum(record['value'] for record in records if record['status'] == 'ACCR')

        
        if isinstance(key, int):
            counter_party = 'Total'
            legal_entity = 'Total'
            tier = key
        elif 'C' in key: 
            counter_party = key
            legal_entity = 'Total'
            tier = 'Total'
        else:
            counter_party = 'Total'
            legal_entity = key
            tier = 'Total'

        yield {
            'counter_party': counter_party,
            'legal_entity': legal_entity,
            'tier': tier,
            'max_rating': max_rating,
            'sum_arap': sum_value_arap,
            'sum_accr': sum_value_accr
        }


def dict_to_csv(record):
    # Convert a dictionary to a CSV formatted string
    return ','.join(str(record[key]) for key in record)


def generate_apache_file(dataset1_path: str, dataset2_path: str):
    with beam.Pipeline(options=options) as p:
        # Read datasets
        dataset1_pcollection = (
            p | 'Read dataset 1' >> beam.io.ReadFromText(dataset1_path, skip_header_lines=1)
            | 'Parse dataset 1' >> beam.ParDo(ParseDataset1())
        )
        dataset2_pcollection = (
            p | 'Read dataset 2' >> beam.io.ReadFromText(dataset2_path, skip_header_lines=1)
            | 'Parse dataset 2' >> beam.ParDo(ParseDataset2())
        )

        # Key the PCollections by 'counter_party'
        dataset1_keyed = dataset1_pcollection | 'Key dataset1' >> beam.Map(lambda x: (x['counter_party'], x))
        dataset2_keyed = dataset2_pcollection | 'Key dataset2' >> beam.Map(lambda x: (x['counter_party'], x))

        # CoGroupByKey to join based on 'counter_party'
        joined_data = {'dataset1': dataset1_keyed, 'dataset2': dataset2_keyed} | beam.CoGroupByKey()
        processed_data = joined_data | 'Process Joined Data' >> beam.ParDo(JoinData())

        # Group by 'counter_party', 'legal_entity', and 'tier' and calculate aggregates
        grouped_data_multip_cols = processed_data | 'Group by keys' >> beam.GroupBy(lambda x: (x['counter_party'], x['legal_entity'], x['tier']))
        aggregated_data_by_multi_cols = grouped_data_multip_cols | 'Calculate Aggregates' >> beam.ParDo(CalculateAggregates())

        #Group by counter_party and calculate aggregates 
        grouped_by_counter_party = processed_data | 'Group by couter party' >> beam.GroupBy(lambda x: (x['counter_party']))
        aggregated_by_counter_party = grouped_by_counter_party | 'Calculate totals by counter party' >> beam.ParDo(CalculateTotals())
        
        #Group by legal_entity and calculate aggregates 
        grouped_by_legal_entity = processed_data | 'Group by legal_entity' >> beam.GroupBy(lambda x: (x['legal_entity']))
        aggregated_by_legal_entity = grouped_by_legal_entity | 'Calculate totals by legal_entity' >> beam.ParDo(CalculateTotals())

        #Group by tier and calculate aggregates 
        grouped_by_tier = processed_data | 'Group by tier' >> beam.GroupBy(lambda x: (x['tier']))
        aggregated_by_tier = grouped_by_tier | 'Calculate totals by tier' >> beam.ParDo(CalculateTotals())

        #join all rows together 
        header = 'counter_party,legal_entity,tier,max_rating,sum_ARAP,sum_ACCR'
        final_data = ((aggregated_data_by_multi_cols,aggregated_by_counter_party, aggregated_by_legal_entity, aggregated_by_tier) | 'Merge PCollections' >> beam.Flatten())
        
        #Write to csv file 
        csv_lines = final_data | "Convert to CSV lines" >> beam.Map(dict_to_csv)
        csv_lines | 'Write to csv' >> WriteToText(file_path_prefix='apache_output', file_name_suffix='.csv', header=header)
