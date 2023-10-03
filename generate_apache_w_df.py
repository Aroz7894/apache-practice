import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

options = PipelineOptions()

return_dict = {}

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


class GroupByKeys(beam.DoFn):
     def process(self, element):
        for key in element:
            if key not in return_dict:
                return_dict[key] = [element[key]]
            else:
                return_dict[key].append(element[key])
        return return_dict
         
class CalculateAggregates(beam.DoFn):
    def process(self, element):
        (counter_party, legal_entity, tier), records = element
        max_rating = max(records, key=lambda x: x['rating'])['rating']
        sum_value_arap = sum(record['value'] for record in records if record['status'] == 'ARAP')
        sum_value_accr = sum(record['value'] for record in records if record['status'] == 'ACCR')

        yield {
            'counter_party': counter_party,
            'legal_entity': legal_entity,
            'tier': tier,
            'max_rating': max_rating,
            'sum_value_arap': sum_value_arap,
            'sum_value_accr': sum_value_accr
        }

if __name__ == "__main__":
    with beam.Pipeline(options=options) as p:
        # Read datasets
        dataset1_pcollection = (
            p | 'Read dataset 1' >> beam.io.ReadFromText('resources/dataset1.csv', skip_header_lines=1)
              | 'Parse dataset 1' >> beam.ParDo(ParseDataset1())
        )
        dataset2_pcollection = (
            p | 'Read dataset 2' >> beam.io.ReadFromText('resources/dataset2.csv', skip_header_lines=1)
              | 'Parse dataset 2' >> beam.ParDo(ParseDataset2())
        )

        # Key the PCollections by 'counter_party'
        dataset1_keyed = dataset1_pcollection | 'Key dataset1' >> beam.Map(lambda x: (x['counter_party'], x))
        dataset2_keyed = dataset2_pcollection | 'Key dataset2' >> beam.Map(lambda x: (x['counter_party'], x))

        # CoGroupByKey to join based on 'counter_party'
        joined_data = {'dataset1': dataset1_keyed, 'dataset2': dataset2_keyed} | beam.CoGroupByKey()

        # Process the joined data
        processed_data = joined_data | 'Process Joined Data' >> beam.ParDo(JoinData())

        # Group by 'counter_party', 'legal_entity', and 'tier' and calculate aggregates
        grouped_data = processed_data | 'Group by keys' >> beam.Map(lambda x: {(x['counter_party'], x['legal_entity'], x['tier']): x})
        group_single_key = grouped_data | 'group like keys together' >> beam.ParDo(GroupByKeys())

        print(return_dict)
        #aggregated_data = grouped_data | 'Calculate Aggregates' >> beam.ParDo(CalculateAggregates())

        # Print the output
        group_single_key | 'Print Output' >> beam.Map(print)
