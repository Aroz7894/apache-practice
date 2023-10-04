# apache-practice

## Complete exercise using two different frameworks.

1. pandas
2. apache beam python https://beam.apache.org/documentation/sdks/python/

using two input files dataset1 and dataset2 

join dataset1 with dataset2 and get tier

generate below output file

legal_entity, counterparty, tier, max(rating by counterparty), sum(value where status=ARAP), sum(value where status=ACCR)

Also create new record to add total for each of legal entity, counterparty & tier.

Sample data:
legal_entity, counterparty, tier, max(rating by counterparty), sum(value where status=ARAP), sum(value where status=ACCR)
L1, Total, Total, calculated_value, calculated_value, calculated_value
L1, C1, Total, calculated_value, calculated_value, calculated_value
Total, C1, Total, calculated_value, calculated_value, calculated_value
Total, Total, 1, calculated_value, calculated_value, calculated_value
L2, Total, Total, calculated_value, calculated_value, calculated_value
....
like all other values.

where caluclated_value in sample data needs to be calculated using above method.

### My interpretation of the problem:
1. Group by legal_entity, counterparty, tier and calculate max(rating by counterparty), sum(value where status=ARAP), sum(value where status=ACCR) for each rating
2. Add records for total by each of legal entity, counterparty & tier and calcualte max(rating by counterparty), sum(value where status=ARAP), sum(value where status=ACCR)
3. Use total as place holder for empty values. 

## How To run 
1. run comman *pip install -r dependencies.txt* to install necessary libraries 
2. Run *python main.py* to generate the output csv files using both respective frameworks. *Note: if output files already exist they need to be deleted before running the code*.


## Things I would look into more
1. Utilizing apache beams aggregate_field method to do the calculation instead of having to write custom methods. 
    - This works but in the format i needed it in
    '''
    aggregated_by_tier = processed_data | 'Group by tier' >> beam.GroupBy(lambda x: (x['tier'])).aggregate_field(
        lambda x: (x['rating']), max, 'max_rating').aggregate_field(lambda x: x['value'] if x['status'] == 'ACCR' else 0, sum, 'sum_ACCR').aggregate_field(
            lambda x: x['value'] if x['status'] == 'ARAP' else 0, sum, 'sum_ARAP').aggregate_field(lambda x: x, total,'counter_party').aggregate_field(lambda x: x, total,'legal_entity')
    '''
2. Better way of generating csv file from apache beams PCCollection 
3. Using Apache Beams Dataframe library. Tried using it a couple different ways but had trouble merging the 2 datasets together. 
4. Better exception handling to catch specific errors
