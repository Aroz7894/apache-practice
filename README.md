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
To run the code to generate the output csv files, run *python generate_file_pandas.py* and *python generate_file_apache.py* for the respective frameworks. 