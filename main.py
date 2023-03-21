import pandas as pd

# Read in CSV file
df = pd.read_csv('error_log_last7.csv')

#error codes to be ommitted
l2_codes = ['1','2']
dcfc_codes = ['1', '2', '62', '63', '64', '74', '188', '302', '303']

# Create function to aggregate codes and remove duplicates
def unique_codes(x):
    return ','.join(set(x))

def filter_errors(entry):
    codes = entry['code'].split(',')
    if entry['type'] == 'DCFC' and set(codes).issubset(dcfc_codes):
        print(entry['type'] + ' ' + entry['name'] + ' ' + entry['code'])
        return False
    elif set(codes).issubset(dcfc_codes):
        print(entry['type'] + ' ' + entry['name'] + ' ' + entry['code'])
        return False
    else:
        return True

# Create new dataframe to aggregate by serial
aggregate_df = df.groupby('serial').agg({
    'type': 'first',
    'company': 'first',
    'serial': 'first',
    'name': 'first',
    'code': unique_codes
})

output_df = aggregate_df[aggregate_df.apply(filter_errors, axis=1)]

# Output aggregated dataframe to CSV file
output_df.to_csv('output.csv', index=False)
print("Done.")
