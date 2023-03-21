import pandas as pd

# Read in CSV file
df = pd.read_csv('error_log_last7.csv')

# Create function to aggregate codes and remove duplicates
def unique_codes(x):
    return ', '.join(set(x))

# Create new dataframe to aggregate by serial
serial_df = df.groupby('serial').agg({
    'type': 'first',
    'company': 'first',
    'serial': 'first',
    'name': 'first',
    'code': unique_codes
})

# If a charger ONLY has certain errors logged, remove it from the output df
# Mostly to filter timeout codes and vehicle errors
serial_df = serial_df[~(
    (serial_df['code'].str.contains('1|2') & (serial_df['code'].str.len() <= 3)) | # remove serials with only codes 1 or 2
    ((serial_df['type'] == 'DCFC') & serial_df['code'].str.contains('62|63|74|188|302|303') & (serial_df['code'].str.len() <= 9)) # remove serials with DCFC and only codes 62, 63, 74, 188, 302, 303
)]

# Output aggregated dataframe to CSV file
serial_df.to_csv('output.csv', index=False)
print("Done.")
