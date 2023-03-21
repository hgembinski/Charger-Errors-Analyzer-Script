import pandas as pd

# Read in CSV file
df = pd.read_csv('error_log_last7.csv')

# Create new dataframe to aggregate by serial
serial_df = df.groupby('serial').agg({
    'company': 'first',
    'name': 'first',
    'code': lambda x: ', '.join(x)
})

# Print out aggregated dataframe
print(serial_df)