import pandas as pd
from datetime import datetime

# Load the CSV dataset into a DataFrame with specified data type for the 'value' column
df = pd.read_csv("readings-full-2024-01-06.csv", dtype={'value': str})

# Extract the date from the input file
input_date = pd.to_datetime(df['date']).dt.strftime('%Y-%m-%d').iloc[0]

# Filter the DataFrame to get 'Stage' measures only from the station with stationReference "2904TH"
filtered_df = df[(df['stationReference'] == '2904TH') & (df['parameter'] == 'level') & (df['qualifier'] == 'Stage')]

# Create the output file name with the extracted date
output_filename = f"filtered_stage_measures_2904TH_{input_date}.csv"

# Save the filtered DataFrame to the new CSV file
filtered_df.to_csv(output_filename, index=False)

print(f"Filtered Stage measures have been saved to '{output_filename}'")
