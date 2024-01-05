import os
import pandas as pd

# Load data from the CSV file
data = pd.read_csv("readings-full-2024-01-02.csv", dtype={'value': str})

# Load Thames station references from the file
with open("Thames_station_reference.txt", "r") as file:
    thames_station_references = [line.strip() for line in file]

# Filter rows where stationReference is in the list of Thames references
thames_stations = data[data["stationReference"].isin(thames_station_references)]

# Determine the writing mode based on the existence of the file
mode = 'w' if not os.path.isfile("thames_readings.csv") else 'a'

# Save the new dataframe to a CSV file in append mode
thames_stations.to_csv("thames_readings.csv", mode=mode, index=False, header=mode=='w')

# Display the first few rows of the new dataframe
print(thames_stations.head())
print(thames_stations.info())
