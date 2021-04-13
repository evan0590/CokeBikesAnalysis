import pathlib
import pandas as pd
import numpy as np
from utils import *

# Create the output directory if it does not exist.
#
# documentation here:
# https://docs.python.org/3/library/pathlib.html#pathlib.Path.mkdir
pathlib.Path('./output').mkdir(exist_ok=True)

print('*' * 20)
print('Task 1')
print('*' * 20)

# Read in the tables as dataframes.
#
#
print("Reading in the tables as dataframes...")
core_bike_df = query_db("live_bike_data")
core_weather_df = query_db("live_weather_data")

# Make copies of the dataframes.
#
#
print("Making copies of the dataframes...")
bike_df = core_bike_df.copy()
weather_df = core_weather_df.copy()

# Show the number of rows in each of the dataframes.
#
#
print("\tBikes dataframe contains", f'{len(bike_df.index):,}', "rows.")
print("\tWeather dataframe contains", f'{len(weather_df.index):,}', "rows.")

# Remove inactive stations.
#
# documentation here:
# https://data.gov.ie/dataset/coca-cola-zero-bikes
print("Removing inactive stations from bikes dataframe...")
bike_df = bike_df.loc[bike_df['status'] == 0]

# Generate a dd-mm-yy attribute
#
#
print("Generating a dd-mm-yy attribute for the bikes dataframe...")
bike_df['dd_mm_yy'] = pd.to_datetime(bike_df['dt'], unit='s').dt.strftime('%d-%m-%y')

# Generate a corresponding single digit city indicator attribute for each dataframe
# using a ternary expression
# https://stackoverflow.com/questions/39109045/numpy-where-with-multiple-conditions
print("Generating a corresponding single digit city indicator attribute for each dataframe...")
bike_df['city_id'] = np.where(bike_df['station_id'] > 4000, 4,
                              (np.where(bike_df['station_id'] < 3000, 2, 3)))
weather_df['city_id'] = np.where(weather_df.name == "Cork", 2,
                                 (np.where(weather_df.name == "Limerick", 3, 4)))

# Lists of tuples containing df, station_id/city_id, date to be used as arguments for pool_process function
stations_data = [(bike_df, station, day) for day in bike_df['dd_mm_yy'].unique() for station in
                 bike_df['station_id'].unique()]
city_data = [(bike_df, city, day) for day in bike_df['dd_mm_yy'].unique() for city in bike_df['city_id'].unique()]

# Generate a bikes available citywide attribute
#
# calculate the total number of bike available in each city per dt (time entry)
print("Generating a bikes available citywide attribute...")
bike_df['bikes_available_citywide'] = 0
ba_citywide_output = pool_process(func=calculate_available_bikes_citywide, data=city_data, pool_size=8)
for dictionary in ba_citywide_output:
    bike_df.loc[[*dictionary], 'bikes_available_citywide'] = list(dictionary.values())

# Generate a count attribute named count_1 that cumulatively tracks bike usage at each station across every day.
#
#
print("Generating an attribute named count_1 that cumulatively tracks bike usage at each station across every day...")
bike_df['count_1'] = 0
count_1_output = pool_process(func=track_station_usage_by_date, data=stations_data, pool_size=8)
for dictionary in count_1_output:
    bike_df.loc[[*dictionary], 'count_1'] = list(dictionary.values())

# Generate a count attribute named count_2 that shows the total usage for each city across every day
#
#
print("Generating an attribute named count_2 that shows the total usage for each city across every day...")
bike_df['count_2'] = 0
count_2_output = pool_process(func=track_city_usage_by_date, data=city_data, pool_size=8)
for dictionary in count_2_output:
    bike_df.loc[[*dictionary], 'count_2'] = list(dictionary.values())

# Save the dataframes with the changes amended
#
#
print("Saving the dataframes with the changes amended...")
bike_df.to_csv('./output/bike_task1.csv')
weather_df.to_csv('./output/weather_task1.csv')

print("Done!")
print()
