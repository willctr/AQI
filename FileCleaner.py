import os
import pandas as pd 

# function to clean AQI CSV files
def clean_aqi_files(directory):
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            filepath = os.path.join(directory, filename)
            year = filename.split('_')[2].split('.')[0] #extract the year
            output_file = f'cleaned_aqi_{year}.csv'

            df = pd.read_csv(filepath)
            columns_to_drop = ["Defining Site"] # specify which columns to drop
            df.drop(columns = columns_to_drop, inplace=True) # modify current df

            df.to_csv(output_file, index = False)
            print(f"AQI file '{filename}' cleaned and saved as '{output_file}'. ")

# function to clean Temperature CSV files
def clean_temp_files(directory):
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            filepath = os.path.join(directory, filename)
            year = filename.split('_')[2].split('.')[0] # extract the year
            output_file = f'cleaned_temp_{year}.csv'

            df = pd.read_csv(filepath)
            columns_to_drop = ["Parameter Code", "POC", "Datum", "Parameter Name", 
                               "Sample Duration", "Pollutant Standard", "Units of Measure", 
                               "Event Type", "Observation Count", "Observation Percent", 
                               "Method Code", "Method Name", "Date of Last Change"]
            df.drop(columns = columns_to_drop, inpalce = True)
            print(f"Temperature file '{filename}' cleaned and saved as '{output_file}'.")


# directory paths
aqi_directory = 'data/daily_aqi'
temp_directory = 'data/daily_temp'

# clean AQI files
print("Cleaning AQI files...")
clean_aqi_files(aqi_directory)

# clean temperature files
print("Cleaning Temperature files...")
clean_temp_files(temp_directory)

print("\nAll files cleaned and saved.")

