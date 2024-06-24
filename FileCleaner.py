import os
import pandas as pd

class FileCleaner:
    def __init__(self, aqi_directory, temp_directory):
        self.aqi_directory = aqi_directory
        self.temp_directory = temp_directory

    # function to clean AQI CSV files
    def clean_aqi_files(self):
        for filename in os.listdir(self.aqi_directory):
            if filename.startswith("cleaned_"):
                continue  # Skip already cleaned files
            if filename.endswith(".csv"):
                filepath = os.path.join(self.aqi_directory, filename)
                year = filename.split('_')[-1].split('.')[0] #extract the year
                output_file = os.path.join(self.aqi_directory, f'cleaned_aqi_{year}.csv') # save in same directory

                df = pd.read_csv(filepath, low_memory = False)
                columns_to_drop = ["Defining Site"] # specify which columns to drop
                df.drop(columns = columns_to_drop, inplace=True) # modify current df

                df.to_csv(output_file, index = False)
                print(f"AQI file '{filename}' cleaned and saved as '{output_file}'. ")

    # function to clean Temperature CSV files
    def clean_temp_files(self):
        for filename in os.listdir(self.temp_directory):
            if filename.startswith("cleaned_"):
                continue  # Skip already cleaned files
            if filename.endswith(".csv"):
                filepath = os.path.join(self.temp_directory, filename)
                year = filename.split('_')[-1].split('.')[0] # extract the year
                output_file = os.path.join(self.temp_directory, f'cleaned_temp_{year}.csv')

                df = pd.read_csv(filepath, low_memory = False)
                columns_to_drop = ["Parameter Code", "POC", "Datum", "Parameter Name", 
                                "Sample Duration", "Pollutant Standard", "Units of Measure", 
                                "Event Type", "Observation Count", "Observation Percent", 
                                "Method Code", "Method Name", "Date of Last Change"]
                df.drop(columns = columns_to_drop, inplace = True)

                df.to_csv(output_file, index = False)
                print(f"Temperature file '{filename}' cleaned and saved as '{output_file}'.")
    

    def clean_all_files(self):
        print("Cleaning AQI files...")
        self.clean_aqi_files()

        print("Cleaning Temperature files...")
        self.clean_temp_files()

        print("\nAll files cleaned and saved.")


# Usage
if __name__ == "__main__":
    aqi_directory = 'data/daily_aqi'
    temp_directory = 'data/daily_temp'

    file_cleaner = FileCleaner(aqi_directory, temp_directory) #create instance of FileCleaner
    file_cleaner.clean_all_files() # call cleaner functions

# # directory paths
# aqi_directory = 'data/daily_aqi'
# temp_directory = 'data/daily_temp'

# # clean AQI files
# print("Cleaning AQI files...")
# clean_aqi_files(aqi_directory)

# # clean temperature files
# print("Cleaning Temperature files...")
# clean_temp_files(temp_directory)

# print("\nAll files cleaned and saved.")

