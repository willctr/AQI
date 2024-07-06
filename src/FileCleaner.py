import os
import pandas as pd

class FileCleaner:
    def __init__(self, aqi_directory, temp_directory, ozone_directory, pm25_directory, 
                 pm10_directory, no2_directory, so2_directory, co_directory):
        self.aqi_directory = aqi_directory
        self.temp_directory = temp_directory
        self.ozone_directory = ozone_directory
        self.pm25_directory = pm25_directory
        self.pm10_directory = pm10_directory
        self.no2_directory = no2_directory
        self.so2_directory = so2_directory
        self.co_directory = co_directory


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
                columns_to_drop = ["Defining Site", "Number of Sites Reporting"] # specify which columns to drop
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
                columns_to_drop = ["State Code", "County Code", "Site Num", "Parameter Code", "POC", "Latitude", "Longitude", "Datum", "Parameter Name", 
                                "Sample Duration", "Pollutant Standard", "Units of Measure", 
                                "Event Type", "Observation Count", "Observation Percent", "AQI",
                                "Method Code", "Method Name", "Local Site Name", "Address", "Date of Last Change"]
                df.drop(columns = columns_to_drop, inplace = True)

                df.to_csv(output_file, index = False)
                print(f"Temperature file '{filename}' cleaned and saved as '{output_file}'.")

    # function to clean Ozone CSV files
    def clean_ozone_files(self):
        for filename in os.listdir(self.ozone_directory):
            if filename.startswith("cleaned_"):
                continue  # Skip already cleaned files
            if filename.endswith(".csv"):
                filepath = os.path.join(self.ozone_directory, filename)
                year = filename.split('_')[-1].split('.')[0] # extract the year
                output_file = os.path.join(self.ozone_directory, f'cleaned_ozone_{year}.csv')

                df = pd.read_csv(filepath, low_memory = False)
                columns_to_drop = ["State Code", "County Code", "Site Num", "Parameter Code", "POC", "Latitude", "Longitude", "Datum", "Parameter Name", 
                                "Sample Duration", "Pollutant Standard", "Units of Measure", 
                                "Event Type", "Observation Count", "Observation Percent",
                                "Method Code", "Method Name", "Local Site Name", "Address", "Date of Last Change"]
                df.drop(columns = columns_to_drop, inplace = True)

                df.to_csv(output_file, index = False)
                print(f"Ozone file '{filename}' cleaned and saved as '{output_file}'.")
    
    # function to clean pm2.5 CSV files
    def clean_pm25_files(self):
        for filename in os.listdir(self.pm25_directory):
            if filename.startswith("cleaned_"):
                continue  # Skip already cleaned files
            if filename.endswith(".csv"):
                filepath = os.path.join(self.pm25_directory, filename)
                year = filename.split('_')[-1].split('.')[0] # extract the year
                output_file = os.path.join(self.pm25_directory, f'cleaned_pm25_{year}.csv')

                df = pd.read_csv(filepath, low_memory = False)
                columns_to_drop = ["State Code", "County Code", "Site Num", "Parameter Code", "POC", "Latitude", "Longitude", "Datum", "Parameter Name", 
                                "Sample Duration", "Pollutant Standard", "Units of Measure", 
                                "Event Type", "Observation Count", "Observation Percent",
                                "Method Code", "Method Name", "Local Site Name", "Address", "Date of Last Change"]
                df.drop(columns = columns_to_drop, inplace = True)

                df.to_csv(output_file, index = False)
                print(f"PM2.5 file '{filename}' cleaned and saved as '{output_file}'.")
      
    # function to clean pm10 CSV files
    def clean_pm10_files(self):
        for filename in os.listdir(self.pm10_directory):
            if filename.startswith("cleaned_"):
                continue  # Skip already cleaned files
            if filename.endswith(".csv"):
                filepath = os.path.join(self.pm10_directory, filename)
                year = filename.split('_')[-1].split('.')[0] # extract the year
                output_file = os.path.join(self.pm10_directory, f'cleaned_pm10_{year}.csv')

                df = pd.read_csv(filepath, low_memory = False)
                columns_to_drop = ["State Code", "County Code", "Site Num", "Parameter Code", "POC", "Latitude", "Longitude", "Datum", "Parameter Name", 
                                "Sample Duration", "Pollutant Standard", "Units of Measure", 
                                "Event Type", "Observation Count", "Observation Percent",
                                "Method Code", "Method Name", "Local Site Name", "Address", "Date of Last Change"]
                df.drop(columns = columns_to_drop, inplace = True)

                df.to_csv(output_file, index = False)
                print(f"PM10 file '{filename}' cleaned and saved as '{output_file}'.")
      

    # function to clean NO2 CSV files
    def clean_no2_files(self):
        for filename in os.listdir(self.no2_directory):
            if filename.startswith("cleaned_"):
                continue  # Skip already cleaned files
            if filename.endswith(".csv"):
                filepath = os.path.join(self.no2_directory, filename)
                year = filename.split('_')[-1].split('.')[0] # extract the year
                output_file = os.path.join(self.no2_directory, f'cleaned_no2_{year}.csv')

                df = pd.read_csv(filepath, low_memory = False)
                columns_to_drop = ["State Code", "County Code", "Site Num", "Parameter Code", "POC", "Latitude", "Longitude", "Datum", "Parameter Name", 
                                "Sample Duration", "Pollutant Standard", "Units of Measure", 
                                "Event Type", "Observation Count", "Observation Percent",
                                "Method Code", "Method Name", "Local Site Name", "Address", "Date of Last Change"]
                df.drop(columns = columns_to_drop, inplace = True)

                df.to_csv(output_file, index = False)
                print(f"NO2 file '{filename}' cleaned and saved as '{output_file}'.")
    
    # function to clean SO2 CSV files
    def clean_so2_files(self):
        for filename in os.listdir(self.so2_directory):
            if filename.startswith("cleaned_"):
                continue  # Skip already cleaned files
            if filename.endswith(".csv"):
                filepath = os.path.join(self.so2_directory, filename)
                year = filename.split('_')[-1].split('.')[0] # extract the year
                output_file = os.path.join(self.so2_directory, f'cleaned_so2_{year}.csv')

                df = pd.read_csv(filepath, low_memory = False)
                columns_to_drop = ["State Code", "County Code", "Site Num", "Parameter Code", "POC", "Latitude", "Longitude", "Datum", "Parameter Name", 
                                "Sample Duration", "Pollutant Standard", "Units of Measure", 
                                "Event Type", "Observation Count", "Observation Percent",
                                "Method Code", "Method Name", "Local Site Name", "Address", "Date of Last Change"]
                df.drop(columns = columns_to_drop, inplace = True)

                df.to_csv(output_file, index = False)
                print(f"SO2 file '{filename}' cleaned and saved as '{output_file}'.")
    
    # function to clean CO CSV files
    def clean_co_files(self):
        for filename in os.listdir(self.co_directory):
            if filename.startswith("cleaned_"):
                continue  # Skip already cleaned files
            if filename.endswith(".csv"):
                filepath = os.path.join(self.co_directory, filename)
                year = filename.split('_')[-1].split('.')[0] # extract the year
                output_file = os.path.join(self.co_directory, f'cleaned_co_{year}.csv')

                df = pd.read_csv(filepath, low_memory = False)
                columns_to_drop = ["State Code", "County Code", "Site Num", "Parameter Code", "POC", "Latitude", "Longitude", "Datum", "Parameter Name", 
                                "Sample Duration", "Pollutant Standard", "Units of Measure", 
                                "Event Type", "Observation Count", "Observation Percent", "Address",
                                "Method Code", "Method Name", "Local Site Name", "Address", "Date of Last Change"]
                df.drop(columns = columns_to_drop, inplace = True)

                df.to_csv(output_file, index = False)
                print(f"CO file '{filename}' cleaned and saved as '{output_file}'.")
    
    

    def clean_all_files(self):
        print("Cleaning AQI files...")
        self.clean_aqi_files()

        print("Cleaning Temperature files...")
        self.clean_temp_files()

        print("Cleaning Ozone files...")
        self.clean_ozone_files()

        print("Cleaning PM2.5 files...")
        self.clean_pm25_files()

        print("Cleaning PM10 files...")
        self.clean_pm10_files()

        print("Cleaning NO2 files...")
        self.clean_no2_files()

        print("Cleaning SO2 files...")
        self.clean_so2_files()

        print("Cleaning CO files...")
        self.clean_co_files()

        print("\nAll files cleaned and saved.")


# Usage
if __name__ == "__main__":
    aqi_directory = 'data/daily_aqi'
    temp_directory = 'data/daily_temp'
    ozone_directory = 'data/daily_ozone'
    pm25_directory = 'data/daily_pm2.5'
    pm10_directory = 'data/daily_pm10'
    no2_directory = 'data/daily_no2'
    so2_directory = 'data/daily_so2'
    co_directory = 'data/daily_co'

    file_cleaner = FileCleaner(aqi_directory, temp_directory, ozone_directory, pm25_directory,
                               pm10_directory, no2_directory, so2_directory, co_directory) #create instance of FileCleaner
    file_cleaner.clean_all_files() # call cleaner functions
