import os
import pandas as pd 

# function to clean temp CSV files
def clean_temp_files(directory):
    for filename in os.listdir(directory):
        if filename.endswith(".csv"):
            filepath = os.path.join(directory, filename)
            #year = filename.split('_')