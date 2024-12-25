import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np 
import plotly.express as px 
import geopandas as gpd

from nestedMap import create_nested_map_by_state


class EDA:

    # Mapping dictionary of state abbreviations to full names
    state_abbreviation_map = {
        'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
        'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
        'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
        'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
        'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
        'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
        'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
        'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
        'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
        'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
        'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
        'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
        'WI': 'Wisconsin', 'WY': 'Wyoming'
    }

    def __init__(self, db_name='air.db'):
        self.conn = sqlite3.connect(db_name)

    def get_dataset_choice(self):
        print("Choose a dataset from the following options:")
        datasets = ['AQIdata', 'temperatures', 'ozone', 'pm25', 'pm10', 'no2', 'so2', 'co']
        for i, dataset in enumerate(datasets): 
            print(f"{i}. {dataset}")
        while True:
            try:
                choice = int(input("Enter the number corresponding to your choice: "))
                if 0 <= choice < len(datasets):
                    return datasets[choice]
                else:
                    print("Invalid choice. Please enter a valid number.")
            except ValueError:
                print("Invalid input. Please enter a number.")

    # load data from table parameter into dataframe
    def load_data(self, table_name):
        df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', self.conn)
        
        return df
    
    # user inputs state selection
    def get_state_choice(self, table_name):
        if table_name == 'AQIdata':
            df_aqi = self.load_data(table_name)
            df_aqi['State'] = df_aqi['CBSA'].apply(lambda x: x.split(', ')[-1])
            states = df_aqi['State'].unique()
        else:
            df = self.load_data(table_name)
            df['State'] = df['CBSA Name'].apply(lambda x: x.split(', ')[-1])
            states = df['State'].unique()

        print("Choose a state from the following options:")
        for i, state in enumerate(states):
            print(f"{i}. {state}")
        
        while True:
            try:
                choice = int(input("Enter the number corresponding to your choice: "))
                if 0 <= choice < len(states):
                    return states[choice]
                else:
                    print("Invalid choice. Please enter a valid number.")
            except ValueError:
                print("Invalid input. Please enter a number.")

    # filter the df by the state selection
    def filter_by_state(self, df, state_name):
        # if CBSA is a df column then this is aqi df
        if 'CBSA' in df.columns:
            # filter based on condition that the state in the CBSA matches state_name
            filtered_df = df[df['CBSA'].str.endswith(state_name)]
        # non aqi table has CBSA Name column not CBSA column
        elif 'CBSA Name' in df.columns:
            filtered_df = df[df['CBSA Name'].str.endswith(state_name)]
        else:
            print("State column not found in the dataset.")
            return pd.DataFrame()
        
        if filtered_df.empty:
            print(f"No data found for state: {state_name}")
        return filtered_df



    def print_summary_stats(self, df, table_name):
        print(f"Summary statistics for table: {table_name}")
        print(df.describe())

    def plot_histogram(self, df, dataset_name):
        plt.figure(figsize=(8, 6))
        
        if dataset_name == 'AQIdata':
            column = 'AQI'
            plt.title('Histogram of AQI')
            plt.xlabel('AQI')
        else:
            column = 'Arithmetic Mean'
            plt.title('Histogram of Arithmetic Mean')
            plt.xlabel('Arithmetic Mean')

            if column not in df.columns:
                print(f"Warning: '{column}' column not found in the dataset.")
                return
        
        sns.histplot(df[column], kde=True)
        plt.ylabel('Frequency')
        plt.show()
        
    def plot_facet_grid_paginated(self, df, dataset_name, regions_per_page=4):
        date_col = 'Date' if dataset_name == 'AQIdata' else 'Date Local'
        if date_col not in df.columns:
            print("Date column not found in the dataset.")
            return
        
        df[date_col] = pd.to_datetime(df[date_col])
        unique_cbsa = df['CBSA'].unique() if 'CBSA' in df.columns else df['CBSA Name'].unique()
        num_pages = (len(unique_cbsa) + regions_per_page - 1) // regions_per_page

        for i in range(num_pages):
            subset_cbsa = unique_cbsa[i * regions_per_page:(i + 1) * regions_per_page]
            subset_df = df[df['CBSA'].isin(subset_cbsa)] if 'CBSA' in df.columns else df[df['CBSA Name'].isin(subset_cbsa)]
            g = sns.FacetGrid(subset_df, col="CBSA" if 'CBSA' in df.columns else "CBSA Name", col_wrap=4, height=4, aspect=1.5)

            if dataset_name == 'AQIdata':
                g.map(sns.lineplot, date_col, "AQI")
                g.set_axis_labels("Date", "AQI")
                plt.subplots_adjust(top=0.9)
                g.figure.suptitle(f'Time Series of AQI by CBSA Region (Page {i+1})', fontsize=16)
            else:
                g.map(sns.lineplot, date_col, "Arithmetic Mean")
                g.set_axis_labels("Date Local", "Arithmetic Mean")
                plt.subplots_adjust(top=0.9)
                g.figure.suptitle(f'Time Series of Arithmetic Mean ({dataset_name}) by CBSA Region (Page {i+1})', fontsize=16)

            g.set_titles(col_template="{col_name}")
            plt.show()

    def plot_interactive_facet_grid(self, df, dataset_name):
        date_col = 'Date' if dataset_name == 'AQIdata' else 'Date Local'
        if date_col not in df.columns:
            print("Date column not found in the dataset.")
            return
        
        fig = px.line(df, x=date_col, y='AQI' if dataset_name == 'AQIdata' else 'Arithmetic Mean', 
                      facet_col='CBSA' if 'CBSA' in df.columns else 'CBSA Name', facet_col_wrap=4,
                      title=f'Time Series of {"AQI" if dataset_name == "AQIdata" else f"Arithmetic Mean {dataset_name.upper()}"} by CBSA Region',
                      height=800)
        fig.update_layout(title_font_size=16)
        fig.show()

    def plot_spatial_heatmap(self, df, dataset_name, state_name):
        # Check for necessary columns
        if 'CBSA' in df.columns:
            location_col = 'CBSA'
        elif 'CBSA Name' in df.columns:
            location_col = 'CBSA Name'
        else:
            print("Location column not found in the dataset.")
            return

        if dataset_name == 'AQIdata':
            print("Spatial heatmaps are not available for AQIdata due to lack of latitude and longitude data.")
            return
        
        # Assuming Latitude and Longitude columns are present in the dataset
        if 'Latitude' not in df.columns or 'Longitude' not in df.columns:
            print("Latitude and Longitude columns are required for spatial heatmaps.")
            return

        if 'Arithmetic Mean' not in df.columns:
            print("Arithmetic Mean column not found in the dataset.")
            return

        # Ensure latitude and longitude data are numeric
        df['Latitude'] = pd.to_numeric(df['Latitude'], errors='coerce')
        df['Longitude'] = pd.to_numeric(df['Longitude'], errors='coerce')

        # Drop rows with invalid coordinates
        df = df.dropna(subset=['Latitude', 'Longitude'])

        # Aggregate data for the entire state
        aggregated_df = df.groupby(['Latitude', 'Longitude']).agg({'Arithmetic Mean': 'mean'}).reset_index()

        # Check if there is any data left after aggregation
        if aggregated_df.empty:
            print("No valid data available for heatmap.")
            return
        
        # # Print some of the data to verify
        # print("Sample data for scatter plot:")
        # print(aggregated_df[['Latitude', 'Longitude']].head(20))  # Print 20 rows

        # Plot heatmap
        fig = px.density_mapbox(aggregated_df, lat='Latitude', lon='Longitude', z='Arithmetic Mean', radius=10,
                                center=dict(lat=df['Latitude'].mean(), lon=df['Longitude'].mean()), zoom=5,
                                mapbox_style="open-street-map",
                                title=f'Spatial Heatmap of Arithmetic Mean {dataset_name.upper()} ({state_name})')
        fig.update_layout(title_font_size=16)
        fig.show()



    # reduce memory footprint by loading chunks
    def load_data_in_chunks(self, query, chunk_size=10000):
        # empty list to store 10000 row chunks
        chunks = []
        # query chunks of data
        for chunk in pd.read_sql(query, self.conn, chunksize=chunk_size):
            # append chunk to chunks list
            chunks.append(chunk)
        # concat to get one dataframe, ignore index for continuous indexing
        return pd.concat(chunks, ignore_index=True)

    # load sql queries into df, merge component df into combined df
    def load_combined_data(self, state_name=None, geometry=False):

        # Load AQI data in chunks
        aqi_query = 'SELECT Date, CBSA, AQI FROM AQIdata'
        aqi_df = self.load_data_in_chunks(aqi_query)
        print("Loaded AQI data")

        # initialize combined df with aqi df
        combined_df = aqi_df

        # Latitude and Longitude pulled from tables
        if geometry:
            # Load other data in chunks and merge incrementally to prevent memory overload
            queries = {
                'Temperature': 'SELECT "Date Local" AS Date, "CBSA Name" AS CBSA, "Arithmetic Mean" AS Temperature, Longitude, Latitude FROM temperatures',
                'SO2': 'SELECT "Date Local" AS Date, "CBSA Name" AS CBSA, "Arithmetic Mean" AS SO2, Longitude, Latitude FROM so2',
                'Ozone': 'SELECT "Date Local" AS Date, "CBSA Name" AS CBSA, "Arithmetic Mean" AS Ozone, Longitude, Latitude FROM ozone',
                'PM10': 'SELECT "Date Local" AS Date, "CBSA Name" AS CBSA, "Arithmetic Mean" AS PM10, Longitude, Latitude FROM pm10',
                # ! df's not included because of memory pressure during merge
                # 'NO2': 'SELECT "Date Local" AS Date, "CBSA Name" AS CBSA, "Arithmetic Mean" AS NO2, Longitude, Latitude FROM no2',
                # 'PM25': 'SELECT "Date Local" AS Date, "CBSA Name" AS CBSA, "Arithmetic Mean" AS PM25, Longitude, Latitude FROM pm25',
                # 'CO': 'SELECT "Date Local" AS Date, "CBSA Name" AS CBSA, "Arithmetic Mean" AS CO, Longitude, Latitude FROM co'
            } 
            
            # iterate over dictionary of queries 
            for key, query in queries.items():
                # chunk loading
                df = self.load_data_in_chunks(query)
                print(f"Loaded {key} data")

                # filter df by state name
                if state_name:
                    df = self.filter_by_state(df, state_name)
                    print(f"Filtered {key} data by state")

                # Merge df on Date and CBSA with specific suffix handling
                suffix = f'_{key.lower()}'
                combined_df = combined_df.merge(df, on=['Date', 'CBSA'], how='left', suffixes=('', suffix))
                print(f"Merged {key} data")

                # Rename Latitude and Longitude columns with specific suffixes
                latitude_col = f'Latitude{suffix}'
                longitude_col = f'Longitude{suffix}'
                
                # Fill Latitude and Longitude only if they are currently NaN in combined_df
                if latitude_col in combined_df.columns:
                    combined_df['Latitude'] = combined_df['Latitude'].fillna(combined_df[latitude_col])
                if longitude_col in combined_df.columns:
                    combined_df['Longitude'] = combined_df['Longitude'].fillna(combined_df[longitude_col])

                # Drop the columns with suffixes after merging
                combined_df.drop(columns=[latitude_col, longitude_col], inplace=True, errors='ignore')
                

            # Filter the combined_df by state, long and lat introduce unfiltered data
            if state_name:
                combined_df = self.filter_by_state(combined_df, state_name)
                print(f"Filtered final combined_df by state")


            # Print sample of the combined df
            print(f"{combined_df.sample(n=10)}")
            # Count the number of unique CBSA values
            unique_cbsa_count = combined_df['CBSA'].nunique()
            # Print the count
            print(f"Number of unique CBSAs: {unique_cbsa_count}")
            
            return combined_df
        


        # no location coords pulled from tables
        else:
            # Load other data in chunks and merge incrementally to prevent memory overload
            queries = {
                'Temperature': 'SELECT "Date Local" AS Date, "CBSA Name" AS CBSA, "Arithmetic Mean" AS Temperature FROM temperatures',
                'SO2': 'SELECT "Date Local" AS Date, "CBSA Name" AS CBSA, "Arithmetic Mean" AS SO2 FROM so2',
                'Ozone': 'SELECT "Date Local" AS Date, "CBSA Name" AS CBSA, "Arithmetic Mean" AS Ozone FROM ozone',
                'PM10': 'SELECT "Date Local" AS Date, "CBSA Name" AS CBSA, "Arithmetic Mean" AS PM10 FROM pm10',
                # ! df's not included because of memory pressure during merge
                # 'NO2': 'SELECT "Date Local" AS Date, "CBSA Name" AS CBSA, "Arithmetic Mean" AS NO2 FROM no2',
                # 'PM25': 'SELECT "Date Local" AS Date, "CBSA Name" AS CBSA, "Arithmetic Mean" AS PM25 FROM pm25',
                # 'CO': 'SELECT "Date Local" AS Date, "CBSA Name" AS CBSA, "Arithmetic Mean" AS CO FROM co'
            }

            # iterate over dictionary of queries 
            for key, query in queries.items():
                # chunk loading
                df = self.load_data_in_chunks(query)
                print(f"Loaded {key} data")

                # filter df by state name
                if state_name:
                    df = self.filter_by_state(df, state_name)
                    print(f"Filtered {key} data by state")

                # merge df on Date and CBSA and join left to keep all rows
                combined_df = combined_df.merge(df, on=['Date', 'CBSA'], how='left')
                print(f"Merged {key} data")

            # Filter rows where all specified columns have values that are not negative and not null
            combined_df = combined_df[
                (combined_df["AQI"] >= 0) &
                (combined_df["Temperature"] >= 0) &
                (combined_df["SO2"] >= 0) &
                (combined_df["Ozone"] >= 0) &
                (combined_df["PM10"] >= 0) #&
                # ! df's not inlcuded bc of memory pressure
                # (combined_df["PM25"] >= 0) &
                # (combined_df["NO2"] >= 0) &
                # (combined_df["CO"] >= 0)
            ]

            # Print sample of the combined df
            print(f"{combined_df.sample(n=10)}")
            # Count the number of unique CBSA values
            unique_cbsa_count = combined_df['CBSA'].nunique()
            # Print the count
            print(f"Number of unique CBSAs: {unique_cbsa_count}")

            return combined_df
    
        df = self.load_data_in_chunks('SELECT "Date Local" AS Date, "CBSA Name" AS CBSA, "Arithmetic Mean" AS Ozone, Longitude, Latitude FROM ozone')
        print(df.head(5))
        return df


    def plot_correlation_matrix(self, df, state_name):
        # Select only numeric columns for correlation analysis
        numeric_df = df.select_dtypes(include=['float64', 'int64'])

        # Compute the correlation matrix
        corr_matrix = numeric_df.corr()

        # Plot the correlation matrix as a heatmap
        plt.figure(figsize=(10, 8))
        sns.heatmap(corr_matrix, annot=True, cmap='coolwarm', center=0, linewidths=0.5)
        plt.title(f'Correlation Matrix ({state_name})')
        plt.show()

    # get combined df and pass to plot correlation matrix
    def analyze_correlations(self, state_name):
        # Load the combined data
        combined_df = self.load_combined_data(state_name=state_name, geometry=True)

        # Plot the correlation matrix
        self.plot_correlation_matrix(combined_df, state_name=state_name)


    def create_geodataframe(self, df):
        gdf = gpd.GeoDataFrame(
            df, geometry=gpd.points_from_xy(df['Longitude'], df['Latitude'])
        )
        print("This is geo df: \n" )
        print(gdf.sample(n=5))
        return gdf
    
    def create_layers(self, gdf):
        layers = {}
        variables = ['AQI', 'Temperature', 'SO2', 'Ozone', 'PM10']
        # ! if memory pressue is solved use full variables list
        #variables = ['AQI', 'Temperature', 'SO2', 'Ozone', 'PM10', 'PM25', 'NO2', 'CO']

        for variable in variables:
            # Create a layer for each variable
            layers[variable] = gdf.dropna(subset=[variable])

        return layers
    
    def plot_overlay_map(self, state_name, geo_df):

        # Mapping dictionary of state abbreviations to full names
        state_abbreviation_map = {
            'AL': 'Alabama', 'AK': 'Alaska', 'AZ': 'Arizona', 'AR': 'Arkansas',
            'CA': 'California', 'CO': 'Colorado', 'CT': 'Connecticut', 'DE': 'Delaware',
            'FL': 'Florida', 'GA': 'Georgia', 'HI': 'Hawaii', 'ID': 'Idaho',
            'IL': 'Illinois', 'IN': 'Indiana', 'IA': 'Iowa', 'KS': 'Kansas',
            'KY': 'Kentucky', 'LA': 'Louisiana', 'ME': 'Maine', 'MD': 'Maryland',
            'MA': 'Massachusetts', 'MI': 'Michigan', 'MN': 'Minnesota', 'MS': 'Mississippi',
            'MO': 'Missouri', 'MT': 'Montana', 'NE': 'Nebraska', 'NV': 'Nevada',
            'NH': 'New Hampshire', 'NJ': 'New Jersey', 'NM': 'New Mexico', 'NY': 'New York',
            'NC': 'North Carolina', 'ND': 'North Dakota', 'OH': 'Ohio', 'OK': 'Oklahoma',
            'OR': 'Oregon', 'PA': 'Pennsylvania', 'RI': 'Rhode Island', 'SC': 'South Carolina',
            'SD': 'South Dakota', 'TN': 'Tennessee', 'TX': 'Texas', 'UT': 'Utah',
            'VT': 'Vermont', 'VA': 'Virginia', 'WA': 'Washington', 'WV': 'West Virginia',
            'WI': 'Wisconsin', 'WY': 'Wyoming'
        }

        # Convert state abbreviation to full name if needed
        if state_name in state_abbreviation_map:
            state_name = state_abbreviation_map[state_name]

        # Load the shapefile for US CBSA boundaries
        shapefile_path = 'tl_2023_us_state/tl_2023_us_state.shp'
        state_boundaries = gpd.read_file(shapefile_path)
        
        # Print valid state names in shapefile (for debugging)
        print("Valid state names in shapefile:")
        print(state_boundaries['NAME'].unique())
        
        # # Extract CBSA codes from the GeoDataFrame
        # cbsa_codes = geo_df['CBSA'].unique()
        # print("cbsa names in the geo df: \n")
        # print(cbsa_codes.unique())

        # Filter the boundaries for the selected state or region
        if '-' in state_name:
            states = state_name.split('-')
            state_boundaries = state_boundaries[state_boundaries['NAME'].isin(states)]
        else:
            state_boundaries = state_boundaries[state_boundaries['NAME'] == state_name]

        if state_boundaries.empty:
            raise ValueError(f"State(s) '{state_name}' not found in shapefile.")
        
        
        # Plot the boundaries
        base = state_boundaries.plot(color='white', edgecolor='black', figsize=(10, 10))
        
        # Define the layers to plot
        variables = ['AQI', 'Temperature', 'SO2', 'PM10', 'Ozone']
        colors = ['viridis', 'coolwarm', 'plasma', 'magma', 'cividis']  # Different color maps for variety
        
        # Plot each layer
        for variable, color in zip(variables, colors):
            if variable in geo_df.columns:
                plt.scatter(x=geo_df['Longitude'], y=geo_df['Latitude'], c=geo_df[variable], 
                            cmap=color, label=variable, alpha=0.5, edgecolor='k', s=20)
        
        plt.title(f'Spatial Overlay Map ({state_name})')
        plt.legend(loc='upper left')
        plt.show()

    def create_overlay(self, state_name):
        # Load the combined df with geometry
        df = self.load_combined_data(state_name=state_name, geometry=True)

        # Create a geo df
        geo_df = self.create_geodataframe(df)

        # Create layers for map
        layers = self.create_layers(geo_df)

        # Plot the overlay map
        self.plot_overlay_map(state_name, geo_df)


    def close_connection(self):
        self.conn.close()
        print("SQLite connection closed.")
