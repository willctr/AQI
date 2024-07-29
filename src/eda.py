import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np 
import plotly.express as px 

class EDA:
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

    def load_aqi_data(self):
        return pd.read_sql_query("SELECT CBSA FROM AQIdata", self.conn)

    def get_state_choice(self, table_name):
        if table_name == 'AQIdata':
            df_aqi = self.load_aqi_data()
            df_aqi['State'] = df_aqi['CBSA'].apply(lambda x: x.split(', ')[-1])
            states = df_aqi['State'].unique()
        else:
            df = self.load_data(table_name)
            states = df['State Name'].unique()
        
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

    def load_data(self, table_name):
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, self.conn)
        return df

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

    def filter_by_state(self, df, state_name):
        if 'CBSA' in df.columns:
            filtered_df = df[df['CBSA'].str.endswith(state_name)]
        elif 'State Name' in df.columns:
            filtered_df = df[df['State Name'] == state_name]
        else:
            print("State column not found in the dataset.")
            return pd.DataFrame()
        
        if filtered_df.empty:
            print(f"No data found for state: {state_name}")
        return filtered_df

    def close_connection(self):
        self.conn.close()
        print("SQLite connection closed.")
