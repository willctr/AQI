import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
import numpy as np 
import plotly.express as px 
import warnings as wr 
wr.filterwarnings('ignore')


class EDA:
    def __init__(self, db_name='air.db'):
        self.conn = sqlite3.connect(db_name)

    def load_data(self, table_name):
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, self.conn)
        return df

    def print_summary_stats(self, df, table_name):
        print(f"Summary statistics for table: {table_name}")
        print(df.describe())

    # Plot histogram based on dataset and specified column.
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

            # Check if the column exists before plotting
            if column not in df.columns:
                print(f"Warning: '{column}' column not found in the dataset.")
                return
        
        sns.histplot(df[column], kde=True)
        plt.ylabel('Frequency')
        plt.show()

    # def plot_facet_grid_line(self, df, dataset_name):
    #     # Ensure 'Date' column is in datetime format
    #     df['Date'] = pd.to_datetime(df['Date'])

    #     # Create a FacetGrid
    #     g = sns.FacetGrid(df, col="CBSA", col_wrap=4, height=4, aspect=1.5) 

    #     # which dataset
    #     if dataset_name == 'AQIdata':
    #         # Map the lineplot to the FacetGrid
    #         g.map(sns.lineplot, "Date", "AQI")
    #         # titles and layout
    #         g.set_titles(col_template="{col_name}")
    #         g.set_axis_labels("Date", "AQI")
    #         plt.subplots_adjust(top=0.9)
    #         g.figure.suptitle('Time Series of AQI by CBSA Region', fontsize=16)
    #     else:
    #         # Map the lineplot to the FacetGrid
    #         g.map(sns.lineplot, "Date", "Arithmetic Mean")
    #         # titles and layout
    #         g.set_titles(col_template="{col_name}")
    #         g.set_axis_labels("Date", "Arithmetic Mean")
    #         plt.subplots_adjust(top=0.9)
    #         g.figure.suptitle('Time Series of Arithmetic Mean by CBSA Region', fontsize=16)


    #     # Show plot
    #     plt.show()
        
    def plot_facet_grid_paginated(self, df, dataset_name, regions_per_page=4):
        # Ensure 'Date' column is in datetime format
        df['Date'] = pd.to_datetime(df['Date'])

        # Get unique CBSA regions
        unique_cbsa = df['CBSA'].unique()
        # Calculate the number of pages
        num_pages = (len(unique_cbsa) + regions_per_page - 1) // regions_per_page

        # Iterate over each page
        for i in range(num_pages):
            # Get the CBSA regions for the current page
            subset_cbsa = unique_cbsa[i * regions_per_page:(i + 1) * regions_per_page]
            # Filter the DataFrame to only include rows with the selected CBSA regions
            subset_df = df[df['CBSA'].isin(subset_cbsa)]

            # Create a FacetGrid for the subset
            g = sns.FacetGrid(subset_df, col="CBSA", col_wrap=4, height=4, aspect=1.5)

            if dataset_name == 'AQIdata':
                # Map the lineplot to the FacetGrid
                g.map(sns.lineplot, "Date", "AQI")
                # Set axis labels and titles
                g.set_axis_labels("Date", "AQI")
                plt.subplots_adjust(top=0.9)
                g.figure.suptitle(f'Time Series of AQI by CBSA Region (Page {i+1})', fontsize=16)
            else:
                # Map the lineplot to the FacetGrid
                g.map(sns.lineplot, "Date", "Arithmetic Mean")
                # Set axis labels and titles
                g.set_axis_labels("Date", "Arithmetic Mean")
                plt.subplots_adjust(top=0.9)
                g.figure.suptitle(f'Time Series of Arithmetic Mean by CBSA Region (Page {i+1})', fontsize=16)

            # Set titles for each facet
            g.set_titles(col_template="{col_name}")
            # Show the plot
            plt.show()

    def plot_interactive_facet_grid(self, df, dataset_name):
        # Create an interactive line plot using Plotly
        fig = px.line(df, x='Date', y='AQI' if dataset_name == 'AQIdata' else 'Arithmetic Mean', 
                      facet_col='CBSA', facet_col_wrap=4,
                      title=f'Time Series of {"AQI" if dataset_name == "AQIdata" else "Arithmetic Mean"} by CBSA Region',
                      height=800)
        # Update layout for the title
        fig.update_layout(title_font_size=16)
        # Show the interactive plot
        fig.show()



    def close_connection(self):
        self.conn.close()
        print("SQLite connection closed.")

def get_dataset_choice():
    print("Choose a dataset from the following options:")
    datasets = ['AQIdata', 'temperatures', 'ozone', 'pm25', 'pm10', 'no2', 'so2', 'co']
    #loop through datasets
    for i, dataset in enumerate(datasets): 
        #prints datasets and corresponding number
        print(f"{i}. {dataset}")
    while True:
        try:
            #get user input for dataset to use
            choice = int(input("Enter the number corresponding to your choice: "))
            #validation
            if 0 <= choice < len(datasets):
                return datasets[choice]
            else:
                print("Invalid choice. Please enter a valid number.")
        except ValueError:
            print("Invalid input. Please enter a number.")


if __name__ == '__main__':
    eda = EDA()


    while True:
        dataset_choice = get_dataset_choice()
        df = eda.load_data(dataset_choice)
        eda.print_summary_stats(df, dataset_choice)

        print(f"This is the dataset: {dataset_choice}")
        print(df.nunique())

        # Call the function for paginated plotting
        eda.plot_facet_grid_paginated(df, dataset_name='AQIdata', regions_per_page=4)

        # Call the function for interactive plotting
        eda.plot_interactive_facet_grid(df, dataset_name='AQIdata')

        # if dataset_choice == 'AQIdata':
        #     #eda.plot_histogram(df, 'AQIdata')
        #     eda.plot_facet_grid_line(df, 'AQIdata')
        # else:
        #     eda.plot_histogram(df, None)
        #     eda.plot_facet_grid_line(df, None)

        prompt = input("Do you want to analyze another dataset? (yes/no): ").lower()
        if prompt != 'yes':
            break

    
    eda.close_connection()













# import sqlite3
# import pandas as pd
# import matplotlib.pyplot as plt
# import seaborn as sns

# class EDA:
#     def __init__(self, db_name='air.db'):
#         self.conn = sqlite3.connect(db_name)

#     # Load data from a specified table into a Pandas DataFrame.
#     def load_data(self, table_name):
#         query = f"SELECT * FROM {table_name}"
#         df = pd.read_sql_query(query, self.conn)
#         return df

#     # Print summary statistics for a DataFrame.
#     def print_summary_stats(self, df, table_name):
#         print(f"Summary statistics for table: {table_name}")
#         print(df.describe())

#     # Plot histogram for a specified column in the DataFrame.
#     def plot_histogram(self, df, column):
#         plt.figure(figsize=(8, 6))
#         sns.histplot(df[column], kde=True)
#         plt.title(f'Histogram of {column}')
#         plt.xlabel(column)
#         plt.ylabel('Frequency')

#         # # Customize x-axis
#         # plt.xlim(0, 300)  # Set x-axis limit from 0 to 300

#         plt.show()

#     # Close the SQLite connection.
#     def close_connection(self):
#         self.conn.close()
#         print("SQLite connection closed.")

# if __name__ == '__main__':
#     eda = EDA()

#     # Load data from AQIdata table
#     aqi_df = eda.load_data('AQIdata')

#     # Print summary statistics for AQIdata
#     eda.print_summary_stats(aqi_df, 'AQIdata')


#     # Load data from temperatures table
#     temp_df = eda.load_data('temperatures')

#     # Print summary statistics for temperatures
#     eda.print_summary_stats(temp_df, 'temperatures')

#     # Plot histogram for AQI column
#     eda.plot_histogram(aqi_df, 'AQI')


#     # # Load data from temperatures table
#     # temp_df = eda.load_data('temperatures')

#     # # Print summary statistics for temperatures
#     # eda.print_summary_stats(temp_df, 'temperatures')

#     # Plot histograms for avg temperature
#     eda.plot_histogram(temp_df, 'Arithmetic Mean')

#     # Close connection
#     eda.close_connection()
