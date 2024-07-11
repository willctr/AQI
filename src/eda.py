import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

class EDA:
    def __init__(self, db_name='air.db'):
        self.conn = sqlite3.connect(db_name)

    # Load data from a specified table into a Pandas DataFrame.
    def load_data(self, table_name):
        query = f"SELECT * FROM {table_name}"
        df = pd.read_sql_query(query, self.conn)
        return df

    # Print summary statistics for a DataFrame.
    def print_summary_stats(self, df, table_name):
        print(f"Summary statistics for table: {table_name}")
        print(df.describe())

    # Plot histogram for a specified column in the DataFrame.
    def plot_histogram(self, df, column):
        plt.figure(figsize=(8, 6))
        sns.histplot(df[column], kde=True)
        plt.title(f'Histogram of {column}')
        plt.xlabel(column)
        plt.ylabel('Frequency')

        # # Customize x-axis
        # plt.xlim(0, 300)  # Set x-axis limit from 0 to 300

        plt.show()

    # Close the SQLite connection.
    def close_connection(self):
        self.conn.close()
        print("SQLite connection closed.")

if __name__ == '__main__':
    eda = EDA()

    # Load data from AQIdata table
    aqi_df = eda.load_data('AQIdata')

    # Print summary statistics for AQIdata
    eda.print_summary_stats(aqi_df, 'AQIdata')


    # Load data from temperatures table
    temp_df = eda.load_data('temperatures')

    # Print summary statistics for temperatures
    eda.print_summary_stats(temp_df, 'temperatures')

    # Plot histogram for AQI column
    eda.plot_histogram(aqi_df, 'AQI')


    # # Load data from temperatures table
    # temp_df = eda.load_data('temperatures')

    # # Print summary statistics for temperatures
    # eda.print_summary_stats(temp_df, 'temperatures')

    # Plot histograms for avg temperature
    eda.plot_histogram(temp_df, 'Arithmetic Mean')

    # Close connection
    eda.close_connection()
