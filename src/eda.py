import sqlite3
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns

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

        if dataset_choice == 'AQIdata':
            eda.plot_histogram(df, 'AQIdata')
        else:
            eda.plot_histogram(df, None)

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
