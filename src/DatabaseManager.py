import sqlite3
import csv
import os

class DatabaseManager:
    def __init__(self, db_name='air.db'):
        # Initialize DatabaseManager with SQLite db named air.
        self.db_name = db_name
        # Connect to SQLite db
        print(f"Connecting to SQLite database: {self.db_name}")
        self.conn = sqlite3.connect(db_name)
        # Need a pointer to move through db
        self.cursor = self.conn.cursor()

    # Define schema for temperature table and AQI table in the SQLite db.
    def create_schema(self):
        # temperature table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS temperatures (
                "Date Local" TEXT,
                "Arithmetic Mean" REAL,
                "1st Max Value" REAL,
                "1st Max Hour" INTEGER,
                AQI INTEGER,
                "Local Site Name" TEXT,
                Address TEXT,
                "State Name" TEXT,
                "County Name" TEXT,
                "City Name" TEXT,
                "CBSA Name" TEXT
            )
        ''')

        # AQI table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS AQIdata (
                CBSA TEXT,
                "CBSA Code" TEXT,
                "Date" TEXT,
                AQI INTEGER,
                Category TEXT,
                "Defining Parameter" TEXT
            )
        ''')

        print("Tables created: temperatures, AQIdata")

        # commit transaction, save state
        self.conn.commit()

    # Load data from CSV into temperatures table
    def load_temperature_data(self):
        for year in range(2014, 2024):  # Years from 2014 to 2024
            csv_file = f'data/daily_temp/cleaned_temp_{year}.csv'
            if os.path.exists(csv_file):
                with open(csv_file, 'r', newline='') as csvfile:
                    csvreader = csv.reader(csvfile)
                    next(csvreader)  # Skip header row
                    for row in csvreader:
                        # Insert data into table
                        self.cursor.execute('''
                            INSERT OR IGNORE INTO temperatures 
                            ("Date Local", "Arithmetic Mean", "1st Max Value", "1st Max Hour", AQI, 
                            "Local Site Name", Address, "State Name", "County Name", "City Name", "CBSA Name")
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        ''', row)
                print(f"Data loaded into temperatures table from {csv_file}")
            else:
                print(f"File {csv_file} not found.")

        self.conn.commit()

    # Load data from CSV into AQI table
    def load_aqi_data(self):
        for year in range(2014, 2024):  # Years from 2014 to 2024
            csv_file = f'data/daily_aqi/cleaned_aqi_{year}.csv'
            if os.path.exists(csv_file):
                with open(csv_file, 'r', newline='') as csvfile:
                    csvreader = csv.reader(csvfile)
                    next(csvreader)  # Skip header row
                    for row in csvreader:
                        # Insert data into table
                        self.cursor.execute('''
                            INSERT OR IGNORE INTO AQIdata 
                            (CBSA, "CBSA Code", Date, AQI, Category, "Defining Parameter")
                            VALUES (?, ?, ?, ?, ?, ?)
                        ''', row)
                print(f"Data loaded into AQIdata table from {csv_file}")
            else:
                print(f"File {csv_file} not found.")

        self.conn.commit()

    # Free resources
    def close_connection(self):
        self.conn.close()
        print("SQLite connection closed.")

if __name__ == '__main__':
    # Create an instance of DatabaseManager
    db_manager = DatabaseManager()

    # Create schema and load data
    db_manager.create_schema()
    db_manager.load_temperature_data()
    db_manager.load_aqi_data()

    # Close connection
    db_manager.close_connection()
