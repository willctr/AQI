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
                "Latitude" REAL,
                "Longitude" REAL,
                "Date Local" TEXT,
                "Arithmetic Mean" REAL,
                "1st Max Value" REAL,
                "1st Max Hour" INTEGER,
                "Address" TEXT,
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

        # ozone table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS ozone (
                "Latitude" REAL,
                "Longitude" REAL,
                "Date Local" TEXT,
                "Arithmetic Mean" REAL,
                "1st Max Value" REAL,
                "1st Max Hour" INTEGER,
                "Address" TEXT,
                "CBSA Name" TEXT
            )
        ''')

        # PM2.5 table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS "pm2.5" (
                "Latitude" REAL,
                "Longitude" REAL,
                "Date Local" TEXT,
                "Arithmetic Mean" REAL,
                "1st Max Value" REAL,
                "1st Max Hour" INTEGER,
                "Address" TEXT,
                "CBSA Name" TEXT
            )
        ''')

        # PM10 table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS pm10 (
                "Latitude" REAL,
                "Longitude" REAL,
                "Date Local" TEXT,
                "Arithmetic Mean" REAL,
                "1st Max Value" REAL,
                "1st Max Hour" INTEGER,
                "Address" TEXT,
                "CBSA Name" TEXT
            )
        ''')

        # NO2 table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS no2 (
                "Latitude" REAL,
                "Longitude" REAL,
                "Date Local" TEXT,
                "Arithmetic Mean" REAL,
                "1st Max Value" REAL,
                "1st Max Hour" INTEGER,
                "Address" TEXT,
                "CBSA Name" TEXT
            )
        ''')

        # SO2 table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS so2 (
                "Latitude" REAL,
                "Longitude" REAL,
                "Date Local" TEXT,
                "Arithmetic Mean" REAL,
                "1st Max Value" REAL,
                "1st Max Hour" INTEGER,
                "Address" TEXT,
                "CBSA Name" TEXT
            )
        ''')

        # CO table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS co (
                "Latitude" REAL,
                "Longitude" REAL,
                "Date Local" TEXT,
                "Arithmetic Mean" REAL,
                "1st Max Value" REAL,
                "1st Max Hour" INTEGER,
                "Address" TEXT,
                "CBSA Name" TEXT
            )
        ''')


        print("Tables created: temperatures, AQIdata, ozone, pm2.5, pm10, no2, so2, co")

        # commit transaction, save state
        self.conn.commit()

    # ! generic method for loading data, reduce code duplication
    # def load_data(self, table_name, csv_path, columns):
    #     for year in range(2013, 2024):
    #         csv_file = f'{csv_path}/cleaned_{table_name}_{year}.csv'
    #         if os.path.exists(csv_file):
    #             with open(csv_file, 'r', newline='') as csvfile:
    #                 csvreader = csv.reader(csvfile)
    #                 next(csvreader)  # Skip header row
    #                 for row in csvreader:
    #                     self.cursor.execute(f'''
    #                         INSERT OR IGNORE INTO {table_name} ({', '.join(columns)})
    #                         VALUES ({', '.join('?' for _ in columns)})
    #                     ''', row)
    #             print(f"Data loaded into {table_name} table from {csv_file}")
    #         else:
    #             print(f"File {csv_file} not found.")
    #     self.conn.commit()

    # Load data from CSV into temperatures table
    def load_temperature_data(self):
        for year in range(2017, 2024):  # Years from 2013 to 2023
            csv_file = f'data/daily_temp/cleaned_temp_{year}.csv'
            if os.path.exists(csv_file):
                with open(csv_file, 'r', newline='') as csvfile:
                    csvreader = csv.reader(csvfile)
                    next(csvreader)  # Skip header row
                    for row in csvreader:
                        # Insert data into table
                        self.cursor.execute('''
                            INSERT OR IGNORE INTO temperatures 
                            ("Latitude", "Longitude", "Date Local", "Arithmetic Mean", "1st Max Value", "1st Max Hour", 
                            "Address", "CBSA Name")
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', row)
                print(f"Data loaded into temperatures table from {csv_file}")
            else:
                print(f"File {csv_file} not found.")

        self.conn.commit()

    # Load data from CSV into AQI table
    def load_aqi_data(self):
        for year in range(2017, 2024):  # Years from 2013 to 2023
            csv_file = f'data/daily_aqi/cleaned_aqi_{year}.csv'
            if os.path.exists(csv_file):
                with open(csv_file, 'r', newline='') as csvfile:
                    csvreader = csv.reader(csvfile)
                    next(csvreader)  # Skip header row
                    for row in csvreader:
                        # Trim whitespace and check length
                        row = [col.strip() for col in row]  
                        
                        if len(row) != 6:
                            print(f"Skipping row with incorrect column count ({len(row)}): {row}")
                            continue  # Skip invalid rows
                        
                        # Insert data into table
                        try:
                            self.cursor.execute('''
                                INSERT OR IGNORE INTO AQIdata 
                                (CBSA, "CBSA Code", Date, AQI, Category, "Defining Parameter")
                                VALUES (?, ?, ?, ?, ?, ?)
                            ''', row)
                        except Exception as e:
                            print(f"Error inserting row {row}: {e}")
                        # # Insert data into table
                        # self.cursor.execute('''
                        #     INSERT OR IGNORE INTO AQIdata 
                        #     (CBSA, "CBSA Code", Date, AQI, Category, "Defining Parameter")
                        #     VALUES (?, ?, ?, ?, ?, ?)
                        # ''', row)
                print(f"Data loaded into AQIdata table from {csv_file}")
            else:
                print(f"File {csv_file} not found.")

        self.conn.commit()

    # Load data from CSV into ozone table
    def load_ozone_data(self):
        for year in range(2017, 2024):  # Years from 2013 to 2023
            csv_file = f'data/daily_ozone/cleaned_ozone_{year}.csv'
            if os.path.exists(csv_file):
                with open(csv_file, 'r', newline='') as csvfile:
                    csvreader = csv.reader(csvfile)
                    next(csvreader)  # Skip header row
                    for row in csvreader:
                        # Insert data into table
                        self.cursor.execute('''
                            INSERT OR IGNORE INTO ozone 
                            ("Latitude", "Longitude", "Date Local", "Arithmetic Mean", "1st Max Value", "1st Max Hour", 
                            "Address", "CBSA Name")
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', row)
                print(f"Data loaded into ozone table from {csv_file}")
            else:
                print(f"File {csv_file} not found.")

        self.conn.commit()

    

    # Load data from CSV into PM2.5 table
    def load_pm25_data(self):
        for year in range(2017, 2024):  # Years from 2013 to 2023
            csv_file = f'data/daily_pm2.5/cleaned_pm25_{year}.csv'
            if os.path.exists(csv_file):
                with open(csv_file, 'r', newline='') as csvfile:
                    csvreader = csv.reader(csvfile)
                    next(csvreader)  # Skip header row
                    for row in csvreader:
                        # Insert data into table
                        self.cursor.execute('''
                            INSERT OR IGNORE INTO "pm2.5" 
                            ("Latitude", "Longitude", "Date Local", "Arithmetic Mean", "1st Max Value", "1st Max Hour", 
                            "Address", "CBSA Name")
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', row)
                print(f"Data loaded into pm2.5 table from {csv_file}")
            else:
                print(f"File {csv_file} not found.")

        self.conn.commit()

    # Load data from CSV into PM10 table
    def load_pm10_data(self):
        for year in range(2017, 2024):  # Years from 2013 to 2023
            csv_file = f'data/daily_pm10/cleaned_pm10_{year}.csv'
            if os.path.exists(csv_file):
                with open(csv_file, 'r', newline='') as csvfile:
                    csvreader = csv.reader(csvfile)
                    next(csvreader)  # Skip header row
                    for row in csvreader:
                        # Insert data into table
                        self.cursor.execute('''
                            INSERT OR IGNORE INTO pm10
                            ("Latitude", "Longitude", "Date Local", "Arithmetic Mean", "1st Max Value", "1st Max Hour", 
                            "Address", "CBSA Name")
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', row)
                print(f"Data loaded into pm10 table from {csv_file}")
            else:
                print(f"File {csv_file} not found.")

        self.conn.commit()

    # Load data from CSV into NO2 table
    def load_no2_data(self):
        for year in range(2017, 2024):  # Years from 2013 to 2023
            csv_file = f'data/daily_no2/cleaned_no2_{year}.csv'
            if os.path.exists(csv_file):
                with open(csv_file, 'r', newline='') as csvfile:
                    csvreader = csv.reader(csvfile)
                    next(csvreader)  # Skip header row
                    for row in csvreader:
                        # Insert data into table
                        self.cursor.execute('''
                            INSERT OR IGNORE INTO no2 
                            ("Latitude", "Longitude", "Date Local", "Arithmetic Mean", "1st Max Value", "1st Max Hour", 
                            "Address", "CBSA Name")
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', row)
                print(f"Data loaded into NO2 table from {csv_file}")
            else:
                print(f"File {csv_file} not found.")

        self.conn.commit()

    # Load data from CSV into SO2 table
    def load_so2_data(self):
        for year in range(2017, 2024):  # Years from 2013 to 2023
            csv_file = f'data/daily_so2/cleaned_so2_{year}.csv'
            if os.path.exists(csv_file):
                with open(csv_file, 'r', newline='') as csvfile:
                    csvreader = csv.reader(csvfile)
                    next(csvreader)  # Skip header row
                    for row in csvreader:
                        # Insert data into table
                        self.cursor.execute('''
                            INSERT OR IGNORE INTO so2 
                            ("Latitude", "Longitude", "Date Local", "Arithmetic Mean", "1st Max Value", "1st Max Hour", 
                            "Address", "CBSA Name")
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', row)
                print(f"Data loaded into SO2 table from {csv_file}")
            else:
                print(f"File {csv_file} not found.")

        self.conn.commit()

    # Load data from CSV into CO table
    def load_co_data(self):
        for year in range(2017, 2024):  # Years from 2013 to 2023
            csv_file = f'data/daily_co/cleaned_co_{year}.csv'
            if os.path.exists(csv_file):
                with open(csv_file, 'r', newline='') as csvfile:
                    csvreader = csv.reader(csvfile)
                    next(csvreader)  # Skip header row
                    for row in csvreader:
                        # Insert data into table
                        self.cursor.execute('''
                            INSERT OR IGNORE INTO co 
                            ("Latitude", "Longitude", "Date Local", "Arithmetic Mean", "1st Max Value", "1st Max Hour", 
                            "Address", "CBSA Name")
                            VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        ''', row)
                print(f"Data loaded into CO table from {csv_file}")
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
    db_manager.load_ozone_data()
    db_manager.load_pm25_data()
    db_manager.load_pm10_data()
    db_manager.load_no2_data()
    db_manager.load_so2_data()
    db_manager.load_co_data()

    # Close connection
    db_manager.close_connection()
