import sqlite3
import csv

class DatabaseManager:
    def __init__(self, db_name = 'air.db'):
        # Initialize DatabaseManager with SQLite db named air.
        self.db_name = db_name
        # Connect to SQLite db
        self.conn = sqlite3.connect(db_name)
        # Need a pointer to move through db
        self.curser = self.conn.cursor()

    # Define schema for temperature table and AQI table in the SQLite db.
    def create_schema(self):
        # temperature. send command to create table
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS temperatures (
                "State Code" TEXT,
                "County Code" TEXT,
                "Site Num" TEXT,
                "Parameter Code" TEXT,
                POC INTEGER,
                Latitude REAL,
                Longitude REAL,
                Datum TEXT,
                "Parameter Name" TEXT,
                "Sample Duration" TEXT,
                "Pollutant Standard" TEXT,
                "Date Local" TEXT,
                "Units of Measure" TEXT,
                "Event Type" TEXT,
                "Observation Count" INTEGER,
                "Observation Percent" REAL,
                "Arithmetic Mean" REAL,
                "1st Max Value" REAL,
                "1st Max Hour" INTEGER,
                AQI INTEGER,
                "Method Code" TEXT,
                "Method Name" TEXT,
                "Local Site Name" TEXT,
                Address TEXT,
                "State Name" TEXT,
                "County Name" TEXT,
                "City Name" TEXT,
                "CBSA Name" TEXT,
                "Date of Last Change" TEXT
            )
        ''')

        # AQI
        self.cursor.execute('''
            CREATE TABLE IF NOT EXISTS AQIDate (
                CBSA TEXT,
                "CBSA Code" TEXT,
                "Date" TEXT,
                AQI INTEGER,
                Category TEXT,
                "Defining Parameter" TEXT,
                "Defining Site" TEXT,
                "Number of Sites Reporting" INTEGER
            )
        ''')

        # commit transaction, save state
        self.conn.commit()

    # load data from csv into TemperatureData table
    def load_temperature_data(self, csv_file):
        with open(csv_file, 'r', newline='') as csvfile: #open passed csv file in read mode
            csvreader = csv.reader(csvfile) #create csvreader object
            next(csvreader) # skip header row
            for row in csvreader: #iterate through each row
                # insert data into table
                self.cursor.execute('''
                    INSERT OR IGNORE INTO TemperatureData (StateCode, CountyCode, SiteNum, ParameterCode, POC, Latitude, Longitude,
                                                          Datum, ParameterName, SampleDuration, PollutantStandard, DateLocal,
                                                          UnitsOfMeasure, EventType, ObservationCount, ObservationPercent,
                                                          ArithmeticMean, FirstMaxValue, FirstMaxHour, AQI, MethodCode, MethodName,
                                                          LocalSiteName, Address, StateName, CountyName, CityName, CBSAName, DateOfLastChange)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ''', row)
        
        self.conn.commit()

    def load_aqi_data(self, csv_file):
        with open(csv_file, 'r', newline='') as csvfile:
            csvreader = csv.reader(csvfile)
            next(csvreader) # skip header row
            for row in csvreader:
                self.cursor.execute('''
                    INSERT OR IGNORE INTO AQIData (CBSA, CBSACode, Date, AQI, Category, DefiningParameter, DefiningSite, NumSitesReporting)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', row)
        
        self.conn.commit()

    # free resource
    def close_connection(self):
        self.conn.close()