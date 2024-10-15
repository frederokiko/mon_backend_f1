import pandas as pd
from sqlalchemy import create_engine
import pyodbc
import os

def get_sqlalchemy_engine():
    engine = create_engine(
        "mssql+pyodbc://DESKTOP-8EFA22F\frede@DESKTOP-8EFA22F\SQLEXPRESS/Course_oki?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
    )
    return engine

def get_pyodbc_connection():
    conn_str = (
        r"DRIVER={ODBC Driver 17 for SQL Server};"
        r"SERVER=GOS-VDI307\TFTIC;"
        r"DATABASE=Course_oki;"
        r"UID=technofuturtic\f.renaux;"
        r"Trusted_Connection=yes;"
    )
    return pyodbc.connect(conn_str)

def create_tables(cursor):
     # Creation la table circuits 1
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='circuits' AND xtype='U')
        CREATE TABLE circuits (
            circuitId INT PRIMARY KEY,
            circuitRef VARCHAR(255),
            name VARCHAR(255),
            location VARCHAR(255),
            country VARCHAR(255),
            lat FLOAT,
            lng FLOAT,
            alt INT,
            url VARCHAR(255)
        );
    ''')
    #raceId,year,round,circuitId,name,date,url odre des colonnes
    # Création table race 2
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='races' AND xtype='U')
        CREATE TABLE races (
            raceId INT PRIMARY KEY,
            year INT,
            round INT,
            circuitId INT,
            name VARCHAR(255),
            date DATE,
            url VARCHAR(255),
            FOREIGN KEY (circuitId) REFERENCES circuits(circuitId)
        );
    ''')
        # Création table constuctors 3
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='constructors' AND xtype='U')
        CREATE TABLE constructors (
            constructorId INT PRIMARY KEY,
            name VARCHAR(255),
            nationality VARCHAR(255),
            url VARCHAR(255)
        );
    ''')
        # Création table drivers 4
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='drivers' AND xtype='U')
        CREATE TABLE drivers (
            driverId INT PRIMARY KEY,
            forename VARCHAR(255),
            surname VARCHAR(255),
            dob VARCHAR(255),
            nationality VARCHAR(255),
            url VARCHAR(255)
        
        );
    ''')
        # Création table constuctor_results 5
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='constructor_results' AND xtype='U')
        CREATE TABLE constructor_results (
            constructorResultsId INT PRIMARY KEY,
            raceId INT,
            constructorId INT,
            points FLOAT,
            FOREIGN KEY (raceId) REFERENCES races(raceId),
            FOREIGN KEY (constructorId) REFERENCES constructors(constructorId)
        );
    ''')
            # Création table constuctor_standings 6
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='constructor_standings' AND xtype='U')
        CREATE TABLE constructor_standings (
            constructorStandingsId INT PRIMARY KEY,
            raceId INT,
            constructorId INT,
            points FLOAT,
            position INT,
            wins INT,
            FOREIGN KEY (raceId) REFERENCES races(raceId),
            FOREIGN KEY (constructorId) REFERENCES constructors(constructorId)
        );
    ''')
            # Création table driver_standings 7
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='driver_standings' AND xtype='U')
        CREATE TABLE driver_standings (
            driverStandingsId INT PRIMARY KEY,
            raceId INT,
            driverId INT,
            points FLOAT,
            position INT,
            wins INT,
            FOREIGN KEY (raceId) REFERENCES races(raceId),
            FOREIGN KEY (driverId) REFERENCES drivers(driverId)
        );
    ''')
            # Création table lap_times 8
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='lap_times' AND xtype='U')
        CREATE TABLE lap_times (
            raceId INT,
            driverId INT,
            lap INT,
            position INT,
            time VARCHAR(255),
            milliseconds INT,
            PRIMARY KEY (raceId, driverId, lap),
            FOREIGN KEY (raceId) REFERENCES races(raceId),
            FOREIGN KEY (driverId) REFERENCES drivers(driverId)
        );
    ''')
                # Création table pit_stop 9
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='pit_stops' AND xtype='U')
        CREATE TABLE pit_stops (
            raceId INT,
            driverId INT,
            stop INT,
            lap INT,
            time VARCHAR(255),
            duration VARCHAR(255),
            milliseconds INT,
            FOREIGN KEY (raceId) REFERENCES races(raceId),
            FOREIGN KEY (driverId) REFERENCES drivers(driverId)
        );
    ''')
                # Création table qualifying 10
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='qualifying' AND xtype='U')
        CREATE TABLE qualifying (
            qualifyId INT PRIMARY KEY,
            raceId INT,
            driverId INT,
            constructorId INT,
            number INT,
            position INT,
            q1 VARCHAR(255),
            q2 VARCHAR(255),
            q3 VARCHAR(255),
            FOREIGN KEY (raceId) REFERENCES races(raceId),
            FOREIGN KEY (driverId) REFERENCES drivers(driverId),
            FOREIGN KEY (constructorId) REFERENCES constructors(constructorId)
        );
    ''') 
            # Création table results 11
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='results' AND xtype='U')
        CREATE TABLE results (
            resultId INT PRIMARY KEY,
            raceId INT,
            driverId INT,
            constructorId INT,
            number VARCHAR(255),
            grid INT,
            position VARCHAR(255),
            positionText VARCHAR(255),
            positionOrder INT,
            points FLOAT,
            laps INT,
            time VARCHAR(255),
            milliseconds VARCHAR(255),
            fastestLap VARCHAR(255),
            rank VARCHAR(255),
            fastestLapTime VARCHAR(255),
            fastestLapSpeed VARCHAR(255),
            statusId INT,
            FOREIGN KEY (raceId) REFERENCES races(raceId),
            FOREIGN KEY (driverId) REFERENCES drivers(driverId),
            FOREIGN KEY (constructorId) REFERENCES constructors(constructorId)
        
        );
    ''')
                # Création table seasons 12
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='seasons' AND xtype='U')
        CREATE TABLE seasons (
            year INT PRIMARY KEY,
            url VARCHAR(255)
        );
    ''')
                # Création table status 13
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='status' AND xtype='U')
        CREATE TABLE status (
            statusId INT PRIMARY KEY,
            status VARCHAR(255)
        );
    ''')
    cursor.connection.commit()

def preprocess_dataframe(df):
    # Convertir les colonnes de date en datetime
    date_columns = ['date', 'dob']  # Ajoutez d'autres noms de colonnes de date si nécessaire
    for col in date_columns:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    
    # Gérer les valeurs NULL pour les colonnes numériques
    numeric_columns = ['number', 'position', 'milliseconds', 'fastestLap', 'rank']
    for col in numeric_columns:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')
    
    return df

def load_csv_to_sql(file_path, table_name, engine):
    try:
        # Lire le fichier CSV en ignorant la première colonne (index)
        df = pd.read_csv(file_path, index_col=0)
        
        # Supprimer la colonne 'Unnamed: 0' si elle existe
        if 'Unnamed: 0' in df.columns:
            df = df.drop('Unnamed: 0', axis=1)
            
        
        df = preprocess_dataframe(df)
        
        # Utiliser to_sql avec method='multi' pour de meilleures performances
        #, method='multi', chunksize=2000
        df.to_sql(table_name, con=engine, if_exists='append', index=False)
        print(f"Les données du fichier {file_path} ont été insérées dans la table {table_name}.")
    except Exception as e:
        print(f"Erreur lors du chargement de {file_path} dans {table_name}: {str(e)}")

def main():
    try:
        conn = get_pyodbc_connection()
        cursor = conn.cursor()
        create_tables(cursor)
        
        engine = get_sqlalchemy_engine()
        
        files_and_tables = {
        './circuit_ok.csv': 'circuits',
        './race_ok.csv': 'races',
        './drivers_ok.csv': 'drivers',
        './constructors_ok.csv': 'constructors',
        './constructor_results_ok.csv': 'constructor_results',
        './constructor_standings_ok.csv': 'constructor_standings',
        './driver_standings_ok.csv': 'driver_standings',
        './results_ok.csv': 'results',
        './lap_times_ok.csv': 'lap_times',
        './pit_stops_ok.csv': 'pit_stops',
        './qualifying_ok.csv': 'qualifying',
        './seasons_ok.csv': 'seasons',
        './status_ok.csv': 'status'
        }
        
        for file_path, table_name in files_and_tables.items():
            if os.path.exists(file_path):
                load_csv_to_sql(file_path, table_name, engine)
            else:
                print(f"Le fichier {file_path} n'existe pas.")
    
    except Exception as e:
        print(f"Une erreur est survenue : {str(e)}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()