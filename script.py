import pandas as pd
from sqlalchemy import create_engine
import pyodbc

# Connexion SQLAlchemy pour SQL Server
def get_sqlalchemy_engine():
    engine = create_engine(
        # "mssql+pyodbc://TECHNOFUTURTIC\f.renaux@GOS-VDI307\TFTIC/Course_oki?driver=ODBC+Driver+17+for+SQL+Server"
        #"mssql+pyodbc://TECHNOFUTURTIC\\f.renaux/GOS-VDI307\TFTIC/Course_oki?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
        "mssql+pyodbc://technofuturtic\f.renaux@GOS-VDI307\TFTIC/Course_oki?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
    )
    return engine

# Connexion avec pyodbc pour exécuter des requêtes SQL
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
    # Creation la table circuits
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
    
    # Création table race
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='races' AND xtype='U')
        CREATE TABLE races (
            raceId INT PRIMARY KEY,
            year INT,
            round INT,
            circuitId INT,
            name VARCHAR(255),
            date VARCHAR(255),
            url VARCHAR(255),
            FOREIGN KEY (circuitId) REFERENCES circuits(circuitId)
        );
    ''')

        # Création table drivers
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

            # Création table results
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
            statusId INT
        
        );
    ''')

    # Ajouter la création des autres tables ici (results, drivers, etc.)
    cursor.connection.commit()
def load_csv_to_sql(file_path, table_name, engine):
    # Lire le fichier CSV
    df = pd.read_csv(file_path)
    
    # Insérer les données dans SQL Server
    df.to_sql(table_name, con=engine, if_exists='append', index=False)
    print(f"Les données du fichier {file_path} ont été insérées dans la table {table_name}.")

def preprocess_dataframe(df):
    # Exemple : convertir les colonnes date en format de date
    if 'date' in df.columns:
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
    # Vous pouvez ajouter d'autres transformations ici
    
    return df

def main():
    # Connexion à SQL Server avec pyodbc pour la création des tables
    conn = get_pyodbc_connection()
    cursor = conn.cursor()
    
    # Créer les tables si elles n'existent pas
    create_tables(cursor)
    
    # Connexion SQLAlchemy pour l'insertion des données
    engine = get_sqlalchemy_engine()
    
    # Liste des fichiers CSV et des tables correspondantes
    files_and_tables = {
        './circuit_ok.csv': 'circuits',
        './race_ok.csv': 'races',
        './drivers_ok.csv': 'drivers',
        './results_ok.csv': 'results',
        # Ajoutez d'autres fichiers ici
    }
    
    # Charger chaque fichier CSV dans la table correspondante
    for file_path, table_name in files_and_tables.items():
        load_csv_to_sql(file_path, table_name, engine)
    
    # Fermer la connexion
    conn.close()

if __name__ == "__main__":
    main()
