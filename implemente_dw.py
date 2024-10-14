import pandas as pd
from sqlalchemy import create_engine, text
import pyodbc
import os
from datetime import datetime

def get_sqlalchemy_engine():
    engine = create_engine(
        "mssql+pyodbc://technofuturtic\f.renaux@GOS-VDI307\TFTIC/Course_oki?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
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

def execute_query(engine, query):
    with engine.connect() as connection:
        connection.execute(text(query))
        connection.commit()

def create_datawarehouse_tables(cursor):
    # Création de la table de faits fact_race_results
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='fact_race_results' AND xtype='U')
        CREATE TABLE fact_race_results (
            race_result_id INT PRIMARY KEY,
            date_id INT,
            race_id INT,
            circuit_id INT,
            driver_id INT,
            constructor_id INT,
            qualifying_position INT,
            grid_position INT,
            finish_position INT,
            points FLOAT,
            laps_completed INT,
            fastest_lap_time VARCHAR(255),
            fastest_lap_speed FLOAT,
            status_id INT,
            pit_stops INT,
            total_pit_stop_duration FLOAT
        );
    ''')

    # Création des tables de dimensions
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dim_date' AND xtype='U')
        CREATE TABLE dim_date (
            date_id INT PRIMARY KEY,
            full_date DATE,
            year INT,
            month INT,
            day INT,
            quarter INT,
            season INT
        );
    ''')

    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dim_race' AND xtype='U')
        CREATE TABLE dim_race (
            race_id INT PRIMARY KEY,
            name VARCHAR(255),
            round INT,
            url VARCHAR(255)
        );
    ''')

    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dim_circuit' AND xtype='U')
        CREATE TABLE dim_circuit (
            circuit_id INT PRIMARY KEY,
            name VARCHAR(255),
            location VARCHAR(255),
            country VARCHAR(255),
            lat FLOAT,
            lng FLOAT,
            alt INT
        );
    ''')

    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dim_driver' AND xtype='U')
        CREATE TABLE dim_driver (
            driver_id INT PRIMARY KEY,
            full_name VARCHAR(255),
            dob DATE,
            nationality VARCHAR(255)
        );
    ''')

    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dim_constructor' AND xtype='U')
        CREATE TABLE dim_constructor (
            constructor_id INT PRIMARY KEY,
            name VARCHAR(255),
            nationality VARCHAR(255)
        );
    ''')

    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='dim_status' AND xtype='U')
        CREATE TABLE dim_status (
            status_id INT PRIMARY KEY,
            status VARCHAR(255)
        );
    ''')

    # Création des tables de faits supplémentaires
    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='fact_lap_times' AND xtype='U')
        CREATE TABLE fact_lap_times (
            race_id INT,
            driver_id INT,
            lap INT,
            position INT,
            lap_time FLOAT,
            PRIMARY KEY (race_id, driver_id, lap)
        );
    ''')

    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='fact_constructor_standings' AND xtype='U')
        CREATE TABLE fact_constructor_standings (
            race_id INT,
            constructor_id INT,
            points FLOAT,
            position INT,
            wins INT,
            PRIMARY KEY (race_id, constructor_id)
        );
    ''')

    cursor.execute('''
        IF NOT EXISTS (SELECT * FROM sysobjects WHERE name='fact_driver_standings' AND xtype='U')
        CREATE TABLE fact_driver_standings (
            race_id INT,
            driver_id INT,
            points FLOAT,
            position INT,
            wins INT,
            PRIMARY KEY (race_id, driver_id)
        );
    ''')

    cursor.connection.commit()

def load_dim_date(engine):
    query = """
    INSERT INTO dim_date (date_id, full_date, year, month, day, quarter, season)
    SELECT DISTINCT 
        CAST(REPLACE(CAST(date AS VARCHAR), '-', '') AS INT) as date_id,
        date as full_date,
        year,
        MONTH(date) as month,
        DAY(date) as day,
        DATEPART(QUARTER, date) as quarter,
        year as season
    FROM races
    """
    execute_query(engine, query)
    print("Dimension date chargée.")

def load_dim_race(engine):
    query = """
    INSERT INTO dim_race (race_id, name, round, url)
    SELECT raceId, name, round, url
    FROM races
    """
    execute_query(engine, query)
    print("Dimension race chargée.")

def load_dim_circuit(engine):
    query = """
    INSERT INTO dim_circuit (circuit_id, name, location, country, lat, lng, alt)
    SELECT circuitId, name, location, country, lat, lng, alt
    FROM circuits
    """
    execute_query(engine, query)
    print("Dimension circuit chargée.")

def load_dim_driver(engine):
    query = """
    INSERT INTO dim_driver (driver_id, full_name, dob, nationality)
    SELECT driverId, CONCAT(forename, ' ', surname), dob, nationality
    FROM drivers
    """
    execute_query(engine, query)
    print("Dimension driver chargée.")

def load_dim_constructor(engine):
    query = """
    INSERT INTO dim_constructor (constructor_id, name, nationality)
    SELECT constructorId, name, nationality
    FROM constructors
    """
    execute_query(engine, query)
    print("Dimension constructor chargée.")

def load_dim_status(engine):
    query = """
    INSERT INTO dim_status (status_id, status)
    SELECT statusId, status
    FROM status
    """
    execute_query(engine, query)
    print("Dimension status chargée.")

def load_fact_race_results(engine):
    query = """
    INSERT INTO fact_race_results (
        race_result_id, date_id, race_id, circuit_id, driver_id, constructor_id,
        qualifying_position, grid_position, finish_position, points, laps_completed,
        fastest_lap_time, fastest_lap_speed, status_id, pit_stops, total_pit_stop_duration
    )
    SELECT 
        r.resultId,
        CAST(REPLACE(CAST(ra.date AS VARCHAR), '-', '') AS INT) as date_id,
        r.raceId,
        ra.circuitId,
        r.driverId,
        r.constructorId,
        q.position as qualifying_position,
        r.grid as grid_position,
        CASE WHEN r.position = '\\N' THEN NULL ELSE CAST(r.position AS INT) END as finish_position,
        r.points,
        r.laps,
        r.fastestLapTime,
        CASE WHEN r.fastestLapSpeed = '\\N' THEN NULL ELSE CAST(r.fastestLapSpeed AS FLOAT) END,
        r.statusId,
        COUNT(p.stop) as pit_stops,
        SUM(CAST(p.milliseconds AS FLOAT)) / 1000 as total_pit_stop_duration
    FROM results r
    JOIN races ra ON r.raceId = ra.raceId
    LEFT JOIN qualifying q ON r.raceId = q.raceId AND r.driverId = q.driverId
    LEFT JOIN pit_stops p ON r.raceId = p.raceId AND r.driverId = p.driverId
    GROUP BY r.resultId, ra.date, r.raceId, ra.circuitId, r.driverId, r.constructorId,
             q.position, r.grid, r.position, r.points, r.laps, r.fastestLapTime,
             r.fastestLapSpeed, r.statusId
    """
    execute_query(engine, query)
    print("Fait race_results chargé.")

def load_fact_lap_times(engine):
    query = """
    INSERT INTO fact_lap_times (race_id, driver_id, lap, position, lap_time)
    SELECT raceId, driverId, lap, position, CAST(milliseconds AS FLOAT) / 1000 as lap_time
    FROM lap_times
    """
    execute_query(engine, query)
    print("Fait lap_times chargé.")

def load_fact_constructor_standings(engine):
    query = """
    INSERT INTO fact_constructor_standings (race_id, constructor_id, points, position, wins)
    SELECT raceId, constructorId, points, position, wins
    FROM constructor_standings
    """
    execute_query(engine, query)
    print("Fait constructor_standings chargé.")

def load_fact_driver_standings(engine):
    query = """
    INSERT INTO fact_driver_standings (race_id, driver_id, points, position, wins)
    SELECT raceId, driverId, points, position, wins
    FROM driver_standings
    """
    execute_query(engine, query)
    print("Fait driver_standings chargé.")

def load_data_to_datawarehouse(engine):
    # Charger les dimensions
    load_dim_date(engine)
    load_dim_race(engine)
    load_dim_circuit(engine)
    load_dim_driver(engine)
    load_dim_constructor(engine)
    load_dim_status(engine)

    # Charger les faits
    load_fact_race_results(engine)
    load_fact_lap_times(engine)
    load_fact_constructor_standings(engine)
    load_fact_driver_standings(engine)

def main():
    try:
        start_time = datetime.now()
        print(f"Début du chargement de la datawarehouse: {start_time}")

        conn = get_pyodbc_connection()
        cursor = conn.cursor()
        create_datawarehouse_tables(cursor)
        
        engine = get_sqlalchemy_engine()
        
        load_data_to_datawarehouse(engine)
        
        end_time = datetime.now()
        print(f"Fin du chargement de la datawarehouse: {end_time}")
        print(f"Durée totale: {end_time - start_time}")
        print("Datawarehouse créé et chargé avec succès.")
    
    except Exception as e:
        print(f"Une erreur est survenue : {str(e)}")
    finally:
        if conn:
            conn.close()

if __name__ == "__main__":
    main()