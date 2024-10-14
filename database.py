from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker

def get_sqlalchemy_engine():
    return create_engine(
        "mssql+pyodbc://technofuturtic\f.renaux@GOS-VDI307\TFTIC/Course_oki?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
    )

engine = get_sqlalchemy_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()

def test_connection():
    try:
        with engine.connect() as connection:
            result = connection.execute(text("SELECT 1"))
            result.fetchone()
        print("Database connection successful!")
        return True
    except Exception as e:
        print(f"Database connection failed: {str(e)}")
        return False

# Fonction pour obtenir une session de base de données
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

# Test the connection when this module is imported
connection_status = test_connection()