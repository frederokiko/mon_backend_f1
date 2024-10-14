from sqlalchemy import create_engine, text

def get_sqlalchemy_engine():
    return create_engine(
        "mssql+pyodbc://technofuturtic\f.renaux@GOS-VDI307\TFTIC/Course_oki?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
    )

engine = get_sqlalchemy_engine()

with engine.connect() as connection:
    result = connection.execute(text("SELECT 1"))
    print(result.fetchone())

print("Connection successful!")