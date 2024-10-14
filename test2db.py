from fastapi import FastAPI, Depends, HTTPException, status
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from sqlalchemy import create_engine, Column, Integer, String, Boolean, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, Session
from pydantic import BaseModel
from typing import List
import jwt
from datetime import datetime, timedelta
from passlib.context import CryptContext
import pyodbc  # Ensure pyodbc is installed for SQL Server

# Configuration de la base de données
#SQLALCHEMY_DATABASE_URL = r"mssql+pyodbc://technofuturtic\f.renaux@GOS-VDI307\TFTIC/Course_oki?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
#"mssql+pyodbc://technofuturtic\f.renaux@GOS-VDI307\TFTIC/Course_oki?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
#engine = create_engine(SQLALCHEMY_DATABASE_URL)
#SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
def get_sqlalchemy_engine():
    return create_engine(
        "mssql+pyodbc://technofuturtic\f.renaux@GOS-VDI307\TFTIC/Course_oki?driver=ODBC+Driver+17+for+SQL+Server&trusted_connection=yes"
    )

engine = get_sqlalchemy_engine()
SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)
Base = declarative_base()

# Modèle de données pour la table users
class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    username = Column(String(255), unique=True, index=True)
    hashed_password = Column(String)
    email = Column(String, unique=True, index=True)
    is_active = Column(Boolean, default=True)

# Créer les tables si elles n'existent pas déjà
Base.metadata.create_all(bind=engine)

# Configuration de la sécurité
SECRET_KEY = "votre_clé_secrète"  # Remplacez par une clé secrète robuste
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")

app = FastAPI()

# Fonctions utilitaires pour la gestion des utilisateurs
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()

def verify_password(plain_password, hashed_password):
    return pwd_context.verify(plain_password, hashed_password)

def get_password_hash(password):
    return pwd_context.hash(password)

def get_user(db, username: str):
    return db.query(User).filter(User.username == username).first()

def authenticate_user(db, username: str, password: str):
    user = get_user(db, username)
    if not user or not verify_password(password, user.hashed_password):
        return False
    return user

def create_access_token(data: dict):
    to_encode = data.copy()
    expire = datetime.utcnow() + timedelta(minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
    to_encode.update({"exp": expire})
    encoded_jwt = jwt.encode(to_encode, SECRET_KEY, algorithm=ALGORITHM)
    return encoded_jwt

def get_current_user(token: str = Depends(oauth2_scheme), db: Session = Depends(get_db)):
    try:
        payload = jwt.decode(token, SECRET_KEY, algorithms=[ALGORITHM])
        username: str = payload.get("sub")
        if username is None:
            raise HTTPException(status_code=401, detail="Invalid authentication credentials")
    except jwt.PyJWTError:
        raise HTTPException(status_code=401, detail="Invalid authentication credentials")
    user = get_user(db, username=username)
    if user is None:
        raise HTTPException(status_code=401, detail="User not found")
    return user

# Route d'accueil    
@app.get("/")
async def read_root():
    return {"message": "Welcome to the API!"}
#verif de la connexion db
@app.get("/test_db")
async def test_db_connection(db: Session = Depends(get_db)):
    try:
        result = db.execute(text("SELECT 1")).fetchone()
        return {"message": "Database connection is successful!", "result": result}
    except Exception as e:
        return {"message": "Database connection failed!", "error": str(e), "error_type": type(e).__name__}
                


# Route pour l'authentification
@app.post("/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = authenticate_user(db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    access_token = create_access_token(data={"sub": user.username})
    return {"access_token": access_token, "token_type": "bearer"}

# Route pour créer un nouvel utilisateur
@app.post("/users")
async def create_user(username: str, password: str, email: str, db: Session = Depends(get_db)):
    db_user = get_user(db, username)
    if db_user:
        raise HTTPException(status_code=400, detail="Username already registered")
    hashed_password = get_password_hash(password)
    new_user = User(username=username, hashed_password=hashed_password, email=email)
    db.add(new_user)
    db.commit()
    db.refresh(new_user)
    return {"message": "User created successfully"}

# Exemple de route protégée
@app.get("/protected")
async def protected_route(current_user: User = Depends(get_current_user)):
    return {"message": f"Hello, {current_user.username}"}

# Route pour la requête SQL spécifique
@app.get("/driver_points")
async def get_driver_points(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    try:
        query = text("""
            SELECT r.year, SUM(res.points) as total_points
            FROM races r
            JOIN results res ON r.raceId = res.raceId
            JOIN drivers d ON res.driverId = d.driverId
            WHERE d.forename = 'Alain' AND d.surname = 'Prost'
            GROUP BY r.year
            ORDER BY r.year;
        """)
        
        result = db.execute(query)
        
        data = [{"year": row[0], "total_points": row[1]} for row in result]
        
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
