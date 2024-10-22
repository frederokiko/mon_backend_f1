from fastapi import FastAPI, Depends, HTTPException, status #status ajouté
from sqlalchemy.orm import Session,sessionmaker #sessionmaker ajouté
from sqlalchemy import text
from database import get_db, Base, engine, connection_status
from models import User
from fastapi.middleware.cors import CORSMiddleware
from schemas import UserCreate
#importer le reste (copié-collé)
from fastapi.security import OAuth2PasswordBearer, OAuth2PasswordRequestForm
from typing import List
import jwt
from datetime import UTC, datetime, timedelta
from passlib.context import CryptContext
from schema2 import DriverRequest,PosRequest,PosConstru
# Configuration de la sécurité (copié-collé)
SECRET_KEY = "votre_clé_secrète1"  # Remplacez par une clé secrète robuste
ALGORITHM = "HS256"
ACCESS_TOKEN_EXPIRE_MINUTES = 30

pwd_context = CryptContext(schemes=["bcrypt"], deprecated="auto")
oauth2_scheme = OAuth2PasswordBearer(tokenUrl="token")
app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Autorise toutes les origines. 
    allow_credentials=True,
    allow_methods=["*"],  # Autorise toutes les méthodes (GET, POST, PUT, etc.).
    allow_headers=["*"],  # Autorise tous les headers.
)
# Créer les tables dans la base de données (si nécessaire)
if connection_status:
    Base.metadata.create_all(bind=engine)
else:
    print("Warning: Tables not created due to connection failure.")

@app.get("/") #cette foncion fonctionne
async def read_root():
    return {"message": "Welcome to the API!", "database_connected": connection_status}

@app.get("/test_db") #cette foncion fonctionne
async def test_db_connection(db: Session = Depends(get_db)):
    if not connection_status:
        raise HTTPException(status_code=500, detail="Database connection is not established")
    try:
        result = db.execute(text("SELECT 1")).fetchone()
        return {"message": "Database connection is successful!", "result": result[0]}
    except Exception as e:
        print(f"Error during database query: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Database query failed: {str(e)}")

# copié-collé ici mes autres routes et la logique de mon application...
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
    expire = datetime.now(UTC) + timedelta(hours=2, minutes=ACCESS_TOKEN_EXPIRE_MINUTES)
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


# Route pour l'authentification
@app.post("/token")
async def login(form_data: OAuth2PasswordRequestForm = Depends(), db: Session = Depends(get_db)):
    user = authenticate_user(db, form_data.username, form_data.password)
    if not user:
        raise HTTPException(status_code=400, detail="Incorrect username or password")
    access_token = create_access_token(data={"sub": user.username})
    return {"access_token": access_token, "token_type": "bearer"}


@app.post("/users")
async def create_user(user: UserCreate, db: Session = Depends(get_db)):
    try:
        db_user = get_user(db, user.username)
        if db_user:
            raise HTTPException(status_code=400, detail="Username already registered")
        hashed_password = get_password_hash(user.password)
        new_user = User(username=user.username, hashed_password=hashed_password, email=user.email)
        db.add(new_user)
        db.commit()
        db.refresh(new_user)
        return {"message": "User created successfully"}
    except Exception as e:
        # Capture l'exception pour plus de détails
        print(f"Error: {str(e)}")
        raise HTTPException(status_code=500, detail=f"Internal Server Error: {str(e)}")



# Exemple de route protégée
@app.get("/protected")
async def protected_route(current_user: User = Depends(get_current_user)):
    return {"message": f"Hello, {current_user.username} ,THE Best!"}

# Route pour la requête SQL spécifique des points
@app.post("/driver_points")
async def get_driver_points(driver: DriverRequest, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    try:
        query = text("""
            SELECT r.year, SUM(res.points) as total_points
            FROM races r
            JOIN results res ON r.raceId = res.raceId
            JOIN drivers d ON res.driverId = d.driverId
            WHERE d.forename = :forename AND d.surname = :surname
            GROUP BY r.year
            ORDER BY r.year;
        """)
        
        result = db.execute(query, {"forename": driver.forename, "surname": driver.surname})
        data = [{"year": row[0], "total_points": row[1]} for row in result]
        
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

# Route pour la requête SQL spécifique des abandons par années
@app.get("/abandon_annee")
async def get_abandon_annee(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    try:
        query = text("""
            SELECT r.year, COUNT(*) as total_retirements,
            COUNT(*) * 100.0 / (SELECT COUNT(*) FROM results WHERE raceId IN (SELECT raceId FROM races WHERE year = r.year)) as retirement_percentage
            FROM results res
            JOIN races r ON res.raceId = r.raceId
            JOIN status s ON res.statusId = s.statusId
            WHERE s.status LIKE '%Brakes%' --OR s.status LIKE '%Collision%'
            GROUP BY r.year
            ORDER BY r.year;
        """)
        
        result = db.execute(query)
        
        data = [{"year": row[0], "total_retirements": row[1],"retirement_percentage": row[2]} for row in result]
        
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

# Route pour la requête SQL spécifique total des pole position par année
@app.get("/pole_position_annee")
async def get_pole_position_annee(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    try:
        query = text("""
            SELECT d.forename, d.surname, COUNT(*) as pole_positions
            FROM qualifying q
            JOIN drivers d ON q.driverId = d.driverId
            WHERE q.position = 1
            GROUP BY d.driverId, d.forename, d.surname
            ORDER BY pole_positions DESC
        """)
        
        result = db.execute(query)
        
        data = [{"forename": row[0], "surname": row[1],"pole_positions": row[2]} for row in result]
        
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Route pour la requête SQL spécifique vitoire totale par constructeur
@app.get("/constructor_victory")
async def get_constructor_victory(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    try:
        query = text("""
            SELECT co.name, COUNT(*) as wins  
            FROM results r
            JOIN constructors co ON r.constructorId = co.constructorId
            WHERE r.position = '1'
            GROUP BY co.constructorId, co.name
            ORDER BY wins DESC
        """)
        
        result = db.execute(query)
        
        data = [{"name": row[0], "wins": row[1]} for row in result]
        
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Route pour la requête SQL spécifique nombre de courses disputées par circuits
@app.get("/nbr_course_circuit")
async def get_nbr_course_circuit(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    try:
        query = text("""
            SELECT c.name, c.country, COUNT(*) as race_count
            FROM circuits c
            JOIN races r ON c.circuitId = r.circuitId
            GROUP BY c.circuitId, c.name, c.country
            ORDER BY race_count DESC
        """)
        
        result = db.execute(query)
        
        data = [{"name": row[0], "country": row[1],"race_count": row[2]} for row in result]
        
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Route pour la requête SQL spécifique nombre de victoire totale par pilote
@app.get("/nbr_win_driver")
async def get_nbr_win_driver(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    try:
        query = text("""
            SELECT d.forename, d.surname, COUNT(*) as victories
            FROM results r
            JOIN drivers d ON r.driverId = d.driverId
            WHERE r.position = '1'
            GROUP BY d.driverId, d.forename, d.surname
            ORDER BY victories DESC
        """)
        
        result = db.execute(query)
        
        data = [{"forename": row[0], "surname": row[1],"victories": row[2]} for row in result]
        
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Route pour la requête SQL spécifique afficher la localistion des circuits
@app.get("/circuit_localisation")
async def get_circuit_localisation(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    try:
        query = text("""
            SELECT name,country,lat,lng,url
            FROM circuits
        """)
        
        result = db.execute(query)
        
        data = [{"name": row[0], "country": row[1],"lat": row[2],"lng":row[3],"url":row[4]} for row in result]
        
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    

# Route pour la requête SQL spécifique afficher la liste de tous les pilotes
@app.get("/detail_pilote")
async def get_detail_pilote(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    try:
        query = text("""
            SELECT forename,surname,nationality,url
            FROM drivers
        """)
        
        result = db.execute(query)
        
        data = [{"forename": row[0], "surname": row[1],"nationality": row[2],"url":row[3]} for row in result]
        
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Route pour la requête SQL spécifique afficher la liste de tous les pilotes
@app.get("/detail_constructor")
async def get_detail_constructor(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    try:
        query = text("""
            SELECT name,nationality,url
            FROM constructors
            ORDER BY name 
        """)
        
        result = db.execute(query)
        
        data = [{"name": row[0], "nationality": row[1],"url": row[2]} for row in result]
        
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
    
# Route pour la requête SQL spécifique afficher lien wiki vers les infos des grand-prix par année
@app.get("/info_gp")
async def get_info_gp(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    try:
        query = text("""
            SELECT year,url
            FROM races
            WHERE round =1
            ORDER BY year DESC
        """)
        
        result = db.execute(query)
        
        data = [{"year": row[0], "url": row[1]} for row in result]
        
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Route pour la requête SQL recherche par annee et rank 
@app.post("/result_year")
async def get_result_year(result1: PosRequest, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    try:
        query = text("""
                     SELECT r.date,c.country,r.name,d.forename,d.surname,res.position
                     FROM races r 
                     JOIN  results res ON r.raceId= res.raceId
                     JOIN drivers d ON  d.driverId=res.driverId
                     JOIN circuits c ON c.circuitId=r.circuitId
                     WHERE r.year= :year AND res.position= :rank
                     ORDER BY r.date
        """)
        
        result = db.execute(query, {"year": result1.year, "rank": result1.rank})
        data = [{"date": row[0], "country": row[1], "name": row[2], "forename": row[3], "surname": row[4], "rank": row[5]} for row in result]
        
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Route pour la requête SQL spécifique afficher la liste de tous les pilotes
@app.get("/tout_constructeur")
async def get_tout_constructeur(current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    try:
        query = text("""
            SELECT name
            FROM constructors
            ORDER BY name
        """)
        
        result = db.execute(query)
        
        data = [{"name": row[0]} for row in result]
        
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

# Route pour la requête SQL recherche par annee et rank 
@app.post("/result_pilote_constructeur")
async def get_result_pilote_constructeur(result : PosConstru, current_user: User = Depends(get_current_user), db: Session = Depends(get_db)):
    try:
        query = text("""
                    SELECT DISTINCT c.name AS 'ecurie' ,
				d.forename,
				d.surname,
				MAX(ra.year+1)-MIN(ra.year) AS'annee',
				MIN(ra.year) AS'debut',
				MAX(ra.year) AS'fin',
				COUNT(res.points ) AS 'nbr_gp'
FROM drivers d  
	JOIN results res on d.driverId=res.driverId
	JOIN constructors c ON c.constructorId=res.constructorId 
	JOIN races ra ON res.raceId =ra.raceId
WHERE c.name = :name
GROUP BY c.name, d.forename, d.surname
ORDER BY annee DESC
        """)
        
        result = db.execute(query, {"name": result.constru})
        data = [{"ecurie": row[0], "forname": row[1], "surname": row[2], "annee": row[3], "debut": row[4], "fin": row[5],"nbr_gp":row[6]} for row in result]
        
        return {"data": data}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)