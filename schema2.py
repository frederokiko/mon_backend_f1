from pydantic import BaseModel

class DriverRequest(BaseModel):
    forename: str
    surname: str

class PosRequest(BaseModel):
    year:int
    rank:int

class PosConstru(BaseModel):
    constru: str