from pydantic import BaseModel

class DriverRequest(BaseModel):
    forename: str
    surname: str