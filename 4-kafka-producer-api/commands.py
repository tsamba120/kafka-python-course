"""
This class is a Pydantic model that represents a "post" body that we send into our FastAPI post endpoint
"""
from pydantic import BaseModel

class CreatePeopleCommand(BaseModel):
    count: int
    