"""
Person model to hold the Person data
"""

from pydantic import BaseModel

class Person(BaseModel):
    id: str
    name: str
    title: str