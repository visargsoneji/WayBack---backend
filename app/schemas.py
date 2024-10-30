from pydantic import BaseModel, EmailStr, Field
from typing import Optional, List
from datetime import datetime

class AppResults(BaseModel):
    id: int
    name: str
    created_on: Optional[datetime] = None
    app_id: int
    package_name: str

class AppDetails(BaseModel):
    id: int
    text: str
    name: str
    package_name: str
    created_on: Optional[datetime]
    app_id: int
    developer_id: str
    categories: List[str]
    maturity: List[str]

class VersionDetails(BaseModel):
    hash: str
    size: int
    version: str
    created_on: Optional[datetime]
    permissions: List[str]
    rating: float
    total_ratings: int
    min_sdk: Optional[int] = None
    target_sdk: Optional[int] = None

class UserCreate(BaseModel):
    email: EmailStr
    first_name: str = Field(..., min_length=1)
    last_name: str = Field(..., min_length=1)
    password: str = Field(..., min_length=1)

class UserLogin(BaseModel):
    email: EmailStr
    password: str