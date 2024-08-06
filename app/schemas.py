from pydantic import BaseModel
from typing import Optional, List
from datetime import datetime

class ModelName(BaseModel):
    id: int
    name: str
    created_on: Optional[datetime] = None
    app_id: int
    package_name: str

class ModelDescription(BaseModel):
    id: int
    text: str
    name: str
    package_name: str
    created_on: Optional[datetime]
    app_id: int
    developer_id: str
    categories: List[str]

class DownloadDetails(BaseModel):
    hash: str
    size: int
    version: str
    created_on: Optional[datetime]
    permissions: List[str]
    rating: float
    total_ratings: int