from pydantic import BaseModel, Field
from typing import Dict, List, Any

class VisitCount(BaseModel):
    count: int
    served_via: str
