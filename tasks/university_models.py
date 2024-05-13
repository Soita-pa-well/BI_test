from typing import Optional, Any
from pydantic import BaseModel


class University(BaseModel):
    country: str
    alpha_two_code: str
    state_province: Optional[str]
    name: str
    type: Optional[Any] = None
