from typing import Optional
from pydantic import BaseModel


class University(BaseModel):
    country: str
    alpha_two_code: str
    state_province: Optional[str]
    name: str
    type: str
