from pydantic import BaseModel
from typing import Optional, Dict


class Content(BaseModel):
    contentType: Optional[str]
    lastModified: Optional[int]
    length: Optional[int]
    url: Optional[str]


class ShakeMap(BaseModel):
    indexid: int
    indexTime: int
    id: str
    type: str
    code: str
    source: str
    updateTime: int
    status: str
    contents: Dict[str, Content]
