from pydantic import BaseModel
from typing import Optional


class WikiMeta(BaseModel):
    uri: str


class WikiRecord(BaseModel):
    id: Optional[int]
    wiki: str
    title: str
    user: str
    comment: str
    meta: WikiMeta
