from pydantic import BaseModel, computed_field
from typing import Optional, List
from utils.get_file_paths import get_shakemap_dir
from datetime import datetime
import os


class Properties(BaseModel):
    mag: float
    place: str
    time: int
    updated: int
    tz: Optional[int] = None
    url: str
    detail: Optional[str] = None
    felt: Optional[int] = None
    cdi: Optional[float] = None
    mmi: Optional[float] = None
    alert: Optional[str] = None
    status: Optional[str] = None
    tsunami: Optional[int] = None
    sig: Optional[int] = None
    net: Optional[str] = None
    code: Optional[str] = None
    ids: Optional[str] = None
    sources: Optional[str] = None
    types: Optional[str] = None
    nst: Optional[int] = None
    dmin: Optional[float] = None
    rms: Optional[float] = None
    gap: Optional[float] = None
    magType: Optional[str] = None
    type: Optional[str] = None
    title: Optional[str] = None

    @computed_field
    @property
    def time_pretty(self) -> float:
        return datetime.fromtimestamp(self.time / 1000).strftime("%c")

    @computed_field
    @property
    def updated_pretty(self) -> float:
        return datetime.fromtimestamp(self.updated / 1000).strftime("%c")


class Geometry(BaseModel):
    type: str
    coordinates: List[float]  # [longitude, latitude, depth]

    @computed_field
    @property
    def lat(self) -> float:
        return self.coordinates[0]

    @computed_field
    @property
    def lon(self) -> float:
        return self.coordinates[1]

    @computed_field
    @property
    def depth(self) -> float:
        return self.coordinates[2]


class Event(BaseModel):
    type: str
    properties: Properties
    geometry: Geometry
    id: str
    test: Optional[bool] = False

    @computed_field
    @property
    def shakemap_dir(self) -> str:
        if self.test:
            return os.path.join("data", "testing", self.id, "shape")
        else:
            return os.path.join(get_shakemap_dir(), self.id)
