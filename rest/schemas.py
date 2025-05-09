from typing import List
from pydantic import BaseModel


class Metric(BaseModel):
    id:                int
    region:            str
    az:                str
    hostname:          str
    ts:                str
    cpu_utilization:   float
    memory_utilization: float


class MetricsListResponse(BaseModel):
    metrics: List[Metric]


class MetricsCountResponse(BaseModel):
    count: int