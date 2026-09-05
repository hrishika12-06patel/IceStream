"""
IceStream Observability API Backend Application.

FastAPI web application exposing REST API endpoints for stream processing pipeline
health, component status, runtime metrics, data quality, incident tracking,
and Apache Iceberg lakehouse storage monitoring.
"""

import logging
import os
from typing import Any, Dict, List, Optional
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel, Field

from backend.services.pipeline_metrics import (
    get_data_quality_metrics,
    get_health_status,
    get_incidents,
    get_lakehouse_status,
    get_pipeline_metrics,
    get_pipeline_metrics_history,
    get_pipeline_status,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("icestream.api")

app = FastAPI(
    title="IceStream Observability API",
    description="Lightweight backend observability API for IceStream real-time streaming pipeline",
    version="1.0.0",
)

# Configure CORS Middleware
default_origins = "http://localhost:5173,http://127.0.0.1:5173"
raw_origins = os.getenv("CORS_ORIGINS", default_origins)
cors_origins = [orig.strip() for orig in raw_origins.split(",") if orig.strip()]

app.add_middleware(
    CORSMiddleware,
    allow_origins=cors_origins,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


# =========================================================
# Pydantic Response Models
# =========================================================

class HealthResponse(BaseModel):
    status: str = Field(..., examples=["healthy"])
    service: str = Field(..., examples=["icestream-observability-api"])


class ComponentDetail(BaseModel):
    status: str = Field(..., examples=["healthy"])

    model_config = {"extra": "allow"}


class PipelineStatusResponse(BaseModel):
    overall_status: str = Field(..., examples=["healthy"])
    components: Dict[str, ComponentDetail]


class PipelineMetricsResponse(BaseModel):
    source: str = Field("unavailable", examples=["runtime"])
    metric_source: Optional[str] = Field("unavailable", examples=["iceberg_snapshot", "flink_rest", "kafka_offsets", "unavailable"])
    pipeline_status: Optional[str] = Field(None, examples=["healthy"])
    transactions_processed: Optional[int] = Field(None, examples=[1200])
    valid_records: Optional[int] = Field(None, examples=[1180])
    invalid_records: Optional[int] = Field(None, examples=[20])
    processing_errors: Optional[int] = Field(None, examples=[0])
    records_per_second: Optional[float] = Field(None, examples=[120.5])
    runtime: Optional[Dict[str, Any]] = Field(None)


class PipelineMetricsSnapshot(BaseModel):
    timestamp: str = Field(..., examples=["2026-09-05T20:22:07.123456+00:00"])
    source: str = Field("unavailable", examples=["runtime"])
    metric_source: Optional[str] = Field("unavailable", examples=["iceberg_snapshot", "flink_rest", "kafka_offsets", "unavailable"])
    pipeline_status: Optional[str] = Field(None, examples=["healthy"])
    transactions_processed: Optional[int] = Field(None, examples=[1200])
    valid_records: Optional[int] = Field(None, examples=[1180])
    invalid_records: Optional[int] = Field(None, examples=[20])
    processing_errors: Optional[int] = Field(None, examples=[0])
    records_per_second: Optional[float] = Field(None, examples=[120.5])
    runtime: Optional[Dict[str, Any]] = Field(None)


class PipelineMetricsHistoryResponse(BaseModel):
    count: int = Field(0, examples=[2])
    history: List[PipelineMetricsSnapshot] = Field(default_factory=list)



class DataQualityRule(BaseModel):
    rule: str
    description: str
    status: str


class DataQualityResponse(BaseModel):
    total_records: Optional[int] = Field(0, examples=[1200])
    valid_records: Optional[int] = Field(None, examples=[1180])
    invalid_records: Optional[int] = Field(None, examples=[20])
    quality_score: Optional[float] = Field(None, examples=[98.33])
    status: str = Field("no_data", examples=["measured", "metrics_unavailable", "no_data"])
    rules: List[DataQualityRule] = Field(default_factory=list)


class IncidentItem(BaseModel):
    id: str = Field(..., examples=["INC-KAFKA-OFFLINE"])
    severity: str = Field(..., examples=["high"])
    component: str = Field(..., examples=["kafka"])
    message: str = Field(..., examples=["Kafka broker is not reachable."])
    timestamp: str
    status: str = Field(..., examples=["open"])


class IncidentsResponse(BaseModel):
    total_incidents: int = Field(0, examples=[0])
    incidents: List[IncidentItem] = Field(default_factory=list)


class LakehouseResponse(BaseModel):
    catalog: str = Field(..., examples=["local"])
    namespace: str = Field(..., examples=["icestream"])
    table: str = Field(..., examples=["transactions"])
    warehouse: str = Field(..., examples=["./warehouse"])
    table_exists: bool = Field(False, examples=[True])
    snapshot_count: int = Field(0, examples=[1])
    latest_snapshot_id: Optional[str] = Field(None, examples=["123456789"])
    latest_metadata_file: Optional[str] = Field(None, examples=["v1.metadata.json"])
    record_count: Optional[int] = Field(None, examples=[100])
    status: str = Field(..., examples=["healthy"])


# =========================================================
# Exception Handlers
# =========================================================

@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception):
    """
    Catches unhandled exceptions gracefully without exposing internal stack traces or secrets.
    """
    logger.error(f"Unhandled API Exception on {request.url.path}: {exc}")
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "message": "An unexpected error occurred while processing the request.",
        },
    )


# =========================================================
# API Endpoints
# =========================================================

@app.get("/health", response_model=HealthResponse, tags=["Health"])
def health_check():
    """
    Returns service health status.
    """
    return get_health_status()


@app.get("/api/pipeline/status", response_model=PipelineStatusResponse, tags=["Pipeline"])
def pipeline_status():
    """
    Returns derived status for streaming pipeline components (Kafka, Flink, Iceberg).
    """
    return get_pipeline_status()


@app.get("/api/pipeline/metrics", response_model=PipelineMetricsResponse, tags=["Pipeline"])
def pipeline_metrics():
    """
    Exposes stream processing runtime metrics.
    """
    return get_pipeline_metrics()


@app.get("/api/pipeline/metrics/history", response_model=PipelineMetricsHistoryResponse, tags=["Pipeline"])
def pipeline_metrics_history():
    """
    Exposes bounded history of recent stream processing runtime metrics snapshots.
    """
    return get_pipeline_metrics_history()



@app.get("/api/data-quality", response_model=DataQualityResponse, tags=["Data Quality"])
def data_quality():
    """
    Exposes data quality statistics, quality score, and validation rules.
    """
    return get_data_quality_metrics()


@app.get("/api/incidents", response_model=IncidentsResponse, tags=["Incidents"])
def list_incidents():
    """
    Returns current active pipeline incidents.
    """
    return get_incidents()


@app.get("/api/lakehouse", response_model=LakehouseResponse, tags=["Lakehouse"])
def lakehouse_status():
    """
    Exposes Apache Iceberg catalog, namespace, table, and warehouse status.
    """
    return get_lakehouse_status()
