"""
Unit test suite for IceStream Observability Metrics API.

Tests API contracts, JSON response schemas, status codes, CORS headers,
and graceful handling of offline/unavailable infrastructure components.
All tests run standalone without requiring Docker, Kafka, Flink, or Iceberg.
"""

from unittest.mock import patch
import pytest
from fastapi.testclient import TestClient

from backend.app import app

client = TestClient(app)


# =========================================================
# 1. Health Endpoint Tests
# =========================================================

def test_health_endpoint_returns_200_and_expected_contract():
    response = client.get("/health")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    
    data = response.json()
    assert data["status"] == "healthy"
    assert data["service"] == "icestream-observability-api"
    assert "secret" not in data
    assert "env" not in data


# =========================================================
# 2. Pipeline Status Endpoint Tests
# =========================================================

def test_pipeline_status_offline_infrastructure_returns_200_and_fallback_statuses():
    with patch("backend.services.pipeline_metrics.check_kafka_status", return_value="not_running"), \
         patch("backend.services.pipeline_metrics.check_flink_status", return_value=("not_running", None)), \
         patch("backend.services.pipeline_metrics.check_iceberg_storage_status", return_value=("not_running", {})):
        
        response = client.get("/api/pipeline/status")
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("application/json")
        
        data = response.json()
        assert "overall_status" in data
        assert data["overall_status"] == "unknown"
        assert "components" in data
        assert "kafka" in data["components"]
        assert "flink" in data["components"]
        assert "iceberg" in data["components"]
        
        assert data["components"]["kafka"]["status"] == "not_running"
        assert data["components"]["flink"]["status"] == "not_running"
        assert data["components"]["iceberg"]["status"] == "not_running"


def test_pipeline_status_healthy_infrastructure():
    with patch("backend.services.pipeline_metrics.check_kafka_status", return_value="healthy"), \
         patch("backend.services.pipeline_metrics.check_flink_status", return_value=("healthy", {"jobs-running": 1})), \
         patch("backend.services.pipeline_metrics.check_iceberg_storage_status", return_value=("healthy", {})):
        
        response = client.get("/api/pipeline/status")
        assert response.status_code == 200
        
        data = response.json()
        assert data["overall_status"] == "healthy"
        assert data["components"]["kafka"]["status"] == "healthy"
        assert data["components"]["flink"]["status"] == "healthy"
        assert data["components"]["iceberg"]["status"] == "healthy"


def test_pipeline_status_degraded_infrastructure():
    with patch("backend.services.pipeline_metrics.check_kafka_status", return_value="healthy"), \
         patch("backend.services.pipeline_metrics.check_flink_status", return_value=("not_running", None)), \
         patch("backend.services.pipeline_metrics.check_iceberg_storage_status", return_value=("not_running", {})):
        
        response = client.get("/api/pipeline/status")
        assert response.status_code == 200
        
        data = response.json()
        assert data["overall_status"] == "degraded"
        assert data["components"]["kafka"]["status"] == "healthy"
        assert data["components"]["flink"]["status"] == "not_running"


# =========================================================
# 3. Pipeline Metrics Endpoint Tests
# =========================================================

def test_pipeline_metrics_offline_fallback():
    with patch("backend.services.pipeline_metrics.check_flink_status", return_value=("not_running", None)):
        response = client.get("/api/pipeline/metrics")
        assert response.status_code == 200
        assert response.headers["content-type"].startswith("application/json")
        
        data = response.json()
        assert data["source"] == "unavailable"
        assert isinstance(data["transactions_processed"], int)
        assert isinstance(data["valid_records"], int)
        assert isinstance(data["invalid_records"], int)
        assert isinstance(data["processing_errors"], int)
        assert isinstance(data["records_per_second"], (int, float))
        
        assert data["transactions_processed"] == 0
        assert data["valid_records"] == 0
        assert data["invalid_records"] == 0


def test_pipeline_metrics_runtime_source():
    overview_data = {"jobs-running": 1, "slots-total": 4}
    with patch("backend.services.pipeline_metrics.check_flink_status", return_value=("healthy", overview_data)):
        response = client.get("/api/pipeline/metrics")
        assert response.status_code == 200
        
        data = response.json()
        assert data["source"] == "runtime"
        assert data["transactions_processed"] > 0
        assert data["valid_records"] > 0


# =========================================================
# 4. Data Quality Endpoint Tests
# =========================================================

def test_data_quality_endpoint_contract():
    response = client.get("/api/data-quality")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    
    data = response.json()
    assert "total_records" in data
    assert "valid_records" in data
    assert "invalid_records" in data
    assert "quality_score" in data
    assert "rules" in data
    
    assert isinstance(data["quality_score"], (int, float))
    assert isinstance(data["rules"], list)
    assert len(data["rules"]) > 0
    
    rule = data["rules"][0]
    assert "rule" in rule
    assert "description" in rule
    assert "status" in rule


# =========================================================
# 5. Incidents Endpoint Tests
# =========================================================

def test_incidents_endpoint_returns_expected_structure():
    response = client.get("/api/incidents")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    
    data = response.json()
    assert "total_incidents" in data
    assert "incidents" in data
    assert isinstance(data["total_incidents"], int)
    assert isinstance(data["incidents"], list)
    assert data["total_incidents"] == len(data["incidents"])
    
    for incident in data["incidents"]:
        assert "id" in incident
        assert "severity" in incident
        assert "component" in incident
        assert "message" in incident
        assert "timestamp" in incident
        assert "status" in incident
        assert incident["severity"] in ("low", "medium", "high", "critical")
        assert incident["status"] in ("open", "resolved")


def test_incidents_empty_when_all_healthy():
    with patch("backend.services.pipeline_metrics.check_kafka_status", return_value="healthy"), \
         patch("backend.services.pipeline_metrics.check_flink_status", return_value=("healthy", {"jobs-running": 1})), \
         patch("backend.services.pipeline_metrics.check_iceberg_storage_status", return_value=("healthy", {})):
        
        response = client.get("/api/incidents")
        assert response.status_code == 200
        data = response.json()
        assert data["total_incidents"] == 0
        assert data["incidents"] == []


# =========================================================
# 6. Lakehouse Endpoint Tests
# =========================================================

def test_lakehouse_endpoint_contract():
    response = client.get("/api/lakehouse")
    assert response.status_code == 200
    assert response.headers["content-type"].startswith("application/json")
    
    data = response.json()
    assert "catalog" in data
    assert "namespace" in data
    assert "table" in data
    assert "warehouse" in data
    assert "table_exists" in data
    assert "snapshot_count" in data
    assert "record_count" in data
    assert "status" in data
    
    assert isinstance(data["table_exists"], bool)
    assert isinstance(data["snapshot_count"], int)


# =========================================================
# 7. CORS Headers Test
# =========================================================

def test_cors_headers_allowed_origin():
    response = client.get(
        "/health",
        headers={"Origin": "http://localhost:5173"}
    )
    assert response.status_code == 200
    assert response.headers.get("access-control-allow-origin") == "http://localhost:5173"
