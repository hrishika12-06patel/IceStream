"""
Unit test suite for IceStream Observability Metrics API.

Tests API contracts, JSON response schemas, status codes, CORS headers,
runtime metrics collection, status aggregation, data quality zero-record behavior,
incident generation, and graceful handling of offline/unavailable infrastructure components.
All tests run standalone without requiring Docker, Kafka, Flink, or Iceberg.
"""

from unittest.mock import MagicMock, patch
import pytest
from fastapi.testclient import TestClient

from backend.app import app
from backend.services.pipeline_metrics import (
    clear_pipeline_metrics_history,
    get_data_quality_metrics,
    get_flink_runtime_metrics,
    get_incidents,
    get_iceberg_runtime_metrics,
    get_kafka_runtime_metrics,
    get_pipeline_metrics,
    get_pipeline_metrics_history,
    get_pipeline_status,
    set_pipeline_metrics_history_maxlen,
)

client = TestClient(app)


@pytest.fixture(autouse=True)
def reset_history_state():
    clear_pipeline_metrics_history()
    set_pipeline_metrics_history_maxlen(60)
    yield
    clear_pipeline_metrics_history()
    set_pipeline_metrics_history_maxlen(60)



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
# 2. Kafka Runtime Metrics Tests
# =========================================================

def test_kafka_runtime_metrics_healthy_shape_and_offsets():
    mock_consumer = MagicMock()
    mock_consumer.partitions_for_topic.return_value = {0, 1, 2}
    mock_consumer.beginning_offsets.return_value = {("transactions", 0): 0, ("transactions", 1): 0, ("transactions", 2): 0}
    mock_consumer.end_offsets.return_value = {("transactions", 0): 10, ("transactions", 1): 5, ("transactions", 2): 5}

    with patch("socket.create_connection"), \
         patch("kafka.KafkaConsumer", return_value=mock_consumer):
        metrics = get_kafka_runtime_metrics()
        assert metrics["status"] == "healthy"
        assert metrics["bootstrap_servers"] == "localhost:9092"
        assert metrics["topic"] == "transactions"
        assert metrics["topic_exists"] is True
        assert metrics["partition_count"] == 3
        assert metrics["total_messages"] == 20


def test_kafka_runtime_metrics_offline_fallback():
    with patch("socket.create_connection", side_effect=OSError("Connection refused")):
        metrics = get_kafka_runtime_metrics(timeout=0.1)
        assert metrics["status"] == "not_running"
        assert metrics["bootstrap_servers"] == "localhost:9092"
        assert metrics["topic"] == "transactions"
        assert metrics["topic_exists"] is None
        assert metrics["partition_count"] is None
        assert metrics["total_messages"] is None


# =========================================================
# 3. Flink Runtime Metrics Tests
# =========================================================

def test_flink_runtime_metrics_healthy_shape_and_records():
    overview_json = (
        '{"flink-version": "1.18.1", "taskmanagers": 1, "slots-total": 2, '
        '"slots-available": 1, "jobs-running": 1, "jobs-failed": 0}'
    ).encode("utf-8")
    jobs_json = (
        '{"jobs": [{"jid": "job123", "name": "icestream_job", "state": "RUNNING"}]}'
    ).encode("utf-8")
    job_detail_json = (
        '{"vertices": ['
        '{"name": "Source: Kafka", "metrics": {"read-records": 50, "write-records": 50}},'
        '{"name": "IcebergSink", "metrics": {"read-records": 50, "write-records": 50}}'
        ']}'
    ).encode("utf-8")

    def mock_urlopen(req, timeout=0.5):
        url = req.full_url if hasattr(req, "full_url") else str(req)
        resp = MagicMock()
        resp.status = 200
        if "jobs/overview" in url:
            resp.read.return_value = jobs_json
        elif "jobs/job123" in url:
            resp.read.return_value = job_detail_json
        else:
            resp.read.return_value = overview_json
        resp.__enter__.return_value = resp
        return resp

    with patch("urllib.request.urlopen", side_effect=mock_urlopen):
        metrics = get_flink_runtime_metrics(timeout=0.1)
        assert metrics["status"] == "healthy"
        assert metrics["flink_version"] == "1.18.1"
        assert metrics["taskmanagers"] == 1
        assert metrics["slots_total"] == 2
        assert metrics["slots_available"] == 1
        assert metrics["jobs_running"] == 1
        assert metrics["records_in"] == 100
        assert metrics["records_out"] == 100
        assert len(metrics["jobs"]) == 1
        assert metrics["jobs"][0]["id"] == "job123"


def test_flink_runtime_metrics_offline_fallback():
    with patch("urllib.request.urlopen", side_effect=OSError("Connection refused")):
        metrics = get_flink_runtime_metrics(timeout=0.1)
        assert metrics["status"] == "not_running"
        assert metrics["flink_version"] is None
        assert metrics["taskmanagers"] is None
        assert metrics["jobs_running"] == 0
        assert metrics["records_in"] is None
        assert metrics["records_out"] is None
        assert metrics["jobs"] == []


# =========================================================
# 4. Iceberg Runtime Information Tests
# =========================================================

def test_iceberg_runtime_metrics_inspection_and_fallback():
    with patch("os.path.exists", return_value=False):
        metrics = get_iceberg_runtime_metrics()
        assert metrics["status"] == "unavailable"
        assert metrics["table_exists"] is False
        assert metrics["snapshot_count"] == 0
        assert metrics["latest_snapshot_id"] is None
        assert metrics["latest_metadata_file"] is None
        assert metrics["record_count"] is None


def test_iceberg_runtime_metrics_healthy_table():
    metadata_mock = {
        "current-snapshot-id": 12345,
        "snapshots": [
            {"snapshot-id": 12345, "summary": {"total-records": "315"}}
        ],
    }
    with patch("os.path.exists", return_value=True), \
         patch("json.load", return_value=metadata_mock), \
         patch("builtins.open", MagicMock()):
        metrics = get_iceberg_runtime_metrics()
        assert metrics["status"] == "healthy"
        assert metrics["table_exists"] is True
        assert metrics["snapshot_count"] == 1
        assert metrics["latest_snapshot_id"] == "12345"
        assert metrics["record_count"] == 315


# =========================================================
# 5. Pipeline Status Aggregation Tests
# =========================================================

def test_pipeline_status_all_healthy():
    mock_kafka = {"status": "healthy"}
    mock_flink = {"status": "healthy"}
    mock_iceberg = {"status": "healthy"}

    with patch("backend.services.pipeline_metrics.get_kafka_runtime_metrics", return_value=mock_kafka), \
         patch("backend.services.pipeline_metrics.get_flink_runtime_metrics", return_value=mock_flink), \
         patch("backend.services.pipeline_metrics.get_iceberg_runtime_metrics", return_value=mock_iceberg):

        response = client.get("/api/pipeline/status")
        assert response.status_code == 200

        data = response.json()
        assert data["overall_status"] == "healthy"
        assert data["components"]["kafka"]["status"] == "healthy"
        assert data["components"]["flink"]["status"] == "healthy"
        assert data["components"]["iceberg"]["status"] == "healthy"


def test_pipeline_status_degraded_when_kafka_healthy_and_flink_offline():
    mock_kafka = {"status": "healthy"}
    mock_flink = {"status": "not_running"}
    mock_iceberg = {"status": "unavailable"}

    with patch("backend.services.pipeline_metrics.get_kafka_runtime_metrics", return_value=mock_kafka), \
         patch("backend.services.pipeline_metrics.get_flink_runtime_metrics", return_value=mock_flink), \
         patch("backend.services.pipeline_metrics.get_iceberg_runtime_metrics", return_value=mock_iceberg):

        response = client.get("/api/pipeline/status")
        assert response.status_code == 200

        data = response.json()
        assert data["overall_status"] == "degraded"
        assert data["components"]["kafka"]["status"] == "healthy"
        assert data["components"]["flink"]["status"] == "not_running"


def test_pipeline_status_unavailable_when_all_offline():
    mock_kafka = {"status": "not_running"}
    mock_flink = {"status": "not_running"}
    mock_iceberg = {"status": "unavailable"}

    with patch("backend.services.pipeline_metrics.get_kafka_runtime_metrics", return_value=mock_kafka), \
         patch("backend.services.pipeline_metrics.get_flink_runtime_metrics", return_value=mock_flink), \
         patch("backend.services.pipeline_metrics.get_iceberg_runtime_metrics", return_value=mock_iceberg):

        response = client.get("/api/pipeline/status")
        assert response.status_code == 200

        data = response.json()
        assert data["overall_status"] == "unavailable"
        assert data["components"]["kafka"]["status"] == "not_running"
        assert data["components"]["flink"]["status"] == "not_running"
        assert data["components"]["iceberg"]["status"] == "unavailable"


# =========================================================
# 6. Pipeline Metrics Endpoint & Priority Tests
# =========================================================

def test_pipeline_metrics_priority_iceberg_snapshot():
    mock_status = {
        "overall_status": "healthy",
        "components": {
            "kafka": {"status": "healthy", "topic": "transactions", "partition_count": 3, "total_messages": 100},
            "flink": {"status": "healthy", "jobs_running": 1, "taskmanagers": 1, "records_in": 100, "records_out": 100},
            "iceberg": {"status": "healthy", "snapshot_count": 10, "latest_snapshot_id": "123", "record_count": 250},
        },
    }
    with patch("backend.services.pipeline_metrics.get_pipeline_status", return_value=mock_status):
        response = client.get("/api/pipeline/metrics")
        assert response.status_code == 200

        data = response.json()
        assert data["source"] == "runtime"
        assert data["metric_source"] == "iceberg_snapshot"
        assert data["transactions_processed"] == 250
        assert data["processing_errors"] == 0
        assert data["runtime"]["iceberg"]["record_count"] == 250


def test_pipeline_metrics_priority_flink_rest_fallback():
    mock_status = {
        "overall_status": "healthy",
        "components": {
            "kafka": {"status": "healthy", "topic": "transactions", "partition_count": 3, "total_messages": 100},
            "flink": {"status": "healthy", "jobs_running": 1, "taskmanagers": 1, "records_in": 80, "records_out": 80},
            "iceberg": {"status": "healthy", "snapshot_count": 0, "latest_snapshot_id": None, "record_count": None},
        },
    }
    with patch("backend.services.pipeline_metrics.get_pipeline_status", return_value=mock_status):
        response = client.get("/api/pipeline/metrics")
        assert response.status_code == 200

        data = response.json()
        assert data["source"] == "runtime"
        assert data["metric_source"] == "flink_rest"
        assert data["transactions_processed"] == 80


def test_pipeline_metrics_priority_kafka_offsets_fallback():
    mock_status = {
        "overall_status": "healthy",
        "components": {
            "kafka": {"status": "healthy", "topic": "transactions", "partition_count": 3, "total_messages": 45},
            "flink": {"status": "healthy", "jobs_running": 1, "taskmanagers": 1, "records_in": None, "records_out": None},
            "iceberg": {"status": "healthy", "snapshot_count": 0, "latest_snapshot_id": None, "record_count": None},
        },
    }
    with patch("backend.services.pipeline_metrics.get_pipeline_status", return_value=mock_status):
        response = client.get("/api/pipeline/metrics")
        assert response.status_code == 200

        data = response.json()
        assert data["source"] == "runtime"
        assert data["metric_source"] == "kafka_offsets"
        assert data["transactions_processed"] == 45


def test_pipeline_metrics_unavailable_source_when_offline():
    mock_status = {
        "overall_status": "unavailable",
        "components": {
            "kafka": {"status": "not_running", "topic": "transactions", "partition_count": None, "total_messages": None},
            "flink": {"status": "not_running", "jobs_running": 0, "taskmanagers": None, "records_in": None, "records_out": None},
            "iceberg": {"status": "unavailable", "snapshot_count": 0, "latest_snapshot_id": None, "record_count": None},
        },
    }
    with patch("backend.services.pipeline_metrics.get_pipeline_status", return_value=mock_status):
        response = client.get("/api/pipeline/metrics")
        assert response.status_code == 200

        data = response.json()
        assert data["source"] == "unavailable"
        assert data["metric_source"] == "unavailable"
        assert data["pipeline_status"] == "unavailable"
        assert data["transactions_processed"] is None
        assert data["valid_records"] is None


# =========================================================
# 7. Data Quality Endpoint Tests
# =========================================================

def test_data_quality_zero_record_behavior():
    mock_metrics = {
        "source": "unavailable",
        "metric_source": "unavailable",
        "transactions_processed": None,
        "valid_records": None,
        "invalid_records": None,
    }
    with patch("backend.services.pipeline_metrics.get_pipeline_metrics", return_value=mock_metrics):
        response = client.get("/api/data-quality")
        assert response.status_code == 200

        data = response.json()
        assert data["total_records"] == 0
        assert data["valid_records"] == 0
        assert data["invalid_records"] == 0
        assert data["quality_score"] is None
        assert data["status"] == "no_data"
        assert isinstance(data["rules"], list)
        assert len(data["rules"]) > 0


def test_data_quality_unmeasured_records_behavior():
    mock_metrics = {
        "source": "runtime",
        "metric_source": "iceberg_snapshot",
        "transactions_processed": 315,
        "valid_records": None,
        "invalid_records": None,
    }
    with patch("backend.services.pipeline_metrics.get_pipeline_metrics", return_value=mock_metrics):
        response = client.get("/api/data-quality")
        assert response.status_code == 200

        data = response.json()
        assert data["total_records"] == 315
        assert data["valid_records"] is None
        assert data["invalid_records"] is None
        assert data["quality_score"] is None
        assert data["status"] == "metrics_unavailable"


# =========================================================
# 8. Incidents Endpoint Tests
# =========================================================

def test_incidents_when_components_offline():
    mock_status = {
        "overall_status": "unavailable",
        "components": {
            "kafka": {"status": "not_running"},
            "flink": {"status": "not_running"},
            "iceberg": {"status": "unavailable"},
        },
    }
    with patch("backend.services.pipeline_metrics.get_pipeline_status", return_value=mock_status):
        response = client.get("/api/incidents")
        assert response.status_code == 200

        data = response.json()
        assert data["total_incidents"] == 3
        incidents_by_id = {inc["id"]: inc for inc in data["incidents"]}

        assert "INC-KAFKA-OFFLINE" in incidents_by_id
        assert incidents_by_id["INC-KAFKA-OFFLINE"]["severity"] == "high"
        assert incidents_by_id["INC-KAFKA-OFFLINE"]["component"] == "kafka"

        assert "INC-FLINK-OFFLINE" in incidents_by_id
        assert incidents_by_id["INC-FLINK-OFFLINE"]["severity"] == "high"
        assert incidents_by_id["INC-FLINK-OFFLINE"]["component"] == "flink"

        assert "INC-ICEBERG-UNAVAILABLE" in incidents_by_id
        assert incidents_by_id["INC-ICEBERG-UNAVAILABLE"]["severity"] == "medium"
        assert incidents_by_id["INC-ICEBERG-UNAVAILABLE"]["component"] == "iceberg"


def test_incidents_empty_when_all_healthy():
    mock_status = {
        "overall_status": "healthy",
        "components": {
            "kafka": {"status": "healthy"},
            "flink": {"status": "healthy"},
            "iceberg": {"status": "healthy"},
        },
    }
    with patch("backend.services.pipeline_metrics.get_pipeline_status", return_value=mock_status):
        response = client.get("/api/incidents")
        assert response.status_code == 200
        data = response.json()
        assert data["total_incidents"] == 0
        assert data["incidents"] == []


# =========================================================
# 9. Lakehouse Endpoint Tests
# =========================================================

def test_lakehouse_endpoint_contract():
    mock_iceberg = {
        "catalog": "local",
        "namespace": "icestream",
        "table": "transactions",
        "warehouse": "./warehouse",
        "table_exists": True,
        "snapshot_count": 5,
        "latest_snapshot_id": "1122334455",
        "latest_metadata_file": "v5.metadata.json",
        "record_count": 315,
        "status": "healthy",
    }
    with patch("backend.services.pipeline_metrics.get_iceberg_runtime_metrics", return_value=mock_iceberg):
        response = client.get("/api/lakehouse")
        assert response.status_code == 200
        data = response.json()

        assert data["catalog"] == "local"
        assert data["table_exists"] is True
        assert data["snapshot_count"] == 5
        assert data["latest_snapshot_id"] == "1122334455"
        assert data["latest_metadata_file"] == "v5.metadata.json"
        assert data["record_count"] == 315
        assert data["status"] == "healthy"


# =========================================================
# 10. CORS Headers Test
# =========================================================

def test_cors_headers_allowed_origin():
    response = client.get(
        "/health",
        headers={"Origin": "http://localhost:5173"}
    )
    assert response.status_code == 200
    assert response.headers.get("access-control-allow-origin") == "http://localhost:5173"


# =========================================================
# 11. Pipeline Metrics History Tests
# =========================================================

def test_metrics_history_empty_initial_state():
    response = client.get("/api/pipeline/metrics/history")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 0
    assert data["history"] == []


def test_metrics_history_stores_snapshots_and_expected_structure():
    mock_status = {
        "overall_status": "healthy",
        "components": {
            "kafka": {"status": "healthy", "topic": "transactions", "partition_count": 3, "total_messages": 100},
            "flink": {"status": "healthy", "jobs_running": 1, "taskmanagers": 1, "records_in": 100, "records_out": 100},
            "iceberg": {"status": "healthy", "snapshot_count": 10, "latest_snapshot_id": "123", "record_count": 335},
        },
    }
    with patch("backend.services.pipeline_metrics.get_pipeline_status", return_value=mock_status):
        metrics_resp = client.get("/api/pipeline/metrics")
        assert metrics_resp.status_code == 200

        history_resp = client.get("/api/pipeline/metrics/history")
        assert history_resp.status_code == 200
        data = history_resp.json()

        assert data["count"] == 1
        assert len(data["history"]) == 1

        entry = data["history"][0]
        assert "timestamp" in entry
        assert entry["source"] == "runtime"
        assert entry["pipeline_status"] == "healthy"
        assert entry["transactions_processed"] == 335
        assert entry["valid_records"] is None
        assert entry["invalid_records"] is None
        assert entry["processing_errors"] == 0
        assert entry["records_per_second"] is None


def test_metrics_history_entries_contain_iso_timestamps():
    from datetime import datetime
    mock_status = {
        "overall_status": "healthy",
        "components": {
            "kafka": {"status": "healthy", "topic": "transactions", "partition_count": 1, "total_messages": 10},
            "flink": {"status": "healthy", "jobs_running": 1, "taskmanagers": 1, "records_in": 10, "records_out": 10},
            "iceberg": {"status": "healthy", "snapshot_count": 1, "latest_snapshot_id": "1", "record_count": 10},
        },
    }
    with patch("backend.services.pipeline_metrics.get_pipeline_status", return_value=mock_status):
        client.get("/api/pipeline/metrics")

    history_resp = client.get("/api/pipeline/metrics/history")
    data = history_resp.json()
    timestamp_str = data["history"][0]["timestamp"]

    parsed_dt = datetime.fromisoformat(timestamp_str)
    assert parsed_dt is not None


def test_metrics_history_multiple_snapshots_chronological_order():
    mock_status_1 = {
        "overall_status": "healthy",
        "components": {
            "kafka": {"status": "healthy", "topic": "transactions", "partition_count": 1, "total_messages": 10},
            "flink": {"status": "healthy", "jobs_running": 1, "taskmanagers": 1, "records_in": 10, "records_out": 10},
            "iceberg": {"status": "healthy", "snapshot_count": 1, "latest_snapshot_id": "1", "record_count": 100},
        },
    }
    mock_status_2 = {
        "overall_status": "healthy",
        "components": {
            "kafka": {"status": "healthy", "topic": "transactions", "partition_count": 1, "total_messages": 20},
            "flink": {"status": "healthy", "jobs_running": 1, "taskmanagers": 1, "records_in": 20, "records_out": 20},
            "iceberg": {"status": "healthy", "snapshot_count": 2, "latest_snapshot_id": "2", "record_count": 200},
        },
    }

    with patch("backend.services.pipeline_metrics.get_pipeline_status", return_value=mock_status_1):
        client.get("/api/pipeline/metrics")

    with patch("backend.services.pipeline_metrics.get_pipeline_status", return_value=mock_status_2):
        client.get("/api/pipeline/metrics")

    response = client.get("/api/pipeline/metrics/history")
    data = response.json()
    assert data["count"] == 2
    assert data["history"][0]["transactions_processed"] == 100
    assert data["history"][1]["transactions_processed"] == 200
    assert data["history"][0]["timestamp"] <= data["history"][1]["timestamp"]


def test_metrics_history_bounded_capacity_removes_oldest():
    set_pipeline_metrics_history_maxlen(3)

    for i in range(5):
        mock_status = {
            "overall_status": "healthy",
            "components": {
                "kafka": {"status": "healthy", "topic": "t", "partition_count": 1, "total_messages": i},
                "flink": {"status": "healthy", "jobs_running": 1, "taskmanagers": 1, "records_in": i, "records_out": i},
                "iceberg": {"status": "healthy", "snapshot_count": i, "latest_snapshot_id": str(i), "record_count": 100 + i},
            },
        }
        with patch("backend.services.pipeline_metrics.get_pipeline_status", return_value=mock_status):
            client.get("/api/pipeline/metrics")

    response = client.get("/api/pipeline/metrics/history")
    data = response.json()
    assert data["count"] == 3
    processed_values = [item["transactions_processed"] for item in data["history"]]
    assert processed_values == [102, 103, 104]


def test_metrics_history_handles_offline_snapshots_safely():
    mock_status_offline = {
        "overall_status": "unavailable",
        "components": {
            "kafka": {"status": "not_running", "topic": "transactions", "partition_count": None, "total_messages": None},
            "flink": {"status": "not_running", "jobs_running": 0, "taskmanagers": None, "records_in": None, "records_out": None},
            "iceberg": {"status": "unavailable", "snapshot_count": 0, "latest_snapshot_id": None, "record_count": None},
        },
    }

    with patch("backend.services.pipeline_metrics.get_pipeline_status", return_value=mock_status_offline):
        client.get("/api/pipeline/metrics")

    response = client.get("/api/pipeline/metrics/history")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    snapshot = data["history"][0]
    assert snapshot["source"] == "unavailable"
    assert snapshot["pipeline_status"] == "unavailable"
    assert snapshot["transactions_processed"] is None
    assert snapshot["processing_errors"] is None


# =========================================================
# 12. Query Controls (limit) Tests for Pipeline Metrics History
# =========================================================

def helper_populate_history(n: int = 5):
    """Helper to populate history with N distinct snapshots."""
    for i in range(1, n + 1):
        mock_status = {
            "overall_status": "healthy",
            "components": {
                "kafka": {"status": "healthy", "topic": "transactions", "partition_count": 1, "total_messages": i * 10},
                "flink": {"status": "healthy", "jobs_running": 1, "taskmanagers": 1, "records_in": i * 10, "records_out": i * 10},
                "iceberg": {"status": "healthy", "snapshot_count": i, "latest_snapshot_id": str(i), "record_count": i * 100},
            },
        }
        with patch("backend.services.pipeline_metrics.get_pipeline_status", return_value=mock_status):
            client.get("/api/pipeline/metrics")


def test_metrics_history_default_returns_all_snapshots():
    helper_populate_history(5)
    response = client.get("/api/pipeline/metrics/history")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 5
    assert len(data["history"]) == 5
    assert [s["transactions_processed"] for s in data["history"]] == [100, 200, 300, 400, 500]


def test_metrics_history_limit_1_returns_latest_snapshot():
    helper_populate_history(5)
    response = client.get("/api/pipeline/metrics/history?limit=1")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    assert len(data["history"]) == 1
    assert data["history"][0]["transactions_processed"] == 500


def test_metrics_history_limit_10_returns_latest_available_up_to_10():
    helper_populate_history(5)
    response = client.get("/api/pipeline/metrics/history?limit=10")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 5
    assert len(data["history"]) == 5
    assert [s["transactions_processed"] for s in data["history"]] == [100, 200, 300, 400, 500]


def test_metrics_history_limit_exceeding_maximum_returns_validation_error():
    response = client.get("/api/pipeline/metrics/history?limit=61")
    assert response.status_code == 422


def test_metrics_history_limit_below_minimum_returns_validation_error():
    response = client.get("/api/pipeline/metrics/history?limit=0")
    assert response.status_code == 422

    response_neg = client.get("/api/pipeline/metrics/history?limit=-5")
    assert response_neg.status_code == 422


def test_metrics_history_limit_preserves_chronological_ordering():
    helper_populate_history(5)
    response = client.get("/api/pipeline/metrics/history?limit=3")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 3
    history = data["history"]
    assert history[0]["transactions_processed"] == 300
    assert history[1]["transactions_processed"] == 400
    assert history[2]["transactions_processed"] == 500
    assert history[0]["timestamp"] <= history[1]["timestamp"] <= history[2]["timestamp"]


def test_metrics_history_limit_returns_latest_n_not_oldest_n():
    helper_populate_history(5)
    response = client.get("/api/pipeline/metrics/history?limit=3")
    assert response.status_code == 200
    data = response.json()
    processed_values = [s["transactions_processed"] for s in data["history"]]
    assert processed_values == [300, 400, 500]
    assert processed_values != [100, 200, 300]


def test_metrics_history_empty_history_returns_zero_count_and_empty_list():
    response = client.get("/api/pipeline/metrics/history?limit=5")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 0
    assert data["history"] == []


def test_metrics_history_snapshot_fields_preserved_with_limit():
    helper_populate_history(1)
    response = client.get("/api/pipeline/metrics/history?limit=1")
    assert response.status_code == 200
    data = response.json()
    snapshot = data["history"][0]

    expected_fields = {
        "timestamp",
        "source",
        "metric_source",
        "pipeline_status",
        "transactions_processed",
        "valid_records",
        "invalid_records",
        "processing_errors",
        "records_per_second",
        "runtime",
    }
    assert expected_fields.issubset(snapshot.keys())


def test_metrics_history_service_direct_call_validation():
    with pytest.raises(ValueError, match="limit must be between 1 and 60"):
        get_pipeline_metrics_history(limit=0)

    with pytest.raises(ValueError, match="limit must be between 1 and 60"):
        get_pipeline_metrics_history(limit=61)

    with pytest.raises(ValueError, match="minutes must be between 1 and 60"):
        get_pipeline_metrics_history(minutes=0)

    with pytest.raises(ValueError, match="minutes must be between 1 and 60"):
        get_pipeline_metrics_history(minutes=61)


# =========================================================
# 13. Time-Range (minutes) Filtering Tests for Metrics History
# =========================================================

def helper_inject_snapshot_at_offset(minutes_ago: float, transactions_processed: int):
    from datetime import datetime, timezone, timedelta
    from backend.services.pipeline_metrics import _metrics_history
    now_utc = datetime.now(timezone.utc)
    ts = (now_utc - timedelta(minutes=minutes_ago)).isoformat()
    snapshot = {
        "timestamp": ts,
        "source": "runtime",
        "metric_source": "iceberg_snapshot",
        "pipeline_status": "healthy",
        "transactions_processed": transactions_processed,
        "valid_records": None,
        "invalid_records": None,
        "processing_errors": 0,
        "records_per_second": None,
        "runtime": {},
    }
    _metrics_history.append(snapshot)
    return snapshot


def test_metrics_history_no_minutes_param_preserves_existing_behavior():
    helper_inject_snapshot_at_offset(30.0, 100)
    helper_inject_snapshot_at_offset(10.0, 200)
    helper_inject_snapshot_at_offset(0.5, 300)

    response = client.get("/api/pipeline/metrics/history")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 3
    assert [s["transactions_processed"] for s in data["history"]] == [100, 200, 300]


def test_metrics_history_minutes_1_returns_only_last_minute_snapshots():
    helper_inject_snapshot_at_offset(10.0, 100)
    helper_inject_snapshot_at_offset(5.0, 200)
    helper_inject_snapshot_at_offset(0.2, 300)

    response = client.get("/api/pipeline/metrics/history?minutes=1")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 1
    assert len(data["history"]) == 1
    assert data["history"][0]["transactions_processed"] == 300


def test_metrics_history_valid_larger_window():
    helper_inject_snapshot_at_offset(45.0, 100)
    helper_inject_snapshot_at_offset(20.0, 200)
    helper_inject_snapshot_at_offset(5.0, 300)
    helper_inject_snapshot_at_offset(0.1, 400)

    response = client.get("/api/pipeline/metrics/history?minutes=30")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 3
    assert [s["transactions_processed"] for s in data["history"]] == [200, 300, 400]


def test_metrics_history_minutes_below_minimum_returns_422():
    response = client.get("/api/pipeline/metrics/history?minutes=0")
    assert response.status_code == 422

    response_neg = client.get("/api/pipeline/metrics/history?minutes=-5")
    assert response_neg.status_code == 422


def test_metrics_history_minutes_above_maximum_returns_422():
    response = client.get("/api/pipeline/metrics/history?minutes=61")
    assert response.status_code == 422


def test_metrics_history_no_snapshots_within_range_returns_empty():
    helper_inject_snapshot_at_offset(25.0, 100)

    response = client.get("/api/pipeline/metrics/history?minutes=5")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 0
    assert data["history"] == []


def test_metrics_history_combination_minutes_and_limit():
    helper_inject_snapshot_at_offset(50.0, 10)
    helper_inject_snapshot_at_offset(12.0, 20)
    helper_inject_snapshot_at_offset(10.0, 30)
    helper_inject_snapshot_at_offset(8.0, 40)
    helper_inject_snapshot_at_offset(6.0, 50)
    helper_inject_snapshot_at_offset(2.0, 60)

    response = client.get("/api/pipeline/metrics/history?minutes=15&limit=3")
    assert response.status_code == 200
    data = response.json()
    assert data["count"] == 3
    assert [s["transactions_processed"] for s in data["history"]] == [40, 50, 60]


def test_metrics_history_chronological_ordering_preserved_with_minutes():
    helper_inject_snapshot_at_offset(15.0, 100)
    helper_inject_snapshot_at_offset(10.0, 200)
    helper_inject_snapshot_at_offset(5.0, 300)

    response = client.get("/api/pipeline/metrics/history?minutes=20")
    assert response.status_code == 200
    data = response.json()
    history = data["history"]
    assert len(history) == 3
    assert history[0]["timestamp"] <= history[1]["timestamp"] <= history[2]["timestamp"]


def test_metrics_history_latest_n_after_filtering_not_oldest_n():
    helper_inject_snapshot_at_offset(12.0, 100)
    helper_inject_snapshot_at_offset(10.0, 200)
    helper_inject_snapshot_at_offset(8.0, 300)
    helper_inject_snapshot_at_offset(5.0, 400)
    helper_inject_snapshot_at_offset(2.0, 500)

    response = client.get("/api/pipeline/metrics/history?minutes=15&limit=2")
    assert response.status_code == 200
    data = response.json()
    processed_values = [s["transactions_processed"] for s in data["history"]]
    assert processed_values == [400, 500]
    assert processed_values != [100, 200]


def test_metrics_history_boundary_handling_exact_cutoff():
    from datetime import datetime, timezone, timedelta
    from backend.services.pipeline_metrics import _metrics_history

    fixed_now = datetime(2026, 9, 8, 12, 0, 0, tzinfo=timezone.utc)
    exact_cutoff_ts = (fixed_now - timedelta(minutes=5)).isoformat()
    inside_ts = (fixed_now - timedelta(minutes=2)).isoformat()
    outside_ts = (fixed_now - timedelta(minutes=5, seconds=1)).isoformat()

    _metrics_history.append({"timestamp": outside_ts, "transactions_processed": 10, "source": "runtime"})
    _metrics_history.append({"timestamp": exact_cutoff_ts, "transactions_processed": 20, "source": "runtime"})
    _metrics_history.append({"timestamp": inside_ts, "transactions_processed": 30, "source": "runtime"})

    with patch("backend.services.pipeline_metrics.datetime") as mock_datetime:
        mock_datetime.now.return_value = fixed_now
        mock_datetime.fromisoformat.side_effect = datetime.fromisoformat

        result = get_pipeline_metrics_history(minutes=5)
        assert result["count"] == 2
        assert [s["transactions_processed"] for s in result["history"]] == [20, 30]




