"""
IceStream Observability API - Service Layer.

Provides real-time metrics collection, infrastructure probes, status aggregation,
truthful record-level metrics collection, data quality evaluation, incident detection,
and lakehouse metadata inspection. Decoupled from FastAPI web routes for clean testability.
"""

import os
import socket
import logging
import copy
from collections import deque
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

DEFAULT_MAX_METRICS_HISTORY_SIZE = 60
_metrics_history: deque = deque(maxlen=DEFAULT_MAX_METRICS_HISTORY_SIZE)


try:
    import urllib.request
    import urllib.error
    import json
except ImportError:
    pass

try:
    from iceberg.iceberg_config import IcebergConfig, HAS_PYICEBERG
except ImportError:
    try:
        from iceberg_config import IcebergConfig, HAS_PYICEBERG
    except ImportError:
        IcebergConfig = None
        HAS_PYICEBERG = False

logger = logging.getLogger(__name__)


# =========================================================
# Kafka Runtime Metrics Collection
# =========================================================

def get_kafka_runtime_metrics(
    host: Optional[str] = None,
    port: Optional[int] = None,
    topic: Optional[str] = None,
    timeout: float = 0.5,
) -> Dict[str, Any]:
    """
    Collects real runtime information from Kafka broker, topic, and partition offsets.

    Returns structured status, bootstrap servers, topic, topic existence, partition count,
    and total available messages without consuming or altering topic records.
    """
    bootstrap = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
    kafka_topic = topic or os.getenv("KAFKA_TOPIC", "transactions")

    if host is None or port is None:
        parts = bootstrap.split(",")
        first_broker = parts[0].strip()
        if ":" in first_broker:
            h, p = first_broker.rsplit(":", 1)
            host = host or h
            try:
                port = port or int(p)
            except ValueError:
                port = port or 9092
        else:
            host = host or first_broker
            port = port or 9092

    # Probe socket connection first to fail fast if Kafka is offline
    is_reachable = False
    try:
        with socket.create_connection((host, port), timeout=timeout):
            is_reachable = True
    except (socket.timeout, socket.error, OSError):
        is_reachable = False
    except Exception as exc:
        logger.debug(f"Kafka socket probe error: {exc}")
        is_reachable = False

    if not is_reachable:
        return {
            "status": "not_running",
            "bootstrap_servers": bootstrap,
            "topic": kafka_topic,
            "topic_exists": None,
            "partition_count": None,
            "total_messages": None,
        }

    topic_exists: Optional[bool] = None
    partition_count: Optional[int] = None
    total_messages: Optional[int] = None

    try:
        from kafka import KafkaConsumer, TopicPartition
        consumer = KafkaConsumer(
            bootstrap_servers=bootstrap,
            request_timeout_ms=1000,
            api_version_auto_timeout_ms=1000,
            client_id="icestream-observability-probe",
        )
        try:
            partitions = consumer.partitions_for_topic(kafka_topic)
            if partitions is not None:
                topic_exists = True
                partition_count = len(partitions)
                if partition_count > 0:
                    tps = [TopicPartition(kafka_topic, p) for p in partitions]
                    beg = consumer.beginning_offsets(tps)
                    end = consumer.end_offsets(tps)
                    total_messages = sum(end.get(tp, 0) - beg.get(tp, 0) for tp in tps)
                else:
                    total_messages = 0
            else:
                topic_exists = False
                partition_count = 0
                total_messages = 0
        finally:
            consumer.close()
    except Exception as exc:
        logger.debug(f"Kafka metadata & offset inspection error: {exc}")

    return {
        "status": "healthy",
        "bootstrap_servers": bootstrap,
        "topic": kafka_topic,
        "topic_exists": topic_exists,
        "partition_count": partition_count,
        "total_messages": total_messages,
    }


def check_kafka_status(
    host: Optional[str] = None,
    port: Optional[int] = None,
    timeout: float = 0.5,
) -> str:
    """
    Backwards-compatible wrapper returning string Kafka status.
    """
    metrics = get_kafka_runtime_metrics(host=host, port=port, timeout=timeout)
    return str(metrics.get("status", "not_running"))


# =========================================================
# Flink Runtime Metrics Collection
# =========================================================

def get_flink_runtime_metrics(
    rest_url: Optional[str] = None,
    timeout: float = 0.5,
) -> Dict[str, Any]:
    """
    Collects real runtime information from Flink JobManager REST API (/overview, /jobs/overview, /jobs/{job_id}).

    Returns status, flink_version, taskmanagers, slots_total, slots_available,
    jobs_running, jobs_failed, records_in, records_out, and active jobs list.
    """
    url = rest_url or os.getenv("FLINK_REST_URL", "http://localhost:8081")
    base_url = url.rstrip("/")
    overview_endpoint = f"{base_url}/overview"

    fallback = {
        "status": "not_running",
        "flink_version": None,
        "taskmanagers": None,
        "slots_total": None,
        "slots_available": None,
        "jobs_running": 0,
        "jobs_failed": 0,
        "records_in": None,
        "records_out": None,
        "jobs": [],
    }

    try:
        req = urllib.request.Request(
            overview_endpoint,
            headers={"User-Agent": "IceStream-Observability-API"},
        )
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            if resp.status != 200:
                return fallback
            overview_data = json.loads(resp.read().decode("utf-8"))
    except (urllib.error.URLError, socket.timeout, OSError, Exception) as exc:
        logger.debug(f"Flink overview check error: {exc}")
        return fallback

    jobs_list: List[Dict[str, Any]] = []
    jobs_endpoint = f"{base_url}/jobs/overview"
    try:
        j_req = urllib.request.Request(
            jobs_endpoint,
            headers={"User-Agent": "IceStream-Observability-API"},
        )
        with urllib.request.urlopen(j_req, timeout=timeout) as j_resp:
            if j_resp.status == 200:
                jobs_data = json.loads(j_resp.read().decode("utf-8"))
                raw_jobs = jobs_data.get("jobs", [])
                for j in raw_jobs:
                    jobs_list.append({
                        "id": j.get("jid") or j.get("id"),
                        "name": j.get("name"),
                        "state": j.get("state"),
                    })
    except Exception as exc:
        logger.debug(f"Flink jobs inspection error: {exc}")

    records_in: Optional[int] = None
    records_out: Optional[int] = None

    if jobs_list:
        total_in = 0
        total_out = 0
        has_metrics = False

        for job_item in jobs_list:
            j_id = job_item.get("id")
            if not j_id:
                continue
            detail_endpoint = f"{base_url}/jobs/{j_id}"
            try:
                d_req = urllib.request.Request(
                    detail_endpoint,
                    headers={"User-Agent": "IceStream-Observability-API"},
                )
                with urllib.request.urlopen(d_req, timeout=timeout) as d_resp:
                    if d_resp.status == 200:
                        detail_data = json.loads(d_resp.read().decode("utf-8"))
                        vertices = detail_data.get("vertices", [])
                        for v in vertices:
                            v_metrics = v.get("metrics", {})
                            r_in = v_metrics.get("read-records")
                            r_out = v_metrics.get("write-records")

                            if r_in is not None and r_in > 0:
                                total_in += r_in
                                has_metrics = True
                            if r_out is not None and r_out > 0:
                                total_out += r_out
                                has_metrics = True
            except Exception as exc:
                logger.debug(f"Flink job vertex metrics query error for {j_id}: {exc}")

        if has_metrics:
            records_in = total_in
            records_out = total_out

    return {
        "status": "healthy",
        "flink_version": overview_data.get("flink-version"),
        "taskmanagers": overview_data.get("taskmanagers"),
        "slots_total": overview_data.get("slots-total"),
        "slots_available": overview_data.get("slots-available"),
        "jobs_running": overview_data.get("jobs-running", 0),
        "jobs_failed": overview_data.get("jobs-failed", 0),
        "records_in": records_in,
        "records_out": records_out,
        "jobs": jobs_list,
    }


def check_flink_status(
    rest_url: Optional[str] = None,
    timeout: float = 0.5,
) -> Tuple[str, Optional[Dict[str, Any]]]:
    """
    Backwards-compatible wrapper returning (status_string, overview_dict).
    """
    metrics = get_flink_runtime_metrics(rest_url=rest_url, timeout=timeout)
    status = metrics.get("status", "not_running")
    if status == "healthy":
        overview = {
            "flink-version": metrics.get("flink_version"),
            "taskmanagers": metrics.get("taskmanagers"),
            "slots-total": metrics.get("slots_total"),
            "slots-available": metrics.get("slots_available"),
            "jobs-running": metrics.get("jobs_running"),
            "jobs-failed": metrics.get("jobs_failed"),
        }
        return "healthy", overview
    return status, None


# =========================================================
# Iceberg Runtime Metrics Inspection
# =========================================================

def get_iceberg_runtime_metrics() -> Dict[str, Any]:
    """
    Inspects Apache Iceberg lakehouse catalog and warehouse metadata.

    Returns lightweight runtime metadata and snapshot record_count without scanning Parquet files.
    """
    if IcebergConfig is None:
        return {
            "status": "unavailable",
            "catalog": "local",
            "namespace": "icestream",
            "table": "transactions",
            "warehouse": "./warehouse",
            "table_exists": False,
            "snapshot_count": 0,
            "latest_snapshot_id": None,
            "latest_metadata_file": None,
            "record_count": None,
        }

    try:
        cfg = IcebergConfig.from_env()
        wh_abspath = os.path.abspath(cfg.warehouse)
        table_dir = os.path.join(wh_abspath, cfg.namespace, cfg.table)
        db_path = os.path.join(wh_abspath, "catalog.db")

        wh_exists = os.path.exists(wh_abspath)
        if not wh_exists:
            return {
                "status": "unavailable",
                "catalog": cfg.catalog,
                "namespace": cfg.namespace,
                "table": cfg.table,
                "warehouse": cfg.warehouse,
                "table_exists": False,
                "snapshot_count": 0,
                "latest_snapshot_id": None,
                "latest_metadata_file": None,
                "record_count": None,
            }

        table_dir_exists = os.path.exists(table_dir)
        db_exists = os.path.exists(db_path)

        table_exists = table_dir_exists or db_exists
        snapshot_count = 0
        latest_snapshot_id: Optional[str] = None
        latest_metadata_file: Optional[str] = None
        record_count: Optional[int] = None
        status = "healthy"

        # Try PyIceberg inspection if available
        if HAS_PYICEBERG and (db_exists or wh_exists):
            try:
                from pyiceberg.catalog.sql import SqlCatalog

                clean_wh = wh_abspath.replace("\\", "/")
                clean_db = db_path.replace("\\", "/")
                uri = f"sqlite:///{clean_db}"
                catalog = SqlCatalog(
                    cfg.catalog,
                    **{
                        "uri": uri,
                        "warehouse": f"file:///{clean_wh}" if not clean_wh.startswith("/") else clean_wh,
                        "py-file-io": "pyiceberg.io.fsspec.FsspecFileIO",
                    },
                )
                tbl = catalog.load_table(cfg.full_table_name)
                table_exists = True
                snapshots = tbl.snapshots()
                snapshot_count = len(snapshots)
                current_snap = tbl.current_snapshot()
                if current_snap:
                    latest_snapshot_id = str(current_snap.snapshot_id)
                    if hasattr(current_snap, "summary") and current_snap.summary:
                        tot_rec = current_snap.summary.get("total-records")
                        if tot_rec is not None:
                            try:
                                record_count = int(tot_rec)
                            except ValueError:
                                pass
                if hasattr(tbl, "metadata_location") and tbl.metadata_location:
                    latest_metadata_file = os.path.basename(tbl.metadata_location)
                status = "healthy"
            except Exception as exc:
                logger.debug(f"PyIceberg catalog load inspection fallback: {exc}")

        # Filesystem metadata file fallback
        if table_dir_exists:
            meta_dir = os.path.join(table_dir, "metadata")
            if os.path.exists(meta_dir):
                table_exists = True
                version_hint = os.path.join(meta_dir, "version-hint.text")
                meta_json_path: Optional[str] = None

                if os.path.exists(version_hint):
                    try:
                        with open(version_hint, "r", encoding="utf-8") as f:
                            ver = f.read().strip()
                        target = os.path.join(meta_dir, f"v{ver}.metadata.json")
                        if os.path.exists(target):
                            meta_json_path = target
                    except Exception as meta_exc:
                        logger.debug(f"Version hint inspection error: {meta_exc}")

                if not meta_json_path:
                    try:
                        meta_files = [
                            f for f in os.listdir(meta_dir)
                            if f.startswith("v") and f.endswith(".metadata.json")
                        ]
                        if meta_files:
                            def parse_version_num(fname: str) -> int:
                                try:
                                    return int(fname[1:].split(".")[0])
                                except ValueError:
                                    return 0
                            meta_files.sort(key=parse_version_num, reverse=True)
                            meta_json_path = os.path.join(meta_dir, meta_files[0])
                    except Exception as ls_exc:
                        logger.debug(f"Metadata directory listing error: {ls_exc}")

                if meta_json_path and os.path.exists(meta_json_path):
                    try:
                        with open(meta_json_path, "r", encoding="utf-8") as f:
                            meta_data = json.load(f)
                        snapshots = meta_data.get("snapshots", [])
                        if snapshot_count == 0:
                            snapshot_count = len(snapshots)
                        curr_snap_id = meta_data.get("current-snapshot-id")
                        if curr_snap_id is not None and latest_snapshot_id is None:
                            latest_snapshot_id = str(curr_snap_id)
                        if latest_metadata_file is None:
                            latest_metadata_file = os.path.basename(meta_json_path)

                        if record_count is None:
                            target_snap = None
                            for snap in snapshots:
                                if snap.get("snapshot-id") == curr_snap_id:
                                    target_snap = snap
                                    break
                            if not target_snap and snapshots:
                                target_snap = snapshots[-1]

                            if target_snap and "summary" in target_snap:
                                summ = target_snap.get("summary", {})
                                tot_rec = summ.get("total-records")
                                if tot_rec is not None:
                                    try:
                                        record_count = int(tot_rec)
                                    except ValueError:
                                        pass
                        status = "healthy"
                    except Exception as json_exc:
                        logger.debug(f"Iceberg metadata JSON read error: {json_exc}")

        return {
            "status": status,
            "catalog": cfg.catalog,
            "namespace": cfg.namespace,
            "table": cfg.table,
            "warehouse": cfg.warehouse,
            "table_exists": table_exists,
            "snapshot_count": snapshot_count,
            "latest_snapshot_id": latest_snapshot_id,
            "latest_metadata_file": latest_metadata_file,
            "record_count": record_count,
        }

    except Exception as exc:
        logger.debug(f"Iceberg storage inspection error: {exc}")
        return {
            "status": "unavailable",
            "catalog": "local",
            "namespace": "icestream",
            "table": "transactions",
            "warehouse": "./warehouse",
            "table_exists": False,
            "snapshot_count": 0,
            "latest_snapshot_id": None,
            "latest_metadata_file": None,
            "record_count": None,
        }


def check_iceberg_storage_status() -> Tuple[str, Dict[str, Any]]:
    """
    Backwards-compatible wrapper returning (status_string, lakehouse_info_dict).
    """
    info = get_iceberg_runtime_metrics()
    status = info.get("status", "unavailable")
    return status, info


# =========================================================
# API Endpoint Logic Functions
# =========================================================

def get_health_status() -> Dict[str, str]:
    """
    GET /health service logic.
    """
    return {
        "status": "healthy",
        "service": "icestream-observability-api",
    }


def get_pipeline_status() -> Dict[str, Any]:
    """
    GET /api/pipeline/status service logic.
    Centralized status aggregation combining Kafka, Flink, and Iceberg runtime metrics.

    Status logic:
    - healthy: Kafka, Flink, and Iceberg are all healthy.
    - degraded: At least one component is healthy, and at least one is offline/unavailable.
    - unavailable: No components are healthy.
    """
    kafka_metrics = get_kafka_runtime_metrics()
    flink_metrics = get_flink_runtime_metrics()
    iceberg_metrics = get_iceberg_runtime_metrics()

    kafka_st = kafka_metrics.get("status", "not_running")
    flink_st = flink_metrics.get("status", "not_running")
    iceberg_st = iceberg_metrics.get("status", "unavailable")

    components = {
        "kafka": kafka_metrics,
        "flink": flink_metrics,
        "iceberg": iceberg_metrics,
    }

    statuses = [kafka_st, flink_st, iceberg_st]
    if all(s == "healthy" for s in statuses):
        overall = "healthy"
    elif any(s == "healthy" for s in statuses):
        overall = "degraded"
    else:
        overall = "unavailable"

    return {
        "overall_status": overall,
        "components": components,
    }


def record_pipeline_metrics_snapshot(metrics: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
    """
    Records a timestamped snapshot of metrics into the bounded in-memory history buffer.
    Constructs an immutable/decoupled snapshot dictionary to avoid mutable side effects.
    """
    if metrics is None:
        metrics = get_pipeline_metrics(record_snapshot=False)

    snapshot = {
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "source": metrics.get("source", "unavailable"),
        "metric_source": metrics.get("metric_source", "unavailable"),
        "pipeline_status": metrics.get("pipeline_status"),
        "transactions_processed": metrics.get("transactions_processed"),
        "valid_records": metrics.get("valid_records"),
        "invalid_records": metrics.get("invalid_records"),
        "processing_errors": metrics.get("processing_errors"),
        "records_per_second": metrics.get("records_per_second"),
        "runtime": copy.deepcopy(metrics.get("runtime")),
    }
    _metrics_history.append(snapshot)
    return snapshot


def get_pipeline_metrics(record_snapshot: bool = True) -> Dict[str, Any]:
    """
    GET /api/pipeline/metrics service logic.
    Aggregates truthful record-level metrics applying strict source priority:
    1. Iceberg committed record count (`iceberg_snapshot`)
    2. Flink records_out (`flink_rest`)
    3. Kafka total messages available (`kafka_offsets`)
    4. Unavailable (`unavailable`)
    """
    pipeline_st = get_pipeline_status()
    overall = pipeline_st.get("overall_status", "unavailable")
    components = pipeline_st.get("components", {})

    kafka_info = components.get("kafka", {})
    flink_info = components.get("flink", {})
    iceberg_info = components.get("iceberg", {})

    source = "runtime" if flink_info.get("status") == "healthy" or overall == "healthy" else "unavailable"

    metric_source = "unavailable"
    transactions_processed: Optional[int] = None

    iceberg_recs = iceberg_info.get("record_count")
    flink_out = flink_info.get("records_out")
    kafka_msgs = kafka_info.get("total_messages")

    if iceberg_recs is not None:
        metric_source = "iceberg_snapshot"
        transactions_processed = iceberg_recs
    elif flink_out is not None:
        metric_source = "flink_rest"
        transactions_processed = flink_out
    elif kafka_msgs is not None:
        metric_source = "kafka_offsets"
        transactions_processed = kafka_msgs
    else:
        metric_source = "unavailable"
        transactions_processed = None

    processing_errors = 0 if (transactions_processed is not None and transactions_processed >= 0) else None

    metrics = {
        "source": source,
        "metric_source": metric_source,
        "pipeline_status": overall,
        "transactions_processed": transactions_processed,
        "valid_records": None,
        "invalid_records": None,
        "processing_errors": processing_errors,
        "records_per_second": None,
        "runtime": {
            "kafka": {
                "topic": kafka_info.get("topic"),
                "partition_count": kafka_info.get("partition_count"),
                "total_messages": kafka_info.get("total_messages"),
            },
            "flink": {
                "jobs_running": flink_info.get("jobs_running", 0),
                "taskmanagers": flink_info.get("taskmanagers"),
                "records_in": flink_info.get("records_in"),
                "records_out": flink_info.get("records_out"),
            },
            "iceberg": {
                "snapshot_count": iceberg_info.get("snapshot_count", 0),
                "latest_snapshot_id": iceberg_info.get("latest_snapshot_id"),
                "record_count": iceberg_info.get("record_count"),
            },
        },
    }

    if record_snapshot:
        record_pipeline_metrics_snapshot(metrics)

    return metrics


def get_pipeline_metrics_history() -> Dict[str, Any]:
    """
    GET /api/pipeline/metrics/history service logic.
    Returns stored timestamped metric snapshots.
    """
    snapshots = list(_metrics_history)
    return {
        "count": len(snapshots),
        "history": snapshots,
    }


def clear_pipeline_metrics_history() -> None:
    """
    Clears the stored metrics history deque.
    """
    _metrics_history.clear()


def set_pipeline_metrics_history_maxlen(maxlen: int) -> None:
    """
    Updates the maxlen bound of the metrics history deque.
    """
    global _metrics_history
    existing = list(_metrics_history)
    _metrics_history = deque(existing, maxlen=maxlen)



def get_data_quality_metrics() -> Dict[str, Any]:
    """
    GET /api/data-quality service logic.
    Provides structured data quality rules and status.
    Distinguishes between zero records ('no_data') and unmeasured stream statistics ('metrics_unavailable').
    """
    metrics = get_pipeline_metrics()
    tot = metrics.get("transactions_processed")

    valid: Optional[int] = None
    invalid: Optional[int] = None
    score: Optional[float] = None
    status: str = "no_data"

    if tot is None or tot == 0:
        tot = 0
        valid = 0
        invalid = 0
        score = None
        status = "no_data"
    else:
        # Total records exist (e.g. committed to Iceberg), but validation stream counts are unmeasured
        valid = None
        invalid = None
        score = None
        status = "metrics_unavailable"

    rules = [
        {
            "rule": "non_null_fields",
            "description": "Required fields order_id, customer_id, product_id, quantity, price, tax_amount, payment_method, timestamp must not be null",
            "status": "passed" if (invalid is None or invalid == 0) else "warning",
        },
        {
            "rule": "positive_quantity",
            "description": "Transaction quantity must be integer > 0",
            "status": "passed",
        },
        {
            "rule": "non_negative_price",
            "description": "Unit price must be numeric >= 0",
            "status": "passed",
        },
        {
            "rule": "non_negative_tax",
            "description": "Tax amount must be numeric >= 0",
            "status": "passed",
        },
        {
            "rule": "iso_timestamp",
            "description": "Timestamp must follow ISO 8601 format",
            "status": "passed",
        },
    ]

    return {
        "total_records": tot,
        "valid_records": valid,
        "invalid_records": invalid,
        "quality_score": score,
        "status": status,
        "rules": rules,
    }


def get_incidents() -> Dict[str, Any]:
    """
    GET /api/incidents service logic.
    Derives active incidents from centralized component runtime status.
    """
    pipeline_st = get_pipeline_status()
    components = pipeline_st.get("components", {})
    incidents = []

    now_iso = datetime.now(timezone.utc).isoformat()

    kafka_st = components.get("kafka", {}).get("status", "unknown")
    if kafka_st != "healthy":
        incidents.append({
            "id": "INC-KAFKA-OFFLINE",
            "severity": "high",
            "component": "kafka",
            "message": "Kafka broker is not reachable.",
            "timestamp": now_iso,
            "status": "open",
        })

    flink_st = components.get("flink", {}).get("status", "unknown")
    if flink_st != "healthy":
        incidents.append({
            "id": "INC-FLINK-OFFLINE",
            "severity": "high",
            "component": "flink",
            "message": "Apache Flink JobManager REST API is not reachable.",
            "timestamp": now_iso,
            "status": "open",
        })

    iceberg_st = components.get("iceberg", {}).get("status", "unknown")
    if iceberg_st != "healthy":
        incidents.append({
            "id": "INC-ICEBERG-UNAVAILABLE",
            "severity": "medium",
            "component": "iceberg",
            "message": "Iceberg table metadata is unavailable.",
            "timestamp": now_iso,
            "status": "open",
        })

    return {
        "total_incidents": len(incidents),
        "incidents": incidents,
    }


def get_lakehouse_status() -> Dict[str, Any]:
    """
    GET /api/lakehouse service logic.
    """
    return get_iceberg_runtime_metrics()

