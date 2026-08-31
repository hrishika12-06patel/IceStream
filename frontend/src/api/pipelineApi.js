const API_BASE_URL =
  import.meta.env.VITE_API_BASE_URL ||
  "http://127.0.0.1:8000";


async function apiRequest(endpoint) {
  const response = await fetch(
    `${API_BASE_URL}${endpoint}`,
    {
      headers: {
        Accept: "application/json",
      },
    }
  );

  if (!response.ok) {
    throw new Error(
      `API request failed: ${response.status}`
    );
  }

  return response.json();
}


/* =========================
   HEALTH
========================= */

export function getBackendHealth() {
  return apiRequest("/health");
}


/* =========================
   RAW API FUNCTIONS
========================= */

export function getPipelineStatusRaw() {
  return apiRequest(
    "/api/pipeline/status"
  );
}


export function getPipelineMetrics() {
  return apiRequest(
    "/api/pipeline/metrics"
  );
}


export function getDataQuality() {
  return apiRequest(
    "/api/data-quality"
  );
}


export function getIncidents() {
  return apiRequest(
    "/api/incidents"
  );
}


export function getLakehouseStatus() {
  return apiRequest(
    "/api/lakehouse"
  );
}


/* =========================
   NORMALISED PIPELINE DATA
========================= */

export async function getPipelineStatus() {
  const [
    status,
    metrics,
    quality,
    incidents,
    lakehouse,
  ] = await Promise.all([
    getPipelineStatusRaw(),
    getPipelineMetrics(),
    getDataQuality(),
    getIncidents(),
    getLakehouseStatus(),
  ]);


  return {
    overallStatus:
      status.overall_status,

    services: {
      kafka: {
        name: "Kafka",

        status:
          status.components?.kafka?.status ??
          "unknown",

        eventsPerSecond:
          metrics.records_per_second ?? 0,

        latency: 0,
      },


      flink: {
        name: "Flink",

        status:
          status.components?.flink?.status ??
          "unknown",

        processingRate:
          metrics.records_per_second ?? 0,

        latency: 0,
      },


      iceberg: {
        name: "Iceberg",

        status:
          status.components?.iceberg?.status ??
          lakehouse.status ??
          "unknown",

        recordsStored:
          lakehouse.record_count ?? 0,

        latency: 0,
      },
    },


    metrics: {
      eventsPerSecond:
        metrics.records_per_second ?? 0,

      processingRate:
        metrics.records_per_second ?? 0,

      pipelineLatency: 0,

      dataQuality:
        quality.quality_score ?? 0,

      recordsProcessed:
        metrics.transactions_processed ?? 0,

      validRecords:
        metrics.valid_records ?? 0,

      invalidRecords:
        metrics.invalid_records ?? 0,

      processingErrors:
        metrics.processing_errors ?? 0,
    },


    alerts:
      (incidents.incidents || []).map(
        (incident) => ({
          id: incident.id,

          type:
            incident.severity === "high"
              ? "warning"
              : incident.severity === "medium"
              ? "warning"
              : "info",

          title:
            `${incident.component} incident`,

          message:
            incident.message,

          time:
            incident.timestamp,

          status:
            incident.status,
        })
      ),


    lakehouse: {
      catalog:
        lakehouse.catalog,

      namespace:
        lakehouse.namespace,

      table:
        lakehouse.table,

      warehouse:
        lakehouse.warehouse,

      tableExists:
        lakehouse.table_exists,

      snapshotCount:
        lakehouse.snapshot_count,

      recordCount:
        lakehouse.record_count,

      status:
        lakehouse.status,
    },
  };
}