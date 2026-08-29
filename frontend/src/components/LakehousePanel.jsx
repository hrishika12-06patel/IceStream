import {
  CheckCircle2,
  Database,
  FileStack,
  HardDrive,
  History,
  Layers3,
  Table2,
} from "lucide-react";

import {
  lakehouseMetrics,
  recentSnapshots,
  storageHealth,
} from "../data/lakehouseMock";


function LakehouseMetricCard({
  icon,
  label,
  value,
  description,
}) {
  return (
    <div className="lakehouse-metric-card">

      <div className="lakehouse-metric-icon">
        {icon}
      </div>

      <div>
        <span>{label}</span>

        <strong>{value}</strong>

        <small>{description}</small>
      </div>

    </div>
  );
}


function LakehouseStatusBadge({
  status,
}) {
  return (
    <span
      className={`lakehouse-status lakehouse-status-${status}`}
    >
      {status}
    </span>
  );
}


function LakehousePanel() {
  return (
    <section className="lakehouse-section">

      {/* =========================
          HEADING
      ========================= */}

      <div className="lakehouse-heading">

        <div>

          <div className="eyebrow">
            LAKEHOUSE MONITORING
          </div>

          <h2>
            Apache Iceberg storage
          </h2>

          <p>
            Monitor table health, snapshots,
            storage activity and Iceberg metadata.
          </p>

        </div>


        <div className="lakehouse-main-status">

          <Database size={17} />

          <div>

            <span>
              Table status
            </span>

            <strong>
              {lakehouseMetrics.status}
            </strong>

          </div>

        </div>

      </div>


      {/* =========================
          SUMMARY METRICS
      ========================= */}

      <div className="lakehouse-summary-grid">

        <LakehouseMetricCard
          icon={<Table2 size={18} />}
          label="Records stored"
          value={
            lakehouseMetrics.recordsStored.toLocaleString()
          }
          description="Total table records"
        />

        <LakehouseMetricCard
          icon={<History size={18} />}
          label="Snapshots"
          value={
            lakehouseMetrics.snapshotCount
          }
          description="Available Iceberg snapshots"
        />

        <LakehouseMetricCard
          icon={<HardDrive size={18} />}
          label="Table size"
          value={
            lakehouseMetrics.tableSize
          }
          description="Current storage footprint"
        />

        <LakehouseMetricCard
          icon={<FileStack size={18} />}
          label="Last commit"
          value={
            lakehouseMetrics.lastCommit
          }
          description="Latest successful write"
        />

      </div>


      {/* =========================
          TABLE INFORMATION
      ========================= */}

      <div className="lakehouse-grid">

        <div className="dashboard-card lakehouse-info-card">

          <div className="card-header">

            <div>

              <h3>
                Iceberg table
              </h3>

              <span>
                Current table configuration
              </span>

            </div>

          </div>


          <div className="lakehouse-info-list">

            <div className="lakehouse-info-row">

              <span>
                Table
              </span>

              <strong>
                {lakehouseMetrics.tableName}
              </strong>

            </div>


            <div className="lakehouse-info-row">

              <span>
                Format
              </span>

              <strong>
                {lakehouseMetrics.format}
              </strong>

            </div>


            <div className="lakehouse-info-row">

              <span>
                Namespace
              </span>

              <strong>
                {lakehouseMetrics.namespace}
              </strong>

            </div>


            <div className="lakehouse-info-row">

              <span>
                Schema version
              </span>

              <strong>
                v{lakehouseMetrics.schemaVersion}
              </strong>

            </div>


            <div className="lakehouse-info-row">

              <span>
                Partitions
              </span>

              <strong>
                {lakehouseMetrics.partitionCount}
              </strong>

            </div>


            <div className="lakehouse-info-row">

              <span>
                Latest snapshot
              </span>

              <strong>
                {lakehouseMetrics.latestSnapshot}
              </strong>

            </div>

          </div>

        </div>


        {/* =========================
            STORAGE HEALTH
        ========================= */}

        <div className="dashboard-card lakehouse-health-card">

          <div className="card-header">

            <div>

              <h3>
                Storage health
              </h3>

              <span>
                Iceberg component status
              </span>

            </div>

          </div>


          <div className="lakehouse-health-list">

            <div className="lakehouse-health-row">

              <div>
                <CheckCircle2 size={15} />
                <span>Table health</span>
              </div>

              <LakehouseStatusBadge
                status={
                  storageHealth.tableHealth
                }
              />

            </div>


            <div className="lakehouse-health-row">

              <div>
                <Layers3 size={15} />
                <span>Schema</span>
              </div>

              <LakehouseStatusBadge
                status={
                  storageHealth.schemaStatus
                }
              />

            </div>


            <div className="lakehouse-health-row">

              <div>
                <Database size={15} />
                <span>Catalog</span>
              </div>

              <LakehouseStatusBadge
                status={
                  storageHealth.catalogStatus
                }
              />

            </div>


            <div className="lakehouse-health-row">

              <div>
                <HardDrive size={15} />
                <span>Writes</span>
              </div>

              <LakehouseStatusBadge
                status={
                  storageHealth.writeStatus
                }
              />

            </div>

          </div>

        </div>

      </div>


      {/* =========================
          SNAPSHOT HISTORY
      ========================= */}

      <div className="dashboard-card snapshot-card">

        <div className="card-header">

          <div>

            <h3>
              Recent snapshots
            </h3>

            <span>
              Latest Iceberg table commits
            </span>

          </div>

          <div className="snapshot-total">
            {recentSnapshots.length} recent
          </div>

        </div>


        <div className="snapshot-table">

          <div className="snapshot-row snapshot-head">

            <span>
              Snapshot
            </span>

            <span>
              Operation
            </span>

            <span>
              Records added
            </span>

            <span>
              Total records
            </span>

            <span>
              Status
            </span>

            <span>
              Created
            </span>

          </div>


          {recentSnapshots.map(
            (snapshot) => (

              <div
                className="snapshot-row"
                key={snapshot.id}
              >

                <strong>
                  {snapshot.id}
                </strong>

                <span>
                  {snapshot.operation}
                </span>

                <span>
                  +
                  {snapshot.recordsAdded.toLocaleString()}
                </span>

                <span>
                  {snapshot.totalRecords.toLocaleString()}
                </span>

                <LakehouseStatusBadge
                  status={
                    snapshot.status
                  }
                />

                <small>
                  {snapshot.timestamp}
                </small>

              </div>

            )
          )}

        </div>

      </div>

    </section>
  );
}


export default LakehousePanel;