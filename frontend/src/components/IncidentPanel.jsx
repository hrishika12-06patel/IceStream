import { useMemo, useState } from "react";

import {
  AlertTriangle,
  CheckCircle2,
  CircleAlert,
  Search,
  ShieldAlert,
  X,
} from "lucide-react";

import { incidents } from "../data/incidentMock";


function IncidentSummaryCard({
  label,
  value,
  icon,
  variant,
}) {
  return (
    <div className={`incident-summary-card ${variant}`}>
      <div className="incident-summary-icon">
        {icon}
      </div>

      <div>
        <span>{label}</span>
        <strong>{value}</strong>
      </div>
    </div>
  );
}


function SeverityBadge({ severity }) {
  return (
    <span
      className={`incident-badge incident-severity-${severity}`}
    >
      {severity}
    </span>
  );
}


function StatusBadge({ status }) {
  return (
    <span
      className={`incident-badge incident-status-${status}`}
    >
      {status}
    </span>
  );
}


function IncidentPanel() {
  const [severityFilter, setSeverityFilter] =
    useState("all");

  const [statusFilter, setStatusFilter] =
    useState("all");

  const [searchTerm, setSearchTerm] =
    useState("");

  const [selectedIncident, setSelectedIncident] =
    useState(null);


  const incidentStats = useMemo(() => {
    return {
      open: incidents.filter(
        (incident) =>
          incident.status === "open"
      ).length,

      critical: incidents.filter(
        (incident) =>
          incident.severity === "critical"
      ).length,

      warning: incidents.filter(
        (incident) =>
          incident.severity === "warning"
      ).length,

      resolved: incidents.filter(
        (incident) =>
          incident.status === "resolved"
      ).length,
    };
  }, []);


  const filteredIncidents = useMemo(() => {
    return incidents.filter((incident) => {
      const matchesSeverity =
        severityFilter === "all" ||
        incident.severity === severityFilter;

      const matchesStatus =
        statusFilter === "all" ||
        incident.status === statusFilter;

      const searchValue =
        searchTerm.toLowerCase();

      const matchesSearch =
        incident.id
          .toLowerCase()
          .includes(searchValue) ||
        incident.orderId
          .toLowerCase()
          .includes(searchValue) ||
        incident.title
          .toLowerCase()
          .includes(searchValue) ||
        incident.field
          .toLowerCase()
          .includes(searchValue);

      return (
        matchesSeverity &&
        matchesStatus &&
        matchesSearch
      );
    });
  }, [
    severityFilter,
    statusFilter,
    searchTerm,
  ]);


  return (
    <section className="incident-section">

      {/* =========================
          HEADING
      ========================= */}

      <div className="incident-heading">

        <div>
          <div className="eyebrow">
            INCIDENT MONITORING
          </div>

          <h2>
            Data quality incidents
          </h2>

          <p>
            Investigate validation failures and
            pipeline-quality events.
          </p>
        </div>

      </div>


      {/* =========================
          SUMMARY
      ========================= */}

      <div className="incident-summary-grid">

        <IncidentSummaryCard
          label="Open incidents"
          value={incidentStats.open}
          icon={<CircleAlert size={18} />}
          variant="open"
        />

        <IncidentSummaryCard
          label="Critical"
          value={incidentStats.critical}
          icon={<ShieldAlert size={18} />}
          variant="critical"
        />

        <IncidentSummaryCard
          label="Warnings"
          value={incidentStats.warning}
          icon={<AlertTriangle size={18} />}
          variant="warning"
        />

        <IncidentSummaryCard
          label="Resolved"
          value={incidentStats.resolved}
          icon={<CheckCircle2 size={18} />}
          variant="resolved"
        />

      </div>


      {/* =========================
          INCIDENT TABLE
      ========================= */}

      <div className="dashboard-card incident-card">

        <div className="incident-toolbar">

          <div className="incident-search">

            <Search size={14} />

            <input
              type="text"
              placeholder="Search incidents..."
              value={searchTerm}
              onChange={(event) =>
                setSearchTerm(
                  event.target.value
                )
              }
            />

          </div>


          <select
            className="incident-filter"
            value={severityFilter}
            onChange={(event) =>
              setSeverityFilter(
                event.target.value
              )
            }
          >
            <option value="all">
              All severity
            </option>

            <option value="critical">
              Critical
            </option>

            <option value="warning">
              Warning
            </option>

            <option value="info">
              Info
            </option>
          </select>


          <select
            className="incident-filter"
            value={statusFilter}
            onChange={(event) =>
              setStatusFilter(
                event.target.value
              )
            }
          >
            <option value="all">
              All status
            </option>

            <option value="open">
              Open
            </option>

            <option value="investigating">
              Investigating
            </option>

            <option value="resolved">
              Resolved
            </option>
          </select>

        </div>


        <div className="incident-table">

          <div className="incident-row incident-head">

            <span>Incident</span>
            <span>Order</span>
            <span>Issue</span>
            <span>Stage</span>
            <span>Severity</span>
            <span>Status</span>
            <span>Detected</span>

          </div>


          {filteredIncidents.length === 0 ? (

            <div className="incident-empty">
              No incidents match the selected filters.
            </div>

          ) : (

            filteredIncidents.map(
              (incident) => (

                <button
                  type="button"
                  key={incident.id}
                  className="incident-row incident-data-row"
                  onClick={() =>
                    setSelectedIncident(
                      incident
                    )
                  }
                >

                  <strong>
                    {incident.id}
                  </strong>

                  <span>
                    {incident.orderId}
                  </span>

                  <span>
                    {incident.title}
                  </span>

                  <span>
                    {incident.stage}
                  </span>

                  <SeverityBadge
                    severity={
                      incident.severity
                    }
                  />

                  <StatusBadge
                    status={
                      incident.status
                    }
                  />

                  <small>
                    {incident.detectedAt}
                  </small>

                </button>

              )
            )

          )}

        </div>

      </div>


      {/* =========================
          INCIDENT DETAILS
      ========================= */}

      {selectedIncident && (

        <div className="dashboard-card incident-details">

          <div className="incident-details-header">

            <div>

              <div className="eyebrow">
                INCIDENT DETAILS
              </div>

              <h3>
                {selectedIncident.id}
              </h3>

            </div>


            <button
              type="button"
              className="incident-close"
              onClick={() =>
                setSelectedIncident(null)
              }
            >
              <X size={16} />
            </button>

          </div>


          <div className="incident-details-grid">

            <div>
              <span>Order ID</span>
              <strong>
                {selectedIncident.orderId}
              </strong>
            </div>

            <div>
              <span>Field</span>
              <strong>
                {selectedIncident.field}
              </strong>
            </div>

            <div>
              <span>Stage</span>
              <strong>
                {selectedIncident.stage}
              </strong>
            </div>

            <div>
              <span>Type</span>
              <strong>
                {selectedIncident.type}
              </strong>
            </div>

            <div>
              <span>Severity</span>

              <SeverityBadge
                severity={
                  selectedIncident.severity
                }
              />
            </div>

            <div>
              <span>Status</span>

              <StatusBadge
                status={
                  selectedIncident.status
                }
              />
            </div>

          </div>


          <div className="incident-description">

            <span>Description</span>

            <p>
              {selectedIncident.description}
            </p>

          </div>

        </div>

      )}

    </section>
  );
}


export default IncidentPanel;