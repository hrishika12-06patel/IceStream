import {
  AlertTriangle,
  CheckCircle2,
  CircleAlert,
  Copy,
  CreditCard,
  FileWarning,
  ShieldCheck,
} from "lucide-react";

import {
  dataQualityMetrics,
  issueCounts,
  recentQualityIssues,
} from "../data/dataQualityMock";


function QualityMetricCard({
  label,
  value,
  description,
}) {
  return (
    <div className="quality-metric-card">

      <div className="quality-metric-top">

        <span>
          {label}
        </span>

        <ShieldCheck size={15} />

      </div>

      <strong>
        {value}%
      </strong>

      <div className="quality-progress">

        <div
          style={{
            width: `${value}%`,
          }}
        ></div>

      </div>

      <small>
        {description}
      </small>

    </div>
  );
}


function IssueCountCard({
  icon,
  label,
  count,
}) {
  return (
    <div className="quality-issue-card">

      <div className="quality-issue-icon">
        {icon}
      </div>

      <div>

        <span>
          {label}
        </span>

        <strong>
          {count}
        </strong>

      </div>

    </div>
  );
}


function SeverityBadge({
  severity,
}) {
  return (
    <span
      className={`severity-badge severity-${severity}`}
    >
      {severity}
    </span>
  );
}


function DataQualityPanel() {

  const overallHealthy =
    dataQualityMetrics.overallScore >= 95;


  return (
    <section className="data-quality-section">

      {/* =========================
          SECTION HEADER
      ========================= */}

      <div className="data-quality-heading">

        <div>

          <div className="eyebrow">
            QUALITY MONITORING
          </div>

          <h2>
            Data quality overview
          </h2>

          <p>
            Monitor validation health and recent
            transaction-quality issues.
          </p>

        </div>


        <div
          className={`quality-overall-status ${
            overallHealthy
              ? "quality-healthy"
              : "quality-warning"
          }`}
        >

          <CheckCircle2 size={16} />

          <div>

            <span>
              Overall quality
            </span>

            <strong>
              {dataQualityMetrics.overallScore}%
            </strong>

          </div>

        </div>

      </div>


      {/* =========================
          QUALITY METRICS
      ========================= */}

      <div className="quality-metrics-grid">

        <QualityMetricCard
          label="Completeness"
          value={
            dataQualityMetrics.completeness
          }
          description="Required values present"
        />

        <QualityMetricCard
          label="Validity"
          value={
            dataQualityMetrics.validity
          }
          description="Values passing validation"
        />

        <QualityMetricCard
          label="Uniqueness"
          value={
            dataQualityMetrics.uniqueness
          }
          description="Unique order identifiers"
        />

        <QualityMetricCard
          label="Schema consistency"
          value={
            dataQualityMetrics.schemaConsistency
          }
          description="Records matching expected schema"
        />

      </div>


      {/* =========================
          ISSUE COUNTERS
      ========================= */}

      <div className="dashboard-card quality-issues-card">

        <div className="card-header">

          <div>

            <h3>
              Detected quality issues
            </h3>

            <span>
              Current validation issue counts
            </span>

          </div>

        </div>


        <div className="quality-issue-grid">

          <IssueCountCard
            icon={
              <CircleAlert size={17} />
            }
            label="NULL values"
            count={
              issueCounts.nullValues
            }
          />

          <IssueCountCard
            icon={
              <CreditCard size={17} />
            }
            label="Invalid payments"
            count={
              issueCounts.invalidPayments
            }
          />

          <IssueCountCard
            icon={
              <FileWarning size={17} />
            }
            label="Schema violations"
            count={
              issueCounts.schemaViolations
            }
          />

          <IssueCountCard
            icon={
              <Copy size={17} />
            }
            label="Duplicate orders"
            count={
              issueCounts.duplicateOrders
            }
          />

        </div>

      </div>


      {/* =========================
          RECENT QUALITY ISSUES
      ========================= */}

      <div className="dashboard-card quality-table-card">

        <div className="card-header">

          <div>

            <h3>
              Recent data quality issues
            </h3>

            <span>
              Latest transactions failing quality checks
            </span>

          </div>


          <div className="quality-table-count">

            <AlertTriangle size={14} />

            {recentQualityIssues.length} issues

          </div>

        </div>


        <div className="quality-table">

          <div className="quality-table-row quality-table-head">

            <span>
              Order ID
            </span>

            <span>
              Issue
            </span>

            <span>
              Field
            </span>

            <span>
              Severity
            </span>

            <span>
              Time
            </span>

          </div>


          {recentQualityIssues.map(
            (issue) => (

              <div
                className="quality-table-row"
                key={issue.id}
              >

                <strong>
                  {issue.orderId}
                </strong>

                <span>
                  {issue.issue}
                </span>

                <code>
                  {issue.field}
                </code>

                <SeverityBadge
                  severity={
                    issue.severity
                  }
                />

                <small>
                  {issue.time}
                </small>

              </div>

            )
          )}

        </div>

      </div>

    </section>
  );
}


export default DataQualityPanel;