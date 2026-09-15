import {
  Activity,
  AlertTriangle,
  ArrowDown,
  ArrowRight,
  ArrowUp,
  RefreshCw,
} from "lucide-react";


function isValidNumber(value) {
  return (
    typeof value === "number" &&
    Number.isFinite(value)
  );
}


function formatValue(value) {
  if (!isValidNumber(value)) {
    return "—";
  }

  return value.toLocaleString();
}

function parseTimestamp(timestamp) {
  if (!timestamp) {
    return null;
  }

  const date =
    new Date(timestamp);

  if (
    Number.isNaN(
      date.getTime()
    )
  ) {
    return null;
  }

  return date;
}

function formatSnapshotTime(timestamp) {
  const date =
    parseTimestamp(timestamp);

  if (!date) {
    return "—";
  }

  return date.toLocaleTimeString(
    [],
    {
      hour: "2-digit",
      minute: "2-digit",
      second: "2-digit",
    }
  );
}

function formatTimeDifference(
  currentTimestamp,
  previousTimestamp
) {
  const currentDate =
    parseTimestamp(
      currentTimestamp
    );

  const previousDate =
    parseTimestamp(
      previousTimestamp
    );


  if (
    !currentDate ||
    !previousDate
  ) {
    return "—";
  }


  const differenceMs =
    Math.abs(
      currentDate.getTime() -
      previousDate.getTime()
    );


  const differenceSeconds =
    Math.floor(
      differenceMs / 1000
    );


  if (differenceSeconds < 60) {
    return `${differenceSeconds} sec apart`;
  }


  const differenceMinutes =
    Math.floor(
      differenceSeconds / 60
    );


  if (differenceMinutes < 60) {
    return `${differenceMinutes} min apart`;
  }


  const differenceHours =
    Math.floor(
      differenceMinutes / 60
    );


  return `${differenceHours} hr apart`;
}

function calculatePercentageChange(
  current,
  previous
) {
  if (
    !isValidNumber(current) ||
    !isValidNumber(previous)
  ) {
    return null;
  }

  if (previous === 0) {
    return null;
  }

  return (
    ((current - previous) /
      previous) *
    100
  );
}


function getAbsoluteChange(
  current,
  previous
) {
  if (
    !isValidNumber(current) ||
    !isValidNumber(previous)
  ) {
    return null;
  }

  return current - previous;
}


function ComparisonIndicator({
  current,
  previous,
  type = "percentage",
  reverseMeaning = false,
}) {

  const change =
    type === "absolute"
      ? getAbsoluteChange(
          current,
          previous
        )
      : calculatePercentageChange(
          current,
          previous
        );


  if (change === null) {
    return (
      <span className="comparison-change comparison-neutral">
        <ArrowRight size={13} />
        —
      </span>
    );
  }


  if (change > 0) {
    return (
      <span
        className={
          reverseMeaning
            ? "comparison-change comparison-down"
            : "comparison-change comparison-up"
        }
      >
        <ArrowUp size={13} />

        {type === "percentage"
          ? `${change.toFixed(1)}%`
          : `+${change.toLocaleString()}`}
      </span>
    );
  }

  if (change < 0) {
    return (
      <span
        className={
          reverseMeaning
            ? "comparison-change comparison-up"
            : "comparison-change comparison-down"
        }
      >
        <ArrowDown size={13} />

        {type === "percentage"
          ? `${Math.abs(change).toFixed(1)}%`
          : change.toLocaleString()}
      </span>
    );
  }

  return (
    <span className="comparison-change comparison-neutral">
      <ArrowRight size={13} />
      No change
    </span>
  );
}


function ComparisonMetric({
  label,
  current,
  previous,
  type = "percentage",
  reverseMeaning = false,
}) {
  return (
    <div className="comparison-metric">

      <span className="comparison-metric-label">
        {label}
      </span>


      <div className="comparison-current-row">

        <strong className="comparison-current-value">
          {formatValue(current)}
        </strong>


        <ComparisonIndicator
          current={current}
          previous={previous}
          type={type}
          reverseMeaning={
            reverseMeaning
          }
        />

      </div>


      <span className="comparison-previous-value">
        Previous: {formatValue(previous)}
      </span>

    </div>
  );
}


function MetricsComparisonCard({
  history,
  loading,
  error,
  onRefresh,
}) {

  if (loading) {
    return (
      <section className="dashboard-card comparison-card">

        <div className="comparison-state">

          <Activity size={20} />

          <span>
            Loading comparison...
          </span>

        </div>

      </section>
    );
  }


  if (error) {
    return (
      <section className="dashboard-card comparison-card">

        <div className="comparison-state comparison-state-error">

          <AlertTriangle size={20} />

          <strong>
            Unable to load comparison data
          </strong>

          <button
            type="button"
            className="comparison-refresh-button"
            onClick={onRefresh}
          >
            <RefreshCw size={13} />
            Retry
          </button>

        </div>

      </section>
    );
  }


  if (
    !Array.isArray(history) ||
    history.length < 2
  ) {
    return (
      <section className="dashboard-card comparison-card">

        <div className="comparison-card-header">

          <div>
            <h3>
              Pipeline Metrics Comparison
            </h3>

            <span>
              Latest snapshot vs previous snapshot
            </span>
          </div>


          <button
            type="button"
            className="comparison-refresh-button"
            onClick={onRefresh}
          >
            <RefreshCw size={13} />
            Refresh
          </button>

        </div>


        <div className="comparison-state">

          <Activity size={20} />

          <strong>
            Not enough history for comparison
          </strong>

          <span>
            At least two pipeline snapshots are required.
          </span>

        </div>

      </section>
    );
  }


  const previous =
    history[
      history.length - 2
    ];

  const current =
    history[
      history.length - 1
    ];

  const currentSnapshotTime =
    formatSnapshotTime(
      current?.timestamp
    );


  const previousSnapshotTime =
    formatSnapshotTime(
      previous?.timestamp
    );


  const snapshotTimeDifference =
    formatTimeDifference(
      current?.timestamp,
      previous?.timestamp
    );


  return (
    <section className="dashboard-card comparison-card">

      <div className="comparison-card-header">

        <div>
          <h3>
            Pipeline Metrics Comparison
          </h3>

          <span>
            Latest snapshot vs previous snapshot
          </span>
        </div>


        <button
          type="button"
          className="comparison-refresh-button"
          onClick={onRefresh}
        >
          <RefreshCw size={13} />
          Refresh
        </button>

      </div>

      <div className="comparison-snapshot-info">

        <div className="comparison-snapshot-item">

          <span className="comparison-snapshot-label">
            Current snapshot
          </span>

          <strong>
            {currentSnapshotTime}
          </strong>

        </div>


        <div className="comparison-snapshot-divider" />


        <div className="comparison-snapshot-item">

          <span className="comparison-snapshot-label">
            Previous snapshot
          </span>

          <strong>
            {previousSnapshotTime}
          </strong>

        </div>


        <div className="comparison-snapshot-gap">

          <span>
            {snapshotTimeDifference}
          </span>

        </div>

      </div>


      <div className="comparison-grid">

        <ComparisonMetric
          label="Transactions Processed"
          current={
            current?.transactions_processed
          }
          previous={
            previous?.transactions_processed
          }
        />


        <ComparisonMetric
          label="Records / Second"
          current={
            current?.records_per_second
          }
          previous={
            previous?.records_per_second
          }
        />


        <ComparisonMetric
          label="Processing Errors"
          current={
            current?.processing_errors
          }
          previous={
            previous?.processing_errors
          }
          type="absolute"
          reverseMeaning
        />

      </div>

    </section>
  );
}


export default MetricsComparisonCard;