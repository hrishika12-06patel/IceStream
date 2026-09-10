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