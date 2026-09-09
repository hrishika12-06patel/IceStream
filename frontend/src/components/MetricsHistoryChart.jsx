import {
  Activity,
  AlertTriangle,
  RefreshCw,
} from "lucide-react";


const TIME_RANGES = [
  {
    label: "All",
    value: null,
  },
  {
    label: "5m",
    value: 5,
  },
  {
    label: "15m",
    value: 15,
  },
  {
    label: "30m",
    value: 30,
  },
  {
    label: "60m",
    value: 60,
  },
];


const METRICS = [
  {
    label: "Transactions",
    key: "transactions_processed",
  },
  {
    label: "Throughput",
    key: "records_per_second",
  },
  {
    label: "Errors",
    key: "processing_errors",
  },
];


function formatTime(timestamp) {
  if (!timestamp) {
    return "—";
  }

  const date =
    new Date(timestamp);

  if (
    Number.isNaN(
      date.getTime()
    )
  ) {
    return "—";
  }

  return date.toLocaleTimeString(
    [],
    {
      hour: "2-digit",
      minute: "2-digit",
    }
  );
}


function getMetricLabel(metric) {
  switch (metric) {

    case "records_per_second":
      return "Records / second";

    case "processing_errors":
      return "Processing errors";

    case "transactions_processed":
    default:
      return "Transactions processed";
  }
}


function buildPoints(
  values,
  width,
  height
) {
  const numericValues =
    values.filter(
      (value) =>
        typeof value === "number" &&
        Number.isFinite(value)
    );

  if (
    numericValues.length === 0
  ) {
    return "";
  }


  const max =
    Math.max(...numericValues);

  const min =
    Math.min(...numericValues);

  const range =
    max - min || 1;


  return values
    .map(
      (value, index) => {

        if (
          typeof value !== "number" ||
          !Number.isFinite(value)
        ) {
          return null;
        }


        const x =
          values.length === 1
            ? width / 2
            : (
                index /
                (values.length - 1)
              ) *
              width;


        const y =
          height -
          (
            (value - min) /
            range
          ) *
          height;


        return `${x},${y}`;
      }
    )
    .filter(Boolean)
    .join(" ");
}


function MetricsHistoryChart({
  history,
  loading,
  error,
  onRefresh,
  historyMinutes,
  onHistoryMinutesChange,
  selectedMetric,
  onMetricChange,
}) {

  if (loading) {
    return (
      <section
        className="dashboard-card history-card"
      >
        <div className="history-state">
          <Activity size={20} />

          <span>
            Loading pipeline history...
          </span>
        </div>
      </section>
    );
  }


  if (error) {
    return (
      <section
        className="dashboard-card history-card"
      >
        <div
          className="
            history-state
            history-state-error
          "
        >
          <AlertTriangle size={20} />

          <strong>
            Unable to load metrics history
          </strong>

          <button
            type="button"
            className="history-refresh-button"
            onClick={onRefresh}
          >
            <RefreshCw size={13} />

            Retry
          </button>
        </div>
      </section>
    );
  }


  const hasHistory =
    Array.isArray(history) &&
    history.length > 0;


  const chartWidth = 900;
  const chartHeight = 220;


  const values =
    hasHistory
      ? history.map(
          (item) => {

            const value =
              item?.[selectedMetric];

            return (
              typeof value === "number" &&
              Number.isFinite(value)
            )
              ? value
              : null;
          }
        )
      : [];


  const points =
    buildPoints(
      values,
      chartWidth,
      chartHeight
    );


  const latest =
    hasHistory
      ? history[
          history.length - 1
        ]
      : null;


  const latestTransactions =
    typeof latest
      ?.transactions_processed ===
      "number"
      ? latest.transactions_processed
      : null;


  const latestRate =
    typeof latest
      ?.records_per_second ===
      "number"
      ? latest.records_per_second
      : null;


  const latestErrors =
    typeof latest
      ?.processing_errors ===
      "number"
      ? latest.processing_errors
      : null;


  return (
    <section
      className="dashboard-card history-card"
    >

      <div
        className="
          card-header
          history-card-header
        "
      >

        <div>
          <h3>
            Pipeline metrics history
          </h3>

          <span>
            Recent observability snapshots
          </span>
        </div>


        <button
          type="button"
          className="history-refresh-button"
          onClick={onRefresh}
        >
          <RefreshCw size={13} />

          Refresh
        </button>

      </div>


      <div className="history-controls">

        <div className="history-control-group">

          <span className="history-control-label">
            Time range
          </span>


          <div className="history-button-group">

            {TIME_RANGES.map(
              (range) => (

                <button
                  key={range.label}
                  type="button"
                  className={
                    historyMinutes ===
                    range.value
                      ? "history-control-button active"
                      : "history-control-button"
                  }
                  onClick={() =>
                    onHistoryMinutesChange(
                      range.value
                    )
                  }
                >
                  {range.label}
                </button>

              )
            )}

          </div>

        </div>


        <div className="history-control-group">

          <span className="history-control-label">
            Metric
          </span>


          <div className="history-button-group">

            {METRICS.map(
              (metric) => (

                <button
                  key={metric.key}
                  type="button"
                  className={
                    selectedMetric ===
                    metric.key
                      ? "history-control-button active"
                      : "history-control-button"
                  }
                  onClick={() =>
                    onMetricChange(
                      metric.key
                    )
                  }
                >
                  {metric.label}
                </button>

              )
            )}

          </div>

        </div>

      </div>


      {hasHistory ? (
        <>

          <div className="history-summary-grid">

            <div className="history-summary-item">
              <span>
                Latest processed
              </span>

              <strong>
                {latestTransactions !== null
                  ? latestTransactions
                      .toLocaleString()
                  : "—"}
              </strong>
            </div>


            <div className="history-summary-item">
              <span>
                Records / second
              </span>

              <strong>
                {latestRate !== null
                  ? latestRate
                      .toLocaleString()
                  : "—"}
              </strong>
            </div>


            <div className="history-summary-item">
              <span>
                Processing errors
              </span>

              <strong>
                {latestErrors !== null
                  ? latestErrors
                      .toLocaleString()
                  : "—"}
              </strong>
            </div>


            <div className="history-summary-item">
              <span>
                Snapshots
              </span>

              <strong>
                {history.length}
              </strong>
            </div>

          </div>


          <div className="history-chart-wrapper">

            <div className="history-chart-label">
              {getMetricLabel(
                selectedMetric
              )}
            </div>


            {points ? (

              <svg
                className="history-chart-svg"
                viewBox={
                  `0 0 ${chartWidth} ${chartHeight}`
                }
                role="img"
                aria-label={
                  `${getMetricLabel(
                    selectedMetric
                  )} history`
                }
              >

                <line
                  x1="0"
                  y1={chartHeight}
                  x2={chartWidth}
                  y2={chartHeight}
                  className="history-axis"
                />


                <line
                  x1="0"
                  y1="0"
                  x2="0"
                  y2={chartHeight}
                  className="history-axis"
                />


                <polyline
                  points={points}
                  className="history-line"
                />

              </svg>

            ) : (

              <div className="history-no-values">
                No values available for this metric
              </div>

            )}


            <div className="history-time-row">

              <span>
                {formatTime(
                  history[0]
                    ?.timestamp
                )}
              </span>

              <span>
                {formatTime(
                  latest?.timestamp
                )}
              </span>

            </div>

          </div>

        </>

      ) : (

        <div className="history-state">

          <Activity size={20} />

          <strong>
            No metrics history available
          </strong>

          <span>
            No snapshots were found for
            the selected time range.
          </span>

        </div>

      )}

    </section>
  );
}


export default MetricsHistoryChart;