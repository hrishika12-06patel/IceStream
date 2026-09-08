import {
  Activity,
  AlertTriangle,
  RefreshCw,
} from "lucide-react";


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
    Math.max(
      ...numericValues
    );

  const min =
    Math.min(
      ...numericValues
    );

  const range =
    max - min || 1;


  return values
    .map((value, index) => {

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

    })
    .filter(Boolean)
    .join(" ");
}


function MetricsHistoryChart({
  history,
  loading,
  error,
  onRefresh,
}) {

  if (loading) {
    return (
      <section className="dashboard-card history-card">

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
      <section className="dashboard-card history-card">

        <div className="history-state history-state-error">

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


  if (
    !Array.isArray(history) ||
    history.length === 0
  ) {
    return (
      <section className="dashboard-card history-card">

        <div className="history-state">

          <Activity size={20} />

          <strong>
            No metrics history available
          </strong>

          <span>
            Keep IceStream running to collect
            pipeline snapshots.
          </span>

        </div>

      </section>
    );
  }


  const chartWidth = 900;

  const chartHeight = 220;


  const transactions =
    history.map(
      (item) =>
        typeof item.transactions_processed ===
        "number"
          ? item.transactions_processed
          : null
    );


  const transactionPoints =
    buildPoints(
      transactions,
      chartWidth,
      chartHeight
    );


  const latest =
    history[
      history.length - 1
    ];


  const latestTransactions =
    typeof latest?.transactions_processed ===
    "number"
      ? latest.transactions_processed
      : null;


  const latestRate =
    typeof latest?.records_per_second ===
    "number"
      ? latest.records_per_second
      : null;


  const latestErrors =
    typeof latest?.processing_errors ===
    "number"
      ? latest.processing_errors
      : null;


  return (
    <section className="dashboard-card history-card">

      <div className="card-header history-card-header">

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


      <div className="history-summary-grid">

        <div className="history-summary-item">

          <span>
            Latest processed
          </span>

          <strong>
            {latestTransactions != null
              ? latestTransactions.toLocaleString()
              : "—"}
          </strong>

        </div>


        <div className="history-summary-item">

          <span>
            Records / second
          </span>

          <strong>
            {latestRate != null
              ? latestRate.toLocaleString()
              : "—"}
          </strong>

        </div>


        <div className="history-summary-item">

          <span>
            Processing errors
          </span>

          <strong>
            {latestErrors != null
              ? latestErrors.toLocaleString()
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
          Transactions processed
        </div>


        <svg
          className="history-chart-svg"
          viewBox={`0 0 ${chartWidth} ${chartHeight}`}
          role="img"
          aria-label="Transactions processed history"
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


          {transactionPoints && (
            <polyline
              points={transactionPoints}
              className="history-line"
            />
          )}

        </svg>


        <div className="history-time-row">

          <span>
            {formatTime(
              history[0]?.timestamp
            )}
          </span>

          <span>
            {formatTime(
              latest?.timestamp
            )}
          </span>

        </div>

      </div>

    </section>
  );
}


export default MetricsHistoryChart;