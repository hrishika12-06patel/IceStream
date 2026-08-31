import {
  useCallback,
  useEffect,
  useState,
} from "react";

import {
  getPipelineMetrics,
} from "../api/pipelineApi";


function MetricsView() {
  const [metrics, setMetrics] =
    useState(null);

  const [loading, setLoading] =
    useState(true);

  const [error, setError] =
    useState("");


  const loadMetrics =
    useCallback(async () => {

      try {
        setError("");

        const response =
          await getPipelineMetrics();

        setMetrics(response);

      } catch (err) {

        console.error(
          "Metrics API error:",
          err
        );

        setError(
          "Unable to load pipeline metrics."
        );

      } finally {
        setLoading(false);
      }

    }, []);


  useEffect(() => {
    loadMetrics();
  }, [loadMetrics]);


  if (loading) {
    return (
      <div className="view-state">
        Loading pipeline metrics...
      </div>
    );
  }


  if (error) {
    return (
      <div className="view-error">

        <strong>
          Metrics unavailable
        </strong>

        <p>{error}</p>

        <button
          className="primary-button"
          onClick={loadMetrics}
        >
          Retry
        </button>

      </div>
    );
  }


  return (
    <div className="view-container">

      <div className="view-heading">

        <div className="eyebrow">
          METRICS
        </div>

        <h2>
          Pipeline metrics
        </h2>

        <p>
          Live runtime metrics from IceStream.
        </p>

      </div>


      <div className="dashboard-card api-json-card">

        <pre className="api-json">
          {JSON.stringify(
            metrics,
            null,
            2
          )}
        </pre>

      </div>

    </div>
  );
}


export default MetricsView;