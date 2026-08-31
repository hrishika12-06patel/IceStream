import {
  useCallback,
  useEffect,
  useState,
} from "react";

import {
  getIncidents,
} from "../api/pipelineApi";


function IncidentsView() {
  const [data, setData] =
    useState(null);

  const [loading, setLoading] =
    useState(true);

  const [error, setError] =
    useState("");


  const loadIncidents =
    useCallback(async () => {

      try {
        setError("");

        const response =
          await getIncidents();

        setData(response);

      } catch (err) {

        console.error(
          "Incidents API error:",
          err
        );

        setError(
          "Unable to load incidents."
        );

      } finally {
        setLoading(false);
      }

    }, []);


  useEffect(() => {
    loadIncidents();
  }, [loadIncidents]);


  if (loading) {
    return (
      <div className="view-state">
        Loading incidents...
      </div>
    );
  }


  if (error) {
    return (
      <div className="view-error">

        <strong>
          Incidents unavailable
        </strong>

        <p>{error}</p>

        <button
          className="primary-button"
          onClick={loadIncidents}
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
          INCIDENTS
        </div>

        <h2>
          Pipeline incidents
        </h2>

        <p>
          Live incidents reported by the
          IceStream observability API.
        </p>

      </div>


      <div className="dashboard-card api-json-card">

        <pre className="api-json">
          {JSON.stringify(
            data,
            null,
            2
          )}
        </pre>

      </div>

    </div>
  );
}


export default IncidentsView;