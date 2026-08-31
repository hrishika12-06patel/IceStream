import {
  useCallback,
  useEffect,
  useState,
} from "react";

import {
  getLakehouseStatus,
} from "../api/pipelineApi";


function LakehouseView() {
  const [data, setData] =
    useState(null);

  const [loading, setLoading] =
    useState(true);

  const [error, setError] =
    useState("");


  const loadLakehouse =
    useCallback(async () => {

      try {
        setError("");

        const response =
          await getLakehouseStatus();

        setData(response);

      } catch (err) {

        console.error(
          "Lakehouse API error:",
          err
        );

        setError(
          "Unable to load lakehouse information."
        );

      } finally {
        setLoading(false);
      }

    }, []);


  useEffect(() => {
    loadLakehouse();
  }, [loadLakehouse]);


  if (loading) {
    return (
      <div className="view-state">
        Loading Iceberg information...
      </div>
    );
  }


  if (error) {
    return (
      <div className="view-error">

        <strong>
          Lakehouse unavailable
        </strong>

        <p>{error}</p>

        <button
          className="primary-button"
          onClick={loadLakehouse}
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
          LAKEHOUSE
        </div>

        <h2>
          Apache Iceberg
        </h2>

        <p>
          Live table and snapshot information.
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


export default LakehouseView;