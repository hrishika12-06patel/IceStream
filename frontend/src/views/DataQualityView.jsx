import {
  useCallback,
  useEffect,
  useState,
} from "react";

import {
  getDataQuality,
} from "../api/pipelineApi";


function DataQualityView() {
  const [data, setData] =
    useState(null);

  const [loading, setLoading] =
    useState(true);

  const [error, setError] =
    useState("");


  const loadDataQuality =
    useCallback(async () => {

      try {
        setError("");

        const response =
          await getDataQuality();

        setData(response);

      } catch (err) {

        console.error(
          "Data quality API error:",
          err
        );

        setError(
          "Unable to load data-quality information."
        );

      } finally {
        setLoading(false);
      }

    }, []);


  useEffect(() => {
    loadDataQuality();
  }, [loadDataQuality]);


  if (loading) {
    return (
      <div className="view-state">
        Loading data-quality metrics...
      </div>
    );
  }


  if (error) {
    return (
      <div className="view-error">

        <strong>
          Data quality unavailable
        </strong>

        <p>
          {error}
        </p>

        <button
          className="primary-button"
          onClick={loadDataQuality}
        >
          Retry
        </button>

      </div>
    );
  }


  if (!data) {
    return (
      <div className="view-state">
        No data-quality information available.
      </div>
    );
  }


  return (
    <div className="view-container">

      <div className="view-heading">

        <div className="eyebrow">
          DATA QUALITY
        </div>

        <h2>
          Data quality monitoring
        </h2>

        <p>
          Live validation information from
          the IceStream processing pipeline.
        </p>

      </div>


      <div className="dashboard-card api-json-card">

        <div className="card-header">

          <div>
            <h3>
              Live data-quality response
            </h3>

            <span>
              Observability API
            </span>
          </div>

        </div>


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


export default DataQualityView;