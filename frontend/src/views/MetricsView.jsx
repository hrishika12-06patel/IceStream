import {
  Activity,
  Boxes,
  Gauge,
  ShieldCheck,
  Zap,
} from "lucide-react";


const metrics = [
  {
    label: "Events / second",
    value: "1,240",
    description: "Kafka ingestion rate",
    icon: <Zap size={19} />,
  },

  {
    label: "Processing rate",
    value: "1,238/s",
    description: "Flink processing throughput",
    icon: <Activity size={19} />,
  },

  {
    label: "Pipeline latency",
    value: "47 ms",
    description: "End-to-end latency",
    icon: <Gauge size={19} />,
  },

  {
    label: "Data quality",
    value: "99.8%",
    description: "Current quality score",
    icon: <ShieldCheck size={19} />,
  },

  {
    label: "Records processed",
    value: "1.25M",
    description: "Total processed records",
    icon: <Boxes size={19} />,
  },
];


function MetricsView() {
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
          Review performance and
          observability metrics.
        </p>

      </div>


      <div className="standalone-metrics-grid">

        {metrics.map((metric) => (

          <div
            key={metric.label}
            className="metric-card"
          >

            <div className="metric-icon">
              {metric.icon}
            </div>


            <div className="metric-content">

              <span>
                {metric.label}
              </span>

              <strong>
                {metric.value}
              </strong>

              <small>
                {metric.description}
              </small>

            </div>

          </div>

        ))}

      </div>

    </div>
  );
}


export default MetricsView;