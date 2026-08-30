import {
  Activity,
  AlertTriangle,
  Bell,
  ShieldCheck,
} from "lucide-react";


const alerts = [
  {
    id: 1,
    type: "warning",
    title: "Kafka consumer lag increased",
    description:
      "Consumer lag exceeded the normal threshold.",
    time: "2 min ago",
  },

  {
    id: 2,
    type: "info",
    title: "Flink processing stable",
    description:
      "Transaction processing rate remains stable.",
    time: "8 min ago",
  },

  {
    id: 3,
    type: "success",
    title: "Iceberg snapshot committed",
    description:
      "Latest transaction batch committed successfully.",
    time: "14 min ago",
  },
];


function AlertsView() {
  return (
    <div className="view-container">

      <div className="view-heading">

        <div className="eyebrow">
          ALERTS
        </div>

        <h2>
          Pipeline alerts
        </h2>

        <p>
          Review recent pipeline and
          data-quality events.
        </p>

      </div>


      <div className="dashboard-card full-alerts-card">

        <div className="card-header">

          <div>
            <h3>
              Recent alerts
            </h3>

            <span>
              Latest pipeline events
            </span>
          </div>

        </div>


        <div className="full-alert-list">

          {alerts.map((alert) => (

            <div
              key={alert.id}
              className={`full-alert-item ${alert.type}`}
            >

              <div className="alert-icon">

                {alert.type === "warning" && (
                  <AlertTriangle size={16} />
                )}

                {alert.type === "info" && (
                  <Activity size={16} />
                )}

                {alert.type === "success" && (
                  <ShieldCheck size={16} />
                )}

              </div>


              <div>

                <strong>
                  {alert.title}
                </strong>

                <p>
                  {alert.description}
                </p>

                <small>
                  {alert.time}
                </small>

              </div>

            </div>

          ))}

        </div>

      </div>

    </div>
  );
}


export default AlertsView;