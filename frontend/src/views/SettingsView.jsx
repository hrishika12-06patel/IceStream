import {
  Bell,
  RefreshCw,
  Settings,
} from "lucide-react";


function SettingsView() {
  return (
    <div className="view-container">

      <div className="view-heading">

        <div className="eyebrow">
          CONFIGURATION
        </div>

        <h2>
          Settings
        </h2>

        <p>
          Configure frontend monitoring
          preferences.
        </p>

      </div>


      <div className="settings-grid">

        <div className="dashboard-card settings-card">

          <div className="settings-icon">
            <RefreshCw size={18} />
          </div>

          <div>
            <span>
              Refresh interval
            </span>

            <strong>
              5 seconds
            </strong>

            <small>
              Pipeline metrics refresh frequency
            </small>
          </div>

        </div>


        <div className="dashboard-card settings-card">

          <div className="settings-icon">
            <Bell size={18} />
          </div>

          <div>
            <span>
              Alerts
            </span>

            <strong>
              Enabled
            </strong>

            <small>
              Display pipeline alerts
            </small>
          </div>

        </div>


        <div className="dashboard-card settings-card">

          <div className="settings-icon">
            <Settings size={18} />
          </div>

          <div>
            <span>
              Monitoring mode
            </span>

            <strong>
              Live
            </strong>

            <small>
              Real-time observability mode
            </small>
          </div>

        </div>

      </div>

    </div>
  );
}


export default SettingsView;