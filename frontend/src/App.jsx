import { useCallback, useEffect, useState } from "react";
import {
  ReactFlow,
  Background,
  Controls,
  MiniMap,
  Handle,
  Position,
  MarkerType,
  addEdge,
  useNodesState,
  useEdgesState,
} from "@xyflow/react";

import {
  Activity,
  Bell,
  Boxes,
  ChevronDown,
  CircleHelp,
  Database,
  Gauge,
  GitBranch,
  LayoutDashboard,
  Menu,
  MessageSquareWarning,
  Network,
  Play,
  Settings,
  ShieldCheck,
  Zap,
} from "lucide-react";

import {
  getPipelineStatus,
} from "./api/pipelineApi";

import "@xyflow/react/dist/style.css";
import "./App.css";

import DataQualityPanel
  from "./components/DataQualityPanel";

import IncidentPanel
  from "./components/IncidentPanel";

import LakehousePanel
  from "./components/LakehousePanel";

import DataQualityView
  from "./views/DataQualityView";

import IncidentsView
  from "./views/IncidentsView";

import LakehouseView
  from "./views/LakehouseView";

import MetricsView
  from "./views/MetricsView";

import AlertsView
  from "./views/AlertsView";

import SettingsView
  from "./views/SettingsView";

/* -----------------------------
   Pipeline Custom Node
----------------------------- */

function PipelineNode({ data }) {
  return (
    <div className={`pipeline-node ${data.variant}`}>
      <Handle
        type="target"
        position={Position.Left}
        className="node-handle"
      />

      <div className="node-top">
        <div className="node-icon">
          {data.icon}
        </div>

        <div className="node-status">

          <span className="status-dot"></span>

          {data.status || "Unknown"}

        </div>
      </div>

      <div className="node-name">{data.name}</div>

      <div className="node-stage">{data.stage}</div>

      <div className="node-description">
        {data.description}
      </div>

      <div className="node-metric">
        <span>{data.metricLabel}</span>
        <strong>{data.metric}</strong>
      </div>

      <Handle
        type="source"
        position={Position.Right}
        className="node-handle"
      />
    </div>
  );
}

const nodeTypes = {
  pipeline: PipelineNode,
};

/* -----------------------------
   Pipeline Nodes
----------------------------- */

const initialNodes = [
  {
    id: "kafka",
    type: "pipeline",
    position: { x: 80, y: 190 },
    data: {
      name: "Kafka",
      stage: "INGEST",
      description: "Streaming message ingestion",
      metricLabel: "Events / sec",
      metric: "1,240",
      variant: "kafka",
      icon: <Zap size={20} />,
    },
  },
  {
    id: "flink",
    type: "pipeline",
    position: { x: 390, y: 190 },
    data: {
      name: "Flink",
      stage: "PROCESS",
      description: "Real-time stream processing",
      metricLabel: "Processing rate",
      metric: "1,238/s",
      variant: "flink",
      icon: <Activity size={20} />,
    },
  },
  {
    id: "iceberg",
    type: "pipeline",
    position: { x: 700, y: 190 },
    data: {
      name: "Iceberg",
      stage: "SERVE",
      description: "Lakehouse table storage",
      metricLabel: "Records stored",
      metric: "1.2M",
      variant: "iceberg",
      icon: <Database size={20} />,
    },
  },
];

/* -----------------------------
   Pipeline Edges
----------------------------- */

const initialEdges = [
  {
    id: "kafka-flink",
    source: "kafka",
    target: "flink",
    animated: true,
    label: "STREAM",
    style: {
      strokeWidth: 2,
    },
    markerEnd: {
      type: MarkerType.ArrowClosed,
    },
  },
  {
    id: "flink-iceberg",
    source: "flink",
    target: "iceberg",
    animated: true,
    label: "DATA",
    style: {
      strokeWidth: 2,
    },
    markerEnd: {
      type: MarkerType.ArrowClosed,
    },
  },
];

/* -----------------------------
   Sidebar
----------------------------- */

function Sidebar({
  activeView,
  onNavigate,
  pipelineHealth,
}) {
  const healthy =
    pipelineHealth === "healthy";

  return (
    <aside className="sidebar">

      <div className="brand">
        <div className="brand-mark">
          I
        </div>

        <div>
          <h1>IceStream</h1>
          <span>OBSERVABILITY</span>
        </div>
      </div>

      <div className="workspace-label">
        WORKSPACE
        <ChevronDown size={12} />
      </div>

      <nav className="navigation">

        <div 
          className={
            activeView === "dashboard"
              ? "nav-item active"
              : "nav-item"
          }
          onClick={() =>
            onNavigate("dashboard")
          }
        >
          <LayoutDashboard size={17} />
          <span>Dashboard</span>
        </div>

        <div 
          className={
            activeView === "pipeline"
              ? "nav-item active"
              : "nav-item"
          }
          onClick={() =>
            onNavigate("pipeline")
          }
        >
          <Network size={17} />
          <span>Pipeline</span>
          <span className="nav-badge">LIVE</span>
        </div>

        <div 
          className={
            activeView === "data-quality"
              ? "nav-item active"
              : "nav-item"
          }
          onClick={() =>
            onNavigate("data-quality")
          }
        >
          <ShieldCheck size={17} />
          <span>Data Quality</span>
        </div>

        <div 
          className={
            activeView === "alerts"
            ? "nav-item active"
            : "nav-item"
          }
          onClick={() =>
            onNavigate("alerts")
          }
        >
          <Bell size={17} />
          <span>Alerts</span>
          <span className="alert-badge">2</span>
        </div>

      </nav>

      <div className="sidebar-section">
        <div className="section-label">
          INFRASTRUCTURE
        </div>

        <div 
          className={
            activeView === "pipeline"
              ? "nav-item active"
              : "nav-item"
          }
          onClick={() =>
            onNavigate("pipeline")
          }
        >
          <GitBranch size={16} />
          <span>Pipeline Nodes</span>
        </div>

        <div 
          className={
            activeView === "lakehouse"
              ? "nav-item active"
              : "nav-item"
        }
          onClick={() =>
            onNavigate("lakehouse")
          }
        >
          <Database size={16} />
          <span>Lakehouse</span>
        </div>

        <div 
          className={
            activeView === "metrics"
              ? "nav-item active"
              : "nav-item"
          }
          onClick={() =>
            onNavigate("metrics")
          }
        >
          <Gauge size={16} />
          <span>Metrics</span>
        </div>
      </div>

      <div className="sidebar-section">
        <div className="section-label">
          MONITORING
        </div>

        <div 
          className={
            activeView === "incidents"
              ? "nav-item active"
              : "nav-item"
          }
          onClick={() =>
            onNavigate("incidents")
          }
        >
          <MessageSquareWarning size={16} />
          <span>Issues</span>
        </div>

        <div 
          className={
            activeView === "settings"
              ? "nav-item active"
              : "nav-item"
          }
          onClick={() =>
            onNavigate("settings")
          }
        >
          <Settings size={16} />
          <span>Settings</span>
        </div>
      </div>

      <div className="system-card">

        <div className="system-card-top">
          <span>System health</span>
          <span className={`health-dot ${
              healthy ? "" : "health-dot-error"
            }`}>
          </span>
        </div>

        <strong>{healthy ? "Operational" : "Unavailable"}</strong>

        <div className="health-bar">
          <div style={{
              width: healthy
                ? "99%"
                : "20%",
            }}>
          </div>
        </div>

        <small>{healthy ? "All pipeline services are healthy" : "Unable to reach pipeline backend"}</small>

      </div>

    </aside>
  );
}

/* -----------------------------
   Header
----------------------------- */

function Header({
  activeView,
  pipelineHealth
}) {
  const healthy =
    pipelineHealth === "healthy";

  const viewLabels = {
  dashboard: {
    parent: "Dashboard",
    current: "Overview",
  },

  pipeline: {
    parent: "Pipeline",
    current: "Live Pipeline",
  },

  "data-quality": {
    parent: "Data Quality",
    current: "Monitoring",
  },

  alerts: {
    parent: "Monitoring",
    current: "Alerts",
  },

  lakehouse: {
    parent: "Infrastructure",
    current: "Lakehouse",
  },

  metrics: {
    parent: "Monitoring",
    current: "Metrics",
  },

  incidents: {
    parent: "Monitoring",
    current: "Issues",
  },

  settings: {
    parent: "Configuration",
    current: "Settings",
  },
};

const currentView =
  viewLabels[activeView] ||
  viewLabels.dashboard;


return (
    <header className="topbar">

      <div className="mobile-menu">
        <Menu size={20} />
      </div>

      <div className="breadcrumbs">
        <span>{currentView.parent}</span>
        <span>/</span>
        <strong>{currentView.current}</strong>
      </div>

      <div className="topbar-actions">

        <div className={`operational ${
            healthy
              ? ""
              : "operational-error"
          }`}>
          <span></span>
          {healthy ? "All systems operational" : "Pipeline unavailable"}
        </div>

        <button className="help-button">
          <CircleHelp size={17} />
        </button>

        <div className="avatar">
          IS
        </div>

      </div>

    </header>
  );
}

/* -----------------------------
   Metric Card
----------------------------- */

function MetricCard({ icon, label, value, change }) {
  return (
    <div className="metric-card">

      <div className="metric-icon">
        {icon}
      </div>

      <div className="metric-content">
        <span>{label}</span>
        <strong>{value}</strong>

        <small>
          <span className="positive">↑ {change}</span>
          {" "}from last hour
        </small>
      </div>

    </div>
  );
}

/* -----------------------------
   Recent Alerts
----------------------------- */

function RecentAlerts({ alerts = [] }) {
  return (
    <div className="dashboard-card alerts-card">

      <div className="card-header">

        <div>

          <h3>
            Recent alerts
          </h3>

          <span>
            Latest pipeline events
          </span>

        </div>

        <button className="more-button">
          •••
        </button>

      </div>


      <div className="alerts-list">

        {alerts.length === 0 ? (

          <div className="empty-state">
            No recent alerts
          </div>

        ) : (

          alerts.map((alert) => (

            <div
              className={`alert-item ${alert.type}`}
              key={alert.id}
            >

              <div className="alert-icon">

                {alert.type === "success" ? (
                  <ShieldCheck size={15} />
                ) : alert.type === "info" ? (
                  <Activity size={15} />
                ) : (
                  <Bell size={15} />
                )}

              </div>


              <div>

                <strong>
                  {alert.title}
                </strong>

                <p>
                  {alert.message}
                </p>

                <small>
                  {alert.time}
                </small>

              </div>

            </div>

          ))

        )}

      </div>

    </div>
  );
}

/* -----------------------------
   Active Pipelines
----------------------------- */

function ActivePipelines({
  pipelineData,
}) {

  const kafka =
    pipelineData.services.kafka;

  const flink =
    pipelineData.services.flink;

  const iceberg =
    pipelineData.services.iceberg;
  
  return (
    <div className="dashboard-card">

      <div className="card-header">
        <div>
          <h3>Active pipeline</h3>
          <span>Current processing status</span>
        </div>
      </div>

      <div className="pipeline-table">

        <div className="table-row table-head">
          <span>Pipeline</span>
          <span>Status</span>
          <span>Events</span>
        </div>

        <div className="table-row">
          <span>transaction-stream</span>
          <span className="running">{kafka.status}</span>
          <span>{kafka.eventsPerSecond}/s</span>
        </div>

        <div className="table-row">
          <span>quality-monitor</span>
          <span className="running">{flink.status}</span>
          <span>{flink.processingRate}/s</span>
        </div>

        <div className="table-row">
          <span>iceberg-writer</span>
          <span className="pending">{iceberg.status}</span>
          <span>{iceberg.recordsStored.toLocaleString()}</span>
        </div>

      </div>

    </div>
  );
}

/* -----------------------------
   Latency Card
----------------------------- */

function LatencyCard({
  pipelineData,
}) {
  const kafka =
    pipelineData.services.kafka;

  const flink =
    pipelineData.services.flink;

  const iceberg =
    pipelineData.services.iceberg;

  const total =
    pipelineData.metrics.pipelineLatency;

  return (
    <div className="dashboard-card">

      <div className="card-header">
        <div>
          <h3>Latency by stage</h3>
          <span>Current processing latency</span>
        </div>
      </div>

      <div className="latency-list">

        <div className="latency-row">
          <span>Kafka</span>
          <div className="latency-bar">
            <div style={{
                width: `${Math.min(
                  kafka.latency * 3,
                  100
                )}%`,
              }}>
            </div>
          </div>
          <strong>{kafka.latency}ms</strong>
        </div>

        <div className="latency-row">
          <span>Flink</span>
          <div className="latency-bar">
            <div style={{
                width: `${Math.min(
                  flink.latency * 3,
                  100
                )}%`,
              }}>
            </div>
          </div>
          <strong>{flink.latency}ms</strong>
        </div>

        <div className="latency-row">
          <span>Iceberg</span>
          <div className="latency-bar">
            <div style={{
                width: `${Math.min(
                  iceberg.latency * 3,
                  100
                )}%`,
              }}>
            </div>
          </div>
          <strong>{iceberg.latency}ms</strong>
        </div>

        <div className="latency-row">
          <span>Total</span>
          <div className="latency-bar">
            <div style={{
                width: `${Math.min(
                  total * 2,
                  100
                )}%`,
              }}>
            </div>
          </div>
          <strong>{total}ms</strong>
        </div>

      </div>

    </div>
  );
}

/* -----------------------------
   Main App
----------------------------- */

function App() {

  const [pipelineData, setPipelineData] = useState(null);

  const [loading, setLoading] = useState(true);

  const [error, setError] = useState("");

  const [nodes, setNodes, onNodesChange] =
    useNodesState(initialNodes);

  const [edges, setEdges, onEdgesChange] =
    useEdgesState(initialEdges);

  const [activeView, setActiveView] =
  useState("dashboard");

  const loadPipelineData = useCallback(async () => {
  try {
    setError("");

    const data = await getPipelineStatus();

    setPipelineData(data);
  } catch (err) {
    console.error("Pipeline API error:", err);

    setError("Unable to fetch pipeline metrics");
  } finally {
    setLoading(false);
  }
}, []);

  useEffect(() => {
  loadPipelineData();

  const interval = setInterval(
    loadPipelineData,
    5000
  );

  return () => clearInterval(interval);
}, [loadPipelineData]);

  useEffect(() => {

    if (!pipelineData) {
      return;
    }


    const kafka =
      pipelineData.services.kafka;

    const flink =
      pipelineData.services.flink;

    const iceberg =
      pipelineData.services.iceberg;


    setNodes((currentNodes) =>
      currentNodes.map((node) => {

        if (node.id === "kafka") {

          return {
            ...node,

            data: {
              ...node.data,

              metric:
                kafka.eventsPerSecond.toLocaleString(),

              status:
                kafka.status,
            },
          };

        }


        if (node.id === "flink") {

          return {
            ...node,

            data: {
              ...node.data,

              metric:
                `${flink.processingRate.toLocaleString()}/s`,

              status:
                flink.status,
            },
          };

        }


        if (node.id === "iceberg") {

          return {
            ...node,

            data: {
              ...node.data,

              metric:
                iceberg.recordsStored.toLocaleString(),

              status:
                iceberg.status,
            },
          };

        }


        return node;

      })
    );

  }, [pipelineData, setNodes]);

  useEffect(() => {
  const interval = setInterval(() => {
    loadPipelineData();
  }, 5000);

  return () => {
    clearInterval(interval);
  };
}, [loadPipelineData]);


  /* =========================
     CONNECT REACT FLOW NODES
  ========================= */

  const onConnect = useCallback(
    (connection) => {

      setEdges((currentEdges) =>
        addEdge(
          connection,
          currentEdges
        )
      );

    },
    [setEdges]
  );


  /* =========================
     LOADING STATE
  ========================= */

  if (loading) {

    return (
      <div className="app loading-screen">

        <div className="loading-card">

          <div className="loading-spinner"></div>

          <h2>
            Loading IceStream
          </h2>

          <p>
            Fetching pipeline metrics...
          </p>

        </div>

      </div>
    );

  }


  /* =========================
     FALLBACK
  ========================= */

  if (!pipelineData) {

    return (
      <div className="app loading-screen">

        <div className="loading-card error-card">

          <h2>
            Pipeline unavailable
          </h2>

          <p>
            Unable to load pipeline information.
          </p>

          <button
            className="primary-button"
            onClick={loadPipelineData}
          >
            Retry
          </button>

        </div>

      </div>
    );

  }


  const metrics =
    pipelineData.metrics;

  const pipelineHealth =
    error
      ? "offline"
      : pipelineData?.overallStatus === "healthy"
      ? "healthy"
      : "degraded";

function renderActiveView() {
  switch (activeView) {

    case "data-quality":
      return <DataQualityView />;

    case "alerts":
      return <AlertsView />;

    case "lakehouse":
      return <LakehouseView />;

    case "metrics":
      return <MetricsView />;

    case "incidents":
      return <IncidentsView />;

    case "settings":
      return <SettingsView />;

    default:
      return null;
  }
}

  return (
    <div className="app">

      <Sidebar
        activeView={activeView}
        onNavigate={setActiveView}
        pipelineHealth={pipelineHealth}
      />


      <div className="main">

        <Header
          activeView={activeView}
          pipelineHealth={pipelineHealth}
        />


        <main className="content">

        {activeView === "dashboard" && (
          <>

            {/* =========================
                PAGE HEADING
            ========================= */}

            <div className="page-heading">

              <div>

                <div className="eyebrow">
                  REAL-TIME OBSERVABILITY
                </div>

                <h2>
                  Pipeline overview
                </h2>

                <p>
                  Monitor your streaming data pipeline
                  from ingest to lakehouse storage.
                </p>

              </div>


              <button className="primary-button">

                <Play size={15} />

                Live pipeline

              </button>

            </div>


            {/* =========================
                ERROR BANNER
            ========================= */}

            {error && (

              <div className="backend-error">

                <Bell size={15} />

                <span>
                  {error}
                </span>

              </div>

            )}


            {/* =========================
                PIPELINE
            ========================= */}

            <section className="dashboard-card pipeline-card">

              <div className="card-header">

                <div>

                  <h3>
                    Streaming data pipeline
                  </h3>

                  <span>
                    Ingest → Process → Serve
                  </span>

                </div>


                <div
                  className={`pipeline-status ${
                    error
                      ? "pipeline-status-error"
                      : ""
                  }`}
                >

                  <span></span>

                  {error
                    ? "Unavailable"
                    : "Healthy"}

                </div>

              </div>


              <div className="flow-container">

                <ReactFlow
                  nodes={nodes}
                  edges={edges}
                  nodeTypes={nodeTypes}
                  onNodesChange={onNodesChange}
                  onEdgesChange={onEdgesChange}
                  onConnect={onConnect}
                  fitView
                  fitViewOptions={{
                    padding: 0.2,
                  }}
                  proOptions={{
                    hideAttribution: true,
                  }}
                >

                  <Background
                    gap={24}
                    size={1}
                    color="#1b2942"
                  />

                  <Controls />

                  <MiniMap
                    nodeColor={(node) => {

                      if (
                        node.data?.variant === "kafka"
                      ) {
                        return "#3b82f6";
                      }

                      if (
                        node.data?.variant === "flink"
                      ) {
                        return "#8b5cf6";
                      }

                      return "#22c55e";

                    }}
                  />

                </ReactFlow>

              </div>

            </section>


            {/* =========================
                METRICS
            ========================= */}

            <div className="metrics-grid">

              <MetricCard
                icon={<Zap size={19} />}
                label="Events / second"
                value={
                  metrics.eventsPerSecond.toLocaleString()
                }
                change="Live"
              />


              <MetricCard
                icon={<Activity size={19} />}
                label="Pipeline latency"
                value={`${metrics.pipelineLatency} ms`}
                change="Live"
              />


              <MetricCard
                icon={<ShieldCheck size={19} />}
                label="Data quality"
                value={`${metrics.dataQuality}%`}
                change="Live"
              />


              <MetricCard
                icon={<Boxes size={19} />}
                label="Records processed"
                value={
                  metrics.recordsProcessed.toLocaleString()
                }
                change="Live"
              />

            </div>


            {/* =========================
                LOWER DASHBOARD
            ========================= */}

            <div className="lower-grid">

              <ActivePipelines
                pipelineData={pipelineData}
              />

              <LatencyCard
                pipelineData={pipelineData}
              />

              <RecentAlerts
                alerts={pipelineData?.alerts || []}
              />

            </div>

          </>
        )}


        {/* =========================
            PIPELINE VIEW
        ========================= */}

        {activeView === "pipeline" && (
        <>

          <div className="page-heading">

            <div>

              <div className="eyebrow">
                LIVE STREAMING PIPELINE
              </div>

              <h2>
                Pipeline
              </h2>

              <p>
                Monitor Kafka, Flink and Iceberg
                processing stages in real time.
              </p>

            </div>

          </div>


          {/* Error banner */}

          {error && (
            <div className="backend-error">

              <Bell size={15} />

              <span>
                {error}
              </span>

            </div>
          )}


          {/* Pipeline */}

          <section className="dashboard-card pipeline-card">

            <div className="card-header">

              <div>

                <h3>
                  Streaming data pipeline
                </h3>

                <span>
                  Ingest → Process → Serve
                </span>

              </div>


              <div
                className={`pipeline-status ${
                  error
                    ? "pipeline-status-error"
                    : ""
                }`}
              >

                <span></span>

                {error
                  ? "Unavailable"
                  : "Healthy"}

              </div>

            </div>


            <div className="flow-container">

              <ReactFlow
                nodes={nodes}
                edges={edges}
                nodeTypes={nodeTypes}
                onNodesChange={onNodesChange}
                onEdgesChange={onEdgesChange}
                onConnect={onConnect}
                fitView
                fitViewOptions={{
                  padding: 0.2,
                }}
                proOptions={{
                  hideAttribution: true,
                }}
              >

                <Background
                  gap={24}
                  size={1}
                  color="#1b2942"
                />

                <Controls />

                <MiniMap
                  nodeColor={(node) => {

                    if (
                      node.data?.variant === "kafka"
                    ) {
                      return "#3b82f6";
                    }

                    if (
                      node.data?.variant === "flink"
                    ) {
                      return "#8b5cf6";
                    }

                    return "#22c55e";

                  }}
                />

              </ReactFlow>

            </div>

          </section>


          {/* Pipeline metrics */}

          <div className="metrics-grid">

            <MetricCard
              icon={<Zap size={19} />}
              label="Kafka events / second"
              value={
                pipelineData.services.kafka
                  .eventsPerSecond
                  .toLocaleString()
              }
              change="Live"
            />


            <MetricCard
              icon={<Activity size={19} />}
              label="Flink processing rate"
              value={
                `${pipelineData.services.flink
                  .processingRate
                  .toLocaleString()}/s`
              }
              change="Live"
            />


            <MetricCard
              icon={<Gauge size={19} />}
              label="Pipeline latency"
              value={
                `${metrics.pipelineLatency} ms`
              }
              change="Live"
            />


            <MetricCard
              icon={<Database size={19} />}
              label="Iceberg records"
              value={
                pipelineData.services.iceberg
                  .recordsStored
                  .toLocaleString()
              }
              change="Live"
            />

          </div>


          {/* Active pipeline + latency */}

          <div className="pipeline-view-grid">

            <ActivePipelines
              pipelineData={pipelineData}
            />

            <LatencyCard
              pipelineData={pipelineData}
            />

          </div>

        </>
      )}

      {activeView !== "dashboard" &&
       activeView !== "pipeline" &&
       renderActiveView()}

    </main>   
      


        {/* =========================
            FOOTER
        ========================= */}

        <footer className="footer">

          <div>

            <span
              className="footer-live"
            ></span>

            {error
              ? "Offline"
              : "Live"}

            <span>•</span>

            IceStream

            <span>•</span>

            Real-Time Lakehouse

          </div>


          <span>

            {error
              ? "Backend unavailable"
              : "Live backend data"}

          </span>

        </footer>

      </div>

    </div>
  );
}

export default App;