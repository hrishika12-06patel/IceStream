import { useCallback } from "react";
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

import "@xyflow/react/dist/style.css";
import "./App.css";

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
          Healthy
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

function Sidebar() {
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

        <div className="nav-item active">
          <LayoutDashboard size={17} />
          <span>Dashboard</span>
        </div>

        <div className="nav-item">
          <Network size={17} />
          <span>Pipeline</span>
          <span className="nav-badge">LIVE</span>
        </div>

        <div className="nav-item">
          <ShieldCheck size={17} />
          <span>Data Quality</span>
        </div>

        <div className="nav-item">
          <Bell size={17} />
          <span>Alerts</span>
          <span className="alert-badge">2</span>
        </div>

      </nav>

      <div className="sidebar-section">
        <div className="section-label">
          INFRASTRUCTURE
        </div>

        <div className="nav-item">
          <GitBranch size={16} />
          <span>Pipeline Nodes</span>
        </div>

        <div className="nav-item">
          <Database size={16} />
          <span>Lakehouse</span>
        </div>

        <div className="nav-item">
          <Gauge size={16} />
          <span>Metrics</span>
        </div>
      </div>

      <div className="sidebar-section">
        <div className="section-label">
          MONITORING
        </div>

        <div className="nav-item">
          <MessageSquareWarning size={16} />
          <span>Issues</span>
        </div>

        <div className="nav-item">
          <Settings size={16} />
          <span>Settings</span>
        </div>
      </div>

      <div className="system-card">

        <div className="system-card-top">
          <span>System health</span>
          <span className="health-dot"></span>
        </div>

        <strong>Operational</strong>

        <div className="health-bar">
          <div></div>
        </div>

        <small>All pipeline services are healthy</small>

      </div>

    </aside>
  );
}

/* -----------------------------
   Header
----------------------------- */

function Header() {
  return (
    <header className="topbar">

      <div className="mobile-menu">
        <Menu size={20} />
      </div>

      <div className="breadcrumbs">
        <span>Dashboard</span>
        <span>/</span>
        <strong>Pipeline Overview</strong>
      </div>

      <div className="topbar-actions">

        <div className="operational">
          <span></span>
          All systems operational
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

function RecentAlerts() {
  return (
    <div className="dashboard-card alerts-card">

      <div className="card-header">
        <div>
          <h3>Recent alerts</h3>
          <span>Latest pipeline events</span>
        </div>

        <button className="more-button">•••</button>
      </div>

      <div className="alerts-list">

        <div className="alert-item warning">
          <div className="alert-icon">
            <Bell size={15} />
          </div>

          <div>
            <strong>Consumer lag detected</strong>
            <p>Kafka consumer lag increased slightly.</p>
            <small>2 min ago</small>
          </div>
        </div>

        <div className="alert-item info">
          <div className="alert-icon">
            <Activity size={15} />
          </div>

          <div>
            <strong>Pipeline processing normally</strong>
            <p>Flink processing rate is stable.</p>
            <small>8 min ago</small>
          </div>
        </div>

        <div className="alert-item success">
          <div className="alert-icon">
            <ShieldCheck size={15} />
          </div>

          <div>
            <strong>Iceberg write completed</strong>
            <p>Latest batch stored successfully.</p>
            <small>14 min ago</small>
          </div>
        </div>

      </div>

    </div>
  );
}

/* -----------------------------
   Active Pipelines
----------------------------- */

function ActivePipelines() {
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
          <span className="running">Running</span>
          <span>1,240/s</span>
        </div>

        <div className="table-row">
          <span>quality-monitor</span>
          <span className="running">Running</span>
          <span>1,238/s</span>
        </div>

        <div className="table-row">
          <span>iceberg-writer</span>
          <span className="pending">Monitoring</span>
          <span>1,238/s</span>
        </div>

      </div>

    </div>
  );
}

/* -----------------------------
   Latency Card
----------------------------- */

function LatencyCard() {
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
            <div style={{ width: "28%" }}></div>
          </div>
          <strong>8ms</strong>
        </div>

        <div className="latency-row">
          <span>Flink</span>
          <div className="latency-bar">
            <div style={{ width: "55%" }}></div>
          </div>
          <strong>16ms</strong>
        </div>

        <div className="latency-row">
          <span>Iceberg</span>
          <div className="latency-bar">
            <div style={{ width: "75%" }}></div>
          </div>
          <strong>23ms</strong>
        </div>

        <div className="latency-row">
          <span>Total</span>
          <div className="latency-bar">
            <div style={{ width: "90%" }}></div>
          </div>
          <strong>47ms</strong>
        </div>

      </div>

    </div>
  );
}

/* -----------------------------
   Main App
----------------------------- */

function App() {

  const [nodes, setNodes, onNodesChange] =
    useNodesState(initialNodes);

  const [edges, setEdges, onEdgesChange] =
    useEdgesState(initialEdges);

  const onConnect = useCallback(
    (connection) => {
      setEdges((currentEdges) =>
        addEdge(connection, currentEdges)
      );
    },
    [setEdges]
  );

  return (
    <div className="app">

      <Sidebar />

      <div className="main">

        <Header />

        <main className="content">

          <div className="page-heading">

            <div>
              <div className="eyebrow">
                REAL-TIME OBSERVABILITY
              </div>

              <h2>Pipeline overview</h2>

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

          {/* Pipeline */}

          <section className="dashboard-card pipeline-card">

            <div className="card-header">

              <div>
                <h3>Streaming data pipeline</h3>
                <span>
                  Ingest → Process → Serve
                </span>
              </div>

              <div className="pipeline-status">
                <span></span>
                Healthy
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
                    if (node.data?.variant === "kafka") {
                      return "#3b82f6";
                    }

                    if (node.data?.variant === "flink") {
                      return "#8b5cf6";
                    }

                    return "#22c55e";
                  }}
                />

              </ReactFlow>

            </div>

          </section>

          {/* Metrics */}

          <div className="metrics-grid">

            <MetricCard
              icon={<Zap size={19} />}
              label="Events / second"
              value="1,240"
              change="12.4%"
            />

            <MetricCard
              icon={<Activity size={19} />}
              label="Pipeline latency"
              value="47 ms"
              change="4.8%"
            />

            <MetricCard
              icon={<ShieldCheck size={19} />}
              label="Data quality"
              value="99.8%"
              change="1.2%"
            />

            <MetricCard
              icon={<Boxes size={19} />}
              label="Records processed"
              value="1.2M"
              change="8.7%"
            />

          </div>

          {/* Lower Dashboard */}

          <div className="lower-grid">

            <ActivePipelines />

            <LatencyCard />

            <RecentAlerts />

          </div>

        </main>

        <footer className="footer">

          <div>
            <span className="footer-live"></span>
            Live
            <span>•</span>
            IceStream
            <span>•</span>
            Real-Time Lakehouse
          </div>

          <span>
            Last updated just now
          </span>

        </footer>

      </div>

    </div>
  );
}

export default App;