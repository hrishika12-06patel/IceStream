import {
  useMemo,
  useState,
} from "react";

import {
  Activity,
  AlertTriangle,
  Gauge,
  ShieldCheck,
  Zap,
} from "lucide-react";

import {
  Area,
  AreaChart,
  CartesianGrid,
  Line,
  LineChart,
  ResponsiveContainer,
  Tooltip,
  XAxis,
  YAxis,
} from "recharts";

import {
  pipelineHistory,
} from "../data/historyMock";


function HistorySummaryCard({
  icon,
  label,
  value,
  description,
}) {
  return (
    <div className="history-summary-card">

      <div className="history-summary-icon">
        {icon}
      </div>

      <div>

        <span>
          {label}
        </span>

        <strong>
          {value}
        </strong>

        <small>
          {description}
        </small>

      </div>

    </div>
  );
}


function HistoryView() {
  const [timeRange, setTimeRange] =
    useState("6h");


  const filteredHistory =
    useMemo(() => {

      if (timeRange === "24h") {
        return pipelineHistory;
      }

      if (timeRange === "6h") {
        return pipelineHistory.filter(
          (item) =>
            item.range === "6h" ||
            item.range === "1h"
        );
      }

      return pipelineHistory.filter(
        (item) =>
          item.range === "1h"
      );

    }, [timeRange]);


  const summary =
    useMemo(() => {

      if (filteredHistory.length === 0) {
        return {
          peakThroughput: 0,
          averageLatency: 0,
          averageQuality: 0,
          totalIncidents: 0,
        };
      }


      const peakThroughput =
        Math.max(
          ...filteredHistory.map(
            (item) =>
              item.eventsPerSecond
          )
        );


      const averageLatency =
        filteredHistory.reduce(
          (total, item) =>
            total + item.latency,
          0
        ) /
        filteredHistory.length;


      const averageQuality =
        filteredHistory.reduce(
          (total, item) =>
            total + item.dataQuality,
          0
        ) /
        filteredHistory.length;


      const totalIncidents =
        filteredHistory.reduce(
          (total, item) =>
            total + item.incidents,
          0
        );


      return {
        peakThroughput,
        averageLatency,
        averageQuality,
        totalIncidents,
      };

    }, [filteredHistory]);


  return (
    <div className="history-view">

      {/* =========================
          HEADING
      ========================= */}

      <div className="history-heading">

        <div>

          <div className="eyebrow">
            PERFORMANCE ANALYTICS
          </div>

          <h2>
            Pipeline history
          </h2>

          <p>
            Review historical throughput,
            latency, data quality and incidents.
          </p>

        </div>


        <div className="history-range-selector">

          <button
            type="button"
            className={
              timeRange === "1h"
                ? "history-range active"
                : "history-range"
            }
            onClick={() =>
              setTimeRange("1h")
            }
          >
            1H
          </button>


          <button
            type="button"
            className={
              timeRange === "6h"
                ? "history-range active"
                : "history-range"
            }
            onClick={() =>
              setTimeRange("6h")
            }
          >
            6H
          </button>


          <button
            type="button"
            className={
              timeRange === "24h"
                ? "history-range active"
                : "history-range"
            }
            onClick={() =>
              setTimeRange("24h")
            }
          >
            24H
          </button>

        </div>

      </div>


      {/* =========================
          SUMMARY CARDS
      ========================= */}

      <div className="history-summary-grid">

        <HistorySummaryCard
          icon={<Zap size={18} />}
          label="Peak throughput"
          value={
            `${summary.peakThroughput.toLocaleString()}/s`
          }
          description="Highest ingestion rate"
        />


        <HistorySummaryCard
          icon={<Gauge size={18} />}
          label="Average latency"
          value={
            `${summary.averageLatency.toFixed(1)} ms`
          }
          description="Average pipeline latency"
        />


        <HistorySummaryCard
          icon={<ShieldCheck size={18} />}
          label="Average quality"
          value={
            `${summary.averageQuality.toFixed(1)}%`
          }
          description="Historical quality score"
        />


        <HistorySummaryCard
          icon={
            <AlertTriangle size={18} />
          }
          label="Incidents"
          value={
            summary.totalIncidents
          }
          description="Incidents in selected period"
        />

      </div>


      {/* =========================
          THROUGHPUT
      ========================= */}

      <div className="history-chart-grid">

        <div className="dashboard-card history-chart-card">

          <div className="card-header">

            <div>

              <h3>
                Throughput trend
              </h3>

              <span>
                Events processed per second
              </span>

            </div>

            <Zap size={16} />

          </div>


          <div className="history-chart">

            <ResponsiveContainer
              width="100%"
              height="100%"
            >

              <AreaChart
                data={filteredHistory}
              >

                <CartesianGrid
                  strokeDasharray="3 3"
                  vertical={false}
                  stroke="#17243a"
                />

                <XAxis
                  dataKey="time"
                  stroke="#52627b"
                  tick={{
                    fontSize: 9,
                  }}
                />

                <YAxis
                  stroke="#52627b"
                  tick={{
                    fontSize: 9,
                  }}
                />

                <Tooltip
                  contentStyle={{
                    background: "#0d1728",
                    border:
                      "1px solid #24334b",
                    borderRadius: "8px",
                    fontSize: "10px",
                  }}
                />

                <Area
                  type="monotone"
                  dataKey="eventsPerSecond"
                  stroke="#3b82f6"
                  fill="#2563eb"
                  fillOpacity={0.12}
                  strokeWidth={2}
                />

              </AreaChart>

            </ResponsiveContainer>

          </div>

        </div>


        {/* =========================
            LATENCY
        ========================= */}

        <div className="dashboard-card history-chart-card">

          <div className="card-header">

            <div>

              <h3>
                Latency trend
              </h3>

              <span>
                Pipeline latency in milliseconds
              </span>

            </div>

            <Activity size={16} />

          </div>


          <div className="history-chart">

            <ResponsiveContainer
              width="100%"
              height="100%"
            >

              <LineChart
                data={filteredHistory}
              >

                <CartesianGrid
                  strokeDasharray="3 3"
                  vertical={false}
                  stroke="#17243a"
                />

                <XAxis
                  dataKey="time"
                  stroke="#52627b"
                  tick={{
                    fontSize: 9,
                  }}
                />

                <YAxis
                  stroke="#52627b"
                  tick={{
                    fontSize: 9,
                  }}
                />

                <Tooltip
                  contentStyle={{
                    background: "#0d1728",
                    border:
                      "1px solid #24334b",
                    borderRadius: "8px",
                    fontSize: "10px",
                  }}
                />

                <Line
                  type="monotone"
                  dataKey="latency"
                  stroke="#8b5cf6"
                  strokeWidth={2}
                  dot={{
                    r: 3,
                  }}
                />

              </LineChart>

            </ResponsiveContainer>

          </div>

        </div>


        {/* =========================
            DATA QUALITY
        ========================= */}

        <div className="dashboard-card history-chart-card">

          <div className="card-header">

            <div>

              <h3>
                Data quality trend
              </h3>

              <span>
                Historical validation score
              </span>

            </div>

            <ShieldCheck size={16} />

          </div>


          <div className="history-chart">

            <ResponsiveContainer
              width="100%"
              height="100%"
            >

              <LineChart
                data={filteredHistory}
              >

                <CartesianGrid
                  strokeDasharray="3 3"
                  vertical={false}
                  stroke="#17243a"
                />

                <XAxis
                  dataKey="time"
                  stroke="#52627b"
                  tick={{
                    fontSize: 9,
                  }}
                />

                <YAxis
                  domain={[95, 100]}
                  stroke="#52627b"
                  tick={{
                    fontSize: 9,
                  }}
                />

                <Tooltip
                  contentStyle={{
                    background: "#0d1728",
                    border:
                      "1px solid #24334b",
                    borderRadius: "8px",
                    fontSize: "10px",
                  }}
                />

                <Line
                  type="monotone"
                  dataKey="dataQuality"
                  stroke="#22c55e"
                  strokeWidth={2}
                  dot={{
                    r: 3,
                  }}
                />

              </LineChart>

            </ResponsiveContainer>

          </div>

        </div>


        {/* =========================
            INCIDENTS
        ========================= */}

        <div className="dashboard-card history-chart-card">

          <div className="card-header">

            <div>

              <h3>
                Incident trend
              </h3>

              <span>
                Incidents over time
              </span>

            </div>

            <AlertTriangle size={16} />

          </div>


          <div className="history-chart">

            <ResponsiveContainer
              width="100%"
              height="100%"
            >

              <AreaChart
                data={filteredHistory}
              >

                <CartesianGrid
                  strokeDasharray="3 3"
                  vertical={false}
                  stroke="#17243a"
                />

                <XAxis
                  dataKey="time"
                  stroke="#52627b"
                  tick={{
                    fontSize: 9,
                  }}
                />

                <YAxis
                  allowDecimals={false}
                  stroke="#52627b"
                  tick={{
                    fontSize: 9,
                  }}
                />

                <Tooltip
                  contentStyle={{
                    background: "#0d1728",
                    border:
                      "1px solid #24334b",
                    borderRadius: "8px",
                    fontSize: "10px",
                  }}
                />

                <Area
                  type="monotone"
                  dataKey="incidents"
                  stroke="#f59e0b"
                  fill="#f59e0b"
                  fillOpacity={0.1}
                  strokeWidth={2}
                />

              </AreaChart>

            </ResponsiveContainer>

          </div>

        </div>

      </div>

    </div>
  );
}


export default HistoryView;