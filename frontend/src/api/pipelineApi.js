const initialPipelineData = {
  services: {
    kafka: {
      name: "Kafka",
      status: "healthy",
      eventsPerSecond: 1240,
      latency: 8,
    },

    flink: {
      name: "Flink",
      status: "healthy",
      processingRate: 1238,
      latency: 16,
    },

    iceberg: {
      name: "Iceberg",
      status: "healthy",
      recordsStored: 1200000,
      latency: 23,
    },
  },

  metrics: {
    eventsPerSecond: 1240,
    pipelineLatency: 47,
    dataQuality: 99.8,
    recordsProcessed: 1200000,
  },

  alerts: [
    {
      id: 1,
      type: "warning",
      title: "Consumer lag detected",
      message: "Kafka consumer lag increased slightly.",
      time: "2 min ago",
    },

    {
      id: 2,
      type: "info",
      title: "Pipeline processing normally",
      message: "Flink processing rate is stable.",
      time: "8 min ago",
    },

    {
      id: 3,
      type: "success",
      title: "Iceberg write completed",
      message: "Latest batch stored successfully.",
      time: "14 min ago",
    },
  ],
};


export async function getPipelineStatus() {
  await new Promise((resolve) => setTimeout(resolve, 300));

  return JSON.parse(JSON.stringify(initialPipelineData));
}


export function simulatePipelineUpdate(data) {
  const updated = structuredClone(data);

  const kafkaRate =
    1180 + Math.floor(Math.random() * 180);

  const flinkRate =
    Math.max(
      1100,
      kafkaRate - Math.floor(Math.random() * 15)
    );

  updated.services.kafka.eventsPerSecond = kafkaRate;

  updated.services.flink.processingRate = flinkRate;

  updated.metrics.eventsPerSecond = kafkaRate;

  updated.metrics.pipelineLatency =
    42 + Math.floor(Math.random() * 12);

  updated.metrics.recordsProcessed +=
    Math.floor(Math.random() * 500);

  updated.services.iceberg.recordsStored =
    updated.metrics.recordsProcessed;

  return updated;
}