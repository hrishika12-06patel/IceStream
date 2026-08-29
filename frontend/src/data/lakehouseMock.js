export const lakehouseMetrics = {
  tableName: "icestream.transactions",
  namespace: "icestream",
  table: "transactions",
  format: "Apache Iceberg",
  status: "healthy",

  recordsStored: 1248920,
  snapshotCount: 24,
  tableSize: "486 MB",

  latestSnapshot: "SNAP-024",
  lastCommit: "1 min ago",

  schemaVersion: 1,
  partitionCount: 3,
};


export const storageHealth = {
  tableHealth: "healthy",
  schemaStatus: "current",
  catalogStatus: "connected",
  writeStatus: "active",
};


export const recentSnapshots = [
  {
    id: "SNAP-024",
    operation: "append",
    recordsAdded: 1240,
    totalRecords: 1248920,
    status: "committed",
    timestamp: "1 min ago",
  },

  {
    id: "SNAP-023",
    operation: "append",
    recordsAdded: 1218,
    totalRecords: 1247680,
    status: "committed",
    timestamp: "6 min ago",
  },

  {
    id: "SNAP-022",
    operation: "append",
    recordsAdded: 1197,
    totalRecords: 1246462,
    status: "committed",
    timestamp: "11 min ago",
  },

  {
    id: "SNAP-021",
    operation: "append",
    recordsAdded: 1234,
    totalRecords: 1245265,
    status: "committed",
    timestamp: "16 min ago",
  },

  {
    id: "SNAP-020",
    operation: "append",
    recordsAdded: 1179,
    totalRecords: 1244031,
    status: "committed",
    timestamp: "21 min ago",
  },
];