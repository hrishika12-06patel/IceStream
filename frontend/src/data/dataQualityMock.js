export const dataQualityMetrics = {
  overallScore: 98.6,
  completeness: 98.7,
  validity: 97.9,
  uniqueness: 99.8,
  schemaConsistency: 99.2,
};


export const issueCounts = {
  nullValues: 18,
  invalidPayments: 7,
  schemaViolations: 3,
  duplicateOrders: 2,
};


export const recentQualityIssues = [
  {
    id: 1,
    orderId: "ORD-1042",
    issue: "NULL value detected",
    field: "customer_id",
    severity: "critical",
    time: "2 min ago",
  },

  {
    id: 2,
    orderId: "ORD-1058",
    issue: "Invalid payment method",
    field: "payment_method",
    severity: "warning",
    time: "5 min ago",
  },

  {
    id: 3,
    orderId: "ORD-1063",
    issue: "Schema mismatch",
    field: "transaction_schema",
    severity: "critical",
    time: "9 min ago",
  },

  {
    id: 4,
    orderId: "ORD-1071",
    issue: "Duplicate order ID",
    field: "order_id",
    severity: "warning",
    time: "12 min ago",
  },

  {
    id: 5,
    orderId: "ORD-1084",
    issue: "Negative transaction value",
    field: "price",
    severity: "warning",
    time: "18 min ago",
  },
];