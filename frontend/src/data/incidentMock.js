export const incidents = [
  {
    id: "INC-001",
    orderId: "ORD-1042",
    title: "NULL customer ID detected",
    description:
      "Transaction failed completeness validation because customer_id was NULL.",
    field: "customer_id",
    type: "completeness",
    severity: "critical",
    stage: "Flink",
    status: "open",
    detectedAt: "2 min ago",
  },

  {
    id: "INC-002",
    orderId: "ORD-1058",
    title: "Invalid payment method",
    description:
      "Transaction contained a payment method outside the accepted values.",
    field: "payment_method",
    type: "validity",
    severity: "warning",
    stage: "Flink",
    status: "investigating",
    detectedAt: "7 min ago",
  },

  {
    id: "INC-003",
    orderId: "ORD-1063",
    title: "Schema mismatch",
    description:
      "Incoming transaction did not match the expected IceStream transaction schema.",
    field: "transaction_schema",
    type: "schema",
    severity: "critical",
    stage: "Kafka → Flink",
    status: "open",
    detectedAt: "11 min ago",
  },

  {
    id: "INC-004",
    orderId: "ORD-1071",
    title: "Duplicate order ID",
    description:
      "An order_id already present in the stream appeared again.",
    field: "order_id",
    type: "uniqueness",
    severity: "warning",
    stage: "Flink",
    status: "resolved",
    detectedAt: "18 min ago",
  },

  {
    id: "INC-005",
    orderId: "ORD-1084",
    title: "Negative transaction price",
    description:
      "Transaction failed range validation because price was below zero.",
    field: "price",
    type: "range",
    severity: "warning",
    stage: "Flink",
    status: "investigating",
    detectedAt: "24 min ago",
  },

  {
    id: "INC-006",
    orderId: "ORD-1093",
    title: "Invalid timestamp",
    description:
      "The transaction timestamp was not a valid ISO 8601 datetime.",
    field: "timestamp",
    type: "validity",
    severity: "info",
    stage: "Flink",
    status: "resolved",
    detectedAt: "31 min ago",
  },
];