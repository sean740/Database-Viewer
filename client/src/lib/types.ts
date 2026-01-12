export interface DatabaseConnection {
  name: string;
  url: string;
}

export interface TableInfo {
  schema: string;
  name: string;
  fullName: string;
  displayName?: string | null;
  isVisible?: boolean;
}

export interface ColumnInfo {
  name: string;
  dataType: string;
  isNullable: boolean;
  isPrimaryKey: boolean;
}

export type FilterOperator = "eq" | "contains" | "gt" | "gte" | "lt" | "lte";

export interface FilterDefinition {
  id: string;
  name: string;
  column: string;
  operator: FilterOperator;
}

export interface ActiveFilter {
  column: string;
  operator: FilterOperator;
  value: string;
}

export interface QueryResponse {
  rows: Record<string, unknown>[];
  totalCount: number;
  page: number;
  pageSize: number;
  totalPages: number;
}

export interface NLQPlan {
  table: string;
  page: number;
  filters: Array<{
    column: string;
    op: FilterOperator;
    value: string;
  }>;
}

export const OPERATOR_LABELS: Record<FilterOperator, string> = {
  eq: "equals",
  contains: "contains",
  gt: "greater than",
  gte: "greater than or equal",
  lt: "less than",
  lte: "less than or equal",
};

export type UserRole = "admin" | "washos_user" | "external_customer";

export interface User {
  id: string;
  email: string | null;
  firstName: string | null;
  lastName: string | null;
  profileImageUrl: string | null;
  role: UserRole;
  isActive: boolean;
  createdAt: Date | null;
  updatedAt: Date | null;
}

export interface TableGrant {
  id: string;
  userId: string;
  database: string;
  tableName: string;
  grantedBy: string;
  grantedAt: Date | null;
}

export interface TableSettings {
  database: string;
  tableName: string;
  isVisible: boolean;
  displayName: string | null;
}
