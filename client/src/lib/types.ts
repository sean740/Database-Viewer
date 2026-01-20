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

export type FilterOperator = "eq" | "contains" | "gt" | "gte" | "lt" | "lte" | "between";

export interface FilterDefinition {
  id: string;
  name: string;
  column: string;
  operator: FilterOperator;
}

export interface ActiveFilter {
  column: string;
  operator: FilterOperator;
  value: string | string[];
}

export interface QueryResponse {
  rows: Record<string, unknown>[];
  totalCount: number;
  page: number;
  pageSize: number;
  totalPages: number;
}

export type NLQAction = "clarify" | "plan" | "suggest";

export interface Timeframe {
  start: string;
  end: string;
  timezone: string;
  mode?: "rolling" | "calendar";
}

export interface NLQExplain {
  table: string;
  resolvedDateColumn?: string | null;
  timeframe?: Timeframe | null;
  filtersApplied?: Array<{
    column: string;
    operator: string;
    value: string;
    interpretation?: string;
  }>;
  sortApplied?: { column: string; direction: "asc" | "desc" } | null;
  page?: number;
  limit?: number;
}

export interface NLQSuggestion {
  description: string;
  filters?: Array<{ column: string; op: FilterOperator; value: string | string[] }>;
  chartType?: string;
}

export interface NLQPlan {
  action?: NLQAction;
  table: string;
  page: number;
  filters: Array<{
    column: string;
    op: FilterOperator;
    value: string | string[];
  }>;
  questions?: string[];
  suggestions?: NLQSuggestion[];
  explain?: NLQExplain;
  needsClarification?: boolean;
  clarificationQuestion?: string;
  ambiguousColumns?: string[];
  summary?: string;
}

export type SmartFollowupIssue = 
  | "value_mismatch"
  | "case_mismatch"
  | "date_out_of_range"
  | "null_column"
  | "synonym_mismatch"
  | "typo"
  | "unknown";

export interface SmartFollowupChange {
  filterIndex: number;
  column: string;
  currentValue: string;
  suggestedValue?: string;
  suggestedOperator?: string;
  reason: string;
}

export interface SmartFollowupResponse {
  likelyIssue: SmartFollowupIssue;
  suggestedChanges: SmartFollowupChange[];
  questions?: string[];
  evidence?: {
    sampledValues?: Record<string, string[]>;
    dateRanges?: Record<string, { min: string; max: string }>;
  };
  clarificationQuestion?: string;
  suggestedFilters?: Array<{ column: string; op: FilterOperator; value: string | string[] }>;
  summary?: string;
}

export const OPERATOR_LABELS: Record<FilterOperator, string> = {
  eq: "equals",
  contains: "contains",
  gt: "greater than",
  gte: "greater than or equal",
  lt: "less than",
  lte: "less than or equal",
  between: "between",
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
  hiddenColumns?: string[];
}

export interface FilterHistoryEntry {
  id: string;
  userId: string;
  database: string;
  table: string;
  filters: ActiveFilter[];
  lastUsedAt: string;
}
