import { z } from "zod";

// Database connection from environment
export const databaseConnectionSchema = z.object({
  name: z.string(),
  url: z.string(),
});

export type DatabaseConnection = z.infer<typeof databaseConnectionSchema>;

// Table info from database
export const tableInfoSchema = z.object({
  schema: z.string(),
  name: z.string(),
  fullName: z.string(), // schema.table
  displayName: z.string().nullable().optional(),
  isVisible: z.boolean().optional(),
});

export type TableInfo = z.infer<typeof tableInfoSchema>;

// Column info from database
export const columnInfoSchema = z.object({
  name: z.string(),
  dataType: z.string(),
  isNullable: z.boolean(),
  isPrimaryKey: z.boolean(),
});

export type ColumnInfo = z.infer<typeof columnInfoSchema>;

// Filter operator types
export const filterOperatorSchema = z.enum(["eq", "contains", "gt", "gte", "lt", "lte", "between", "in"]);
export type FilterOperator = z.infer<typeof filterOperatorSchema>;

// Filter definition (admin-configured)
export const filterDefinitionSchema = z.object({
  id: z.string(),
  name: z.string(),
  column: z.string(),
  operator: filterOperatorSchema,
});

export type FilterDefinition = z.infer<typeof filterDefinitionSchema>;

// Active filter (user-applied value)
export const activeFilterSchema = z.object({
  column: z.string(),
  operator: filterOperatorSchema,
  value: z.string(),
});

export type ActiveFilter = z.infer<typeof activeFilterSchema>;

// Query request for fetching rows
export const queryRequestSchema = z.object({
  database: z.string(),
  table: z.string(),
  page: z.number().int().positive().default(1),
  filters: z.array(activeFilterSchema).optional(),
});

export type QueryRequest = z.infer<typeof queryRequestSchema>;

// Query response
export const queryResponseSchema = z.object({
  rows: z.array(z.record(z.unknown())),
  totalCount: z.number(),
  page: z.number(),
  pageSize: z.number(),
  totalPages: z.number(),
});

export type QueryResponse = z.infer<typeof queryResponseSchema>;

// NLQ (Natural Language Query) request
export const nlqRequestSchema = z.object({
  database: z.string(),
  table: z.string().optional(),
  query: z.string(),
  context: z.string().optional(),
});

export type NLQRequest = z.infer<typeof nlqRequestSchema>;

// NLQ action enum for clarify/plan/suggest flow
export const nlqActionSchema = z.enum(["clarify", "plan", "suggest"]);
export type NLQAction = z.infer<typeof nlqActionSchema>;

// Timeframe object for date range queries
export const timeframeSchema = z.object({
  start: z.string(),
  end: z.string(),
  timezone: z.string().default("America/Los_Angeles"),
  mode: z.enum(["rolling", "calendar"]).optional(),
});
export type Timeframe = z.infer<typeof timeframeSchema>;

// NLQ filter with support for between operator (array values)
export const nlqFilterSchema = z.object({
  column: z.string(),
  op: filterOperatorSchema,
  value: z.union([z.string(), z.array(z.string())]),
});
export type NLQFilter = z.infer<typeof nlqFilterSchema>;

// NLQ explain object for transparency
export const nlqExplainSchema = z.object({
  table: z.string(),
  resolvedDateColumn: z.string().nullable().optional(),
  timeframe: timeframeSchema.nullable().optional(),
  filtersApplied: z.array(z.object({
    column: z.string(),
    operator: z.string(),
    value: z.union([z.string(), z.array(z.string())]),
    interpretation: z.string().optional(),
  })).optional(),
  sortApplied: z.object({
    column: z.string(),
    direction: z.enum(["asc", "desc"]),
  }).nullable().optional(),
  page: z.number().optional(),
  limit: z.number().optional(),
});
export type NLQExplain = z.infer<typeof nlqExplainSchema>;

// NLQ suggestion for "suggest" action
export const nlqSuggestionSchema = z.object({
  description: z.string(),
  filters: z.array(nlqFilterSchema).optional(),
  chartType: z.string().optional(),
});
export type NLQSuggestion = z.infer<typeof nlqSuggestionSchema>;

// NLQ parsed plan from OpenAI (extended with action, questions, explain, suggestions)
export const nlqPlanSchema = z.object({
  action: nlqActionSchema.optional().default("plan"),
  table: z.string(),
  page: z.number().default(1),
  filters: z.array(z.object({
    column: z.string(),
    op: filterOperatorSchema,
    value: z.union([z.string(), z.array(z.string())]),
  })),
  questions: z.array(z.string()).optional(),
  suggestions: z.array(nlqSuggestionSchema).optional(),
  explain: nlqExplainSchema.optional(),
  needsClarification: z.boolean().optional(),
  clarificationQuestion: z.string().optional(),
  ambiguousColumns: z.array(z.string()).optional(),
  summary: z.string().optional(),
});

export type NLQPlan = z.infer<typeof nlqPlanSchema>;

// Smart follow-up issue types
export const smartFollowupIssueSchema = z.enum([
  "value_mismatch",
  "case_mismatch",
  "date_out_of_range",
  "null_column",
  "synonym_mismatch",
  "typo",
  "unknown",
]);
export type SmartFollowupIssue = z.infer<typeof smartFollowupIssueSchema>;

// Smart follow-up suggested change
export const smartFollowupChangeSchema = z.object({
  filterIndex: z.number(),
  column: z.string(),
  currentValue: z.string(),
  suggestedValue: z.string().optional(),
  suggestedOperator: z.string().optional(),
  reason: z.string(),
});
export type SmartFollowupChange = z.infer<typeof smartFollowupChangeSchema>;

// Smart follow-up response
export const smartFollowupResponseSchema = z.object({
  likelyIssue: smartFollowupIssueSchema,
  suggestedChanges: z.array(smartFollowupChangeSchema),
  questions: z.array(z.string()).optional(),
  evidence: z.object({
    sampledValues: z.record(z.string(), z.array(z.string())).optional(),
    dateRanges: z.record(z.string(), z.object({ min: z.string(), max: z.string() })).optional(),
  }).optional(),
  clarificationQuestion: z.string().optional(),
  suggestedFilters: z.array(nlqFilterSchema).optional(),
  summary: z.string().optional(),
});
export type SmartFollowupResponse = z.infer<typeof smartFollowupResponseSchema>;

// Filters config file structure
export const filtersConfigSchema = z.record(z.string(), z.array(filterDefinitionSchema));
export type FiltersConfig = z.infer<typeof filtersConfigSchema>;

// Filter history entry (user's recent filters per table)
export const filterHistoryEntrySchema = z.object({
  id: z.string(),
  userId: z.string(),
  database: z.string(),
  table: z.string(),
  filters: z.array(activeFilterSchema),
  lastUsedAt: z.string(), // ISO timestamp
});

export type FilterHistoryEntry = z.infer<typeof filterHistoryEntrySchema>;

// Export auth models
export * from "./models/auth";
