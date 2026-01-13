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
export const filterOperatorSchema = z.enum(["eq", "contains", "gt", "gte", "lt", "lte", "between"]);
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
});

export type NLQRequest = z.infer<typeof nlqRequestSchema>;

// NLQ parsed plan from OpenAI
export const nlqPlanSchema = z.object({
  table: z.string(),
  page: z.number().default(1),
  filters: z.array(z.object({
    column: z.string(),
    op: filterOperatorSchema,
    value: z.string(),
  })),
  needsClarification: z.boolean().optional(),
  clarificationQuestion: z.string().optional(),
  ambiguousColumns: z.array(z.string()).optional(),
  summary: z.string().optional(),
});

export type NLQPlan = z.infer<typeof nlqPlanSchema>;

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
