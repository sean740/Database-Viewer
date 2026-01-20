import type { TableDataDictionary, ColumnStats } from "./data-dictionary";
import { formatDataDictionaryForPrompt } from "./data-dictionary";
import { formatRolesForPrompt } from "./roles";

export function getPacificDateString(): string {
  const today = new Date();
  const pacificFormatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Los_Angeles",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  });
  return pacificFormatter.format(today);
}

export function buildNLQSystemPrompt(params: {
  table: string;
  dictionary: TableDataDictionary | null;
  columns: Array<{ name: string; dataType: string }>;
  dateColumns: string[];
  context?: string;
}): string {
  const { table, dictionary, columns, dateColumns, context } = params;
  const todayStr = getPacificDateString();

  const columnInfo = dictionary
    ? formatDataDictionaryForPrompt(dictionary)
    : columns.map((c) => `  - ${c.name} (${c.dataType})`).join("\n");

  const rolesInfo = dictionary
    ? formatRolesForPrompt(dictionary.columns as ColumnStats[])
    : "";

  return `You are a helpful assistant that converts natural language queries into structured query plans for a database viewer.

IMPORTANT: Today's date is ${todayStr} (Pacific Time - PST/PDT). Use this for any relative date references like "yesterday", "last week", "this month", etc. All date/time queries should be interpreted in Pacific Time.

The user is querying the table: ${table}

${columnInfo}
${rolesInfo}

You must return ONLY a valid JSON object with this structure:
{
  "action": "plan" | "clarify" | "suggest",
  "table": "${table}",
  "page": 1,
  "filters": [
    {"column": "column_name", "op": "operator", "value": "filter_value"}
  ],
  "questions": ["question1", "question2"],
  "suggestions": [
    {"description": "description of suggested analysis", "filters": [...]}
  ],
  "explain": {
    "table": "${table}",
    "resolvedDateColumn": "column_name or null",
    "timeframe": {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD", "timezone": "America/Los_Angeles"},
    "filtersApplied": [{"column": "...", "operator": "...", "value": "...", "interpretation": "..."}],
    "sortApplied": null,
    "page": 1,
    "limit": 50
  },
  "summary": "A brief description of what this filter does"
}

ACTION TYPES:
- "plan": You understand the query and can create filters. Include filters, explain, and summary.
- "clarify": The query is ambiguous. Include 1-3 questions to clarify. Do NOT guess.
- "suggest": User asked for recommendations. Include 2-5 suggestions for useful analyses.

Valid operators are:
- eq: equals (exact match)
- contains: substring match (case-insensitive)
- gt: greater than
- gte: greater than or equal to
- lt: less than
- lte: less than or equal to
- between: range filter (value should be array like ["2024-01-01", "2024-12-31"])

IMPORTANT RULES:
1. Always use the table "${table}" - do not change it
2. Only use columns that exist in the column list above
3. For date queries with multiple date columns (${dateColumns.join(", ") || "none"}):
   - If user says "scheduled", "appointment", "booking" -> use scheduled_at/appointment_at if available
   - If user says "created", "added", "registered" -> use created_at if available
   - If user says "updated", "modified" -> use updated_at if available
   - If still ambiguous, set action: "clarify" and ask which date column to use
4. For date ranges like "last week", "this month", "2024", use the "between" operator with [start, end] array
5. Always include the "explain" object when action is "plan"
6. Only return valid JSON, no explanation or markdown

${context ? `Previous context from conversation:\n${context}\n` : ""}`;
}

export function buildSmartFollowupPrompt(params: {
  table: string;
  filters: Array<{ column: string; op: string; value: string }>;
  samplingInfo: string;
  context?: string;
}): string {
  const { table, filters, samplingInfo, context } = params;
  const todayStr = getPacificDateString();

  return `You are a helpful assistant that helps users find data when their query returns no results.

Today's date is ${todayStr} (Pacific Time - PST/PDT).

The user's query on table "${table}" returned 0 results with these filters:
${filters.map((f) => `- ${f.column} ${f.op} "${f.value}"`).join("\n")}

Here's what we found when sampling the actual data:
${samplingInfo}

ANALYZE the mismatch and return ONLY a valid JSON object:
{
  "likelyIssue": "value_mismatch" | "case_mismatch" | "date_out_of_range" | "null_column" | "synonym_mismatch" | "typo" | "unknown",
  "suggestedChanges": [
    {
      "filterIndex": 0,
      "column": "column_name",
      "currentValue": "what user searched",
      "suggestedValue": "corrected value",
      "suggestedOperator": "contains",
      "reason": "explanation"
    }
  ],
  "questions": ["optional clarifying questions"],
  "evidence": {
    "sampledValues": {"column_name": ["actual", "values"]},
    "dateRanges": {"date_column": {"min": "2023-01-01", "max": "2024-12-31"}}
  },
  "clarificationQuestion": "Main question to ask user",
  "suggestedFilters": [
    {"column": "...", "op": "...", "value": "..."}
  ],
  "summary": "Brief explanation of the issue"
}

RECOVERY STRATEGIES (apply in order):
1. Case/spacing mismatch: Suggest "contains" operator instead of "eq"
2. Synonym mismatch: If user searched "completed" but data has "done", suggest the actual value
3. Typo: Use Levenshtein-like matching to find similar values
4. Date out of range: If date filter is outside actual range, suggest valid range
5. Null column: If the filtered column is mostly null, suggest alternative column
6. Remove filter: Suggest removing one filter to isolate the culprit

${context ? `Previous context:\n${context}\n` : ""}`;
}

export function buildReportChatPrompt(params: {
  pageTitle: string;
  existingBlocks: Array<{ id: string; kind: string; title: string | null }>;
  availableTables: Array<{ database: string; table: string; displayName: string; columns: string[] }>;
  userRole: string;
  reportBrief?: {
    preferredTables?: string[];
    preferredTimeframe?: string;
    definitions?: Record<string, string>;
    preferredDimensions?: string[];
  };
}): string {
  const { pageTitle, existingBlocks, availableTables, userRole, reportBrief } = params;
  const todayStr = getPacificDateString();

  const tablesInfo = availableTables
    .map((t) => {
      const cols = t.columns.slice(0, 20).join(", ");
      const more = t.columns.length > 20 ? ` (and ${t.columns.length - 20} more)` : "";
      return `  - ${t.displayName || t.table} (${t.database}:${t.table})\n    Columns: ${cols}${more}`;
    })
    .join("\n");

  const blocksInfo =
    existingBlocks.length === 0
      ? "None"
      : existingBlocks.map((b) => `- ${b.kind}: ${b.title || "Untitled"} (id: ${b.id})`).join("\n");

  const briefInfo = reportBrief
    ? `
REPORT BRIEF (user preferences learned from conversation):
- Preferred tables: ${reportBrief.preferredTables?.join(", ") || "not set"}
- Preferred timeframe: ${reportBrief.preferredTimeframe || "not set"}
- Custom definitions: ${Object.entries(reportBrief.definitions || {}).map(([k, v]) => `${k}=${v}`).join(", ") || "none"}
- Preferred dimensions: ${reportBrief.preferredDimensions?.join(", ") || "not set"}
`
    : "";

  return `You are a helpful report building assistant. You help users create custom reports with tables, charts, and metrics.

IMPORTANT: Today's date is ${todayStr} (Pacific Time - PST/PDT). Use this for any relative date references like "yesterday", "last week", "this month", etc. All date/time queries should be interpreted in Pacific Time.

IMPORTANT: You MUST only use the exact column names listed below. Do NOT guess or invent column names.

AVAILABLE TABLES AND COLUMNS:
${tablesInfo}

CURRENT REPORT: "${pageTitle}"
CURRENT BLOCKS:
${blocksInfo}
${briefInfo}

You must return a JSON object with this structure:
{
  "action": "clarify" | "create_block" | "create_blocks" | "modify_block" | "delete_block" | "explain" | "none",
  "questions": ["question1", "question2"],
  "block": { "kind": "...", "title": "...", "config": {...} },
  "blocks": [{ "kind": "...", "title": "...", "config": {...} }],
  "blockId": "id of block to modify/delete",
  "explanation": "explanation text"
}

ACTION TYPES:
- "clarify": Query is ambiguous. Return 1-3 questions. Do NOT create blocks when unclear.
- "create_block": Create a single block. Include "block" object.
- "create_blocks": Create multiple blocks (for comparisons). Include "blocks" array.
- "modify_block": Update existing block. Include "blockId" and "block" with changes.
- "delete_block": Remove a block. Include "blockId".
- "explain": User asked a question. Include "explanation".
- "none": Just acknowledge or chat.

BLOCK CONFIGURATIONS:

For "table" blocks:
{
  "database": "DatabaseName",
  "table": "schema.table_name",
  "columns": ["col1", "col2"],
  "filters": [{"column": "...", "operator": "...", "value": "..."}],
  "orderBy": {"column": "...", "direction": "asc" | "desc"},
  "rowLimit": 100,
  "join": {"table": "schema.other_table", "on": ["local_column", "foreign_column"], "type": "left", "columns": ["joined_col1"]}
}

For "chart" blocks:
{
  "database": "DatabaseName",
  "table": "schema.table_name",
  "chartType": "bar" | "line" | "pie" | "area",
  "xColumn": "category_column",
  "yColumn": "value_column",
  "aggregateFunction": "count" | "sum" | "avg" | "min" | "max",
  "groupBy": "group_column",
  "filters": [...],
  "rowLimit": 1000
}

For "metric" blocks:
{
  "database": "DatabaseName",
  "table": "schema.table_name",
  "column": "value_column",
  "aggregateFunction": "count" | "sum" | "avg" | "min" | "max",
  "filters": [...],
  "label": "Display Label",
  "format": "number" | "currency" | "percentage"
}

FILTER OPERATORS: eq, contains, gt, gte, lt, lte, between
IMPORTANT: Do NOT use "in", "like", "!=", or any other operators.

For date comparisons (e.g., "compare last week to this week"):
- Create SEPARATE blocks for each time period
- Use action: "create_blocks" with an array

CRITICAL RULES:
1. Only use columns that exist in the schema above
2. Default database to "Default" if not specified
3. If user's request is unclear, use action: "clarify" and ask questions
4. For joined columns in filters/display, prefix with "joined." like "joined.vendor_name"
5. Return ONLY valid JSON, no prose before or after the JSON`;
}
