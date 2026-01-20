import { z } from "zod";

export const nlqActionSchema = z.enum(["clarify", "plan", "suggest"]);
export type NLQAction = z.infer<typeof nlqActionSchema>;

export const timeframeSchema = z.object({
  start: z.string(),
  end: z.string(),
  timezone: z.string().default("America/Los_Angeles"),
  mode: z.enum(["rolling", "calendar"]).optional(),
});
export type Timeframe = z.infer<typeof timeframeSchema>;

export const nlqFilterSchema = z.object({
  column: z.string(),
  op: z.enum(["eq", "contains", "gt", "gte", "lt", "lte", "between"]),
  value: z.union([z.string(), z.array(z.string())]),
});
export type NLQFilter = z.infer<typeof nlqFilterSchema>;

export const nlqExplainSchema = z.object({
  table: z.string(),
  resolvedDateColumn: z.string().nullable().optional(),
  timeframe: timeframeSchema.nullable().optional(),
  filtersApplied: z.array(z.object({
    column: z.string(),
    operator: z.string(),
    value: z.string(),
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

export const nlqResponseSchema = z.object({
  action: nlqActionSchema,
  table: z.string(),
  page: z.number().default(1),
  filters: z.array(nlqFilterSchema).default([]),
  questions: z.array(z.string()).optional(),
  suggestions: z.array(z.object({
    description: z.string(),
    filters: z.array(nlqFilterSchema).optional(),
    chartType: z.string().optional(),
  })).optional(),
  explain: nlqExplainSchema.optional(),
  summary: z.string().optional(),
  needsClarification: z.boolean().optional(),
  clarificationQuestion: z.string().optional(),
  ambiguousColumns: z.array(z.string()).optional(),
});
export type NLQResponse = z.infer<typeof nlqResponseSchema>;

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

export const smartFollowupResponseSchema = z.object({
  likelyIssue: smartFollowupIssueSchema,
  suggestedChanges: z.array(z.object({
    filterIndex: z.number(),
    column: z.string(),
    currentValue: z.string(),
    suggestedValue: z.string().optional(),
    suggestedOperator: z.string().optional(),
    reason: z.string(),
  })),
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

export const reportBlockActionSchema = z.object({
  kind: z.enum(["table", "chart", "metric", "text"]),
  title: z.string(),
  config: z.record(z.unknown()),
});

export const reportChatResponseSchema = z.object({
  action: z.enum(["clarify", "create_block", "create_blocks", "modify_block", "delete_block", "explain", "none"]),
  questions: z.array(z.string()).optional(),
  block: reportBlockActionSchema.optional(),
  blocks: z.array(reportBlockActionSchema).optional(),
  blockId: z.string().optional(),
  explanation: z.string().optional(),
});
export type ReportChatResponse = z.infer<typeof reportChatResponseSchema>;

function extractJSON(text: string): string | null {
  const cleaned = text.replace(/```json\n?|\n?```/g, "").trim();
  
  const jsonMatch = cleaned.match(/\{[\s\S]*\}/);
  if (jsonMatch) {
    return jsonMatch[0];
  }
  return null;
}

export async function parseAndValidateNLQResponse(
  content: string,
  table: string,
  validColumns: string[],
  retryFn?: () => Promise<string>
): Promise<{ success: true; data: NLQResponse } | { success: false; error: string }> {
  const jsonStr = extractJSON(content);
  if (!jsonStr) {
    if (retryFn) {
      const retryContent = await retryFn();
      return parseAndValidateNLQResponse(retryContent, table, validColumns);
    }
    return { success: false, error: "Failed to extract JSON from response" };
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(jsonStr);
  } catch {
    if (retryFn) {
      const retryContent = await retryFn();
      return parseAndValidateNLQResponse(retryContent, table, validColumns);
    }
    return { success: false, error: "Failed to parse JSON response" };
  }

  const result = nlqResponseSchema.safeParse(parsed);
  if (!result.success) {
    if (retryFn) {
      const retryContent = await retryFn();
      return parseAndValidateNLQResponse(retryContent, table, validColumns);
    }
    return { success: false, error: `Invalid response schema: ${result.error.message}` };
  }

  const data = result.data;
  data.table = table;

  if (data.action === "plan" && data.filters) {
    for (const filter of data.filters) {
      if (validColumns.length > 0 && !validColumns.includes(filter.column)) {
        return { success: false, error: `Invalid column: ${filter.column}` };
      }
    }
  }

  return { success: true, data };
}

export async function parseAndValidateSmartFollowupResponse(
  content: string,
  retryFn?: () => Promise<string>
): Promise<{ success: true; data: SmartFollowupResponse } | { success: false; error: string }> {
  const jsonStr = extractJSON(content);
  if (!jsonStr) {
    if (retryFn) {
      const retryContent = await retryFn();
      return parseAndValidateSmartFollowupResponse(retryContent);
    }
    return { success: false, error: "Failed to extract JSON from response" };
  }

  let parsed: unknown;
  try {
    parsed = JSON.parse(jsonStr);
  } catch {
    if (retryFn) {
      const retryContent = await retryFn();
      return parseAndValidateSmartFollowupResponse(retryContent);
    }
    return { success: false, error: "Failed to parse JSON response" };
  }

  const result = smartFollowupResponseSchema.safeParse(parsed);
  if (!result.success) {
    if (retryFn) {
      const retryContent = await retryFn();
      return parseAndValidateSmartFollowupResponse(retryContent);
    }
    return { success: false, error: `Invalid response schema: ${result.error.message}` };
  }

  return { success: true, data: result.data };
}

export function parseReportChatAction(content: string): ReportChatResponse | null {
  const jsonStr = extractJSON(content);
  if (!jsonStr) return null;

  try {
    const parsed = JSON.parse(jsonStr);
    const result = reportChatResponseSchema.safeParse(parsed);
    if (result.success) {
      return result.data;
    }
  } catch {
    // Not valid JSON or doesn't match schema
  }
  return null;
}
