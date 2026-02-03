import type { Express, Request, Response } from "express";
import { createServer, type Server } from "http";
import { Pool } from "pg";
import OpenAI from "openai";
import rateLimit from "express-rate-limit";
import { eq, and, desc, count } from "drizzle-orm";
import { storage } from "./storage";
import { db } from "./db";
import { getStripeMetricsForWeek, checkStripeConnection, type StripeWeeklyMetrics } from "./stripeClient";
import { 
  calculateOperationsMetrics, 
  calculateOperationsVariance, 
  OPERATIONS_METRIC_SPECS,
  getAllOperationsMetricSpecs,
  getOperationsMetricSpec,
  type OperationsPeriodMetrics 
} from "./operationsMetrics";
import {
  getCacheKey,
  isCurrentPeriod,
  getCacheDuration,
  getFromCache,
  setInCache,
} from "./dashboardCache";
import { setupAuth, registerAuthRoutes, isAuthenticated, authStorage } from "./replit_integrations/auth";
import { 
  users, 
  tableGrants, 
  auditLogs, 
  reportPages, 
  reportBlocks, 
  reportChatSessions,
  type UserRole, 
  type User,
  type ReportPage,
  type ReportBlock,
  type ReportBlockConfig,
  type TableBlockConfig,
  type ChartBlockConfig,
  type MetricBlockConfig,
  type ChatMessage,
} from "@shared/schema";
import type {
  DatabaseConnection,
  TableInfo,
  ColumnInfo,
  ActiveFilter,
  FilterOperator,
  NLQPlan,
} from "@shared/schema";
import {
  getOpenAIClient,
  AI_CONFIG,
  getTableDataDictionary,
  formatDataDictionaryForPrompt,
  getDateColumns,
  getBestDateColumn,
  resolveSemanticReference,
  formatRolesForPrompt,
  buildNLQSystemPrompt,
  buildSmartFollowupPrompt,
  parseAndValidateNLQResponse,
  parseAndValidateSmartFollowupResponse,
  type TableDataDictionary,
} from "./ai";

const PAGE_SIZE = 50;

// Strict SQL identifier validation
const IDENTIFIER_REGEX = /^[a-zA-Z_][a-zA-Z0-9_]*$/;

function isValidIdentifier(name: string): boolean {
  return IDENTIFIER_REGEX.test(name) && name.length <= 128;
}

function validateIdentifier(name: string, type: string): void {
  if (!isValidIdentifier(name)) {
    throw new Error(`Invalid ${type} identifier: ${name}`);
  }
}

// Parse DATABASE_URLS from environment
// Supports two formats:
// 1. JSON array: [{"name":"Production","url":"postgres://..."},{"name":"Staging","url":"postgres://..."}]
// 2. Single connection string: postgres://user:pass@host:5432/dbname (will be named "Default")
function getDatabaseConnections(): DatabaseConnection[] {
  const urlsEnv = process.env.DATABASE_URLS;
  if (!urlsEnv) {
    return [];
  }

  const trimmed = urlsEnv.trim();

  // Check if it looks like a JSON array
  if (trimmed.startsWith("[")) {
    try {
      const parsed = JSON.parse(trimmed);
      if (!Array.isArray(parsed)) {
        console.error("DATABASE_URLS JSON must be an array");
        return [];
      }
      return parsed.filter((db) => db.name && db.url);
    } catch (err) {
      console.error("Failed to parse DATABASE_URLS as JSON:", err);
      return [];
    }
  }

  // Otherwise treat it as a single connection string
  if (trimmed.startsWith("postgres://") || trimmed.startsWith("postgresql://")) {
    return [{ name: "Default", url: trimmed }];
  }

  console.error("DATABASE_URLS must be a JSON array or a valid postgres:// connection string");
  return [];
}

// Connection pool cache
const pools: Map<string, Pool> = new Map();

function getPool(dbName: string): Pool {
  const existing = pools.get(dbName);
  if (existing) return existing;

  const dbs = getDatabaseConnections();
  const db = dbs.find((d) => d.name === dbName);
  if (!db) {
    throw new Error(`Database not found: ${dbName}`);
  }

  // SSL configuration - verify certificates in production for security
  const sslConfig = process.env.DB_SSL_REJECT_UNAUTHORIZED === "false" 
    ? { rejectUnauthorized: false }
    : { rejectUnauthorized: true };

  const pool = new Pool({
    connectionString: db.url,
    max: 5,
    idleTimeoutMillis: 30000,
    ssl: sslConfig,
  });

  pools.set(dbName, pool);
  return pool;
}

// Convert a PST/PDT date/datetime string to UTC timestamp for database queries
// Input: "2026-01-12" or "2026-01-12 00:00:00" or "2026-01-12 23:59:59" (interpreted as PST)
// Output: UTC equivalent timestamp
// For end dates, we want end of day PST which is next day 07:59:59 UTC
function convertPSTDateToUTC(dateStr: string, isEndOfRange: boolean = false): string {
  // Check if it's a simple date string (YYYY-MM-DD format)
  const dateOnlyMatch = dateStr.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (dateOnlyMatch) {
    const [, year, month, day] = dateOnlyMatch;
    
    // PST is UTC-8, PDT is UTC-7
    // For simplicity, we'll use a fixed offset approach
    // Create date in PST context and convert to UTC
    if (isEndOfRange) {
      // End of day in PST: 23:59:59.999 PST = next day 07:59:59.999 UTC (during PST)
      // We'll use 08:00:00 of the next day as the exclusive upper bound
      const date = new Date(`${year}-${month}-${day}T00:00:00-08:00`);
      date.setDate(date.getDate() + 1); // Move to next day
      return date.toISOString();
    } else {
      // Start of day in PST: 00:00:00 PST = 08:00:00 UTC (during PST)
      const date = new Date(`${year}-${month}-${day}T00:00:00-08:00`);
      return date.toISOString();
    }
  }
  
  // Check if it's a datetime string (YYYY-MM-DD HH:MM:SS format) - AI often generates these
  const dateTimeMatch = dateStr.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})$/);
  if (dateTimeMatch) {
    const [, year, month, day, hour, minute, second] = dateTimeMatch;
    
    // If it's 00:00:00 treat as start of day, if 23:59:59 treat as end of day
    if (hour === "00" && minute === "00" && second === "00") {
      // Start of day PST -> convert to UTC
      const date = new Date(`${year}-${month}-${day}T00:00:00-08:00`);
      return date.toISOString();
    } else if (hour === "23" && minute === "59" && second === "59") {
      // End of day PST -> convert to UTC (use start of next day as exclusive bound)
      const date = new Date(`${year}-${month}-${day}T00:00:00-08:00`);
      date.setDate(date.getDate() + 1); // Move to next day
      return date.toISOString();
    } else {
      // Specific time in PST -> convert to UTC
      const date = new Date(`${year}-${month}-${day}T${hour}:${minute}:${second}-08:00`);
      return date.toISOString();
    }
  }
  
  // Not a recognized date format, return as-is
  return dateStr;
}

// Build operator SQL
function getOperatorSQL(
  operator: FilterOperator,
  paramIndex: number
): { sql: string; transform?: (v: any) => any; paramCount?: number } {
  switch (operator) {
    case "eq":
      return { sql: `= $${paramIndex}` };
    case "contains":
      return {
        sql: `ILIKE $${paramIndex}`,
        transform: (v) => `%${v}%`,
      };
    case "gt":
      return { sql: `> $${paramIndex}` };
    case "gte":
      return { sql: `>= $${paramIndex}` };
    case "lt":
      return { sql: `< $${paramIndex}` };
    case "lte":
      return { sql: `<= $${paramIndex}` };
    case "between":
      return { 
        sql: `BETWEEN $${paramIndex} AND $${paramIndex + 1}`,
        paramCount: 2,
      };
    default:
      throw new Error(`Invalid operator: ${operator}`);
  }
}

// Helper to add filter clause and params
function addFilterToQuery(
  f: { column: string; operator: string; value: any },
  params: any[],
  whereClauses: string[]
) {
  validateIdentifier(f.column, "column");
  const opInfo = getOperatorSQL(f.operator as FilterOperator, params.length + 1);
  whereClauses.push(`"${f.column}" ${opInfo.sql}`);
  
  if (f.operator === "between" && Array.isArray(f.value)) {
    // For "between", convert date strings from PST to UTC
    const startValue = convertPSTDateToUTC(f.value[0], false);
    const endValue = convertPSTDateToUTC(f.value[1], true);
    params.push(startValue, endValue);
  } else if (["gt", "gte", "lt", "lte", "eq"].includes(f.operator) && typeof f.value === "string") {
    // Check if this looks like a date filter
    const dateMatch = f.value.match(/^\d{4}-\d{2}-\d{2}$/);
    if (dateMatch) {
      // For single date comparisons, use start of day PST
      const converted = convertPSTDateToUTC(f.value, f.operator === "lte" || f.operator === "lt");
      params.push(opInfo.transform ? opInfo.transform(converted) : converted);
    } else {
      params.push(opInfo.transform ? opInfo.transform(f.value) : f.value);
    }
  } else {
    params.push(opInfo.transform ? opInfo.transform(f.value) : f.value);
  }
}

// Version for already-aliased columns (column string is already properly quoted/aliased)
function addFilterToQueryWithAlias(
  f: { column: string; operator: string; value: any },
  params: any[],
  whereClauses: string[]
) {
  const opInfo = getOperatorSQL(f.operator as FilterOperator, params.length + 1);
  whereClauses.push(`${f.column} ${opInfo.sql}`);
  
  if (f.operator === "between" && Array.isArray(f.value)) {
    // For "between", convert date strings from PST to UTC
    const startValue = convertPSTDateToUTC(f.value[0], false);
    const endValue = convertPSTDateToUTC(f.value[1], true);
    params.push(startValue, endValue);
  } else if (["gt", "gte", "lt", "lte", "eq"].includes(f.operator) && typeof f.value === "string") {
    // Check if this looks like a date filter
    const dateMatch = f.value.match(/^\d{4}-\d{2}-\d{2}$/);
    if (dateMatch) {
      // For single date comparisons, use start of day PST
      const converted = convertPSTDateToUTC(f.value, f.operator === "lte" || f.operator === "lt");
      params.push(opInfo.transform ? opInfo.transform(converted) : converted);
    } else {
      params.push(opInfo.transform ? opInfo.transform(f.value) : f.value);
    }
  } else {
    params.push(opInfo.transform ? opInfo.transform(f.value) : f.value);
  }
}

// Middleware to check user role
function requireRole(...allowedRoles: UserRole[]) {
  return async (req: Request, res: Response, next: Function) => {
    const userId = (req.user as any)?.id;
    if (!userId) {
      return res.status(401).json({ error: "Unauthorized" });
    }
    
    const user = await authStorage.getUser(userId);
    if (!user || !user.isActive) {
      return res.status(403).json({ error: "Account is inactive" });
    }
    
    if (!allowedRoles.includes(user.role as UserRole)) {
      return res.status(403).json({ error: "Insufficient permissions" });
    }
    
    (req as any).currentUser = user;
    next();
  };
}

// Get allowed tables for a user (for external customers)
async function getAllowedTables(userId: string): Promise<string[]> {
  const grants = await db.select().from(tableGrants).where(eq(tableGrants.userId, userId));
  return grants.map(g => `${g.database}:${g.tableName}`);
}

// Helper: Parse table name which may be schema-qualified (e.g., "public.vendors" or "vendors")
function parseTableName(tableName: string): { schema: string; table: string } | null {
  if (tableName.includes(".")) {
    const parts = tableName.split(".");
    if (parts.length !== 2) return null;
    const [schema, table] = parts;
    if (!isValidIdentifier(schema) || !isValidIdentifier(table)) return null;
    return { schema, table };
  } else {
    if (!isValidIdentifier(tableName)) return null;
    return { schema: "public", table: tableName };
  }
}

// Security: Validate table exists and user has access
// bypassVisibility: When true, ignores visibility settings (used for AI access - visibility is cosmetic for UI only)
async function validateTableAccess(
  dbName: string, 
  tableName: string, 
  user: User,
  options: { bypassVisibility?: boolean } = {}
): Promise<{ valid: boolean; error?: string; parsedTable?: { schema: string; table: string } }> {
  try {
    // Parse and validate table name (handles both "vendors" and "public.vendors")
    const parsed = parseTableName(tableName);
    if (!parsed) {
      return { valid: false, error: "Invalid table name" };
    }

    // Get the pool and check if table exists
    const pool = getPool(dbName);
    const tableResult = await pool.query(`
      SELECT table_name FROM information_schema.tables 
      WHERE table_schema = $1 AND table_name = $2
    `, [parsed.schema, parsed.table]);
    
    if (tableResult.rows.length === 0) {
      return { valid: false, error: "Table not found" };
    }

    // For external customers, always check grants (this is real permission, not cosmetic)
    if (user.role === "external_customer") {
      const allowedTables = await getAllowedTables(user.id);
      if (!allowedTables.includes(`${dbName}:${parsed.schema}.${parsed.table}`)) {
        return { valid: false, error: "Access denied to this table" };
      }
    }

    // Visibility is cosmetic for UI only - AI bypasses this check
    // Only apply visibility filtering for non-admin users when not bypassing
    if (!options.bypassVisibility && user.role !== "admin") {
      const allSettings = await storage.getAllTableSettings();
      const settingsKey = `${dbName}:${parsed.schema}.${parsed.table}`;
      const tableSettings = allSettings[settingsKey];
      
      if (tableSettings && tableSettings.isVisible === false) {
        return { valid: false, error: "Access denied to this table" };
      }
    }

    return { valid: true, parsedTable: parsed };
  } catch (err) {
    console.error("Error validating table access:", err);
    return { valid: false, error: "Failed to validate table access" };
  }
}

// Levenshtein distance for column name similarity
function levenshteinDistance(a: string, b: string): number {
  const matrix: number[][] = [];
  const aLower = a.toLowerCase();
  const bLower = b.toLowerCase();
  
  for (let i = 0; i <= bLower.length; i++) {
    matrix[i] = [i];
  }
  for (let j = 0; j <= aLower.length; j++) {
    matrix[0][j] = j;
  }
  
  for (let i = 1; i <= bLower.length; i++) {
    for (let j = 1; j <= aLower.length; j++) {
      if (bLower.charAt(i - 1) === aLower.charAt(j - 1)) {
        matrix[i][j] = matrix[i - 1][j - 1];
      } else {
        matrix[i][j] = Math.min(
          matrix[i - 1][j - 1] + 1,
          matrix[i][j - 1] + 1,
          matrix[i - 1][j] + 1
        );
      }
    }
  }
  return matrix[bLower.length][aLower.length];
}

// Find similar column names based on Levenshtein distance
function findSimilarColumns(target: string, columns: string[], maxSuggestions: number = 3): string[] {
  // Normalize target: remove underscores and lowercase for comparison
  const normalizedTarget = target.toLowerCase().replace(/_/g, "");
  
  const scored = columns.map(col => {
    const normalizedCol = col.toLowerCase().replace(/_/g, "");
    // Calculate distance on normalized versions
    const distance = levenshteinDistance(normalizedTarget, normalizedCol);
    // Calculate similarity score (0 to 1, higher is more similar)
    const maxLen = Math.max(normalizedTarget.length, normalizedCol.length);
    const similarity = maxLen > 0 ? 1 - (distance / maxLen) : 0;
    return { col, similarity, distance };
  });
  
  // Filter to reasonable matches (similarity > 0.4 means roughly 60% different at most)
  // and sort by similarity descending
  return scored
    .filter(s => s.similarity > 0.4)
    .sort((a, b) => b.similarity - a.similarity)
    .slice(0, maxSuggestions)
    .map(s => s.col);
}

// Security: Validate columns exist in table
async function validateColumns(
  dbName: string, 
  tableName: string, 
  columns: string[]
): Promise<{ valid: boolean; error?: string }> {
  try {
    if (columns.length === 0) return { valid: true };

    // Parse table name to get schema
    const parsed = parseTableName(tableName);
    if (!parsed) {
      return { valid: false, error: "Invalid table name" };
    }

    // Validate all column identifiers
    for (const col of columns) {
      if (!isValidIdentifier(col)) {
        return { valid: false, error: `Invalid column name: ${col}` };
      }
    }

    const pool = getPool(dbName);
    const columnResult = await pool.query(`
      SELECT column_name FROM information_schema.columns 
      WHERE table_schema = $1 AND table_name = $2
    `, [parsed.schema, parsed.table]);
    
    const existingColumnsArray = columnResult.rows.map((r: any) => r.column_name);
    const existingColumns = new Set(existingColumnsArray);
    
    for (const col of columns) {
      if (!existingColumns.has(col)) {
        const suggestions = findSimilarColumns(col, existingColumnsArray);
        const suggestionText = suggestions.length > 0 
          ? ` Did you mean: ${suggestions.map(s => `'${s}'`).join(", ")}?`
          : "";
        return { valid: false, error: `Column not found: ${col}.${suggestionText}` };
      }
    }

    return { valid: true };
  } catch (err) {
    console.error("Error validating columns:", err);
    return { valid: false, error: "Failed to validate columns" };
  }
}

// Security: Validate report block config against database metadata
// bypassVisibility: When true, ignores visibility settings (used for AI-generated blocks)
async function validateBlockConfig(
  config: any, 
  kind: string,
  user: User,
  options: { bypassVisibility?: boolean } = {}
): Promise<{ valid: boolean; error?: string }> {
  if (kind === "text") {
    return { valid: true };
  }

  if (!config.database || !config.table) {
    return { valid: false, error: "Database and table are required" };
  }

  // Validate database exists
  const dbs = getDatabaseConnections();
  if (!dbs.find(d => d.name === config.database)) {
    return { valid: false, error: "Database not found" };
  }

  // Validate table access (AI bypasses visibility, but external customer grants are still enforced)
  const tableValidation = await validateTableAccess(config.database, config.table, user, options);
  if (!tableValidation.valid) {
    return tableValidation;
  }

  // If there's a join, validate the joined table access too
  let joinTableColumns: string[] = [];
  let mainTableColumns: string[] = [];
  
  // Get main table columns for validation
  const mainParsed = parseTableName(config.table);
  if (mainParsed) {
    const pool = getPool(config.database);
    const mainColResult = await pool.query(`
      SELECT column_name FROM information_schema.columns 
      WHERE table_schema = $1 AND table_name = $2
    `, [mainParsed.schema, mainParsed.table]);
    mainTableColumns = mainColResult.rows.map((r: any) => r.column_name);
  }
  
  // Track sub-join table columns separately
  let subJoinTableColumns: string[] = [];
  
  if (config.join?.table) {
    const joinTableValidation = await validateTableAccess(config.database, config.join.table, user, options);
    if (!joinTableValidation.valid) {
      return { valid: false, error: `Join table access error: ${joinTableValidation.error}` };
    }
    
    // Validate join "on" columns
    if (!config.join.on || config.join.on.length !== 2) {
      return { valid: false, error: "Join 'on' must specify two columns [fromColumn, toColumn]" };
    }
    
    // Validate the fromColumn exists in main table
    const [fromCol, toCol] = config.join.on;
    if (!mainTableColumns.includes(fromCol)) {
      return { valid: false, error: `Join column '${fromCol}' not found in main table` };
    }
    
    // Get columns from joined table
    const joinParsed = parseTableName(config.join.table);
    if (joinParsed) {
      const pool = getPool(config.database);
      const joinColResult = await pool.query(`
        SELECT column_name FROM information_schema.columns 
        WHERE table_schema = $1 AND table_name = $2
      `, [joinParsed.schema, joinParsed.table]);
      joinTableColumns = joinColResult.rows.map((r: any) => r.column_name);
      
      // Validate the toColumn exists in joined table
      if (!joinTableColumns.includes(toCol)) {
        return { valid: false, error: `Join column '${toCol}' not found in joined table` };
      }
      
      // Handle subJoin if present (nested join: main -> join -> subJoin)
      if (config.join.subJoin?.table) {
        const subJoinTableValidation = await validateTableAccess(config.database, config.join.subJoin.table, user, options);
        if (!subJoinTableValidation.valid) {
          return { valid: false, error: `SubJoin table access error: ${subJoinTableValidation.error}` };
        }
        
        // Validate subJoin "on" columns
        if (!config.join.subJoin.on || config.join.subJoin.on.length !== 2) {
          return { valid: false, error: "SubJoin 'on' must specify two columns [fromColumn, toColumn]" };
        }
        
        const [subFromCol, subToCol] = config.join.subJoin.on;
        // subFromCol should exist in the first join table
        if (!joinTableColumns.includes(subFromCol)) {
          return { valid: false, error: `SubJoin column '${subFromCol}' not found in join table` };
        }
        
        // Get columns from subJoin table
        const subJoinParsed = parseTableName(config.join.subJoin.table);
        if (subJoinParsed) {
          const subJoinColResult = await pool.query(`
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = $1 AND table_name = $2
          `, [subJoinParsed.schema, subJoinParsed.table]);
          subJoinTableColumns = subJoinColResult.rows.map((r: any) => r.column_name);
          
          // Validate the subToColumn exists in subJoin table
          if (!subJoinTableColumns.includes(subToCol)) {
            return { valid: false, error: `SubJoin column '${subToCol}' not found in subJoin table` };
          }
        }
      }
    }
  }

  // Helper to validate dotted column reference format
  // Returns isSubJoin=true if prefix contains "district", "sub", etc. indicating subJoin table
  const validateDottedColumn = (col: string): { valid: boolean; colName?: string; isSubJoin?: boolean; error?: string } => {
    const parts = col.split(".");
    if (parts.length !== 2) {
      return { valid: false, error: `Invalid column reference '${col}': must be 'alias.column' format with exactly one dot` };
    }
    const [prefix, colName] = parts;
    if (!colName || !isValidIdentifier(colName)) {
      return { valid: false, error: `Invalid column name in '${col}'` };
    }
    if (!config.join) {
      return { valid: false, error: `Cannot use join column reference '${col}' without a join configuration` };
    }
    // Check if this references the subJoin table (prefixes like "joined_district", "sub_", etc.)
    const isSubJoin = prefix.includes("_") || prefix.toLowerCase().includes("district") || prefix.toLowerCase().includes("sub");
    return { valid: true, colName, isSubJoin };
  };

  // Collect all columns that need validation
  const columnsToValidate: string[] = [];
  const joinColumnsToValidate: string[] = [];
  const subJoinColumnsToValidate: string[] = [];

  if (kind === "table" && config.columns?.length > 0) {
    // Separate main table columns from join table columns
    for (const col of config.columns) {
      if (col.includes(".")) {
        // Column from joined table (e.g., "joined.email" or "joined_district.name")
        const result = validateDottedColumn(col);
        if (!result.valid) {
          return { valid: false, error: result.error };
        }
        if (result.isSubJoin && config.join?.subJoin) {
          subJoinColumnsToValidate.push(result.colName!);
        } else {
          joinColumnsToValidate.push(result.colName!);
        }
      } else {
        columnsToValidate.push(col);
      }
    }
  }
  if (kind === "table" && config.orderBy?.column) {
    if (config.orderBy.column.includes(".")) {
      const result = validateDottedColumn(config.orderBy.column);
      if (!result.valid) {
        return { valid: false, error: result.error };
      }
      if (result.isSubJoin && config.join?.subJoin) {
        subJoinColumnsToValidate.push(result.colName!);
      } else {
        joinColumnsToValidate.push(result.colName!);
      }
    } else {
      columnsToValidate.push(config.orderBy.column);
    }
  }
  if (kind === "chart") {
    // Helper to validate chart columns that may be from joins
    const validateChartColumn = (col: string) => {
      if (col.includes(".")) {
        const result = validateDottedColumn(col);
        if (!result.valid) {
          return { valid: false, error: result.error };
        }
        if (result.isSubJoin && config.join?.subJoin) {
          subJoinColumnsToValidate.push(result.colName!);
        } else {
          joinColumnsToValidate.push(result.colName!);
        }
      } else {
        columnsToValidate.push(col);
      }
      return { valid: true };
    };
    
    if (config.xColumn) {
      const result = validateChartColumn(config.xColumn);
      if (!result.valid) return { valid: false, error: result.error };
    }
    if (config.yColumn) {
      const result = validateChartColumn(config.yColumn);
      if (!result.valid) return { valid: false, error: result.error };
    }
    // Special date grouping values don't need column validation
    const dateGroupByValues = ["month", "year", "day", "week", "quarter"];
    if (config.groupBy && !dateGroupByValues.includes(config.groupBy.toLowerCase())) {
      const result = validateChartColumn(config.groupBy);
      if (!result.valid) return { valid: false, error: result.error };
    }
  }
  if (kind === "metric" && config.column) {
    if (config.column.includes(".")) {
      // Metric column from joined table (e.g., "joined.price")
      const result = validateDottedColumn(config.column);
      if (!result.valid) {
        return { valid: false, error: result.error };
      }
      if (result.isSubJoin && config.join?.subJoin) {
        subJoinColumnsToValidate.push(result.colName!);
      } else {
        joinColumnsToValidate.push(result.colName!);
      }
    } else {
      columnsToValidate.push(config.column);
    }
  }

  // Validate filter columns (handle join columns with dots)
  if (config.filters?.length > 0) {
    for (const f of config.filters) {
      if (f.column) {
        if (f.column.includes(".")) {
          // Filter column from joined table
          const result = validateDottedColumn(f.column);
          if (!result.valid) {
            return { valid: false, error: result.error };
          }
          if (result.isSubJoin && config.join?.subJoin) {
            subJoinColumnsToValidate.push(result.colName!);
          } else {
            joinColumnsToValidate.push(result.colName!);
          }
        } else {
          columnsToValidate.push(f.column);
        }
      }
      // Validate operator
      const validOps = ["eq", "contains", "gt", "gte", "lt", "lte", "between"];
      if (!validOps.includes(f.operator)) {
        return { valid: false, error: `Invalid filter operator: ${f.operator}` };
      }
    }
  }

  // Validate all main table columns exist
  const columnValidation = await validateColumns(config.database, config.table, columnsToValidate);
  if (!columnValidation.valid) {
    return columnValidation;
  }

  // Validate join table columns exist
  if (joinColumnsToValidate.length > 0 && joinTableColumns.length > 0) {
    for (const col of joinColumnsToValidate) {
      if (!joinTableColumns.includes(col)) {
        const suggestions = findSimilarColumns(col, joinTableColumns);
        const suggestionText = suggestions.length > 0 
          ? ` Did you mean: ${suggestions.map(s => `'${s}'`).join(", ")}?`
          : "";
        console.log(`[DEBUG] Join validation failed - Looking for column '${col}' in joined table '${config.join?.table}'. Available columns: [${joinTableColumns.join(', ')}]`);
        return { valid: false, error: `Column not found in joined table: ${col}.${suggestionText}` };
      }
    }
  }
  
  // Validate sub-join table columns exist
  if (subJoinColumnsToValidate.length > 0 && subJoinTableColumns.length > 0) {
    for (const col of subJoinColumnsToValidate) {
      if (!subJoinTableColumns.includes(col)) {
        const suggestions = findSimilarColumns(col, subJoinTableColumns);
        const suggestionText = suggestions.length > 0 
          ? ` Did you mean: ${suggestions.map(s => `'${s}'`).join(", ")}?`
          : "";
        console.log(`[DEBUG] SubJoin validation failed - Looking for column '${col}' in subJoin table '${config.join?.subJoin?.table}'. Available columns: [${subJoinTableColumns.join(', ')}]`);
        return { valid: false, error: `Column not found in subJoin table: ${col}.${suggestionText}` };
      }
    }
  }

  // Validate aggregate function if present
  if (config.aggregateFunction) {
    const validAggs = ["count", "sum", "avg", "min", "max"];
    if (!validAggs.includes(config.aggregateFunction.toLowerCase())) {
      return { valid: false, error: `Invalid aggregate function: ${config.aggregateFunction}` };
    }
  }

  // Validate chart type if present
  if (kind === "chart" && config.chartType) {
    const validChartTypes = ["bar", "line", "pie", "area"];
    if (!validChartTypes.includes(config.chartType)) {
      return { valid: false, error: `Invalid chart type: ${config.chartType}` };
    }
  }

  return { valid: true };
}

// Audit logging for data access - persisted to database
async function logAudit(entry: {
  userId: string;
  userEmail: string;
  action: string;
  database?: string;
  table?: string;
  details?: string;
  ip?: string;
}) {
  try {
    // Insert into database
    await db.insert(auditLogs).values({
      userId: entry.userId,
      userEmail: entry.userEmail,
      action: entry.action,
      database: entry.database || null,
      tableName: entry.table || null,
      details: entry.details || null,
      ipAddress: entry.ip || null,
    });
    
    // Also log to console for real-time monitoring
    console.log(`[AUDIT] ${new Date().toISOString()} | User: ${entry.userEmail} | Action: ${entry.action} | DB: ${entry.database || '-'} | Table: ${entry.table || '-'} | ${entry.details || ''}`);
  } catch (err) {
    // Don't let audit logging failures break the request
    console.error("Failed to write audit log:", err);
  }
}

export async function registerRoutes(
  httpServer: Server,
  app: Express
): Promise<Server> {
  // Setup auth BEFORE other routes
  await setupAuth(app);
  registerAuthRoutes(app);
  
  // ========== RATE LIMITING ==========
  
  // General API rate limit: 100 requests per minute
  const generalLimiter = rateLimit({
    windowMs: 60 * 1000, // 1 minute
    max: 100,
    message: { error: "Too many requests, please try again later" },
    standardHeaders: true,
    legacyHeaders: false,
  });
  
  // Stricter rate limit for auth endpoints: 10 attempts per minute
  const authLimiter = rateLimit({
    windowMs: 60 * 1000, // 1 minute
    max: 10,
    message: { error: "Too many login attempts, please try again later" },
    standardHeaders: true,
    legacyHeaders: false,
  });
  
  // Rate limit for exports: 10 per minute
  const exportLimiter = rateLimit({
    windowMs: 60 * 1000, // 1 minute
    max: 10,
    message: { error: "Too many export requests, please try again later" },
    standardHeaders: true,
    legacyHeaders: false,
  });
  
  // Rate limit for NLQ: 20 per minute
  const nlqLimiter = rateLimit({
    windowMs: 60 * 1000, // 1 minute
    max: 20,
    message: { error: "Too many AI query requests, please try again later" },
    standardHeaders: true,
    legacyHeaders: false,
  });
  
  // Apply general rate limit to all API routes
  app.use("/api/", generalLimiter);
  
  // Apply stricter limits to specific endpoints
  app.use("/api/auth/login", authLimiter);
  app.use("/api/auth/register", authLimiter);
  app.use("/api/export", exportLimiter);
  app.use("/api/nlq", nlqLimiter);
  
  // ========== ADMIN ROUTES ==========
  
  // Get all users (admin only)
  app.get("/api/admin/users", isAuthenticated, requireRole("admin"), async (req: Request, res: Response) => {
    try {
      const allUsers = await db.select().from(users);
      res.json(allUsers);
    } catch (err) {
      console.error("Error fetching users:", err);
      res.status(500).json({ error: "Failed to fetch users" });
    }
  });
  
  // Update user role/status (admin only)
  app.patch("/api/admin/users/:userId", isAuthenticated, requireRole("admin"), async (req: Request, res: Response) => {
    try {
      const { userId } = req.params;
      const { role, isActive, firstName, lastName, email, password } = req.body;
      
      const updates: Partial<{ role: UserRole; isActive: boolean; firstName: string; lastName: string; email: string; password: string; updatedAt: Date }> = { updatedAt: new Date() };
      
      if (role && ["admin", "washos_user", "external_customer"].includes(role)) {
        updates.role = role;
      }
      
      if (typeof isActive === "boolean") {
        updates.isActive = isActive;
      }
      
      if (typeof firstName === "string") {
        updates.firstName = firstName.trim();
      }
      
      if (typeof lastName === "string") {
        updates.lastName = lastName.trim();
      }
      
      if (typeof email === "string") {
        const normalizedEmail = email.toLowerCase().trim();
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        if (!emailRegex.test(normalizedEmail)) {
          return res.status(400).json({ error: "Invalid email format" });
        }
        // Check if email is already taken by another user
        const existingUser = await db.select().from(users).where(eq(users.email, normalizedEmail)).limit(1);
        if (existingUser.length > 0 && existingUser[0].id !== userId) {
          return res.status(400).json({ error: "Email already in use" });
        }
        updates.email = normalizedEmail;
      }
      
      // Handle password update
      if (typeof password === "string" && password.length > 0) {
        if (password.length < 4) {
          return res.status(400).json({ error: "Password must be at least 4 characters" });
        }
        const bcrypt = await import("bcryptjs");
        updates.password = await bcrypt.hash(password, 10);
      }
      
      const [updated] = await db.update(users).set(updates).where(eq(users.id, userId)).returning();
      
      if (!updated) {
        return res.status(404).json({ error: "User not found" });
      }
      
      res.json(updated);
    } catch (err) {
      console.error("Error updating user:", err);
      res.status(500).json({ error: "Failed to update user" });
    }
  });
  
  // Create new user (admin only)
  app.post("/api/admin/users", isAuthenticated, requireRole("admin"), async (req: Request, res: Response) => {
    try {
      const { email, password, firstName, lastName, role } = req.body;
      
      if (!email || !password) {
        return res.status(400).json({ error: "Email and password are required" });
      }
      
      const normalizedEmail = email.toLowerCase().trim();
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(normalizedEmail)) {
        return res.status(400).json({ error: "Invalid email format" });
      }
      
      if (password.length < 4) {
        return res.status(400).json({ error: "Password must be at least 4 characters" });
      }
      
      const existingUser = await authStorage.getUserByEmail(normalizedEmail);
      if (existingUser) {
        return res.status(400).json({ error: "Email already exists" });
      }
      
      const bcrypt = await import("bcryptjs");
      const hashedPassword = await bcrypt.hash(password, 10);
      
      const validRole = ["admin", "washos_user", "external_customer"].includes(role) ? role : "external_customer";
      
      const [newUser] = await db.insert(users).values({
        email: normalizedEmail,
        password: hashedPassword,
        firstName: firstName?.trim(),
        lastName: lastName?.trim(),
        role: validRole,
      }).returning();
      
      const { password: _, ...userWithoutPassword } = newUser;
      res.json(userWithoutPassword);
    } catch (err) {
      console.error("Error creating user:", err);
      res.status(500).json({ error: "Failed to create user" });
    }
  });
  
  // Delete user (admin only)
  app.delete("/api/admin/users/:userId", isAuthenticated, requireRole("admin"), async (req: Request, res: Response) => {
    try {
      const { userId } = req.params;
      const currentUserId = (req.user as any)?.id;
      
      if (userId === currentUserId) {
        return res.status(400).json({ error: "Cannot delete your own account" });
      }
      
      const [deleted] = await db.delete(users).where(eq(users.id, userId)).returning();
      
      if (!deleted) {
        return res.status(404).json({ error: "User not found" });
      }
      
      res.json({ message: "User deleted" });
    } catch (err) {
      console.error("Error deleting user:", err);
      res.status(500).json({ error: "Failed to delete user" });
    }
  });
  
  // Get table grants for a user (admin/washos)
  app.get("/api/admin/grants/:userId", isAuthenticated, requireRole("admin", "washos_user"), async (req: Request, res: Response) => {
    try {
      const { userId } = req.params;
      const grants = await db.select().from(tableGrants).where(eq(tableGrants.userId, userId));
      res.json(grants);
    } catch (err) {
      console.error("Error fetching grants:", err);
      res.status(500).json({ error: "Failed to fetch grants" });
    }
  });
  
  // Add table grant for a user (admin/washos)
  app.post("/api/admin/grants", isAuthenticated, requireRole("admin", "washos_user"), async (req: Request, res: Response) => {
    try {
      const { userId, database, tableName } = req.body;
      const grantedBy = (req.user as any)?.id;
      
      if (!userId || !database || !tableName) {
        return res.status(400).json({ error: "userId, database, and tableName are required" });
      }
      
      const [grant] = await db.insert(tableGrants).values({
        userId,
        database,
        tableName,
        grantedBy,
      }).returning();
      
      res.json(grant);
    } catch (err) {
      console.error("Error creating grant:", err);
      res.status(500).json({ error: "Failed to create grant" });
    }
  });
  
  // Delete table grant (admin/washos)
  app.delete("/api/admin/grants/:grantId", isAuthenticated, requireRole("admin", "washos_user"), async (req: Request, res: Response) => {
    try {
      const { grantId } = req.params;
      await db.delete(tableGrants).where(eq(tableGrants.id, grantId));
      res.json({ success: true });
    } catch (err) {
      console.error("Error deleting grant:", err);
      res.status(500).json({ error: "Failed to delete grant" });
    }
  });

  // Get audit logs (admin only)
  app.get("/api/admin/audit-logs", isAuthenticated, requireRole("admin"), async (req: Request, res: Response) => {
    try {
      const { limit = "100", offset = "0", userId, action } = req.query;
      const limitNum = Math.min(parseInt(limit as string) || 100, 1000);
      const offsetNum = parseInt(offset as string) || 0;
      
      // Build where conditions for SQL filtering
      const conditions = [];
      if (userId) {
        conditions.push(eq(auditLogs.userId, userId as string));
      }
      if (action) {
        conditions.push(eq(auditLogs.action, action as string));
      }
      
      // Get total count with filters
      let countQuery = db.select({ count: count() }).from(auditLogs);
      if (conditions.length > 0) {
        countQuery = countQuery.where(and(...conditions)) as typeof countQuery;
      }
      const [{ count: total }] = await countQuery;
      
      // Get paginated logs with filters - most recent first
      let logsQuery = db.select().from(auditLogs);
      if (conditions.length > 0) {
        logsQuery = logsQuery.where(and(...conditions)) as typeof logsQuery;
      }
      const logs = await logsQuery
        .orderBy(desc(auditLogs.timestamp))
        .limit(limitNum)
        .offset(offsetNum);
      
      res.json({
        logs,
        total: Number(total),
        limit: limitNum,
        offset: offsetNum,
      });
    } catch (err) {
      console.error("Error fetching audit logs:", err);
      res.status(500).json({ error: "Failed to fetch audit logs" });
    }
  });

  // Get all table settings (admin only - for full management)
  app.get("/api/admin/table-settings", isAuthenticated, requireRole("admin"), async (req: Request, res: Response) => {
    try {
      const settings = await storage.getAllTableSettings();
      res.json(settings);
    } catch (err) {
      console.error("Error fetching table settings:", err);
      res.status(500).json({ error: "Failed to fetch table settings" });
    }
  });

  // Get table settings for display (read-only, all authenticated users)
  // This allows non-admins to see which columns are hidden
  app.get("/api/table-settings", isAuthenticated, async (req: Request, res: Response) => {
    try {
      const settings = await storage.getAllTableSettings();
      res.json(settings);
    } catch (err) {
      console.error("Error fetching table settings:", err);
      res.status(500).json({ error: "Failed to fetch table settings" });
    }
  });

  // Update table settings (admin only)
  app.post("/api/admin/table-settings", isAuthenticated, requireRole("admin"), async (req: Request, res: Response) => {
    try {
      const { database, tableName, isVisible, displayName, hiddenColumns } = req.body;
      
      if (!database || !tableName) {
        return res.status(400).json({ error: "database and tableName are required" });
      }
      
      await storage.setTableSettings(database, tableName, {
        isVisible: isVisible !== false,
        displayName: displayName || null,
        hiddenColumns: Array.isArray(hiddenColumns) ? hiddenColumns : undefined,
      });
      
      res.json({ success: true });
    } catch (err) {
      console.error("Error updating table settings:", err);
      res.status(500).json({ error: "Failed to update table settings" });
    }
  });
  
  // Get current user with role info
  app.get("/api/auth/me", isAuthenticated, async (req: Request, res: Response) => {
    try {
      const userId = (req.user as any)?.id;
      const user = await authStorage.getUser(userId);
      if (!user) {
        return res.status(404).json({ error: "User not found" });
      }
      res.json(user);
    } catch (err) {
      console.error("Error fetching current user:", err);
      res.status(500).json({ error: "Failed to fetch user" });
    }
  });

  // ========== DATABASE VIEWER ROUTES ==========

  // Get available databases
  app.get("/api/databases", isAuthenticated, (req: Request, res: Response) => {
    try {
      const dbs = getDatabaseConnections();
      // Return only names, not URLs for security
      res.json(dbs.map((db) => ({ name: db.name })));
    } catch (err) {
      console.error("Error getting databases:", err);
      res.status(500).json({ error: "Failed to get databases" });
    }
  });

  // Get tables for a database
  app.get("/api/tables/:database", isAuthenticated, async (req: Request, res: Response) => {
    try {
      const { database } = req.params;
      const userId = (req.user as any)?.id;
      const user = await authStorage.getUser(userId);
      
      if (!user || !user.isActive) {
        return res.status(403).json({ error: "Account is inactive" });
      }
      
      const pool = getPool(database);
      const allTableSettings = await storage.getAllTableSettings();

      const result = await pool.query(`
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name
      `);

      let tables: TableInfo[] = result.rows.map((row) => {
        const fullName = `${row.table_schema}.${row.table_name}`;
        const settingsKey = `${database}:${fullName}`;
        const settings = allTableSettings[settingsKey];
        return {
          schema: row.table_schema,
          name: row.table_name,
          fullName,
          displayName: settings?.displayName || null,
          isVisible: settings?.isVisible !== false,
        };
      });
      
      // For external customers, filter to only granted tables
      if (user.role === "external_customer") {
        const allowedTables = await getAllowedTables(userId);
        tables = tables.filter(t => allowedTables.includes(`${database}:${t.fullName}`));
      }
      
      // For non-admins, filter out hidden tables
      if (user.role !== "admin") {
        tables = tables.filter(t => t.isVisible !== false);
      }

      res.json(tables);
    } catch (err) {
      console.error("Error getting tables:", err);
      res.status(500).json({
        error: err instanceof Error ? err.message : "Failed to get tables",
      });
    }
  });

  // Get columns for a table
  app.get(
    "/api/columns/:database/:fullTable",
    isAuthenticated,
    async (req: Request, res: Response) => {
      try {
        const { database, fullTable } = req.params;
        
        // Check table access for external customers
        const userId = (req.user as any)?.id;
        const user = await authStorage.getUser(userId);
        if (user?.role === "external_customer") {
          const allowedTables = await getAllowedTables(userId);
          if (!allowedTables.includes(`${database}:${fullTable}`)) {
            return res.status(403).json({ error: "You don't have access to this table" });
          }
        }
        
        const [schema, table] = fullTable.split(".");
        if (!schema || !table) {
          return res.status(400).json({ error: "Invalid table name format. Expected schema.table" });
        }

        validateIdentifier(schema, "schema");
        validateIdentifier(table, "table");

        const pool = getPool(database);

        // Get columns
        const columnsResult = await pool.query(
          `
          SELECT column_name, data_type, is_nullable
          FROM information_schema.columns
          WHERE table_schema = $1 AND table_name = $2
          ORDER BY ordinal_position
        `,
          [schema, table]
        );

        // Get primary key columns
        const pkResult = await pool.query(
          `
          SELECT a.attname
          FROM pg_index i
          JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
          WHERE i.indrelid = ($1 || '.' || $2)::regclass
            AND i.indisprimary
        `,
          [schema, table]
        );

        const pkColumns = new Set(pkResult.rows.map((r) => r.attname));

        const columns: ColumnInfo[] = columnsResult.rows.map((row) => ({
          name: row.column_name,
          dataType: row.data_type,
          isNullable: row.is_nullable === "YES",
          isPrimaryKey: pkColumns.has(row.column_name),
        }));

        res.json(columns);
      } catch (err) {
        console.error("Error getting columns:", err);
        res.status(500).json({
          error: err instanceof Error ? err.message : "Failed to get columns",
        });
      }
    }
  );

  // Get filter definitions for a table (authenticated users only)
  app.get("/api/filters/:table", isAuthenticated, async (req: Request, res: Response) => {
    try {
      const { table } = req.params;
      const filters = await storage.getFilters(table);
      res.json(filters);
    } catch (err) {
      console.error("Error getting filters:", err);
      res.status(500).json({ error: "Failed to get filters" });
    }
  });

  // Save filter definitions for a table (admin only)
  app.post("/api/filters", isAuthenticated, requireRole("admin"), async (req: Request, res: Response) => {
    try {
      const { table, filters } = req.body;
      if (!table || !Array.isArray(filters)) {
        return res.status(400).json({ error: "Invalid request body" });
      }
      await storage.setFilters(table, filters);
      res.json({ success: true });
    } catch (err) {
      console.error("Error saving filters:", err);
      res.status(500).json({ error: "Failed to save filters" });
    }
  });

  // Get filter history for a user/table
  app.get("/api/filters/history/:database/:table", isAuthenticated, async (req: Request, res: Response) => {
    try {
      const userId = (req.user as any)?.id;
      const { database, table } = req.params;
      
      if (!userId) {
        return res.status(401).json({ error: "Not authenticated" });
      }

      const history = await storage.getFilterHistory(userId, database, table);
      res.json(history);
    } catch (err) {
      console.error("Error getting filter history:", err);
      res.status(500).json({ error: "Failed to get filter history" });
    }
  });

  // Save filter to history
  app.post("/api/filters/history", isAuthenticated, async (req: Request, res: Response) => {
    try {
      const userId = (req.user as any)?.id;
      const { database, table, filters } = req.body;
      
      if (!userId) {
        return res.status(401).json({ error: "Not authenticated" });
      }

      if (!database || !table || !Array.isArray(filters) || filters.length === 0) {
        return res.status(400).json({ error: "Database, table, and non-empty filters are required" });
      }

      const entry = await storage.saveFilterHistory(userId, database, table, filters);
      res.json(entry);
    } catch (err) {
      console.error("Error saving filter history:", err);
      res.status(500).json({ error: "Failed to save filter history" });
    }
  });

  // Delete a filter history entry
  app.delete("/api/filters/history/:id", isAuthenticated, async (req: Request, res: Response) => {
    try {
      const userId = (req.user as any)?.id;
      const { id } = req.params;
      
      if (!userId) {
        return res.status(401).json({ error: "Not authenticated" });
      }

      const deleted = await storage.deleteFilterHistory(id, userId);
      if (!deleted) {
        return res.status(404).json({ error: "Filter history entry not found" });
      }
      res.json({ success: true });
    } catch (err) {
      console.error("Error deleting filter history:", err);
      res.status(500).json({ error: "Failed to delete filter history" });
    }
  });

  // Fetch rows with pagination and filters
  app.post("/api/rows", isAuthenticated, async (req: Request, res: Response) => {
    try {
      const { database, table, page = 1, filters = [], sort } = req.body;
      
      // Check table access for external customers
      const userId = (req.user as any)?.id;
      const user = await authStorage.getUser(userId);
      if (user?.role === "external_customer") {
        const allowedTables = await getAllowedTables(userId);
        if (!allowedTables.includes(`${database}:${table}`)) {
          return res.status(403).json({ error: "You don't have access to this table" });
        }
      }

      if (!database || !table) {
        return res.status(400).json({ error: "Database and table are required" });
      }

      const [schema, tableName] = table.split(".");
      validateIdentifier(schema, "schema");
      validateIdentifier(tableName, "table");

      const pool = getPool(database);

      // Validate columns exist
      const columnsResult = await pool.query(
        `
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
      `,
        [schema, tableName]
      );
      const validColumns = new Set(columnsResult.rows.map((r) => r.column_name));

      // Get primary key for ordering
      const pkResult = await pool.query(
        `
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = ($1 || '.' || $2)::regclass
          AND i.indisprimary
        ORDER BY a.attnum
      `,
        [schema, tableName]
      );

      // Determine ORDER BY - use user-specified sort or default to primary key
      // Support multi-column sorting (sort is now an array)
      let orderByClause: string;
      
      if (sort && Array.isArray(sort) && sort.length > 0) {
        // Multi-column sort
        const sortParts: string[] = [];
        for (const sortItem of sort) {
          if (sortItem.column && validColumns.has(sortItem.column)) {
            validateIdentifier(sortItem.column, "sort column");
            const direction = sortItem.direction === "desc" ? "DESC" : "ASC";
            sortParts.push(`"${sortItem.column}" ${direction}`);
          }
        }
        if (sortParts.length > 0) {
          orderByClause = sortParts.join(", ");
        } else {
          orderByClause = pkResult.rows.length > 0 ? `"${pkResult.rows[0].attname}"` : "ctid";
        }
      } else {
        // Default to primary key
        orderByClause = pkResult.rows.length > 0 ? `"${pkResult.rows[0].attname}"` : "ctid";
      }

      // Build WHERE clause
      const whereClauses: string[] = [];
      const params: unknown[] = [];
      let paramIndex = 1;

      for (const filter of filters as ActiveFilter[]) {
        validateIdentifier(filter.column, "column");
        if (!validColumns.has(filter.column)) {
          return res
            .status(400)
            .json({ error: `Invalid column: ${filter.column}` });
        }

        const op = getOperatorSQL(filter.operator, paramIndex);
        whereClauses.push(`"${filter.column}" ${op.sql}`);
        
        // Handle between operator (needs 2 params) and date conversion
        if (filter.operator === "between" && Array.isArray(filter.value)) {
          const startValue = convertPSTDateToUTC(filter.value[0], false);
          const endValue = convertPSTDateToUTC(filter.value[1], true);
          params.push(startValue, endValue);
          paramIndex += 2;
        } else if (["gt", "gte", "lt", "lte", "eq"].includes(filter.operator) && typeof filter.value === "string") {
          // Check if this looks like a date filter
          const dateMatch = filter.value.match(/^\d{4}-\d{2}-\d{2}$/);
          if (dateMatch) {
            const converted = convertPSTDateToUTC(filter.value, filter.operator === "lte" || filter.operator === "lt");
            params.push(op.transform ? op.transform(converted) : converted);
          } else {
            params.push(op.transform ? op.transform(filter.value) : filter.value);
          }
          paramIndex++;
        } else {
          params.push(op.transform ? op.transform(filter.value) : filter.value);
          paramIndex++;
        }
      }

      const whereSQL =
        whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";

      // Get total count
      const countQuery = `SELECT COUNT(*) as count FROM "${schema}"."${tableName}" ${whereSQL}`;
      const countResult = await pool.query(countQuery, params);
      const totalCount = parseInt(countResult.rows[0].count, 10);

      // Calculate pagination
      const totalPages = Math.max(1, Math.ceil(totalCount / PAGE_SIZE));
      const safePage = Math.min(Math.max(1, page), totalPages);
      const offset = (safePage - 1) * PAGE_SIZE;

      // Fetch rows
      const dataQuery = `
        SELECT * FROM "${schema}"."${tableName}"
        ${whereSQL}
        ORDER BY ${orderByClause}
        LIMIT ${PAGE_SIZE}
        OFFSET ${offset}
      `;
      const dataResult = await pool.query(dataQuery, params);

      // Audit log data access
      logAudit({
        userId: userId,
        userEmail: user?.email || "unknown",
        action: "VIEW_DATA",
        database: database,
        table: table,
        details: `Viewed page ${safePage} of ${totalPages} (${dataResult.rows.length} rows)${filters.length > 0 ? `, ${filters.length} filters applied` : ''}`,
        ip: req.ip || req.socket.remoteAddress,
      });

      res.json({
        rows: dataResult.rows,
        totalCount,
        page: safePage,
        pageSize: PAGE_SIZE,
        totalPages,
      });
    } catch (err) {
      console.error("Error fetching rows:", err);
      res.status(500).json({
        error: err instanceof Error ? err.message : "Failed to fetch rows",
      });
    }
  });

  // Export CSV
  // Export row count check (for client-side validation before export)
  app.post("/api/export/check", isAuthenticated, async (req: Request, res: Response) => {
    try {
      const { database, table, filters } = req.body;

      if (!database || !table) {
        return res.status(400).json({ error: "Database and table are required" });
      }

      const userId = (req.user as any)?.id;
      const user = await authStorage.getUser(userId);
      
      // Check table access for external customers
      if (user?.role === "external_customer") {
        const allowedTables = await getAllowedTables(userId);
        if (!allowedTables.includes(`${database}:${table}`)) {
          return res.status(403).json({ error: "You don't have access to this table" });
        }
      }

      const [schema, tableName] = (table as string).split(".");
      validateIdentifier(schema, "schema");
      validateIdentifier(tableName, "table");

      const pool = getPool(database as string);

      // Parse and validate filters
      const activeFilters: ActiveFilter[] = filters || [];
      
      // Validate columns
      const columnsResult = await pool.query(
        `SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2`,
        [schema, tableName]
      );
      const validColumns = new Set(columnsResult.rows.map((r) => r.column_name));

      // Build WHERE clause
      const whereClauses: string[] = [];
      const params: unknown[] = [];
      let paramIndex = 1;

      for (const filter of activeFilters) {
        validateIdentifier(filter.column, "column");
        if (!validColumns.has(filter.column)) {
          return res.status(400).json({ error: `Invalid column: ${filter.column}` });
        }
        const op = getOperatorSQL(filter.operator, paramIndex);
        whereClauses.push(`"${filter.column}" ${op.sql}`);
        
        // Handle between operator (needs 2 params) and date conversion
        if (filter.operator === "between" && Array.isArray(filter.value)) {
          const startValue = convertPSTDateToUTC(filter.value[0], false);
          const endValue = convertPSTDateToUTC(filter.value[1], true);
          params.push(startValue, endValue);
          paramIndex += 2;
        } else if (["gt", "gte", "lt", "lte", "eq"].includes(filter.operator) && typeof filter.value === "string") {
          // Check if this looks like a date filter
          const dateMatch = filter.value.match(/^\d{4}-\d{2}-\d{2}$/);
          if (dateMatch) {
            const converted = convertPSTDateToUTC(filter.value, filter.operator === "lte" || filter.operator === "lt");
            params.push(op.transform ? op.transform(converted) : converted);
          } else {
            params.push(op.transform ? op.transform(filter.value) : filter.value);
          }
          paramIndex++;
        } else {
          params.push(op.transform ? op.transform(filter.value) : filter.value);
          paramIndex++;
        }
      }

      const whereSQL = whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";

      // Count total rows
      const countQuery = `SELECT COUNT(*) as count FROM "${schema}"."${tableName}" ${whereSQL}`;
      const countResult = await pool.query(countQuery, params);
      const totalCount = parseInt(countResult.rows[0].count, 10);

      // Determine limits based on role
      const isAdmin = user?.role === "admin";
      const maxRowsForRole = isAdmin ? 50000 : 10000;
      const warningThreshold = 2000;

      res.json({
        totalCount,
        isAdmin,
        maxRowsForRole,
        warningThreshold,
        canExport: totalCount <= maxRowsForRole,
        needsWarning: totalCount > warningThreshold,
        exceedsLimit: totalCount > maxRowsForRole,
      });
    } catch (err) {
      console.error("Error checking export:", err);
      res.status(500).json({
        error: err instanceof Error ? err.message : "Failed to check export",
      });
    }
  });

  app.get("/api/export", isAuthenticated, async (req: Request, res: Response) => {
    try {
      const { database, table, page = "1", filters: filtersJson, exportAll } = req.query;

      if (!database || !table) {
        return res.status(400).json({ error: "Database and table are required" });
      }
      
      // Check table access for external customers
      const userId = (req.user as any)?.id;
      const user = await authStorage.getUser(userId);
      if (user?.role === "external_customer") {
        const allowedTables = await getAllowedTables(userId);
        if (!allowedTables.includes(`${database}:${table}`)) {
          return res.status(403).json({ error: "You don't have access to this table" });
        }
      }

      const [schema, tableName] = (table as string).split(".");
      validateIdentifier(schema, "schema");
      validateIdentifier(tableName, "table");

      const pool = getPool(database as string);

      // Parse filters
      let filters: ActiveFilter[] = [];
      if (filtersJson) {
        try {
          filters = JSON.parse(filtersJson as string);
        } catch {
          return res.status(400).json({ error: "Invalid filters format" });
        }
      }

      // Validate columns
      const columnsResult = await pool.query(
        `
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
      `,
        [schema, tableName]
      );
      const columnNames = columnsResult.rows.map((r) => r.column_name);
      const validColumns = new Set(columnNames);

      // Get primary key for ordering
      const pkResult = await pool.query(
        `
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = ($1 || '.' || $2)::regclass
          AND i.indisprimary
        ORDER BY a.attnum
      `,
        [schema, tableName]
      );

      const orderByColumn =
        pkResult.rows.length > 0 ? `"${pkResult.rows[0].attname}"` : "ctid";

      // Build WHERE clause
      const whereClauses: string[] = [];
      const params: unknown[] = [];
      let paramIndex = 1;

      for (const filter of filters) {
        validateIdentifier(filter.column, "column");
        if (!validColumns.has(filter.column)) {
          return res
            .status(400)
            .json({ error: `Invalid column: ${filter.column}` });
        }

        const op = getOperatorSQL(filter.operator, paramIndex);
        whereClauses.push(`"${filter.column}" ${op.sql}`);
        
        // Handle between operator (needs 2 params) and date conversion
        if (filter.operator === "between" && Array.isArray(filter.value)) {
          const startValue = convertPSTDateToUTC(filter.value[0], false);
          const endValue = convertPSTDateToUTC(filter.value[1], true);
          params.push(startValue, endValue);
          paramIndex += 2;
        } else if (["gt", "gte", "lt", "lte", "eq"].includes(filter.operator) && typeof filter.value === "string") {
          // Check if this looks like a date filter
          const dateMatch = filter.value.match(/^\d{4}-\d{2}-\d{2}$/);
          if (dateMatch) {
            const converted = convertPSTDateToUTC(filter.value, filter.operator === "lte" || filter.operator === "lt");
            params.push(op.transform ? op.transform(converted) : converted);
          } else {
            params.push(op.transform ? op.transform(filter.value) : filter.value);
          }
          paramIndex++;
        } else {
          params.push(op.transform ? op.transform(filter.value) : filter.value);
          paramIndex++;
        }
      }

      const whereSQL =
        whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";

      // Determine if exporting all rows
      const isExportAll = exportAll === "true";
      const isAdmin = user?.role === "admin";
      const maxRowsForRole = isAdmin ? 50000 : 10000;

      // If exporting all, verify row count doesn't exceed limits
      let exportTotalCount = 0;
      if (isExportAll) {
        const countQuery = `SELECT COUNT(*) as count FROM "${schema}"."${tableName}" ${whereSQL}`;
        const countResult = await pool.query(countQuery, params);
        exportTotalCount = parseInt(countResult.rows[0].count, 10);

        if (exportTotalCount > maxRowsForRole) {
          return res.status(403).json({
            error: isAdmin
              ? `Export exceeds maximum limit of ${maxRowsForRole.toLocaleString()} rows`
              : `Export exceeds your limit of ${maxRowsForRole.toLocaleString()} rows. Please contact an administrator for larger exports.`,
          });
        }
      }

      // Calculate pagination
      const pageNum = Math.max(1, parseInt(page as string, 10) || 1);
      const offset = (pageNum - 1) * PAGE_SIZE;

      // Build query based on export mode
      let dataQuery: string;
      let filename: string;

      if (isExportAll) {
        // Export all filtered rows - use validated count as LIMIT for consistency
        dataQuery = `
          SELECT * FROM "${schema}"."${tableName}"
          ${whereSQL}
          ORDER BY ${orderByColumn} ASC
          LIMIT ${exportTotalCount}
        `;
        filename = `${tableName}_export.csv`;
      } else {
        // Export single page
        dataQuery = `
          SELECT * FROM "${schema}"."${tableName}"
          ${whereSQL}
          ORDER BY ${orderByColumn} ASC
          LIMIT ${PAGE_SIZE}
          OFFSET ${offset}
        `;
        filename = `${tableName}_page${pageNum}.csv`;
      }

      // Set headers for streaming CSV
      res.setHeader("Content-Type", "text/csv");
      res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);

      // Generate CSV with streaming for large exports
      const escapeCSV = (value: unknown): string => {
        if (value === null || value === undefined) return "";
        const str = typeof value === "object" ? JSON.stringify(value) : String(value);
        if (str.includes(",") || str.includes('"') || str.includes("\n")) {
          return `"${str.replace(/"/g, '""')}"`;
        }
        return str;
      };

      // Write header
      res.write(columnNames.map(escapeCSV).join(",") + "\n");

      // Use cursor for large exports to stream data
      if (isExportAll) {
        const client = await pool.connect();
        const cursorName = `export_cursor_${Date.now()}`;
        let transactionStarted = false;
        
        try {
          // Create a cursor for streaming large result sets
          await client.query("BEGIN");
          transactionStarted = true;
          await client.query(`DECLARE ${cursorName} CURSOR FOR ${dataQuery}`, params);

          const batchSize = 1000;
          let hasMore = true;

          while (hasMore) {
            const batchResult = await client.query(`FETCH ${batchSize} FROM ${cursorName}`);
            if (batchResult.rows.length === 0) {
              hasMore = false;
            } else {
              for (const row of batchResult.rows) {
                res.write(columnNames.map((col) => escapeCSV(row[col])).join(",") + "\n");
              }
            }
          }

          await client.query(`CLOSE ${cursorName}`);
          await client.query("COMMIT");
        } catch (streamError) {
          // Rollback transaction on any streaming error to prevent poisoned connections
          if (transactionStarted) {
            try {
              await client.query("ROLLBACK");
            } catch (rollbackError) {
              console.error("Error rolling back export transaction:", rollbackError);
            }
          }
          throw streamError;
        } finally {
          client.release();
        }
      } else {
        // For single page, fetch all at once
        const dataResult = await pool.query(dataQuery, params);
        for (const row of dataResult.rows) {
          res.write(columnNames.map((col) => escapeCSV(row[col])).join(",") + "\n");
        }
      }

      // Audit log the export
      logAudit({
        userId: userId,
        userEmail: user?.email || "unknown",
        action: isExportAll ? "EXPORT_ALL" : "EXPORT_PAGE",
        database: database as string,
        table: table as string,
        details: isExportAll 
          ? `Exported ${exportTotalCount} rows${filters.length > 0 ? `, ${filters.length} filters applied` : ''}`
          : `Exported page ${pageNum}${filters.length > 0 ? `, ${filters.length} filters applied` : ''}`,
        ip: req.ip || req.socket.remoteAddress,
      });

      res.end();
    } catch (err) {
      console.error("Error exporting CSV:", err);
      if (!res.headersSent) {
        res.status(500).json({
          error: err instanceof Error ? err.message : "Failed to export CSV",
        });
      }
    }
  });

  // NLQ status
  app.get("/api/nlq/status", isAuthenticated, (req: Request, res: Response) => {
    const client = getOpenAIClient();
    res.json({ enabled: client !== null });
  });

  // Natural Language Query (upgraded with modular AI)
  app.post("/api/nlq", isAuthenticated, async (req: Request, res: Response) => {
    try {
      const { database, query, table: currentTable, context } = req.body;

      const client = getOpenAIClient();
      if (!client) {
        return res.status(400).json({ error: "Natural language queries are not enabled" });
      }

      if (!query || typeof query !== "string") {
        return res.status(400).json({ error: "Query is required" });
      }

      if (!currentTable) {
        return res.status(400).json({ error: "Please select a table first" });
      }
      
      const userId = (req.user as any)?.id;
      const user = await authStorage.getUser(userId);
      if (user?.role === "external_customer") {
        const allowedTables = await getAllowedTables(userId);
        if (!allowedTables.includes(`${database}:${currentTable}`)) {
          return res.status(403).json({ error: "You don't have access to this table" });
        }
      }

      const pool = getPool(database);
      const [schema, tableName] = currentTable.split(".");
      
      const dictionary = await getTableDataDictionary(pool, database, schema, tableName);
      
      let columnsWithTypes: Array<{ name: string; dataType: string }> = [];
      if (dictionary) {
        columnsWithTypes = dictionary.columns.map(c => ({ name: c.name, dataType: c.dataType }));
      } else {
        const columnsResult = await pool.query(
          `SELECT column_name, data_type FROM information_schema.columns 
           WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position`,
          [schema, tableName]
        );
        columnsWithTypes = columnsResult.rows.map((r) => ({
          name: r.column_name,
          dataType: r.data_type,
        }));
      }

      const dateColumnNames = columnsWithTypes
        .filter((c) => c.dataType.includes("date") || c.dataType.includes("timestamp"))
        .map((c) => c.name);

      const semanticResolution = dictionary 
        ? resolveSemanticReference(query, dictionary.columns)
        : { resolvedColumn: null, type: null, needsClarification: false };

      const systemPrompt = buildNLQSystemPrompt({
        table: currentTable,
        dictionary,
        columns: columnsWithTypes,
        dateColumns: dateColumnNames,
        context,
      });

      const makeRequest = async () => {
        const response = await client.chat.completions.create({
          model: AI_CONFIG.nlq.model,
          messages: [
            { role: "system", content: systemPrompt },
            { role: "user", content: query },
          ],
          max_completion_tokens: AI_CONFIG.nlq.maxTokens,
          temperature: AI_CONFIG.nlq.temperature,
        });
        return response.choices[0]?.message?.content || "{}";
      };

      const content = await makeRequest();
      const validColumns = columnsWithTypes.map((c) => c.name);
      
      const parseResult = await parseAndValidateNLQResponse(
        content,
        currentTable,
        validColumns,
        makeRequest
      );

      if (!parseResult.success) {
        return res.status(400).json({ error: parseResult.error });
      }

      const plan = parseResult.data;
      plan.table = currentTable;
      plan.page = plan.page || 1;
      plan.action = plan.action || "plan";

      if (plan.action === "clarify" && semanticResolution.needsClarification && semanticResolution.options) {
        plan.questions = plan.questions || [];
        if (plan.questions.length === 0) {
          plan.questions.push(`Which date column should I use? Available options: ${semanticResolution.options.join(", ")}`);
        }
        plan.ambiguousColumns = semanticResolution.options;
      }

      res.json(plan);
    } catch (err) {
      console.error("Error processing NLQ:", err);
      res.status(500).json({
        error: err instanceof Error ? err.message : "Failed to process query",
      });
    }
  });

  // NLQ Smart Follow-up (upgraded with modular AI)
  app.post("/api/nlq/smart-followup", isAuthenticated, async (req: Request, res: Response) => {
    try {
      const { database, table: currentTable, filters, context } = req.body;

      const client = getOpenAIClient();
      if (!client) {
        return res.status(400).json({ error: "Natural language queries are not enabled" });
      }

      if (!currentTable || !database || !filters || !Array.isArray(filters)) {
        return res.status(400).json({ error: "Missing required parameters" });
      }

      const userId = (req.user as any)?.id;
      const user = await authStorage.getUser(userId);
      if (user?.role === "external_customer") {
        const allowedTables = await getAllowedTables(userId);
        if (!allowedTables.includes(`${database}:${currentTable}`)) {
          return res.status(403).json({ error: "You don't have access to this table" });
        }
      }

      const pool = getPool(database);
      const [schema, tableName] = currentTable.split(".");

      const dictionary = await getTableDataDictionary(pool, database, schema, tableName);

      const columnTypes: Record<string, string> = {};
      if (dictionary) {
        dictionary.columns.forEach((c) => {
          columnTypes[c.name] = c.dataType;
        });
      } else {
        const columnsResult = await pool.query(
          `SELECT column_name, data_type FROM information_schema.columns 
           WHERE table_schema = $1 AND table_name = $2`,
          [schema, tableName]
        );
        columnsResult.rows.forEach((r) => {
          columnTypes[r.column_name] = r.data_type;
        });
      }

      const TIMEOUT_MS = 3000;
      const MAX_DISTINCT_VALUES = 15;

      interface ColumnSample {
        column: string;
        dataType: string;
        filteredValue: string;
        operator: string;
        actualValues?: string[];
        dateRange?: { min: string; max: string };
      }
      const columnSamples: ColumnSample[] = [];

      const queryWithTimeout = async (queryStr: string): Promise<any> => {
        return new Promise((resolve, reject) => {
          const timeoutId = setTimeout(() => reject(new Error("Timeout")), TIMEOUT_MS);
          pool.query(queryStr)
            .then((result) => {
              clearTimeout(timeoutId);
              resolve(result);
            })
            .catch((err) => {
              clearTimeout(timeoutId);
              reject(err);
            });
        });
      };

      for (const filter of filters) {
        const colName = filter.column;
        const dataType = columnTypes[colName];
        if (!dataType) continue;

        const sample: ColumnSample = {
          column: colName,
          dataType,
          filteredValue: Array.isArray(filter.value) ? filter.value.join(" - ") : filter.value,
          operator: filter.op,
        };

        try {
          if (dictionary) {
            const colStats = dictionary.columns.find(c => c.name === colName);
            if (colStats?.topValues) {
              sample.actualValues = colStats.topValues.map(v => v.value);
            } else if (colStats?.dateRange) {
              sample.dateRange = colStats.dateRange;
            }
          }

          if (!sample.actualValues && !sample.dateRange) {
            if (dataType.includes("character") || dataType.includes("text") || dataType === "USER-DEFINED") {
              const result = await queryWithTimeout(`
                SELECT DISTINCT "${colName}" as val FROM "${schema}"."${tableName}" 
                WHERE "${colName}" IS NOT NULL ORDER BY "${colName}" LIMIT ${MAX_DISTINCT_VALUES}
              `);
              sample.actualValues = result.rows.map((r: any) => String(r.val));
            } else if (dataType.includes("date") || dataType.includes("timestamp")) {
              const result = await queryWithTimeout(`
                SELECT MIN("${colName}")::text as min_val, MAX("${colName}")::text as max_val 
                FROM "${schema}"."${tableName}" WHERE "${colName}" IS NOT NULL
              `);
              if (result.rows[0]) {
                sample.dateRange = { min: result.rows[0].min_val, max: result.rows[0].max_val };
              }
            }
          }
        } catch (err) {
          console.log(`Skipping column ${colName} sampling:`, err);
        }

        columnSamples.push(sample);
      }

      let samplingInfo = "";
      for (const sample of columnSamples) {
        if (sample.actualValues && sample.actualValues.length > 0) {
          samplingInfo += `\n- Column "${sample.column}" (${sample.dataType}): User searched for "${sample.filteredValue}" using operator "${sample.operator}"`;
          samplingInfo += `\n  Actual values in database: ${sample.actualValues.map(v => `"${v}"`).join(", ")}`;
        } else if (sample.dateRange) {
          samplingInfo += `\n- Column "${sample.column}" (${sample.dataType}): User searched for "${sample.filteredValue}" using operator "${sample.operator}"`;
          samplingInfo += `\n  Date range in database: from ${sample.dateRange.min} to ${sample.dateRange.max}`;
        }
      }

      if (!samplingInfo) {
        return res.json({
          likelyIssue: "unknown",
          suggestedChanges: [],
          clarificationQuestion: "No results found for your query. Try adjusting your filter values or broadening your search.",
          summary: "Unable to sample column values to provide suggestions.",
        });
      }

      const systemPrompt = buildSmartFollowupPrompt({
        table: currentTable,
        filters: filters.map((f: any) => ({ column: f.column, op: f.op, value: Array.isArray(f.value) ? f.value.join(" - ") : f.value })),
        samplingInfo,
        context,
      });

      const makeRequest = async () => {
        const response = await client.chat.completions.create({
          model: AI_CONFIG.smartFollowup.model,
          messages: [
            { role: "system", content: systemPrompt },
            { role: "user", content: "Help me understand why my query returned no results and suggest alternatives." },
          ],
          max_completion_tokens: AI_CONFIG.smartFollowup.maxTokens,
          temperature: AI_CONFIG.smartFollowup.temperature,
        });
        return response.choices[0]?.message?.content || "{}";
      };

      const content = await makeRequest();
      const parseResult = await parseAndValidateSmartFollowupResponse(content, makeRequest);

      if (!parseResult.success) {
        return res.json({
          likelyIssue: "unknown",
          suggestedChanges: [],
          clarificationQuestion: "I couldn't analyze your query. Try adjusting your filter values.",
          summary: parseResult.error,
        });
      }

      res.json(parseResult.data);
    } catch (err) {
      console.error("Error processing smart followup:", err);
      res.status(500).json({
        error: err instanceof Error ? err.message : "Failed to process followup",
      });
    }
  });

  // ========== REPORT API ENDPOINTS ==========
  
  // Rate limiter for report operations
  const reportLimiter = rateLimit({
    windowMs: 60 * 1000,
    max: 30,
    message: { error: "Too many report requests, please try again later" },
    standardHeaders: true,
    legacyHeaders: false,
  });
  
  // Rate limiter for AI report operations
  const reportAILimiter = rateLimit({
    windowMs: 60 * 1000,
    max: 15,
    message: { error: "Too many AI requests, please try again later" },
    standardHeaders: true,
    legacyHeaders: false,
  });

  // Get all report pages for current user
  app.get("/api/reports/pages", isAuthenticated, reportLimiter, async (req, res) => {
    try {
      const userId = (req.user as any)?.id;
      if (!userId) {
        return res.status(401).json({ error: "Unauthorized" });
      }

      const pages = await db
        .select()
        .from(reportPages)
        .where(and(eq(reportPages.userId, userId), eq(reportPages.isArchived, false)))
        .orderBy(desc(reportPages.updatedAt));

      res.json(pages);
    } catch (err) {
      console.error("Error fetching report pages:", err);
      res.status(500).json({ error: "Failed to fetch report pages" });
    }
  });

  // Create a new report page
  app.post("/api/reports/pages", isAuthenticated, reportLimiter, async (req, res) => {
    try {
      const userId = (req.user as any)?.id;
      const user = await authStorage.getUser(userId);
      if (!userId || !user) {
        return res.status(401).json({ error: "Unauthorized" });
      }

      const { title, description } = req.body;
      if (!title || typeof title !== "string" || title.trim().length === 0) {
        return res.status(400).json({ error: "Title is required" });
      }

      const [newPage] = await db
        .insert(reportPages)
        .values({
          userId,
          title: title.trim(),
          description: description?.trim() || null,
        })
        .returning();

      await logAudit({
        userId,
        userEmail: user.email,
        action: "REPORT_CREATE",
        details: `Created report page: ${newPage.title}`,
        ip: req.ip || req.socket.remoteAddress,
      });

      res.status(201).json(newPage);
    } catch (err) {
      console.error("Error creating report page:", err);
      res.status(500).json({ error: "Failed to create report page" });
    }
  });

  // Get a single report page with its blocks
  app.get("/api/reports/pages/:id", isAuthenticated, reportLimiter, async (req, res) => {
    try {
      const userId = (req.user as any)?.id;
      if (!userId) {
        return res.status(401).json({ error: "Unauthorized" });
      }

      const { id } = req.params;
      const [page] = await db
        .select()
        .from(reportPages)
        .where(and(eq(reportPages.id, id), eq(reportPages.userId, userId)));

      if (!page) {
        return res.status(404).json({ error: "Report page not found" });
      }

      const blocks = await db
        .select()
        .from(reportBlocks)
        .where(eq(reportBlocks.pageId, id));

      res.json({ ...page, blocks });
    } catch (err) {
      console.error("Error fetching report page:", err);
      res.status(500).json({ error: "Failed to fetch report page" });
    }
  });

  // Update a report page
  app.patch("/api/reports/pages/:id", isAuthenticated, reportLimiter, async (req, res) => {
    try {
      const userId = (req.user as any)?.id;
      const user = await authStorage.getUser(userId);
      if (!userId || !user) {
        return res.status(401).json({ error: "Unauthorized" });
      }

      const { id } = req.params;
      const { title, description } = req.body;

      // Verify ownership
      const [existing] = await db
        .select()
        .from(reportPages)
        .where(and(eq(reportPages.id, id), eq(reportPages.userId, userId)));

      if (!existing) {
        return res.status(404).json({ error: "Report page not found" });
      }

      const updates: Partial<typeof reportPages.$inferInsert> = {
        updatedAt: new Date(),
      };
      if (title !== undefined) updates.title = title.trim();
      if (description !== undefined) updates.description = description?.trim() || null;

      const [updated] = await db
        .update(reportPages)
        .set(updates)
        .where(eq(reportPages.id, id))
        .returning();

      await logAudit({
        userId,
        userEmail: user.email,
        action: "REPORT_UPDATE",
        details: `Updated report page: ${updated.title}`,
        ip: req.ip || req.socket.remoteAddress,
      });

      res.json(updated);
    } catch (err) {
      console.error("Error updating report page:", err);
      res.status(500).json({ error: "Failed to update report page" });
    }
  });

  // Delete (archive) a report page
  app.delete("/api/reports/pages/:id", isAuthenticated, reportLimiter, async (req, res) => {
    try {
      const userId = (req.user as any)?.id;
      const user = await authStorage.getUser(userId);
      if (!userId || !user) {
        return res.status(401).json({ error: "Unauthorized" });
      }

      const { id } = req.params;

      // Verify ownership
      const [existing] = await db
        .select()
        .from(reportPages)
        .where(and(eq(reportPages.id, id), eq(reportPages.userId, userId)));

      if (!existing) {
        return res.status(404).json({ error: "Report page not found" });
      }

      await db
        .update(reportPages)
        .set({ isArchived: true, updatedAt: new Date() })
        .where(eq(reportPages.id, id));

      await logAudit({
        userId,
        userEmail: user.email,
        action: "REPORT_DELETE",
        details: `Archived report page: ${existing.title}`,
        ip: req.ip || req.socket.remoteAddress,
      });

      res.json({ success: true });
    } catch (err) {
      console.error("Error deleting report page:", err);
      res.status(500).json({ error: "Failed to delete report page" });
    }
  });

  // Create a report block
  app.post("/api/reports/pages/:pageId/blocks", isAuthenticated, reportLimiter, async (req, res) => {
    try {
      const userId = (req.user as any)?.id;
      const user = await authStorage.getUser(userId);
      if (!userId || !user) {
        return res.status(401).json({ error: "Unauthorized" });
      }

      const { pageId } = req.params;
      const { kind, title, position, config } = req.body;

      // Verify page ownership
      const [page] = await db
        .select()
        .from(reportPages)
        .where(and(eq(reportPages.id, pageId), eq(reportPages.userId, userId)));

      if (!page) {
        return res.status(404).json({ error: "Report page not found" });
      }

      // Validate block kind
      const validKinds = ["table", "chart", "metric", "text"];
      if (!validKinds.includes(kind)) {
        return res.status(400).json({ error: "Invalid block kind" });
      }

      // Security: Comprehensive validation of block config
      // My Reports is AI-assisted, so bypass visibility - visibility is cosmetic for UI only
      const validation = await validateBlockConfig(config, kind, user, { bypassVisibility: true });
      if (!validation.valid) {
        return res.status(400).json({ error: validation.error });
      }

      const [newBlock] = await db
        .insert(reportBlocks)
        .values({
          pageId,
          kind,
          title: title || null,
          position: position || { row: 0, col: 0, width: 6, height: 4 },
          config,
        })
        .returning();

      // Update page timestamp
      await db
        .update(reportPages)
        .set({ updatedAt: new Date() })
        .where(eq(reportPages.id, pageId));

      await logAudit({
        userId,
        userEmail: user.email,
        action: "REPORT_BLOCK_CREATE",
        details: `Created ${kind} block in report: ${page.title}`,
        ip: req.ip || req.socket.remoteAddress,
      });

      res.status(201).json(newBlock);
    } catch (err) {
      console.error("Error creating report block:", err);
      res.status(500).json({ error: "Failed to create report block" });
    }
  });

  // Update a report block
  app.patch("/api/reports/blocks/:id", isAuthenticated, reportLimiter, async (req, res) => {
    try {
      const userId = (req.user as any)?.id;
      const user = await authStorage.getUser(userId);
      if (!userId || !user) {
        return res.status(401).json({ error: "Unauthorized" });
      }

      const { id } = req.params;
      const { title, position, config } = req.body;

      // Get block and verify ownership through page
      const [block] = await db.select().from(reportBlocks).where(eq(reportBlocks.id, id));
      if (!block) {
        return res.status(404).json({ error: "Block not found" });
      }

      const [page] = await db
        .select()
        .from(reportPages)
        .where(and(eq(reportPages.id, block.pageId), eq(reportPages.userId, userId)));

      if (!page) {
        return res.status(404).json({ error: "Report page not found" });
      }

      // Security: Validate config if provided
      // My Reports is AI-assisted, so bypass visibility - visibility is cosmetic for UI only
      if (config) {
        const validation = await validateBlockConfig(config, block.kind, user, { bypassVisibility: true });
        if (!validation.valid) {
          return res.status(400).json({ error: validation.error });
        }
      }

      const updates: Partial<typeof reportBlocks.$inferInsert> = {
        updatedAt: new Date(),
      };
      if (title !== undefined) updates.title = title;
      if (position !== undefined) updates.position = position;
      if (config !== undefined) updates.config = config;

      const [updated] = await db
        .update(reportBlocks)
        .set(updates)
        .where(eq(reportBlocks.id, id))
        .returning();

      // Update page timestamp
      await db
        .update(reportPages)
        .set({ updatedAt: new Date() })
        .where(eq(reportPages.id, block.pageId));

      res.json(updated);
    } catch (err) {
      console.error("Error updating report block:", err);
      res.status(500).json({ error: "Failed to update report block" });
    }
  });

  // Delete a report block
  app.delete("/api/reports/blocks/:id", isAuthenticated, reportLimiter, async (req, res) => {
    try {
      const userId = (req.user as any)?.id;
      const user = await authStorage.getUser(userId);
      if (!userId || !user) {
        return res.status(401).json({ error: "Unauthorized" });
      }

      const { id } = req.params;

      // Get block and verify ownership through page
      const [block] = await db.select().from(reportBlocks).where(eq(reportBlocks.id, id));
      if (!block) {
        return res.status(404).json({ error: "Block not found" });
      }

      const [page] = await db
        .select()
        .from(reportPages)
        .where(and(eq(reportPages.id, block.pageId), eq(reportPages.userId, userId)));

      if (!page) {
        return res.status(404).json({ error: "Report page not found" });
      }

      await db.delete(reportBlocks).where(eq(reportBlocks.id, id));

      // Update page timestamp
      await db
        .update(reportPages)
        .set({ updatedAt: new Date() })
        .where(eq(reportPages.id, block.pageId));

      await logAudit({
        userId,
        userEmail: user.email,
        action: "REPORT_BLOCK_DELETE",
        details: `Deleted ${block.kind} block from report: ${page.title}`,
        ip: req.ip || req.socket.remoteAddress,
      });

      res.json({ success: true });
    } catch (err) {
      console.error("Error deleting report block:", err);
      res.status(500).json({ error: "Failed to delete report block" });
    }
  });

  // Execute a report block query (with safety guardrails and pagination)
  const REPORT_BLOCK_PAGE_SIZE = 50;
  
  app.post("/api/reports/blocks/:id/run", isAuthenticated, reportLimiter, async (req, res) => {
    try {
      const userId = (req.user as any)?.id;
      const user = await authStorage.getUser(userId);
      if (!userId || !user) {
        return res.status(401).json({ error: "Unauthorized" });
      }

      const { id } = req.params;
      const { page: pageNum = 1, exportAll = false } = req.body; // Pagination support
      const currentPage = Math.max(1, parseInt(pageNum) || 1);
      const MAX_EXPORT_ROWS = 10000; // Safety limit for exports

      // Get block and verify ownership through page
      const [block] = await db.select().from(reportBlocks).where(eq(reportBlocks.id, id));
      if (!block) {
        return res.status(404).json({ error: "Block not found" });
      }

      const [page] = await db
        .select()
        .from(reportPages)
        .where(and(eq(reportPages.id, block.pageId), eq(reportPages.userId, userId)));

      if (!page) {
        return res.status(404).json({ error: "Report page not found" });
      }

      if (block.kind === "text") {
        return res.json({ type: "text", content: (block.config as any).content });
      }

      const config = block.config as TableBlockConfig | ChartBlockConfig | MetricBlockConfig;
      
      // Security: Comprehensive validation before executing any query
      // My Reports is AI-assisted, so bypass visibility - visibility is cosmetic for UI only
      const validation = await validateBlockConfig(config, block.kind, user, { bypassVisibility: true });
      if (!validation.valid) {
        // Distinguish between permission errors (403) and validation errors (400)
        const statusCode = validation.error?.includes("Access denied") ? 403 : 400;
        return res.status(statusCode).json({ error: validation.error });
      }
      
      const pool = getPool(config.database);
      
      // Parse table name to get schema and table
      const parsedTable = parseTableName(config.table);
      if (!parsedTable) {
        return res.status(400).json({ error: "Invalid table name" });
      }
      const tableRef = `"${parsedTable.schema}"."${parsedTable.table}"`;

      let query: string;
      let params: any[] = [];

      if (block.kind === "table") {
        const tableConfig = config as TableBlockConfig;
        const mainAlias = "t1";
        const joinAlias = "t2";
        const subJoinAlias = "t3";
        
        // Helper to determine if a column reference is for subJoin table
        const isSubJoinColumn = (prefix: string): boolean => {
          return prefix.includes("_") || prefix.toLowerCase().includes("district") || prefix.toLowerCase().includes("sub");
        };
        
        // Build column list with proper table aliases for joins
        let columns: string;
        if (tableConfig.columns?.length > 0) {
          columns = tableConfig.columns.map(c => {
            if (c.includes(".")) {
              // Column from joined table (e.g., "joined.email" or "joined_district.name")
              const [prefix, colName] = c.split(".");
              validateIdentifier(colName, "column");
              // Determine which join table this column belongs to
              if (isSubJoinColumn(prefix) && tableConfig.join?.subJoin) {
                return `${subJoinAlias}."${colName}" AS "${c.replace(".", "_")}"`;
              } else {
                return `${joinAlias}."${colName}" AS "${c.replace(".", "_")}"`;
              }
            } else {
              validateIdentifier(c, "column");
              return `${mainAlias}."${c}"`;
            }
          }).join(", ");
        } else {
          columns = `${mainAlias}.*`;
        }
        
        query = `SELECT ${columns} FROM ${tableRef} AS ${mainAlias}`;
        
        // Add JOIN if specified
        if (tableConfig.join?.table) {
          const joinParsed = parseTableName(tableConfig.join.table);
          if (!joinParsed) {
            return res.status(400).json({ error: "Invalid join table name" });
          }
          const joinTableRef = `"${joinParsed.schema}"."${joinParsed.table}"`;
          const joinType = tableConfig.join.type === "inner" ? "INNER JOIN" : "LEFT JOIN";
          const [fromCol, toCol] = tableConfig.join.on;
          validateIdentifier(fromCol, "column");
          validateIdentifier(toCol, "column");
          query += ` ${joinType} ${joinTableRef} AS ${joinAlias} ON ${mainAlias}."${fromCol}" = ${joinAlias}."${toCol}"`;
          
          // Add subJoin if specified (nested join: main -> join -> subJoin)
          if (tableConfig.join.subJoin?.table) {
            const subJoinParsed = parseTableName(tableConfig.join.subJoin.table);
            if (!subJoinParsed) {
              return res.status(400).json({ error: "Invalid subJoin table name" });
            }
            const subJoinTableRef = `"${subJoinParsed.schema}"."${subJoinParsed.table}"`;
            const subJoinType = tableConfig.join.subJoin.type === "inner" ? "INNER JOIN" : "LEFT JOIN";
            const [subFromCol, subToCol] = tableConfig.join.subJoin.on;
            validateIdentifier(subFromCol, "column");
            validateIdentifier(subToCol, "column");
            query += ` ${subJoinType} ${subJoinTableRef} AS ${subJoinAlias} ON ${joinAlias}."${subFromCol}" = ${subJoinAlias}."${subToCol}"`;
          }
        }
        
        // Add filters (handle join columns with dots)
        if (tableConfig.filters?.length > 0) {
          const whereClauses: string[] = [];
          tableConfig.filters.forEach((f) => {
            let columnRef: string;
            if (f.column.includes(".")) {
              // Column from joined table (e.g., "joined.status" or "joined_district.name")
              const [prefix, colName] = f.column.split(".");
              validateIdentifier(colName, "column");
              if (isSubJoinColumn(prefix) && tableConfig.join?.subJoin) {
                columnRef = `${subJoinAlias}."${colName}"`;
              } else {
                columnRef = `${joinAlias}."${colName}"`;
              }
            } else {
              validateIdentifier(f.column, "column");
              columnRef = `${mainAlias}."${f.column}"`;
            }
            const filterWithAlias = { ...f, column: columnRef };
            addFilterToQueryWithAlias(filterWithAlias as any, params, whereClauses);
          });
          query += ` WHERE ${whereClauses.join(" AND ")}`;
        }
        
        // Build base query for counting
        let baseQuery = `FROM ${tableRef} AS ${mainAlias}`;
        if (tableConfig.join?.table) {
          const joinParsed = parseTableName(tableConfig.join.table);
          if (joinParsed) {
            const joinTableRef = `"${joinParsed.schema}"."${joinParsed.table}"`;
            const joinType = tableConfig.join.type === "inner" ? "INNER JOIN" : "LEFT JOIN";
            const [fromCol, toCol] = tableConfig.join.on;
            baseQuery += ` ${joinType} ${joinTableRef} AS ${joinAlias} ON ${mainAlias}."${fromCol}" = ${joinAlias}."${toCol}"`;
            
            // Add subJoin to base query if specified
            if (tableConfig.join.subJoin?.table) {
              const subJoinParsed = parseTableName(tableConfig.join.subJoin.table);
              if (subJoinParsed) {
                const subJoinTableRef = `"${subJoinParsed.schema}"."${subJoinParsed.table}"`;
                const subJoinType = tableConfig.join.subJoin.type === "inner" ? "INNER JOIN" : "LEFT JOIN";
                const [subFromCol, subToCol] = tableConfig.join.subJoin.on;
                baseQuery += ` ${subJoinType} ${subJoinTableRef} AS ${subJoinAlias} ON ${joinAlias}."${subFromCol}" = ${subJoinAlias}."${subToCol}"`;
              }
            }
          }
        }
        if (tableConfig.filters?.length > 0) {
          baseQuery += ` WHERE ${query.split(" WHERE ")[1]?.split(" ORDER BY ")[0] || "1=1"}`;
        }
        
        // Get total count
        const countResult = await pool.query(`SELECT COUNT(*) as count ${baseQuery}`, params);
        const totalCount = parseInt(countResult.rows[0]?.count || "0");
        const totalPages = Math.max(1, Math.ceil(totalCount / REPORT_BLOCK_PAGE_SIZE));
        const safePage = Math.min(Math.max(1, currentPage), totalPages);
        const offset = (safePage - 1) * REPORT_BLOCK_PAGE_SIZE;
        
        // Add order by (handle join columns with dots)
        if (tableConfig.orderBy) {
          let orderColumnRef: string;
          if (tableConfig.orderBy.column.includes(".")) {
            const [prefix, colName] = tableConfig.orderBy.column.split(".");
            validateIdentifier(colName, "column");
            if (isSubJoinColumn(prefix) && tableConfig.join?.subJoin) {
              orderColumnRef = `${subJoinAlias}."${colName}"`;
            } else {
              orderColumnRef = `${joinAlias}."${colName}"`;
            }
          } else {
            validateIdentifier(tableConfig.orderBy.column, "column");
            orderColumnRef = `${mainAlias}."${tableConfig.orderBy.column}"`;
          }
          query += ` ORDER BY ${orderColumnRef} ${tableConfig.orderBy.direction === "desc" ? "DESC" : "ASC"}`;
        }
        
        // Add pagination or export limit
        if (exportAll) {
          query += ` LIMIT ${MAX_EXPORT_ROWS}`;
        } else {
          query += ` LIMIT ${REPORT_BLOCK_PAGE_SIZE} OFFSET ${offset}`;
        }
        
        const result = await pool.query(query, params);
        
        await logAudit({
          userId,
          userEmail: user.email,
          action: exportAll ? "REPORT_EXPORT" : "REPORT_QUERY",
          database: config.database,
          table: config.table,
          details: exportAll 
            ? `Table block export: ${result.rows.length} rows${tableConfig.join ? ` (joined with ${tableConfig.join.table})` : ''}`
            : `Table block query: page ${safePage} of ${totalPages} (${result.rows.length} rows)${tableConfig.join ? ` (joined with ${tableConfig.join.table})` : ''}`,
          ip: req.ip || req.socket.remoteAddress,
        });
        
        res.json({ 
          type: "table", 
          rows: result.rows, 
          rowCount: result.rows.length,
          totalCount,
          page: exportAll ? 1 : safePage,
          pageSize: exportAll ? result.rows.length : REPORT_BLOCK_PAGE_SIZE,
          totalPages: exportAll ? 1 : totalPages
        });
        
      } else if (block.kind === "chart") {
        const chartConfig = config as ChartBlockConfig;
        validateIdentifier(chartConfig.xColumn, "column");
        validateIdentifier(chartConfig.yColumn, "column");
        
        let selectPart: string;
        if (chartConfig.aggregateFunction && chartConfig.groupBy) {
          const aggFunc = chartConfig.aggregateFunction.toUpperCase();
          if (!["COUNT", "SUM", "AVG", "MIN", "MAX"].includes(aggFunc)) {
            return res.status(400).json({ error: "Invalid aggregate function" });
          }
          
          // Handle special date-based grouping
          const dateGroupByValues = ["month", "year", "day", "week", "quarter"];
          const isDateGroupBy = dateGroupByValues.includes(chartConfig.groupBy.toLowerCase());
          
          let groupByExpr: string;
          let labelExpr: string;
          
          if (isDateGroupBy) {
            // Use xColumn as the date column for date-based grouping
            // Convert to Pacific Time before grouping so dates align with user expectations
            const dateCol = `"${chartConfig.xColumn}"`;
            const dateColPST = `(${dateCol} AT TIME ZONE 'UTC' AT TIME ZONE 'America/Los_Angeles')`;
            const datePart = chartConfig.groupBy.toLowerCase();
            
            if (datePart === "month") {
              labelExpr = `TO_CHAR(${dateColPST}, 'YYYY-MM')`;
              groupByExpr = labelExpr;
            } else if (datePart === "year") {
              labelExpr = `TO_CHAR(${dateColPST}, 'YYYY')`;
              groupByExpr = labelExpr;
            } else if (datePart === "day") {
              labelExpr = `TO_CHAR(${dateColPST}, 'YYYY-MM-DD')`;
              groupByExpr = labelExpr;
            } else if (datePart === "week") {
              labelExpr = `TO_CHAR(${dateColPST}, 'IYYY-IW')`;
              groupByExpr = labelExpr;
            } else if (datePart === "quarter") {
              labelExpr = `TO_CHAR(${dateColPST}, 'YYYY-"Q"Q')`;
              groupByExpr = labelExpr;
            } else {
              labelExpr = dateCol;
              groupByExpr = dateCol;
            }
          } else {
            validateIdentifier(chartConfig.groupBy, "column");
            labelExpr = `"${chartConfig.groupBy}"`;
            groupByExpr = `"${chartConfig.groupBy}"`;
          }
          
          selectPart = `${labelExpr} as label, ${aggFunc}("${chartConfig.yColumn}") as value`;
          query = `SELECT ${selectPart} FROM ${tableRef}`;
          
          // Add filters
          if (chartConfig.filters?.length > 0) {
            const whereClauses: string[] = [];
            chartConfig.filters.forEach((f) => {
              addFilterToQuery(f, params, whereClauses);
            });
            query += ` WHERE ${whereClauses.join(" AND ")}`;
          }
          
          query += ` GROUP BY ${groupByExpr} ORDER BY ${groupByExpr} LIMIT 500`;
        } else {
          selectPart = `"${chartConfig.xColumn}" as label, "${chartConfig.yColumn}" as value`;
          query = `SELECT ${selectPart} FROM ${tableRef}`;
          
          // Add filters
          if (chartConfig.filters?.length > 0) {
            const whereClauses: string[] = [];
            chartConfig.filters.forEach((f) => {
              addFilterToQuery(f, params, whereClauses);
            });
            query += ` WHERE ${whereClauses.join(" AND ")}`;
          }
          
          query += ` LIMIT 500`;
        }
        
        const result = await pool.query(query, params);
        
        await logAudit({
          userId,
          userEmail: user.email,
          action: "REPORT_QUERY",
          database: config.database,
          table: config.table,
          details: `Chart block query: ${result.rows.length} data points`,
          ip: req.ip || req.socket.remoteAddress,
        });
        
        res.json({ 
          type: "chart", 
          chartType: chartConfig.chartType,
          data: result.rows,
        });
        
      } else if (block.kind === "metric") {
        const metricConfig = config as MetricBlockConfig;
        
        const aggFunc = metricConfig.aggregateFunction.toUpperCase();
        if (!["COUNT", "SUM", "AVG", "MIN", "MAX"].includes(aggFunc)) {
          return res.status(400).json({ error: "Invalid aggregate function" });
        }
        
        // Handle JOIN if present
        const mainAlias = "m";
        const joinAlias = "joined";
        let fromClause = `${tableRef} AS ${mainAlias}`;
        let columnRef: string;
        
        // Determine if column is from joined table (e.g., "joined.price")
        if (metricConfig.column.includes(".")) {
          const parts = metricConfig.column.split(".");
          if (parts.length !== 2) {
            return res.status(400).json({ error: "Invalid column reference format" });
          }
          const [prefix, colName] = parts;
          validateIdentifier(colName, "column");
          
          // Check if we have a join config
          if (!(metricConfig as any).join) {
            return res.status(400).json({ error: "Cannot use dotted column reference without join config" });
          }
          
          // Use the join alias
          columnRef = `${joinAlias}."${colName}"`;
        } else {
          validateIdentifier(metricConfig.column, "column");
          columnRef = `${mainAlias}."${metricConfig.column}"`;
        }
        
        // Build JOIN if present
        if ((metricConfig as any).join?.table) {
          const joinConfig = (metricConfig as any).join;
          const joinParsed = parseTableName(joinConfig.table);
          if (!joinParsed) {
            return res.status(400).json({ error: "Invalid join table name" });
          }
          const joinTableRef = `"${joinParsed.schema}"."${joinParsed.table}"`;
          const joinType = joinConfig.type === "inner" ? "INNER JOIN" : "LEFT JOIN";
          const [fromCol, toCol] = joinConfig.on;
          validateIdentifier(fromCol, "column");
          validateIdentifier(toCol, "column");
          fromClause += ` ${joinType} ${joinTableRef} AS ${joinAlias} ON ${mainAlias}."${fromCol}" = ${joinAlias}."${toCol}"`;
        }
        
        query = `SELECT ${aggFunc}(${columnRef}) as value FROM ${fromClause}`;
        
        // Add filters (need to handle aliased columns)
        if (metricConfig.filters?.length > 0) {
          const whereClauses: string[] = [];
          metricConfig.filters.forEach((f) => {
            // For filters, use main alias for non-dotted columns
            const filterCol = f.column.includes(".") 
              ? `${f.column.split(".")[0]}."${f.column.split(".")[1]}"`
              : `${mainAlias}."${f.column}"`;
            
            const paramIndex = params.length + 1;
            if (f.operator === "eq") {
              whereClauses.push(`${filterCol} = $${paramIndex}`);
              params.push(f.value);
            } else if (f.operator === "contains") {
              whereClauses.push(`${filterCol}::text ILIKE $${paramIndex}`);
              params.push(`%${f.value}%`);
            } else if (f.operator === "gt") {
              whereClauses.push(`${filterCol} > $${paramIndex}`);
              params.push(f.value);
            } else if (f.operator === "gte") {
              whereClauses.push(`${filterCol} >= $${paramIndex}`);
              params.push(f.value);
            } else if (f.operator === "lt") {
              whereClauses.push(`${filterCol} < $${paramIndex}`);
              params.push(f.value);
            } else if (f.operator === "lte") {
              whereClauses.push(`${filterCol} <= $${paramIndex}`);
              params.push(f.value);
            } else if (f.operator === "between" && Array.isArray(f.value) && f.value.length === 2) {
              whereClauses.push(`${filterCol} >= $${paramIndex} AND ${filterCol} <= $${paramIndex + 1}`);
              params.push(f.value[0], f.value[1]);
            }
          });
          query += ` WHERE ${whereClauses.join(" AND ")}`;
        }
        
        const result = await pool.query(query, params);
        
        await logAudit({
          userId,
          userEmail: user.email,
          action: "REPORT_QUERY",
          database: config.database,
          table: config.table,
          details: `Metric block query: ${metricConfig.aggregateFunction}(${metricConfig.column})`,
          ip: req.ip || req.socket.remoteAddress,
        });
        
        res.json({
          type: "metric",
          value: result.rows[0]?.value || 0,
          label: metricConfig.label || `${metricConfig.aggregateFunction}(${metricConfig.column})`,
          format: metricConfig.format || "number",
        });
      }
    } catch (err) {
      console.error("Error running report block:", err);
      res.status(500).json({ error: err instanceof Error ? err.message : "Failed to run report block" });
    }
  });

  // Get chat history for a report page
  app.get("/api/reports/pages/:pageId/chat", isAuthenticated, async (req, res) => {
    try {
      const userId = (req.user as any)?.id;
      if (!userId) {
        return res.status(401).json({ error: "Unauthorized" });
      }

      const { pageId } = req.params;

      // Verify page ownership
      const [page] = await db
        .select()
        .from(reportPages)
        .where(and(eq(reportPages.id, pageId), eq(reportPages.userId, userId)));

      if (!page) {
        return res.status(404).json({ error: "Report page not found" });
      }

      // Get chat session
      const [session] = await db
        .select()
        .from(reportChatSessions)
        .where(eq(reportChatSessions.pageId, pageId));

      res.json({ messages: session?.messages || [] });
    } catch (err) {
      console.error("Error fetching chat history:", err);
      res.status(500).json({ error: "Failed to fetch chat history" });
    }
  });

  // AI Chat endpoint for report building
  app.post("/api/reports/ai/chat", isAuthenticated, reportAILimiter, async (req, res) => {
    try {
      const userId = (req.user as any)?.id;
      const user = await authStorage.getUser(userId);
      if (!userId || !user) {
        return res.status(401).json({ error: "Unauthorized" });
      }

      const { pageId, message } = req.body;
      if (!pageId || !message) {
        return res.status(400).json({ error: "pageId and message are required" });
      }

      // Verify page ownership
      const [page] = await db
        .select()
        .from(reportPages)
        .where(and(eq(reportPages.id, pageId), eq(reportPages.userId, userId)));

      if (!page) {
        return res.status(404).json({ error: "Report page not found" });
      }

      const client = getOpenAIClient();
      if (!client) {
        return res.status(503).json({ error: "AI service not available" });
      }

      // Get available tables with columns for context
      const dbs = getDatabaseConnections();
      let availableTablesWithColumns: { database: string; tables: Array<TableInfo & { columns: string[] }> }[] = [];
      
      for (const dbConn of dbs) {
        try {
          const pool = getPool(dbConn.name);
          const tableResult = await pool.query(`
            SELECT table_schema as schema, table_name as name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ORDER BY table_name
          `);
          
          const allSettings = await storage.getAllTableSettings();
          let tables: Array<TableInfo & { columns: string[] }> = [];
          
          // For external customers, get their allowed tables list once
          const allowedTables = user.role === "external_customer" 
            ? await getAllowedTables(userId) 
            : null;
          
          for (const t of tableResult.rows) {
            const fullName = `${t.schema}.${t.name}`;
            const settingsKey = `${dbConn.name}:${fullName}`;
            const isVisible = allSettings[settingsKey]?.isVisible ?? true;
            
            // External customers are limited to their granted tables only
            // (this is a real permission, not cosmetic visibility)
            if (allowedTables && !allowedTables.includes(`${dbConn.name}:${fullName}`)) continue;
            
            // NOTE: We do NOT skip tables based on isVisible for AI
            // Visibility is cosmetic for the UI only - AI has full access to all tables
            
            // Fetch columns for this table
            const columnResult = await pool.query(`
              SELECT column_name FROM information_schema.columns
              WHERE table_schema = $1 AND table_name = $2
              ORDER BY ordinal_position
            `, [t.schema, t.name]);
            
            tables.push({
              schema: t.schema,
              name: t.name,
              fullName,
              displayName: allSettings[settingsKey]?.displayName || null,
              isVisible,
              columns: columnResult.rows.map((c: any) => c.column_name),
            });
          }

          availableTablesWithColumns.push({ database: dbConn.name, tables });
        } catch (err) {
          console.error(`Error fetching tables for ${dbConn.name}:`, err);
        }
      }

      // Get current blocks for context
      const blocks = await db.select().from(reportBlocks).where(eq(reportBlocks.pageId, pageId));

      // Get or create chat session
      let [session] = await db
        .select()
        .from(reportChatSessions)
        .where(eq(reportChatSessions.pageId, pageId));

      if (!session) {
        [session] = await db
          .insert(reportChatSessions)
          .values({ pageId, messages: [] })
          .returning();
      }

      const messages: ChatMessage[] = session.messages || [];
      messages.push({
        role: "user",
        content: message,
        timestamp: new Date().toISOString(),
      });

      // Get current date for relative date references (in Pacific Time)
      const today = new Date();
      const pacificFormatter = new Intl.DateTimeFormat('en-CA', { 
        timeZone: 'America/Los_Angeles', 
        year: 'numeric', 
        month: '2-digit', 
        day: '2-digit' 
      });
      const todayStr = pacificFormatter.format(today); // YYYY-MM-DD format in PST/PDT
      
      const systemPrompt = `You are a helpful report building assistant. You help users create custom reports with tables, charts, and metrics.

IMPORTANT: Today's date is ${todayStr} (Pacific Time - PST/PDT). Use this for any relative date references like "yesterday", "last week", "this month", etc. All date/time queries should be interpreted in Pacific Time.

IMPORTANT: You MUST only use the exact column names listed below. Do NOT guess or invent column names.

AVAILABLE TABLES AND COLUMNS:
${availableTablesWithColumns.map(db => 
  `Database: ${db.database}\n${db.tables.map(t => 
    `  - ${t.displayName || t.name} (${t.fullName})\n    Columns: ${t.columns.slice(0, 20).join(", ")}${t.columns.length > 20 ? ` (and ${t.columns.length - 20} more)` : ""}`
  ).join("\n")}`
).join("\n\n")}

CURRENT REPORT: "${page.title}"
CURRENT BLOCKS: ${blocks.length === 0 ? "None yet" : blocks.map(b => `${b.kind}: ${b.title || "Untitled"}`).join(", ")}

You can help users by:
1. Suggesting which tables to use for their reporting needs
2. Recommending chart types (bar, line, pie, area) for their data
3. Explaining what metrics (count, sum, avg, min, max) would be useful
4. Helping structure their reports

When the user wants to add a block, respond with a JSON action in this format:
{
  "action": "create_block",
  "block": {
    "kind": "table|chart|metric|text",
    "title": "Block title",
    "config": { ... config based on kind ... }
  },
  "explanation": "Why this block is useful"
}

To create MULTIPLE blocks at once (for comparisons), use this format:
{
  "action": "create_blocks",
  "blocks": [
    { "kind": "table", "title": "Period 1", "config": { ... } },
    { "kind": "table", "title": "Period 2", "config": { ... } }
  ],
  "explanation": "Why these blocks are useful for comparison"
}

For table blocks, config should have: database, table, columns (array of exact column names from above), filters (array), orderBy, rowLimit
For table blocks with JOINS (to pull data from related tables):
- Add a "join" object with: table (the related table like "public.vendors"), on (array of two column names [fromColumn, toColumn] like ["vendor_id", "id"])
- For columns from the joined table, prefix with "joined." like "joined.email" or "joined.first_name"
- CRITICAL: When using joins, you MUST use the EXACT column names from the joined table as listed in AVAILABLE TABLES above. For example, if the vendors table has "first_name" and "last_name" columns, use "joined.first_name" and "joined.last_name" (NOT "joined.firstname" or "joined.lastName")
- Example join config: { "table": "public.vendors", "on": ["vendor_id", "id"] }
- Check the column list for the joined table before constructing joined.column_name references

For NESTED JOINS (sub-joins) when you need to traverse through two tables:
- Add a "subJoin" object inside the "join" object to join from the first joined table to a third table
- subJoin has: table (the third table like "public.districts"), on (array of [fromColumnInJoinTable, toColumnInSubJoinTable])
- For columns from the sub-joined table, use prefix "joined_" followed by the table name, like "joined_districts.name" or "joined_district.name"
- Example: To get district name from bookings -> addresses -> districts:
  { "join": { "table": "public.addresses", "on": ["address_id", "id"], "subJoin": { "table": "public.districts", "on": ["district_id", "id"] } } }
  Then use columns like "joined_district.name" to get the district name (NOT "label" - use the EXACT column name from districts table which is "name")
- CRITICAL: Always check the AVAILABLE TABLES list above for the exact column names in the sub-joined table

For chart blocks, config should have: database, table, chartType, xColumn (the date/timestamp column to group by), yColumn (the column to aggregate), aggregateFunction, groupBy (can be a column name OR one of: "month", "year", "day", "week", "quarter" for date-based grouping), filters, rowLimit
For metric blocks, config should have: database, table, column, aggregateFunction, filters, label, format

FILTER OPERATORS - ONLY USE THESE EXACT VALUES (no others allowed):
- "eq" for equals (use this for exact matches, NOT "=" or "==" or "in")
- "contains" for text contains/partial match
- "gt" for greater than
- "gte" for greater than or equal
- "lt" for less than
- "lte" for less than or equal
- "between" for date ranges (value must be array of two dates like ["2025-01-01", "2025-12-31"])

IMPORTANT: Do NOT use "in", "like", "!=", or any other operators. For matching one of multiple values, use multiple filters with "eq" operator or use "contains" for partial matching.

CRITICAL DATE RANGE COMPARISONS: When the user asks to compare TWO different date ranges (e.g., "Jan 5-11 vs Jan 12-18"), you MUST create TWO SEPARATE blocks - one for each date range. This is because all filters are combined with AND logic, so putting two "between" filters on the same column in one block will return zero results (a date cannot be in two non-overlapping ranges simultaneously). For comparisons, create separate blocks like:
- Block 1: "Bookings: Jan 5-11, 2026" with filter between ["2026-01-05", "2026-01-11"]
- Block 2: "Bookings: Jan 12-18, 2026" with filter between ["2026-01-12", "2026-01-18"]
This allows side-by-side comparison of the two periods.

CRITICAL: Only use column names that are listed above. For date-based grouping, use groupBy: "month" (or year/day/week/quarter) with xColumn set to the date column like "created_at".

If you're just providing information or need clarification, respond with plain text.
IMPORTANT: If you cannot create a block because the request is unclear, you don't have enough information, or you're unsure which columns/tables to use, you MUST ask the user a clarifying question. Never leave the user without a response - either create a block OR ask a specific question to help you understand what they need.
Always be helpful and explain your suggestions in simple terms.`;

      const response = await client.chat.completions.create({
        model: "gpt-4o",
        messages: [
          { role: "system", content: systemPrompt },
          ...messages.slice(-10).map(m => ({ role: m.role as "user" | "assistant", content: m.content })),
        ],
        max_completion_tokens: 1000,
        temperature: 0.7,
      });

      let assistantMessage = response.choices[0]?.message?.content || "I'm sorry, I couldn't process that request.";

      messages.push({
        role: "assistant",
        content: assistantMessage,
        timestamp: new Date().toISOString(),
      });

      // Update session
      await db
        .update(reportChatSessions)
        .set({ messages, updatedAt: new Date() })
        .where(eq(reportChatSessions.id, session.id));

      await logAudit({
        userId,
        userEmail: user.email,
        action: "REPORT_AI_CHAT",
        details: `AI chat in report: ${page.title}`,
        ip: req.ip || req.socket.remoteAddress,
      });

      // Try to parse action from response
      let action = null;
      let validatedAction = null;
      let displayMessage = assistantMessage; // Message to show user (without JSON)
      try {
        const jsonMatch = assistantMessage.match(/\{[\s\S]*"action"[\s\S]*\}/);
        if (jsonMatch) {
          action = JSON.parse(jsonMatch[0]);
          // Remove JSON from displayed message - users don't need to see the code
          displayMessage = assistantMessage.replace(jsonMatch[0], "").trim();
          // Clean up any leftover markdown code block markers
          displayMessage = displayMessage.replace(/```json\s*/g, "").replace(/```\s*/g, "").trim();
          
          const validKinds = ["table", "chart", "metric", "text"];
          
          // Helper function to validate a single block
          // AI bypasses visibility settings - visibility is cosmetic for UI only
          const validateSingleBlock = async (block: any) => {
            if (!validKinds.includes(block.kind)) return null;
            
            const config = {
              ...block.config,
              database: block.config?.database || "Default",
              rowLimit: Math.min(block.config?.rowLimit || 500, 10000),
              filters: block.config?.filters || [],
            };
            
            // AI-generated blocks bypass visibility (external customer grants are still enforced)
            const validation = await validateBlockConfig(config, block.kind, user, { bypassVisibility: true });
            
            if (validation.valid) {
              return {
                kind: block.kind,
                title: block.title || `${block.kind} block`,
                config,
              };
            } else {
              console.log(`[SECURITY] AI suggested invalid block config: ${validation.error}`);
              return { error: validation.error };
            }
          };
          
          // Handle single block creation
          if (action?.action === "create_block" && action?.block) {
            const result = await validateSingleBlock(action.block);
            if (result && !('error' in result)) {
              validatedAction = {
                action: "create_block",
                block: result,
                explanation: action.explanation || "",
              };
            } else if (result && 'error' in result) {
              displayMessage += `\n\n**Note:** I wasn't able to create this block because: ${result.error}. Please try rephrasing your request or ask me which columns are available in the table you want to use.`;
            }
          }
          
          // Handle multiple blocks creation (for comparisons)
          if (action?.action === "create_blocks" && Array.isArray(action?.blocks)) {
            const validatedBlocks: any[] = [];
            const errors: string[] = [];
            
            for (const block of action.blocks) {
              const result = await validateSingleBlock(block);
              if (result && !('error' in result)) {
                validatedBlocks.push(result);
              } else if (result && 'error' in result && result.error) {
                errors.push(result.error);
              }
            }
            
            if (validatedBlocks.length > 0) {
              validatedAction = {
                action: "create_blocks",
                blocks: validatedBlocks,
                explanation: action.explanation || "",
              };
            }
            
            if (errors.length > 0) {
              displayMessage += `\n\n**Note:** Some blocks could not be created: ${errors.join("; ")}. Please try rephrasing your request.`;
            }
          }
        }
      } catch {
        // Not a JSON response, that's fine
      }

      // Use displayMessage (with JSON stripped) for user-facing content
      // Add the explanation from the action if available and displayMessage is empty
      let finalMessage = displayMessage;
      if (!finalMessage && validatedAction?.explanation) {
        finalMessage = validatedAction.explanation;
      } else if (validatedAction?.explanation && !finalMessage.includes(validatedAction.explanation)) {
        finalMessage = finalMessage ? `${finalMessage}\n\n${validatedAction.explanation}` : validatedAction.explanation;
      }
      
      res.json({
        message: finalMessage || "I've created the block for you.",
        action: validatedAction,
      });
    } catch (err) {
      console.error("Error in AI chat:", err);
      res.status(500).json({ error: "Failed to process AI request" });
    }
  });

  // Get list of zones (district abbreviations) for filtering
  app.get("/api/zones/:database", isAuthenticated, async (req, res) => {
    try {
      const { database } = req.params;
      const pool = getPool(database);
      
      const result = await pool.query(`
        SELECT DISTINCT abbreviation as zone
        FROM public.districts
        WHERE abbreviation IS NOT NULL AND abbreviation != ''
        ORDER BY abbreviation
      `);
      
      res.json(result.rows.map(r => r.zone));
    } catch (err) {
      console.error("Error fetching zones:", err);
      res.status(500).json({ error: "Failed to fetch zones" });
    }
  });

  // Weekly Performance Dashboard API
  app.get("/api/weekly-performance/:database", isAuthenticated, async (req, res) => {
    try {
      const { database } = req.params;
      const zonesParam = req.query.zones as string | undefined;
      const forceRefresh = req.query.refresh === "true";
      const userId = (req.user as any)?.id;
      const user = await authStorage.getUser(userId);
      
      if (!user) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      
      // Parse zones filter (comma-separated list of zone abbreviations)
      const selectedZones = zonesParam ? zonesParam.split(',').filter(z => z.trim()) : [];
      
      // Check cache first (unless force refresh)
      const cacheKey = getCacheKey("marketing", database, "weekly", undefined, selectedZones);
      if (!forceRefresh) {
        const cachedData = getFromCache<any>(cacheKey);
        if (cachedData) {
          console.log(`[Cache HIT] Marketing dashboard: ${cacheKey}`);
          return res.json({ ...cachedData, fromCache: true });
        }
      }
      console.log(`[Cache MISS] Marketing dashboard: ${cacheKey}${forceRefresh ? ' (force refresh)' : ''}`);
      
      const pool = getPool(database);
      
      // Build zone filter subquery for booking-related queries
      // Join path: bookings.address_id -> addresses.id -> addresses.district_id -> districts.id -> districts.abbreviation
      const buildZoneFilter = (bookingAlias: string = 'b', paramOffset: number = 2): { clause: string; params: string[] } => {
        if (selectedZones.length === 0) {
          return { clause: '', params: [] };
        }
        const placeholders = selectedZones.map((_, i) => `$${paramOffset + i}`).join(', ');
        return {
          clause: `
            AND ${bookingAlias}.address_id IN (
              SELECT addr.id FROM public.addresses addr
              INNER JOIN public.districts d ON d.id = addr.district_id
              WHERE d.abbreviation IN (${placeholders})
            )
          `,
          params: selectedZones
        };
      };
      
      // Generate week ranges from Dec 29, 2025 to current week (Mon-Sun, PST)
      // Use explicit PST dates to handle DST correctly
      const weeks: { startUTC: string; endUTC: string; label: string }[] = [];
      
      // Helper to get PST offset for a given date (handles DST)
      const getPSTOffset = (date: Date): string => {
        // PST = UTC-8, PDT = UTC-7
        // PDT: Second Sunday of March to First Sunday of November
        const month = date.getMonth();
        const day = date.getDate();
        
        // March (2) - PDT starts second Sunday
        // November (10) - PDT ends first Sunday
        if (month > 2 && month < 10) {
          return "-07:00"; // PDT
        } else if (month < 2 || month > 10) {
          return "-08:00"; // PST
        } else if (month === 2) {
          // March - check if after second Sunday
          const secondSunday = 14 - new Date(date.getFullYear(), 2, 1).getDay();
          return day >= secondSunday ? "-07:00" : "-08:00";
        } else {
          // November - check if before first Sunday
          const firstSunday = 7 - new Date(date.getFullYear(), 10, 1).getDay();
          if (firstSunday === 7) return day >= 7 ? "-08:00" : "-07:00";
          return day >= firstSunday ? "-08:00" : "-07:00";
        }
      };
      
      // Start from Dec 29, 2025 (Monday) - iterate by week
      let currentYear = 2025;
      let currentMonth = 11; // December (0-indexed)
      let currentDay = 29;
      
      const now = new Date();
      
      while (true) {
        // Build week start date string in PST
        const weekStartDate = new Date(currentYear, currentMonth, currentDay);
        const startOffset = getPSTOffset(weekStartDate);
        const startDateStr = `${currentYear}-${String(currentMonth + 1).padStart(2, '0')}-${String(currentDay).padStart(2, '0')}T00:00:00${startOffset}`;
        const weekStartUTC = new Date(startDateStr).toISOString();
        
        // Week end is 7 days later (next Monday at midnight) for exclusive upper bound
        // This ensures we capture all of Sunday (the 7th day of the week)
        const weekEndDate = new Date(currentYear, currentMonth, currentDay + 7);
        const endYear = weekEndDate.getFullYear();
        const endMonth = weekEndDate.getMonth();
        const endDay = weekEndDate.getDate();
        const endOffset = getPSTOffset(weekEndDate);
        const endDateStr = `${endYear}-${String(endMonth + 1).padStart(2, '0')}-${String(endDay).padStart(2, '0')}T00:00:00${endOffset}`;
        const weekEndUTC = new Date(endDateStr).toISOString();
        
        // If week start is past today, stop
        if (new Date(weekStartUTC) > now) break;
        
        // Format label as "Dec 29 - Jan 4" (use Sunday, which is 6 days after Monday start)
        const labelEndDate = new Date(currentYear, currentMonth, currentDay + 6); // Sunday
        const startMonthLabel = weekStartDate.toLocaleDateString("en-US", { month: "short" });
        const startDayLabel = weekStartDate.getDate();
        const endMonthLabel = labelEndDate.toLocaleDateString("en-US", { month: "short" });
        const endDayLabel = labelEndDate.getDate();
        
        const label = startMonthLabel === endMonthLabel 
          ? `${startMonthLabel} ${startDayLabel} - ${endDayLabel}`
          : `${startMonthLabel} ${startDayLabel} - ${endMonthLabel} ${endDayLabel}`;
        
        weeks.push({ startUTC: weekStartUTC, endUTC: weekEndUTC, label });
        
        // Move to next Monday (add 7 days)
        const nextMonday = new Date(currentYear, currentMonth, currentDay + 7);
        currentYear = nextMonday.getFullYear();
        currentMonth = nextMonday.getMonth();
        currentDay = nextMonday.getDate();
      }
      
      // Calculate metrics for each week
      const weeklyData: any[] = [];
      
      for (const week of weeks) {
        const weekStartUTC = week.startUTC;
        const weekEndUTC = week.endUTC;
        
        // Build zone filter for this iteration
        const zoneFilter = buildZoneFilter('b', 3); // params start at $3 (after weekStart, weekEnd)
        const zoneFilterNoAlias = buildZoneFilter('public.bookings', 3);
        const baseParams = [weekStartUTC, weekEndUTC];
        const paramsWithZones = [...baseParams, ...zoneFilter.params];
        
        // Run all queries in parallel for efficiency
        const [
          bookingsCreatedResult,
          bookingsDueResult,
          bookingsCompletedResult,
          revenueResult,
          signupsResult,
          newUsersWithBookingsResult,
          subscriptionRevenueResult,
          subscriptionFeesResult,
          memberBookingsResult,
          newSubscriptionsResult,
          memberBookingsRevenueResult,
          customerFeesResult,
          tipsResult,
          creditPacksResult,
          refundsResult,
        ] = await Promise.all([
          // 1. Bookings Created (created_at in week)
          pool.query(`
            SELECT COUNT(*) as count 
            FROM public.bookings b
            WHERE b.created_at >= $1 AND b.created_at < $2
            ${zoneFilter.clause}
          `, paramsWithZones),
          
          // 2. Bookings Due (date_due in week)
          pool.query(`
            SELECT COUNT(*) as count 
            FROM public.bookings b
            WHERE b.date_due >= $1 AND b.date_due < $2
            ${zoneFilter.clause}
          `, paramsWithZones),
          
          // 3. Bookings Completed (date_due in week AND status = 'done')
          pool.query(`
            SELECT COUNT(*) as count 
            FROM public.bookings b
            WHERE b.date_due >= $1 AND b.date_due < $2 AND b.status = 'done'
            ${zoneFilter.clause}
          `, paramsWithZones),
          
          // 6, 7, 8: Revenue metrics for completed bookings (including stripe fees)
          pool.query(`
            SELECT 
              COALESCE(AVG(b.price), 0) as avg_price,
              COALESCE(SUM(b.price), 0) as total_revenue,
              COALESCE(SUM(b.margin), 0) as total_profit,
              COALESCE(SUM(b.stripe_fee), 0) as total_stripe_fees
            FROM public.bookings b
            WHERE b.date_due >= $1 AND b.date_due < $2 AND b.status = 'done'
            ${zoneFilter.clause}
          `, paramsWithZones),
          
          // 10. Sign Ups (new users created_at in week) - NOT zone-filtered (users don't have zones)
          pool.query(`
            SELECT COUNT(*) as count 
            FROM public.users 
            WHERE created_at >= $1 AND created_at < $2
          `, baseParams),
          
          // 11. New Users who have any booking (signed up in week AND have at least one booking ever)
          // Zone filter applies to the booking join
          pool.query(`
            SELECT COUNT(DISTINCT u.id) as count 
            FROM public.users u
            INNER JOIN public.bookings b ON b.user_id = u.id
            WHERE u.created_at >= $1 AND u.created_at < $2
            ${zoneFilter.clause}
          `, paramsWithZones),
          
          // 13. Subscription Revenue and Margin (price and margin of UNIQUE completed bookings with date_due in week, linked to subscription_usages)
          pool.query(`
            SELECT 
              COALESCE(SUM(price), 0) as total_revenue,
              COALESCE(SUM(margin), 0) as total_margin
            FROM (
              SELECT DISTINCT b.id, b.price, b.margin
              FROM public.bookings b
              INNER JOIN public.subscription_usages su ON su.booking_id = b.id
              WHERE b.date_due >= $1 AND b.date_due < $2
                AND b.status = 'done'
                ${zoneFilter.clause}
            ) unique_bookings
          `, paramsWithZones).catch(() => ({ rows: [{ total_revenue: 0, total_margin: 0 }] })),
          
          // 13b. Subscription Fees (paid subscription_invoices updated in week, with price based on price_plan_id)
          // Use DISTINCT ON to avoid double-counting when same subscription has multiple invoices
          // Exclude subscriptions with status='trialing' (first month free)
          // Also includes $59 cancellation fees for subscriptions with valid cancellation_fee_charge_id
          // Note: Subscription fees are NOT zone-filtered (subscriptions don't have zones directly)
          pool.query(`
            SELECT COALESCE(
              (SELECT SUM(fee_amount) FROM (
                SELECT DISTINCT ON (si.subscription_id)
                  CASE 
                    WHEN s.price_plan_id = 11 THEN 96.00
                    WHEN s.price_plan_id = 10 THEN 9.99
                    ELSE 0
                  END as fee_amount
                FROM public.subscription_invoices si
                INNER JOIN public.subscriptions s ON s.id = si.subscription_id
                WHERE si.updated_at >= $1 AND si.updated_at < $2
                  AND si.status = 'paid'
                  AND s.status != 'trialing'
                ORDER BY si.subscription_id, si.updated_at DESC
              ) invoice_fees), 0) +
              COALESCE(
                (SELECT COUNT(*) * 59.00
                FROM public.subscriptions
                WHERE updated_at >= $1 AND updated_at < $2
                  AND cancellation_fee_charge_id IS NOT NULL 
                  AND cancellation_fee_charge_id != ''
              ), 0) as total
          `, baseParams).catch(() => ({ rows: [{ total: 0 }] })),
          
          // 14. Member Bookings (unique completed bookings with date_due in week, linked to subscription_usages)
          pool.query(`
            SELECT COUNT(DISTINCT b.id) as count
            FROM public.bookings b
            INNER JOIN public.subscription_usages su ON su.booking_id = b.id
            WHERE b.date_due >= $1 AND b.date_due < $2
              AND b.status = 'done'
              ${zoneFilter.clause}
          `, paramsWithZones).catch(() => ({ rows: [{ count: 0 }] })),
          
          // 16. New Membership Signups - NOT zone-filtered (subscriptions don't have zones)
          pool.query(`
            SELECT COUNT(*) as count
            FROM public.subscriptions
            WHERE created_at >= $1 AND created_at < $2
          `, baseParams).catch(() => ({ rows: [{ count: 0 }] })),
          
          // Revenue from member bookings (for % calculation) - UNIQUE bookings only
          pool.query(`
            SELECT COALESCE(SUM(price), 0) as total
            FROM (
              SELECT DISTINCT b.id, b.price
              FROM public.bookings b
              INNER JOIN public.subscription_usages su ON su.booking_id = b.id
              WHERE b.date_due >= $1 AND b.date_due < $2 AND b.status = 'done'
                ${zoneFilter.clause}
            ) unique_bookings
          `, paramsWithZones).catch(() => ({ rows: [{ total: 0 }] })),
          
          // Customer fees charged in the week (exclude waived fees and those without charge_id) - NOT zone-filtered
          pool.query(`
            SELECT COALESCE(SUM(amount), 0) as total
            FROM public.customer_fees
            WHERE created_at >= $1 AND created_at < $2
              AND (waived IS NULL OR waived != true)
              AND charge_id IS NOT NULL AND charge_id != ''
          `, baseParams).catch(() => ({ rows: [{ total: 0 }] })),
          
          // Tips from booking_tips where tip was created in the week
          // Zone filter via the linked booking
          pool.query(`
            SELECT 
              COALESCE(SUM(bt.tip_amount), 0) as tip_revenue,
              COALESCE(SUM(bt.tip_amount - bt.vendor_amount), 0) as tip_profit
            FROM public.booking_tips bt
            ${selectedZones.length > 0 ? 'INNER JOIN public.bookings b ON b.id = bt.booking_id' : ''}
            WHERE bt.created_at >= $1 AND bt.created_at < $2
            ${zoneFilter.clause}
          `, paramsWithZones).catch(() => ({ rows: [{ tip_revenue: 0, tip_profit: 0 }] })),
          
          // Credit packs purchased in the week - NOT zone-filtered (credit packs don't have zones)
          pool.query(`
            SELECT COALESCE(SUM(pay_amount), 0) as total
            FROM (
              SELECT DISTINCT ON (uct.id) uct.id, cp.pay_amount
              FROM public.user_credits_transactions uct
              INNER JOIN public.credits_packs cp ON uct.amount = cp.get_amount
              WHERE uct.created_at >= $1 AND uct.created_at < $2
                AND uct.user_credits_transaction_type_id = 16
            ) unique_transactions
          `, baseParams).catch(() => ({ rows: [{ total: 0 }] })),
          
          // Refunds from booking_refunds (created_at in week) - subtract from total revenue
          // Zone filter via the linked booking
          pool.query(`
            SELECT COALESCE(SUM(br.total), 0) as total
            FROM public.booking_refunds br
            ${selectedZones.length > 0 ? 'INNER JOIN public.bookings b ON b.id = br.booking_id' : ''}
            WHERE br.created_at >= $1 AND br.created_at < $2
            ${zoneFilter.clause}
          `, paramsWithZones).catch(() => ({ rows: [{ total: 0 }] })),
        ]);
        
        const bookingsCreated = parseInt(bookingsCreatedResult.rows[0]?.count || "0");
        const bookingsDue = parseInt(bookingsDueResult.rows[0]?.count || "0");
        const bookingsCompleted = parseInt(bookingsCompletedResult.rows[0]?.count || "0");
        const avgPerDay = bookingsCompleted / 7;
        const conversion = bookingsDue > 0 ? (bookingsCompleted / bookingsDue) * 100 : 0;
        const avgBookingPrice = parseFloat(revenueResult.rows[0]?.avg_price || "0");
        
        // Revenue components
        const bookingRevenue = parseFloat(revenueResult.rows[0]?.total_revenue || "0");
        const bookingProfit = parseFloat(revenueResult.rows[0]?.total_profit || "0");
        const stripeFees = parseFloat(revenueResult.rows[0]?.total_stripe_fees || "0");
        const subscriptionBookingRevenue = parseFloat(subscriptionRevenueResult.rows[0]?.total_revenue || "0");
        const subscriptionBookingProfit = parseFloat(subscriptionRevenueResult.rows[0]?.total_margin || "0");
        const subscriptionFees = parseFloat(subscriptionFeesResult.rows[0]?.total || "0");
        const customerFees = parseFloat(customerFeesResult.rows[0]?.total || "0");
        const tipRevenue = parseFloat(tipsResult.rows[0]?.tip_revenue || "0");
        const tipProfit = parseFloat(tipsResult.rows[0]?.tip_profit || "0");
        const creditPackRevenue = parseFloat(creditPacksResult.rows[0]?.total || "0");
        const refundsTotal = parseFloat(refundsResult.rows[0]?.total || "0");
        
        // Subscription Revenue = subscription booking revenue + subscription fees
        // Note: subscriptionBookingRevenue is already included in bookingRevenue (member bookings are a subset of all bookings)
        const subscriptionRevenue = subscriptionBookingRevenue + subscriptionFees;
        
        // Total Revenue = booking revenue + subscription fees + customer fees + tips + credit packs - refunds - stripe fees
        // (subscriptionBookingRevenue is already part of bookingRevenue, so we only add subscriptionFees)
        const totalRevenue = bookingRevenue + subscriptionFees + customerFees + tipRevenue + creditPackRevenue - refundsTotal - stripeFees;
        
        // Debug logging for revenue validation
        console.log(`[REVENUE DEBUG] ${week.label}: Booking=$${bookingRevenue.toFixed(2)}, SubFees=$${subscriptionFees.toFixed(2)}, CustFees=$${customerFees.toFixed(2)}, Tips=$${tipRevenue.toFixed(2)}, CreditPacks=$${creditPackRevenue.toFixed(2)}, Refunds=$${refundsTotal.toFixed(2)}, StripeFees=$${stripeFees.toFixed(2)}, TOTAL=$${totalRevenue.toFixed(2)}`);
        
        // Gross Profit = booking margin + subscription fees (100% margin) + customer fees (100% margin) + tip profit - refunds
        // (subscriptionBookingProfit is already part of bookingProfit, so we don't add it again)
        // Credit packs are 100% revenue, 0% profit (or needs separate margin - currently not adding to profit)
        // Refunds reduce profit by the full refund amount
        const totalProfit = bookingProfit + subscriptionFees + customerFees + tipProfit - refundsTotal;
        const marginPercent = totalRevenue > 0 ? (totalProfit / totalRevenue) * 100 : 0;
        
        const signups = parseInt(signupsResult.rows[0]?.count || "0");
        const newUsersWithBookings = parseInt(newUsersWithBookingsResult.rows[0]?.count || "0");
        const newUserConversion = signups > 0 ? (newUsersWithBookings / signups) * 100 : 0;
        const memberBookings = parseInt(memberBookingsResult.rows[0]?.count || "0");
        const newSubscriptions = parseInt(newSubscriptionsResult.rows[0]?.count || "0");
        const memberBookingsRevenue = parseFloat(memberBookingsRevenueResult.rows[0]?.total || "0");
        
        // % of revenue from memberships = Subscription Revenue / Total Revenue
        const membershipRevenuePercent = totalRevenue > 0 
          ? (subscriptionRevenue / totalRevenue) * 100 
          : 0;
        
        weeklyData.push({
          weekLabel: week.label,
          weekStart: weekStartUTC,
          weekEnd: weekEndUTC,
          metrics: {
            bookingsCreated,
            bookingsDue,
            bookingsCompleted,
            avgPerDay: Math.round(avgPerDay * 100) / 100,
            conversion: Math.round(conversion * 100) / 100,
            avgBookingPrice: Math.round(avgBookingPrice * 100) / 100,
            totalRevenue: Math.round(totalRevenue * 100) / 100,
            totalProfit: Math.round(totalProfit * 100) / 100,
            marginPercent: Math.round(marginPercent * 100) / 100,
            signups,
            newUsersWithBookings,
            newUserConversion: Math.round(newUserConversion * 100) / 100,
            subscriptionRevenue: Math.round(subscriptionRevenue * 100) / 100,
            subscriptionFees: Math.round(subscriptionFees * 100) / 100,
            memberBookings,
            membershipRevenuePercent: Math.round(membershipRevenuePercent * 100) / 100,
            newSubscriptions,
          },
        });
      }
      
      // Calculate variance (compare each week to previous week)
      const weeklyDataWithVariance = weeklyData.map((week, index) => {
        if (index === 0) {
          return { ...week, variance: null };
        }
        
        const prev = weeklyData[index - 1].metrics;
        const curr = week.metrics;
        
        const calcVariance = (current: number, previous: number) => {
          if (previous === 0) return current > 0 ? 100 : 0;
          return Math.round(((current - previous) / previous) * 100 * 100) / 100;
        };
        
        return {
          ...week,
          variance: {
            bookingsCreated: calcVariance(curr.bookingsCreated, prev.bookingsCreated),
            bookingsDue: calcVariance(curr.bookingsDue, prev.bookingsDue),
            bookingsCompleted: calcVariance(curr.bookingsCompleted, prev.bookingsCompleted),
            avgPerDay: calcVariance(curr.avgPerDay, prev.avgPerDay),
            conversion: Math.round((curr.conversion - prev.conversion) * 100) / 100, // pp change
            avgBookingPrice: calcVariance(curr.avgBookingPrice, prev.avgBookingPrice),
            totalRevenue: calcVariance(curr.totalRevenue, prev.totalRevenue),
            totalProfit: calcVariance(curr.totalProfit, prev.totalProfit),
            marginPercent: Math.round((curr.marginPercent - prev.marginPercent) * 100) / 100, // pp change
            signups: calcVariance(curr.signups, prev.signups),
            newUsersWithBookings: calcVariance(curr.newUsersWithBookings, prev.newUsersWithBookings),
            newUserConversion: Math.round((curr.newUserConversion - prev.newUserConversion) * 100) / 100, // pp change
            subscriptionRevenue: calcVariance(curr.subscriptionRevenue, prev.subscriptionRevenue),
            subscriptionFees: calcVariance(curr.subscriptionFees, prev.subscriptionFees),
            memberBookings: calcVariance(curr.memberBookings, prev.memberBookings),
            membershipRevenuePercent: Math.round((curr.membershipRevenuePercent - prev.membershipRevenuePercent) * 100) / 100,
            newSubscriptions: calcVariance(curr.newSubscriptions, prev.newSubscriptions),
          },
        };
      });
      
      // Reverse to show most recent first
      weeklyDataWithVariance.reverse();
      
      await logAudit({
        userId,
        userEmail: user.email,
        action: "VIEW_WEEKLY_PERFORMANCE",
        database,
        table: undefined,
        ip: req.ip || undefined,
        details: `Viewed ${weeklyDataWithVariance.length} weeks`,
      });
      
      const responseData = {
        weeks: weeklyDataWithVariance,
        generatedAt: new Date().toISOString(),
        selectedZones: selectedZones.length > 0 ? selectedZones : null,
      };
      
      // Cache the response - use 1-hour cache since it includes current week
      const cacheDuration = getCacheDuration(true); // Current period = 1 hour
      setInCache(cacheKey, responseData, cacheDuration);
      console.log(`[Cache SET] Marketing dashboard: ${cacheKey} (expires in ${cacheDuration / 60000} minutes)`);
      
      res.json({ ...responseData, fromCache: false });
    } catch (err) {
      console.error("Error fetching weekly performance:", err);
      res.status(500).json({ error: "Failed to fetch weekly performance data" });
    }
  });

  // Weekly Performance Dashboard AI Chat
  app.post("/api/weekly-performance/:database/chat", isAuthenticated, reportAILimiter, async (req, res) => {
    try {
      const { database } = req.params;
      const { message, dashboardData, selectedWeek } = req.body;
      const userId = (req.user as any)?.id;
      const user = await authStorage.getUser(userId);
      
      if (!user) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      
      if (!message) {
        return res.status(400).json({ error: "Message is required" });
      }
      
      const client = getOpenAIClient();
      if (!client) {
        return res.status(503).json({ error: "AI service not available" });
      }
      
      // Check if user can drill down (Admin or WashOS User only)
      const canDrillDown = user.role === "admin" || user.role === "washos_user";
      
      // Import metric specs
      const { METRIC_SPECS, getAllMetricSpecs } = await import("./weeklyMetrics");
      
      // Build context about the dashboard data with explicit week dates
      const currentYear = new Date().getFullYear();
      const availableWeeksContext = dashboardData?.weeks?.length > 0
        ? `Available weeks with their EXACT date ranges (use these dates for drill-down):\n${dashboardData.weeks.map((w: any) => 
            `- "${w.weekLabel}": weekStart="${w.weekStart}", weekEnd="${w.weekEnd}"`
          ).join('\n')}`
        : '';
      
      const metricsContext = dashboardData?.weeks?.length > 0 
        ? `The dashboard currently shows ${dashboardData.weeks.length} weeks of data.
          
The most recent week (${dashboardData.weeks[0]?.weekLabel || 'Current'}) has these metrics:
${JSON.stringify(dashboardData.weeks[0]?.metrics || {}, null, 2)}

${dashboardData.weeks[0]?.variance ? `Week-over-week variance (% change, or percentage point change for rates):
${JSON.stringify(dashboardData.weeks[0].variance, null, 2)}` : ''}

${dashboardData.weeks.length > 1 ? `Previous week (${dashboardData.weeks[1]?.weekLabel}) metrics:
${JSON.stringify(dashboardData.weeks[1]?.metrics || {}, null, 2)}` : ''}
`
        : "No dashboard data is currently loaded.";
      
      // Build metric specs context for AI
      const metricSpecsList = getAllMetricSpecs().map(m => 
        `- ${m.name} (id: ${m.id}): ${m.description}\n  Formula: ${m.formula}${m.subSources ? `\n  Sub-sources: ${m.subSources.map(s => s.name).join(", ")}` : ""}`
      ).join("\n\n");
      
      const systemPrompt = `You are an AI assistant for the WashOS Weekly Marketing Performance Dashboard. Your role is to help users understand and analyze their weekly business metrics.

IMPORTANT: The current year is ${currentYear}. When users mention dates like "Jan 12-18", they mean ${currentYear}, NOT any other year.

DASHBOARD CONTEXT:
The Weekly Marketing Performance Dashboard tracks these key metrics week over week (Monday-Sunday, Pacific Time):

METRIC DEFINITIONS AND FORMULAS:
${metricSpecsList}

CURRENT DATA:
${metricsContext}

${selectedWeek ? `SELECTED WEEK: ${selectedWeek.weekLabel} (${selectedWeek.weekStart} to ${selectedWeek.weekEnd})` : ''}

${availableWeeksContext}

CRITICAL: When calling get_metric_rows, you MUST use the EXACT weekStart and weekEnd values from the available weeks list above. Look up the week label the user mentions (e.g., "Jan 12 - 18") and use its corresponding weekStart and weekEnd values. Do NOT make up date values.

${canDrillDown ? `DRILL-DOWN CAPABILITY:
You have access to tools to fetch the actual database rows that make up each metric.
When users ask "what went into this number", "show me the details", "export the data", or want to see the underlying rows, use the get_metric_rows tool.
When users ask "how is this calculated", use the get_metric_details tool for the exact formula.
For revenue metrics with multiple sources (totalRevenue, totalProfit), you can drill down into specific sources like bookingRevenue, subscriptionFees, tips, refunds, etc.

IMPORTANT FOR BREAKDOWNS: When a user asks to "break down" a metric with sub-sources, you should make SEPARATE calls to get_metric_rows for EACH sub-source. For example:
- subscriptionFees has sub-sources: "invoiceFees" (recurring invoice payments) and "cancellationFees" ($59 cancellation fees)
- To break down subscription fees, call get_metric_rows TWICE: once with subSourceId="invoiceFees" and once with subSourceId="cancellationFees"
- Then present both results to show the complete breakdown.` : 'Note: Drill-down to underlying data rows is not available for this user role.'}

INSTRUCTIONS:
1. Answer questions about the metrics, trends, and performance
2. Help users understand what the numbers mean and provide insights
3. Compare weeks when relevant data is available
4. Explain variances and what might be driving changes
5. Be concise but informative
6. Format numbers appropriately (currency with $, percentages with %, etc.)
7. When discussing variance, positive changes are generally good for revenue/bookings/users metrics
${canDrillDown ? '8. When users want to see underlying data, use the tools to fetch and display it' : ''}`;

      // Define tools for function calling (only if user can drill down)
      const tools = canDrillDown ? [
        {
          type: "function" as const,
          function: {
            name: "get_metric_details",
            description: "Get the exact calculation formula and description for a specific metric",
            parameters: {
              type: "object",
              properties: {
                metricId: {
                  type: "string",
                  description: "The metric ID (e.g., totalRevenue, bookingsCompleted, signups)",
                  enum: Object.keys(METRIC_SPECS),
                },
              },
              required: ["metricId"],
            },
          },
        },
        {
          type: "function" as const,
          function: {
            name: "get_metric_rows",
            description: "Fetch the actual database rows that contribute to a metric for a specific week. Returns up to 50 rows as a preview with a CSV download option for full data.",
            parameters: {
              type: "object",
              properties: {
                metricId: {
                  type: "string",
                  description: "The metric ID (e.g., totalRevenue, bookingsCompleted)",
                  enum: Object.keys(METRIC_SPECS),
                },
                subSourceId: {
                  type: "string",
                  description: "For metrics with multiple sources (like totalRevenue), optionally specify a sub-source (e.g., bookingRevenue, subscriptionFees, tips, refunds, creditPacks, stripeFees)",
                },
                weekStart: {
                  type: "string",
                  description: "ISO date string for the start of the week (UTC)",
                },
                weekEnd: {
                  type: "string",
                  description: "ISO date string for the end of the week (UTC)",
                },
              },
              required: ["metricId", "weekStart", "weekEnd"],
            },
          },
        },
      ] : undefined;

      // Initial AI call
      const messages: any[] = [
        { role: "system", content: systemPrompt },
        { role: "user", content: message }
      ];
      
      let response = await client.chat.completions.create({
        model: AI_CONFIG.reportChat.model,
        messages,
        tools,
        temperature: 0.7,
        max_tokens: 2000,
      });
      
      let assistantMessage = response.choices[0]?.message;
      
      // Handle tool calls if any
      const toolResults: any[] = [];
      
      while (assistantMessage?.tool_calls && assistantMessage.tool_calls.length > 0) {
        messages.push(assistantMessage);
        
        for (const toolCall of assistantMessage.tool_calls) {
          const tc = toolCall as any;
          const args = JSON.parse(tc.function.arguments);
          let result: any;
          
          if (tc.function.name === "get_metric_details") {
            const spec = METRIC_SPECS[args.metricId];
            if (spec) {
              result = {
                name: spec.name,
                category: spec.category,
                formula: spec.formula,
                description: spec.description,
                sourceTables: spec.sourceTables || [spec.sourceTable],
                subSources: spec.subSources?.map(s => ({ id: s.id, name: s.name })),
              };
            } else {
              result = { error: "Metric not found" };
            }
          } else if (tc.function.name === "get_metric_rows") {
            const pool = getPool(database);
            const spec = METRIC_SPECS[args.metricId];
            
            if (!spec) {
              result = { error: "Metric not found" };
            } else {
              let queryConfig;
              
              // Check if requesting a sub-source
              if (args.subSourceId && spec.subSources) {
                const subSource = spec.subSources.find(s => s.id === args.subSourceId);
                if (subSource) {
                  queryConfig = subSource.getDrilldownQuery(args.weekStart, args.weekEnd);
                }
              }
              
              // Fall back to main query
              if (!queryConfig) {
                queryConfig = spec.getDrilldownQuery(args.weekStart, args.weekEnd);
              }
              
              try {
                const queryResult = await pool.query(
                  queryConfig.sql + " LIMIT 50",
                  queryConfig.params
                );
                
                // Get total count
                const countSql = `SELECT COUNT(*) as total FROM (${queryConfig.sql}) as subq`;
                const countResult = await pool.query(countSql, queryConfig.params);
                const totalCount = parseInt(countResult.rows[0]?.total || "0");
                
                result = {
                  metricName: spec.name,
                  subSource: args.subSourceId,
                  columns: queryConfig.columns,
                  rows: queryResult.rows,
                  totalCount,
                  previewCount: queryResult.rows.length,
                  hasMore: totalCount > 50,
                  csvExportAvailable: totalCount > 0,
                };
                
                // Store for CSV export
                toolResults.push({
                  metricId: args.metricId,
                  subSourceId: args.subSourceId,
                  weekStart: args.weekStart,
                  weekEnd: args.weekEnd,
                  ...result,
                });
                
                await logAudit({
                  userId,
                  userEmail: user.email,
                  action: "WEEKLY_PERFORMANCE_DRILLDOWN",
                  database,
                  details: `Metric: ${spec.name}${args.subSourceId ? ` (${args.subSourceId})` : ""}, ${totalCount} rows`,
                  ip: req.ip || undefined,
                });
              } catch (queryErr) {
                console.error("Drilldown query error:", queryErr);
                result = { error: "Failed to fetch data" };
              }
            }
          }
          
          messages.push({
            role: "tool",
            tool_call_id: toolCall.id,
            content: JSON.stringify(result),
          });
        }
        
        // Continue conversation with tool results
        response = await client.chat.completions.create({
          model: AI_CONFIG.reportChat.model,
          messages,
          tools,
          temperature: 0.7,
          max_tokens: 2000,
        });
        
        assistantMessage = response.choices[0]?.message;
      }
      
      const finalMessage = assistantMessage?.content || "I apologize, but I couldn't generate a response. Please try again.";
      
      await logAudit({
        userId,
        userEmail: user.email,
        action: "WEEKLY_PERFORMANCE_AI_CHAT",
        database,
        details: `AI chat message: ${message.substring(0, 100)}...`,
        ip: req.ip || undefined,
      });
      
      res.json({ 
        message: finalMessage,
        drilldownData: toolResults.length > 0 ? toolResults : undefined,
      });
    } catch (err) {
      console.error("Error in weekly performance AI chat:", err);
      res.status(500).json({ error: "Failed to process AI request" });
    }
  });
  
  // CSV Export for drilldown data
  app.get("/api/weekly-performance/:database/drilldown-export", isAuthenticated, exportLimiter, async (req, res) => {
    try {
      const { database } = req.params;
      const { metricId, subSourceId, weekStart, weekEnd } = req.query;
      const userId = (req.user as any)?.id;
      const user = await authStorage.getUser(userId);
      
      if (!user) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      
      // Check role - only Admin and WashOS users can export
      if (user.role !== "admin" && user.role !== "washos_user") {
        return res.status(403).json({ error: "Export not available for your role" });
      }
      
      if (!metricId || !weekStart || !weekEnd) {
        return res.status(400).json({ error: "Missing required parameters" });
      }
      
      const { METRIC_SPECS } = await import("./weeklyMetrics");
      const spec = METRIC_SPECS[metricId as string];
      
      if (!spec) {
        return res.status(400).json({ error: "Invalid metric" });
      }
      
      let queryConfig;
      if (subSourceId && spec.subSources) {
        const subSource = spec.subSources.find(s => s.id === subSourceId);
        if (subSource) {
          queryConfig = subSource.getDrilldownQuery(weekStart as string, weekEnd as string);
        }
      }
      if (!queryConfig) {
        queryConfig = spec.getDrilldownQuery(weekStart as string, weekEnd as string);
      }
      
      const pool = getPool(database);
      
      // Limit to 10,000 rows for safety
      const result = await pool.query(
        queryConfig.sql + " LIMIT 10000",
        queryConfig.params
      );
      
      // Build CSV
      const headers = queryConfig.columns.join(",");
      const rows = result.rows.map((row: Record<string, unknown>) => 
        queryConfig.columns.map((col: string) => {
          const val = row[col];
          if (val === null || val === undefined) return "";
          const strVal = String(val);
          if (strVal.includes(",") || strVal.includes('"') || strVal.includes("\n")) {
            return `"${strVal.replace(/"/g, '""')}"`;
          }
          return strVal;
        }).join(",")
      );
      
      const csv = [headers, ...rows].join("\n");
      
      await logAudit({
        userId,
        userEmail: user.email,
        action: "WEEKLY_PERFORMANCE_DRILLDOWN_EXPORT",
        database,
        details: `Metric: ${spec.name}${subSourceId ? ` (${subSourceId})` : ""}, ${result.rows.length} rows exported`,
        ip: req.ip || undefined,
      });
      
      res.setHeader("Content-Type", "text/csv");
      res.setHeader("Content-Disposition", `attachment; filename="${spec.id}_${subSourceId || 'all'}_drilldown.csv"`);
      res.send(csv);
    } catch (err) {
      console.error("Error exporting drilldown data:", err);
      res.status(500).json({ error: "Failed to export data" });
    }
  });

  // Get available report templates
  app.get("/api/reports/templates", isAuthenticated, async (req, res) => {
    const templates = [
      {
        id: "booking-summary",
        name: "Booking Summary",
        description: "Overview of bookings with status breakdown",
        blocks: [
          { kind: "metric", title: "Total Bookings", config: { table: "bookings", column: "id", aggregateFunction: "count" } },
          { kind: "chart", title: "Bookings by Status", config: { table: "bookings", chartType: "pie", xColumn: "status", yColumn: "id", aggregateFunction: "count", groupBy: "status" } },
        ],
      },
      {
        id: "customer-metrics",
        name: "Customer Metrics",
        description: "Key customer statistics and trends",
        blocks: [
          { kind: "metric", title: "Total Customers", config: { table: "users", column: "id", aggregateFunction: "count" } },
          { kind: "table", title: "Recent Customers", config: { table: "users", columns: ["email", "first_name", "created_at"], orderBy: { column: "created_at", direction: "desc" }, rowLimit: 10 } },
        ],
      },
    ];

    res.json(templates);
  });

  // Stripe Metrics API for Weekly Performance Dashboard
  // Returns Stripe financial metrics (Gross Volume, Net Volume, Refunds, Disputes) for a given week
  app.get("/api/stripe-metrics", isAuthenticated, async (req, res) => {
    try {
      const { weekStart, weekEnd } = req.query;
      
      if (!weekStart || !weekEnd) {
        return res.status(400).json({ error: "weekStart and weekEnd query parameters required" });
      }
      
      // Check if Stripe is connected
      const stripeConnected = await checkStripeConnection();
      if (!stripeConnected) {
        return res.status(503).json({ 
          error: "Stripe not connected",
          message: "Please connect your Stripe account to view financial metrics"
        });
      }
      
      // Parse the week boundaries (these come as ISO strings from the frontend)
      // Convert to Unix timestamps for Stripe API
      const startDate = new Date(weekStart as string);
      const endDate = new Date(weekEnd as string);
      
      const startTimestamp = Math.floor(startDate.getTime() / 1000);
      const endTimestamp = Math.floor(endDate.getTime() / 1000);
      
      const metrics = await getStripeMetricsForWeek(startTimestamp, endTimestamp);
      
      res.json({
        weekStart: weekStart,
        weekEnd: weekEnd,
        metrics,
      });
    } catch (error: any) {
      console.error("Stripe metrics error:", error);
      res.status(500).json({ error: "Failed to fetch Stripe metrics", message: error.message });
    }
  });
  
  // Check Stripe connection status
  app.get("/api/stripe-status", isAuthenticated, async (req, res) => {
    try {
      const connected = await checkStripeConnection();
      res.json({ connected });
    } catch (error) {
      res.json({ connected: false });
    }
  });

  // Operations Performance Dashboard endpoint
  app.get("/api/operations-performance/:database", isAuthenticated, async (req, res) => {
    try {
      const { database } = req.params;
      const periodType = (req.query.periodType as string) || "weekly";
      const forceRefresh = req.query.refresh === "true";
      const userId = (req.user as any)?.id;
      const user = await authStorage.getUser(userId);
      
      if (!user) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      
      // Check cache first (unless force refresh)
      const cacheKey = getCacheKey("operations", database, periodType);
      if (!forceRefresh) {
        const cachedData = getFromCache<any>(cacheKey);
        if (cachedData) {
          console.log(`[Cache HIT] Operations dashboard: ${cacheKey}`);
          return res.json({ ...cachedData, fromCache: true });
        }
      }
      console.log(`[Cache MISS] Operations dashboard: ${cacheKey}${forceRefresh ? ' (force refresh)' : ''}`);
      
      const pool = getPool(database);
      
      // Helper to get PST offset for a given date (handles DST)
      const getPSTOffset = (date: Date): string => {
        const month = date.getMonth();
        if (month > 2 && month < 10) {
          return "-07:00"; // PDT
        } else if (month < 2 || month > 10) {
          return "-08:00"; // PST
        } else if (month === 2) {
          const day = date.getDate();
          const dayOfWeek = date.getDay();
          const secondSunday = 14 - (new Date(date.getFullYear(), 2, 1).getDay() || 7);
          if (day >= secondSunday) return "-07:00";
          return "-08:00";
        } else {
          const day = date.getDate();
          const firstSunday = 7 - (new Date(date.getFullYear(), 10, 1).getDay() || 7);
          if (day >= firstSunday) return "-08:00";
          return "-07:00";
        }
      };
      
      // Generate periods based on periodType
      const periods: { startUTC: string; endUTC: string; label: string }[] = [];
      const now = new Date();
      
      if (periodType === "monthly") {
        // Generate last 12 months
        for (let i = 0; i < 12; i++) {
          const monthStart = new Date(now.getFullYear(), now.getMonth() - i, 1);
          const monthEnd = new Date(now.getFullYear(), now.getMonth() - i + 1, 1);
          
          const startOffset = getPSTOffset(monthStart);
          const endOffset = getPSTOffset(monthEnd);
          
          const startUTC = new Date(`${monthStart.getFullYear()}-${String(monthStart.getMonth() + 1).padStart(2, '0')}-01T00:00:00${startOffset}`).toISOString();
          const endUTC = new Date(`${monthEnd.getFullYear()}-${String(monthEnd.getMonth() + 1).padStart(2, '0')}-01T00:00:00${endOffset}`).toISOString();
          
          const monthLabel = monthStart.toLocaleDateString('en-US', { month: 'short', year: 'numeric' });
          
          periods.push({ startUTC, endUTC, label: monthLabel });
        }
      } else {
        // Weekly: Generate weeks from Dec 29, 2025 to current week (Mon-Sun, PST)
        const startDate = new Date("2025-12-29T00:00:00-08:00");
        
        let currentMonday = new Date(now);
        currentMonday.setDate(currentMonday.getDate() - ((currentMonday.getDay() + 6) % 7));
        currentMonday.setHours(0, 0, 0, 0);
        
        let weekStart = startDate;
        while (weekStart <= currentMonday) {
          const weekEnd = new Date(weekStart);
          weekEnd.setDate(weekEnd.getDate() + 7);
          
          const startOffset = getPSTOffset(weekStart);
          const endOffset = getPSTOffset(weekEnd);
          
          const year = weekStart.getFullYear();
          const month = String(weekStart.getMonth() + 1).padStart(2, '0');
          const day = String(weekStart.getDate()).padStart(2, '0');
          const startUTC = new Date(`${year}-${month}-${day}T00:00:00${startOffset}`).toISOString();
          
          const endYear = weekEnd.getFullYear();
          const endMonth = String(weekEnd.getMonth() + 1).padStart(2, '0');
          const endDay = String(weekEnd.getDate()).padStart(2, '0');
          const endUTC = new Date(`${endYear}-${endMonth}-${endDay}T00:00:00${endOffset}`).toISOString();
          
          const weekLabel = `${weekStart.toLocaleDateString('en-US', { month: 'short', day: 'numeric' })} - ${new Date(weekEnd.getTime() - 86400000).toLocaleDateString('en-US', { month: 'short', day: 'numeric', year: 'numeric' })}`;
          
          periods.push({ startUTC, endUTC, label: weekLabel });
          
          weekStart = weekEnd;
        }
      }
      
      // For weekly: reverse to show most recent first (weeks are built oldest to newest)
      // For monthly: already built most recent first, no reverse needed
      if (periodType === "weekly") {
        periods.reverse();
      }
      
      // Limit to 52 weeks or 12 months
      const limitedPeriods = periods.slice(0, periodType === "monthly" ? 12 : 52);
      
      // Calculate metrics for each period (no Stripe calls - too slow)
      const results: OperationsPeriodMetrics[] = [];
      
      for (let i = 0; i < limitedPeriods.length; i++) {
        const period = limitedPeriods[i];
        
        // Calculate metrics without Stripe data (uses database margin values instead)
        const metrics = await calculateOperationsMetrics(pool, period.startUTC, period.endUTC, null);
        
        // Get previous period metrics for variance
        let variance: Record<string, number | null> = {};
        if (i < limitedPeriods.length - 1) {
          const prevPeriod = limitedPeriods[i + 1];
          const prevMetrics = await calculateOperationsMetrics(pool, prevPeriod.startUTC, prevPeriod.endUTC, null);
          variance = calculateOperationsVariance(metrics, prevMetrics);
        }
        
        results.push({
          periodLabel: period.label,
          periodStart: period.startUTC,
          periodEnd: period.endUTC,
          periodType: periodType as "weekly" | "monthly",
          metrics,
          variance,
        });
      }
      
      const responseData = {
        periods: results,
        stripeConnected: false,
        periodType,
      };
      
      // Cache the response - use 1-hour cache since it includes current period
      const cacheDuration = getCacheDuration(true); // Current period = 1 hour
      setInCache(cacheKey, responseData, cacheDuration);
      console.log(`[Cache SET] Operations dashboard: ${cacheKey} (expires in ${cacheDuration / 60000} minutes)`);
      
      res.json({ ...responseData, fromCache: false });
    } catch (error: any) {
      console.error("Operations performance error:", error);
      res.status(500).json({ error: "Failed to fetch operations metrics", message: error.message });
    }
  });

  return httpServer;
}
