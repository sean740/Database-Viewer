import type { Express, Request, Response } from "express";
import { createServer, type Server } from "http";
import { Pool } from "pg";
import OpenAI from "openai";
import rateLimit from "express-rate-limit";
import { eq, and, desc, count } from "drizzle-orm";
import { storage } from "./storage";
import { db } from "./db";
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
    // For "between", push both values
    params.push(f.value[0], f.value[1]);
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
    params.push(f.value[0], f.value[1]);
  } else {
    params.push(opInfo.transform ? opInfo.transform(f.value) : f.value);
  }
}

// OpenAI client for NLQ
let openai: OpenAI | null = null;

function getOpenAIClient(): OpenAI | null {
  if (openai) return openai;
  
  const apiKey = process.env.AI_INTEGRATIONS_OPENAI_API_KEY;
  const baseURL = process.env.AI_INTEGRATIONS_OPENAI_BASE_URL;
  
  if (!apiKey || !baseURL) {
    return null;
  }

  openai = new OpenAI({
    apiKey,
    baseURL,
  });

  return openai;
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
async function validateTableAccess(
  dbName: string, 
  tableName: string, 
  user: User
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

    // Check table visibility settings
    const allSettings = await storage.getAllTableSettings();
    const settingsKey = `${dbName}:${parsed.schema}.${parsed.table}`;
    const tableSettings = allSettings[settingsKey];
    
    // For non-admin users, check visibility
    if (user.role !== "admin") {
      // Check if table is hidden
      if (tableSettings && tableSettings.isVisible === false) {
        return { valid: false, error: "Access denied to this table" };
      }
      
      // For external customers, also check grants
      if (user.role === "external_customer") {
        const allowedTables = await getAllowedTables(user.id);
        if (!allowedTables.includes(`${dbName}:${parsed.schema}.${parsed.table}`)) {
          return { valid: false, error: "Access denied to this table" };
        }
      }
    }

    return { valid: true, parsedTable: parsed };
  } catch (err) {
    console.error("Error validating table access:", err);
    return { valid: false, error: "Failed to validate table access" };
  }
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
    
    const existingColumns = new Set(columnResult.rows.map((r: any) => r.column_name));
    
    for (const col of columns) {
      if (!existingColumns.has(col)) {
        return { valid: false, error: `Column not found: ${col}` };
      }
    }

    return { valid: true };
  } catch (err) {
    console.error("Error validating columns:", err);
    return { valid: false, error: "Failed to validate columns" };
  }
}

// Security: Validate report block config against database metadata
async function validateBlockConfig(
  config: any, 
  kind: string,
  user: User
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

  // Validate table access
  const tableValidation = await validateTableAccess(config.database, config.table, user);
  if (!tableValidation.valid) {
    return tableValidation;
  }

  // If there's a join, validate the joined table access too
  let joinTableColumns: string[] = [];
  if (config.join?.table) {
    const joinTableValidation = await validateTableAccess(config.database, config.join.table, user);
    if (!joinTableValidation.valid) {
      return { valid: false, error: `Join table access error: ${joinTableValidation.error}` };
    }
    
    // Validate join "on" columns
    if (!config.join.on || config.join.on.length !== 2) {
      return { valid: false, error: "Join 'on' must specify two columns [fromColumn, toColumn]" };
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
    }
  }

  // Collect all columns that need validation
  const columnsToValidate: string[] = [];
  const joinColumnsToValidate: string[] = [];

  if (kind === "table" && config.columns?.length > 0) {
    // Separate main table columns from join table columns
    for (const col of config.columns) {
      if (col.includes(".")) {
        // Column from joined table (e.g., "vendors.email")
        const [tableAlias, colName] = col.split(".");
        if (colName && joinTableColumns.length > 0) {
          joinColumnsToValidate.push(colName);
        }
      } else {
        columnsToValidate.push(col);
      }
    }
  }
  if (kind === "table" && config.orderBy?.column) {
    columnsToValidate.push(config.orderBy.column);
  }
  if (kind === "chart") {
    if (config.xColumn) columnsToValidate.push(config.xColumn);
    if (config.yColumn) columnsToValidate.push(config.yColumn);
    // Special date grouping values don't need column validation
    const dateGroupByValues = ["month", "year", "day", "week", "quarter"];
    if (config.groupBy && !dateGroupByValues.includes(config.groupBy.toLowerCase())) {
      columnsToValidate.push(config.groupBy);
    }
  }
  if (kind === "metric" && config.column) {
    columnsToValidate.push(config.column);
  }

  // Validate filter columns
  if (config.filters?.length > 0) {
    for (const f of config.filters) {
      if (f.column) columnsToValidate.push(f.column);
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
        return { valid: false, error: `Column not found in joined table: ${col}` };
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
      const { role, isActive } = req.body;
      
      const updates: Partial<{ role: UserRole; isActive: boolean; updatedAt: Date }> = { updatedAt: new Date() };
      
      if (role && ["admin", "washos_user", "external_customer"].includes(role)) {
        updates.role = role;
      }
      
      if (typeof isActive === "boolean") {
        updates.isActive = isActive;
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
      const { database, table, page = 1, filters = [] } = req.body;
      
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

      const orderByColumn =
        pkResult.rows.length > 0 ? `"${pkResult.rows[0].attname}"` : "ctid";

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
        params.push(op.transform ? op.transform(filter.value) : filter.value);
        paramIndex++;
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
        ORDER BY ${orderByColumn} ASC
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
        params.push(op.transform ? op.transform(filter.value) : filter.value);
        paramIndex++;
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
        params.push(op.transform ? op.transform(filter.value) : filter.value);
        paramIndex++;
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

  // Natural Language Query
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
      
      // Check table access for external customers
      const userId = (req.user as any)?.id;
      const user = await authStorage.getUser(userId);
      if (user?.role === "external_customer") {
        const allowedTables = await getAllowedTables(userId);
        if (!allowedTables.includes(`${database}:${currentTable}`)) {
          return res.status(403).json({ error: "You don't have access to this table" });
        }
      }

      // Get column information with data types for schema-aware prompts
      interface ColumnWithType {
        name: string;
        dataType: string;
      }
      let columnsWithTypes: ColumnWithType[] = [];

      if (database && currentTable) {
        const pool = getPool(database);
        const [schema, tableName] = currentTable.split(".");
        const columnsResult = await pool.query(
          `
          SELECT column_name, data_type
          FROM information_schema.columns
          WHERE table_schema = $1 AND table_name = $2
          ORDER BY ordinal_position
          `,
          [schema, tableName]
        );
        columnsWithTypes = columnsResult.rows.map((r) => ({
          name: r.column_name,
          dataType: r.data_type,
        }));
      }

      // Format columns for the prompt with data types
      const columnInfo = columnsWithTypes
        .map((c) => `  - ${c.name} (${c.dataType})`)
        .join("\n");

      // Find date/timestamp columns for potential ambiguity detection
      const dateColumns = columnsWithTypes
        .filter((c) => c.dataType.includes("date") || c.dataType.includes("timestamp"))
        .map((c) => c.name);

      // Get current date for relative date calculations
      const today = new Date();
      const todayStr = today.toISOString().split('T')[0]; // YYYY-MM-DD format
      
      const systemPrompt = `You are a helpful assistant that converts natural language queries into structured query plans for a database viewer.

IMPORTANT: Today's date is ${todayStr}. Use this for any relative date references like "yesterday", "last week", "this month", etc.

The user is querying the table: ${currentTable}

Available columns (with data types):
${columnInfo}

You must return ONLY a valid JSON object with this structure:
{
  "table": "${currentTable}",
  "page": 1,
  "filters": [
    {"column": "column_name", "op": "operator", "value": "filter_value"}
  ],
  "needsClarification": false,
  "clarificationQuestion": null,
  "ambiguousColumns": [],
  "summary": "A brief description of what this filter does"
}

Valid operators are:
- eq: equals (exact match)
- contains: substring match (case-insensitive)
- gt: greater than
- gte: greater than or equal
- lt: less than
- lte: less than or equal

IMPORTANT RULES:
1. Always use the table "${currentTable}" - do not change it
2. Only use columns that exist in the column list above
3. For date queries (years, months, dates):
   - If there are multiple date columns (${dateColumns.join(", ")}), ask which one to use
   - Set "needsClarification": true and provide a "clarificationQuestion"
   - List the ambiguous columns in "ambiguousColumns"
4. For year filtering, use gte/lte with date ranges (e.g., >= '2026-01-01' AND < '2027-01-01')
5. Always provide a "summary" field describing what the filter will do
6. If the query is unclear or you need more information, set "needsClarification": true
7. Only return valid JSON, no explanation or markdown

${context ? `Previous context from conversation:\n${context}\n` : ""}`;

      const response = await client.chat.completions.create({
        model: "gpt-4o",
        messages: [
          { role: "system", content: systemPrompt },
          { role: "user", content: query },
        ],
        max_completion_tokens: 800,
        temperature: 0.1,
      });

      const content = response.choices[0]?.message?.content || "{}";
      
      // Parse the response
      let plan: NLQPlan;
      try {
        // Clean up potential markdown code blocks
        const cleanContent = content.replace(/```json\n?|\n?```/g, "").trim();
        plan = JSON.parse(cleanContent);
      } catch {
        return res.status(400).json({ error: "Failed to parse AI response" });
      }

      // Force the table to be the currently selected table
      plan.table = currentTable;

      // Validate filters
      const validOperators = ["eq", "contains", "gt", "gte", "lt", "lte"];
      const validColumns = columnsWithTypes.map((c) => c.name);
      
      if (plan.filters && Array.isArray(plan.filters)) {
        for (const filter of plan.filters) {
          if (!validOperators.includes(filter.op)) {
            return res.status(400).json({ error: `Invalid operator: ${filter.op}` });
          }
          if (validColumns.length > 0 && !validColumns.includes(filter.column)) {
            return res.status(400).json({ error: `Invalid column: ${filter.column}. Available columns: ${validColumns.join(", ")}` });
          }
        }
      } else {
        plan.filters = [];
      }

      plan.page = plan.page || 1;

      res.json(plan);
    } catch (err) {
      console.error("Error processing NLQ:", err);
      res.status(500).json({
        error: err instanceof Error ? err.message : "Failed to process query",
      });
    }
  });

  // NLQ Smart Follow-up - called when a query returns no results
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

      // Check table access for external customers
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

      // Get column information with data types
      const columnsResult = await pool.query(
        `SELECT column_name, data_type
         FROM information_schema.columns
         WHERE table_schema = $1 AND table_name = $2`,
        [schema, tableName]
      );
      const columnTypes: Record<string, string> = {};
      columnsResult.rows.forEach((r) => {
        columnTypes[r.column_name] = r.data_type;
      });

      // Sample distinct values for each filtered column
      interface ColumnSample {
        column: string;
        dataType: string;
        filteredValue: string;
        operator: string;
        actualValues?: string[];
        dateRange?: { min: string; max: string };
      }
      const columnSamples: ColumnSample[] = [];

      const TIMEOUT_MS = 3000;
      const MAX_DISTINCT_VALUES = 15;

      for (const filter of filters) {
        const colName = filter.column;
        const dataType = columnTypes[colName];
        
        if (!dataType) continue;

        const sample: ColumnSample = {
          column: colName,
          dataType: dataType,
          filteredValue: filter.value,
          operator: filter.op,
        };

        try {
          // Helper function for timeout with proper cleanup
          const queryWithTimeout = async (queryStr: string): Promise<any> => {
            return new Promise((resolve, reject) => {
              let timeoutId: NodeJS.Timeout | null = null;
              let settled = false;
              
              timeoutId = setTimeout(() => {
                if (!settled) {
                  settled = true;
                  reject(new Error("Timeout"));
                }
              }, TIMEOUT_MS);
              
              pool.query(queryStr)
                .then((result) => {
                  if (!settled) {
                    settled = true;
                    if (timeoutId) clearTimeout(timeoutId);
                    resolve(result);
                  }
                })
                .catch((err) => {
                  if (!settled) {
                    settled = true;
                    if (timeoutId) clearTimeout(timeoutId);
                    reject(err);
                  }
                });
            });
          };

          // For string/text columns, get distinct values
          if (dataType.includes("character") || dataType.includes("text") || dataType === "USER-DEFINED") {
            const distinctQuery = `
              SELECT DISTINCT "${colName}" as val
              FROM "${schema}"."${tableName}"
              WHERE "${colName}" IS NOT NULL
              ORDER BY "${colName}"
              LIMIT ${MAX_DISTINCT_VALUES}
            `;
            
            const result = await queryWithTimeout(distinctQuery);
            sample.actualValues = result.rows.map((r: any) => String(r.val));
          }
          // For date/timestamp columns, get min/max range
          else if (dataType.includes("date") || dataType.includes("timestamp")) {
            const rangeQuery = `
              SELECT 
                MIN("${colName}")::text as min_val,
                MAX("${colName}")::text as max_val
              FROM "${schema}"."${tableName}"
              WHERE "${colName}" IS NOT NULL
            `;
            
            const result = await queryWithTimeout(rangeQuery);
            
            if (result.rows[0]) {
              sample.dateRange = {
                min: result.rows[0].min_val,
                max: result.rows[0].max_val,
              };
            }
          }
        } catch (err) {
          // Skip this column if query times out or fails
          console.log(`Skipping column ${colName} sampling:`, err);
        }

        columnSamples.push(sample);
      }

      // Build AI prompt with sampled values
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
          clarificationQuestion: "No results found for your query. Try adjusting your filter values or broadening your search.",
          needsClarification: true,
        });
      }

      // Get current date for relative date references
      const today = new Date();
      const todayStr = today.toISOString().split('T')[0];
      
      const systemPrompt = `You are a helpful assistant that helps users find data in a database when their query returns no results.

IMPORTANT: Today's date is ${todayStr}. Use this for any relative date references.

The user's query returned 0 results. Here's what we found about their filters:
${samplingInfo}

Your task:
1. Identify which filter value(s) don't match the actual data
2. For text columns: Look for synonyms or similar values (e.g., "completed" → "done", "cancelled" → "canceled")
3. For date columns: Check if the date range is outside the available data
4. Suggest specific alternatives that would return results

Return ONLY a valid JSON object:
{
  "clarificationQuestion": "A helpful question suggesting alternatives. Be specific about which values to use.",
  "needsClarification": true,
  "suggestedFilters": [
    {"column": "column_name", "op": "operator", "value": "suggested_value"}
  ],
  "summary": "Brief explanation of why the query returned no results"
}

Be conversational and helpful. If you find a likely synonym or alternative, suggest it directly.
Example: "No bookings found with status 'completed'. I found that this table uses 'done' for completed bookings. Would you like to search for bookings with status 'done' instead?"

${context ? `Previous conversation context:\n${context}` : ""}`;

      const response = await client.chat.completions.create({
        model: "gpt-4o",
        messages: [
          { role: "system", content: systemPrompt },
          { role: "user", content: "Help me understand why my query returned no results and suggest alternatives." },
        ],
        max_completion_tokens: 600,
        temperature: 0.3,
      });

      const content = response.choices[0]?.message?.content || "{}";
      
      let result;
      try {
        const cleanContent = content.replace(/```json\n?|\n?```/g, "").trim();
        result = JSON.parse(cleanContent);
      } catch {
        result = {
          clarificationQuestion: content,
          needsClarification: true,
        };
      }

      res.json(result);
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
      const validation = await validateBlockConfig(config, kind, user);
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
      if (config) {
        const validation = await validateBlockConfig(config, block.kind, user);
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

  // Execute a report block query (with safety guardrails)
  app.post("/api/reports/blocks/:id/run", isAuthenticated, reportLimiter, async (req, res) => {
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

      if (block.kind === "text") {
        return res.json({ type: "text", content: (block.config as any).content });
      }

      const config = block.config as TableBlockConfig | ChartBlockConfig | MetricBlockConfig;
      
      // Security: Comprehensive validation before executing any query
      const validation = await validateBlockConfig(config, block.kind, user);
      if (!validation.valid) {
        // Distinguish between permission errors (403) and validation errors (400)
        const statusCode = validation.error?.includes("Access denied") ? 403 : 400;
        return res.status(statusCode).json({ error: validation.error });
      }
      
      const pool = getPool(config.database);
      const configWithLimit = config as TableBlockConfig | ChartBlockConfig;
      const rowLimit = Math.min((configWithLimit as any).rowLimit || 500, 10000); // Max 10k rows
      
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
        
        // Build column list with proper table aliases for joins
        let columns: string;
        if (tableConfig.columns?.length > 0) {
          columns = tableConfig.columns.map(c => {
            if (c.includes(".")) {
              // Column from joined table (e.g., "vendors.email")
              const [, colName] = c.split(".");
              validateIdentifier(colName, "column");
              return `${joinAlias}."${colName}" AS "${c.replace(".", "_")}"`;
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
        }
        
        // Add filters (use main table alias for non-prefixed columns)
        if (tableConfig.filters?.length > 0) {
          const whereClauses: string[] = [];
          tableConfig.filters.forEach((f) => {
            // Add table alias to filter column if not already prefixed
            const filterWithAlias = { ...f, column: `${mainAlias}."${f.column}"` };
            addFilterToQueryWithAlias(filterWithAlias as any, params, whereClauses);
          });
          query += ` WHERE ${whereClauses.join(" AND ")}`;
        }
        
        // Add order by
        if (tableConfig.orderBy) {
          validateIdentifier(tableConfig.orderBy.column, "column");
          query += ` ORDER BY ${mainAlias}."${tableConfig.orderBy.column}" ${tableConfig.orderBy.direction === "desc" ? "DESC" : "ASC"}`;
        }
        
        query += ` LIMIT ${rowLimit}`;
        
        const result = await pool.query(query, params);
        
        await logAudit({
          userId,
          userEmail: user.email,
          action: "REPORT_QUERY",
          database: config.database,
          table: config.table,
          details: `Table block query: ${result.rows.length} rows${tableConfig.join ? ` (joined with ${tableConfig.join.table})` : ''}`,
          ip: req.ip || req.socket.remoteAddress,
        });
        
        res.json({ type: "table", rows: result.rows, rowCount: result.rows.length });
        
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
            const dateCol = `"${chartConfig.xColumn}"`;
            const datePart = chartConfig.groupBy.toLowerCase();
            
            if (datePart === "month") {
              labelExpr = `TO_CHAR(${dateCol}, 'YYYY-MM')`;
              groupByExpr = labelExpr;
            } else if (datePart === "year") {
              labelExpr = `TO_CHAR(${dateCol}, 'YYYY')`;
              groupByExpr = labelExpr;
            } else if (datePart === "day") {
              labelExpr = `TO_CHAR(${dateCol}, 'YYYY-MM-DD')`;
              groupByExpr = labelExpr;
            } else if (datePart === "week") {
              labelExpr = `TO_CHAR(${dateCol}, 'IYYY-IW')`;
              groupByExpr = labelExpr;
            } else if (datePart === "quarter") {
              labelExpr = `TO_CHAR(${dateCol}, 'YYYY-"Q"Q')`;
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
          
          query += ` GROUP BY ${groupByExpr} ORDER BY ${groupByExpr} LIMIT ${rowLimit}`;
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
          
          query += ` LIMIT ${rowLimit}`;
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
        validateIdentifier(metricConfig.column, "column");
        
        const aggFunc = metricConfig.aggregateFunction.toUpperCase();
        if (!["COUNT", "SUM", "AVG", "MIN", "MAX"].includes(aggFunc)) {
          return res.status(400).json({ error: "Invalid aggregate function" });
        }
        
        query = `SELECT ${aggFunc}("${metricConfig.column}") as value FROM ${tableRef}`;
        
        // Add filters
        if (metricConfig.filters?.length > 0) {
          const whereClauses: string[] = [];
          metricConfig.filters.forEach((f) => {
            addFilterToQuery(f, params, whereClauses);
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
          
          for (const t of tableResult.rows) {
            const fullName = `${t.schema}.${t.name}`;
            const settingsKey = `${dbConn.name}:${fullName}`;
            const isVisible = allSettings[settingsKey]?.isVisible ?? true;
            
            // Skip hidden tables for non-admin users
            if (user.role !== "admin" && !isVisible) continue;
            
            // For external customers, check grants
            if (user.role === "external_customer") {
              const allowedTables = await getAllowedTables(userId);
              if (!allowedTables.includes(`${dbConn.name}:${fullName}`)) continue;
            }
            
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

      // Get current date for relative date references
      const today = new Date();
      const todayStr = today.toISOString().split('T')[0];
      
      const systemPrompt = `You are a helpful report building assistant. You help users create custom reports with tables, charts, and metrics.

IMPORTANT: Today's date is ${todayStr}. Use this for any relative date references like "yesterday", "last week", "this month", etc.

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

For table blocks, config should have: database, table, columns (array of exact column names from above), filters (array), orderBy, rowLimit
For table blocks with JOINS (to pull data from related tables):
- Add a "join" object with: table (the related table like "public.vendors"), on (array of two column names [fromColumn, toColumn] like ["vendor_id", "id"])
- For columns from the joined table, prefix with table alias like "joined.email" or "joined.name"
- Example join config: { "table": "public.vendors", "on": ["vendor_id", "id"] }
- The joined table columns will be available as "joined.column_name" in the columns array

For chart blocks, config should have: database, table, chartType, xColumn (the date/timestamp column to group by), yColumn (the column to aggregate), aggregateFunction, groupBy (can be a column name OR one of: "month", "year", "day", "week", "quarter" for date-based grouping), filters, rowLimit
For metric blocks, config should have: database, table, column, aggregateFunction, filters, label, format

FILTER OPERATORS: Use these exact operator values in filters:
- "eq" for equals (NOT "=" or "==")
- "contains" for text contains
- "gt" for greater than
- "gte" for greater than or equal
- "lt" for less than
- "lte" for less than or equal
- "between" for date ranges (value must be array of two dates like ["2025-01-01", "2025-12-31"])

CRITICAL: Only use column names that are listed above. For date-based grouping, use groupBy: "month" (or year/day/week/quarter) with xColumn set to the date column like "created_at".

If you're just providing information or need clarification, respond with plain text.
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

      const assistantMessage = response.choices[0]?.message?.content || "I'm sorry, I couldn't process that request.";

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
      try {
        const jsonMatch = assistantMessage.match(/\{[\s\S]*"action"[\s\S]*\}/);
        if (jsonMatch) {
          action = JSON.parse(jsonMatch[0]);
          
          // Security: Validate AI-generated action before returning it
          if (action?.action === "create_block" && action?.block) {
            const block = action.block;
            const validKinds = ["table", "chart", "metric", "text"];
            
            if (validKinds.includes(block.kind)) {
              // Ensure default values are set
              const config = {
                ...block.config,
                database: block.config?.database || "Default",
                rowLimit: Math.min(block.config?.rowLimit || 500, 10000),
                filters: block.config?.filters || [],
              };
              
              // Validate the config against database metadata and user permissions
              const validation = await validateBlockConfig(config, block.kind, user);
              
              if (validation.valid) {
                validatedAction = {
                  action: "create_block",
                  block: {
                    kind: block.kind,
                    title: block.title || `${block.kind} block`,
                    config,
                  },
                  explanation: action.explanation || "",
                };
              } else {
                // AI suggested an invalid action - log and return null action
                console.log(`[SECURITY] AI suggested invalid block config: ${validation.error}`);
                validatedAction = null;
              }
            }
          }
        }
      } catch {
        // Not a JSON response, that's fine
      }

      res.json({
        message: assistantMessage,
        action: validatedAction,
      });
    } catch (err) {
      console.error("Error in AI chat:", err);
      res.status(500).json({ error: "Failed to process AI request" });
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

  return httpServer;
}
