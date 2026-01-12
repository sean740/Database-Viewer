import type { Express, Request, Response } from "express";
import { createServer, type Server } from "http";
import { Pool } from "pg";
import OpenAI from "openai";
import { eq } from "drizzle-orm";
import { storage } from "./storage";
import { db } from "./db";
import { setupAuth, registerAuthRoutes, isAuthenticated, authStorage } from "./replit_integrations/auth";
import { users, tableGrants, type UserRole, type User } from "@shared/schema";
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

  const pool = new Pool({
    connectionString: db.url,
    max: 5,
    idleTimeoutMillis: 30000,
    ssl: {
      rejectUnauthorized: false,
    },
  });

  pools.set(dbName, pool);
  return pool;
}

// Build operator SQL
function getOperatorSQL(
  operator: FilterOperator,
  paramIndex: number
): { sql: string; transform?: (v: string) => string } {
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
    default:
      throw new Error(`Invalid operator: ${operator}`);
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

export async function registerRoutes(
  httpServer: Server,
  app: Express
): Promise<Server> {
  // Setup auth BEFORE other routes
  await setupAuth(app);
  registerAuthRoutes(app);
  
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

  // Get filter definitions for a table
  app.get("/api/filters/:table", async (req: Request, res: Response) => {
    try {
      const { table } = req.params;
      const filters = await storage.getFilters(table);
      res.json(filters);
    } catch (err) {
      console.error("Error getting filters:", err);
      res.status(500).json({ error: "Failed to get filters" });
    }
  });

  // Save filter definitions for a table
  app.post("/api/filters", async (req: Request, res: Response) => {
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
  app.get("/api/export", isAuthenticated, async (req: Request, res: Response) => {
    try {
      const { database, table, page = "1", filters: filtersJson } = req.query;

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

      // Calculate offset
      const pageNum = Math.max(1, parseInt(page as string, 10) || 1);
      const offset = (pageNum - 1) * PAGE_SIZE;

      // Fetch rows
      const dataQuery = `
        SELECT * FROM "${schema}"."${tableName}"
        ${whereSQL}
        ORDER BY ${orderByColumn} ASC
        LIMIT ${PAGE_SIZE}
        OFFSET ${offset}
      `;
      const dataResult = await pool.query(dataQuery, params);

      // Generate CSV
      const escapeCSV = (value: unknown): string => {
        if (value === null || value === undefined) return "";
        const str = typeof value === "object" ? JSON.stringify(value) : String(value);
        if (str.includes(",") || str.includes('"') || str.includes("\n")) {
          return `"${str.replace(/"/g, '""')}"`;
        }
        return str;
      };

      const header = columnNames.map(escapeCSV).join(",");
      const rows = dataResult.rows.map((row) =>
        columnNames.map((col) => escapeCSV(row[col])).join(",")
      );
      const csv = [header, ...rows].join("\n");

      res.setHeader("Content-Type", "text/csv");
      res.setHeader(
        "Content-Disposition",
        `attachment; filename="${tableName}_page${pageNum}.csv"`
      );
      res.send(csv);
    } catch (err) {
      console.error("Error exporting CSV:", err);
      res.status(500).json({
        error: err instanceof Error ? err.message : "Failed to export CSV",
      });
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
      const { database, query, table: currentTable } = req.body;

      const client = getOpenAIClient();
      if (!client) {
        return res.status(400).json({ error: "Natural language queries are not enabled" });
      }

      if (!query || typeof query !== "string") {
        return res.status(400).json({ error: "Query is required" });
      }
      
      // Check table access for external customers (if a table is specified)
      const userId = (req.user as any)?.id;
      const user = await authStorage.getUser(userId);
      if (user?.role === "external_customer" && currentTable) {
        const allowedTables = await getAllowedTables(userId);
        if (!allowedTables.includes(`${database}:${currentTable}`)) {
          return res.status(403).json({ error: "You don't have access to this table" });
        }
      }

      // Get available tables if we need to discover them
      let availableTables: string[] = [];
      let availableColumns: string[] = [];

      if (database) {
        const pool = getPool(database);
        const tablesResult = await pool.query(`
          SELECT table_schema || '.' || table_name as full_name
          FROM information_schema.tables
          WHERE table_type = 'BASE TABLE'
            AND table_schema NOT IN ('pg_catalog', 'information_schema')
        `);
        availableTables = tablesResult.rows.map((r) => r.full_name);
        
        // For external customers, filter to only granted tables
        if (user?.role === "external_customer") {
          const allowedTables = await getAllowedTables(userId);
          availableTables = availableTables.filter(t => allowedTables.includes(`${database}:${t}`));
          
          // If no tables are allowed, return early
          if (availableTables.length === 0) {
            return res.status(403).json({ error: "No tables available. Contact an admin for access." });
          }
        }

        // If a table is selected, get its columns
        if (currentTable) {
          const [schema, tableName] = currentTable.split(".");
          const columnsResult = await pool.query(
            `
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = $1 AND table_name = $2
          `,
            [schema, tableName]
          );
          availableColumns = columnsResult.rows.map((r) => r.column_name);
        }
      }

      const systemPrompt = `You are a helpful assistant that converts natural language queries into structured query plans for a database viewer.

The user wants to query a PostgreSQL database. You must return ONLY a valid JSON object with this structure:
{
  "table": "schema.table_name",
  "page": 1,
  "filters": [
    {"column": "column_name", "op": "operator", "value": "filter_value"}
  ]
}

Valid operators are: eq (equals), contains (substring match), gt (greater than), gte (greater than or equal), lt (less than), lte (less than or equal).

${currentTable ? `The user is currently viewing table: ${currentTable}. Use this table unless they specify a different one.` : ""}
${availableTables.length > 0 ? `Available tables: ${availableTables.join(", ")}` : ""}
${availableColumns.length > 0 ? `Available columns for ${currentTable}: ${availableColumns.join(", ")}` : ""}

IMPORTANT:
- Only return valid JSON, no explanation or markdown
- Always use page 1 unless the user asks for a specific page
- The "contains" operator is case-insensitive
- If the query doesn't make sense, return {"table": "${currentTable || "public.unknown"}", "page": 1, "filters": []}`;

      const response = await client.chat.completions.create({
        model: "gpt-4o",
        messages: [
          { role: "system", content: systemPrompt },
          { role: "user", content: query },
        ],
        max_completion_tokens: 500,
        temperature: 0.1,
      });

      const content = response.choices[0]?.message?.content || "{}";
      
      // Parse the response
      let plan: NLQPlan;
      try {
        plan = JSON.parse(content);
      } catch {
        return res.status(400).json({ error: "Failed to parse AI response" });
      }

      // Validate the plan
      if (!plan.table || typeof plan.table !== "string") {
        plan.table = currentTable || "public.unknown";
      }

      // Validate table exists
      if (availableTables.length > 0 && !availableTables.includes(plan.table)) {
        return res.status(400).json({ error: `Table not found: ${plan.table}` });
      }

      // Validate filters
      const validOperators = ["eq", "contains", "gt", "gte", "lt", "lte"];
      if (plan.filters && Array.isArray(plan.filters)) {
        for (const filter of plan.filters) {
          if (!validOperators.includes(filter.op)) {
            return res.status(400).json({ error: `Invalid operator: ${filter.op}` });
          }
          if (availableColumns.length > 0 && !availableColumns.includes(filter.column)) {
            return res.status(400).json({ error: `Invalid column: ${filter.column}` });
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

  return httpServer;
}
