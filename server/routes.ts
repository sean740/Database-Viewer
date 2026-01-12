import type { Express, Request, Response } from "express";
import { createServer, type Server } from "http";
import { Pool } from "pg";
import OpenAI from "openai";
import { storage } from "./storage";
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

export async function registerRoutes(
  httpServer: Server,
  app: Express
): Promise<Server> {
  // Get available databases
  app.get("/api/databases", (req: Request, res: Response) => {
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
  app.get("/api/tables/:database", async (req: Request, res: Response) => {
    try {
      const { database } = req.params;
      const pool = getPool(database);

      const result = await pool.query(`
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name
      `);

      const tables: TableInfo[] = result.rows.map((row) => ({
        schema: row.table_schema,
        name: row.table_name,
        fullName: `${row.table_schema}.${row.table_name}`,
      }));

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
    "/api/columns/:database/:schema/:table",
    async (req: Request, res: Response) => {
      try {
        const { database, schema, table } = req.params;

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
  app.post("/api/rows", async (req: Request, res: Response) => {
    try {
      const { database, table, page = 1, filters = [] } = req.body;

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
  app.get("/api/export", async (req: Request, res: Response) => {
    try {
      const { database, table, page = "1", filters: filtersJson } = req.query;

      if (!database || !table) {
        return res.status(400).json({ error: "Database and table are required" });
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
  app.get("/api/nlq/status", (req: Request, res: Response) => {
    const client = getOpenAIClient();
    res.json({ enabled: client !== null });
  });

  // Natural Language Query
  app.post("/api/nlq", async (req: Request, res: Response) => {
    try {
      const { database, query, table: currentTable } = req.body;

      const client = getOpenAIClient();
      if (!client) {
        return res.status(400).json({ error: "Natural language queries are not enabled" });
      }

      if (!query || typeof query !== "string") {
        return res.status(400).json({ error: "Query is required" });
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
