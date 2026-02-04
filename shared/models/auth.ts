import { sql } from "drizzle-orm";
import { boolean, index, jsonb, pgTable, text, timestamp, varchar, integer } from "drizzle-orm/pg-core";

// User role enum type
export type UserRole = "admin" | "washos_user" | "external_customer";

// Session storage table.
// (IMPORTANT) This table is mandatory for Replit Auth, don't drop it.
export const sessions = pgTable(
  "sessions",
  {
    sid: varchar("sid").primaryKey(),
    sess: jsonb("sess").notNull(),
    expire: timestamp("expire").notNull(),
  },
  (table) => [index("IDX_session_expire").on(table.expire)]
);

// User storage table.
// (IMPORTANT) This table is mandatory for Replit Auth, don't drop it.
export const users = pgTable("users", {
  id: integer("id").primaryKey().generatedAlwaysAsIdentity(),
  email: varchar("email").unique().notNull(),
  password: varchar("password_digest"),
  firstName: varchar("first_name"),
  lastName: varchar("last_name"),
  profileImageUrl: varchar("profile_image_url"),
  role: varchar("role").$type<UserRole>().default("external_customer").notNull(),
  isActive: boolean("is_active").default(true).notNull(),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow(),
});

export type UpsertUser = typeof users.$inferInsert;
export type User = typeof users.$inferSelect;

// Table grants for External Customers - which tables they can access
export const tableGrants = pgTable("table_grants", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  userId: integer("user_id").notNull().references(() => users.id, { onDelete: "cascade" }),
  database: varchar("database").notNull(),
  tableName: varchar("table_name").notNull(),
  grantedBy: integer("granted_by").notNull().references(() => users.id),
  grantedAt: timestamp("granted_at").defaultNow(),
});

export type TableGrant = typeof tableGrants.$inferSelect;
export type InsertTableGrant = typeof tableGrants.$inferInsert;

// Audit logs table for tracking data access
export const auditLogs = pgTable("audit_logs", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  userId: varchar("user_id").notNull(),
  userEmail: varchar("user_email").notNull(),
  action: varchar("action").notNull(),
  database: varchar("database"),
  tableName: varchar("table_name"),
  details: text("details"),
  ipAddress: varchar("ip_address"),
  timestamp: timestamp("timestamp").defaultNow().notNull(),
}, (table) => [
  index("idx_audit_logs_user").on(table.userId),
  index("idx_audit_logs_timestamp").on(table.timestamp),
]);

export type AuditLog = typeof auditLogs.$inferSelect;
export type InsertAuditLog = typeof auditLogs.$inferInsert;

// Report block types
export type ReportBlockKind = "table" | "chart" | "metric" | "text";
export type ChartType = "bar" | "line" | "pie" | "area";
export type AggregateFunction = "count" | "sum" | "avg" | "min" | "max";

// User report pages - each user can have multiple report pages
export const reportPages = pgTable("report_pages", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  userId: varchar("user_id").notNull().references(() => users.id, { onDelete: "cascade" }),
  title: varchar("title").notNull(),
  description: text("description"),
  isArchived: boolean("is_archived").default(false).notNull(),
  createdAt: timestamp("created_at").defaultNow().notNull(),
  updatedAt: timestamp("updated_at").defaultNow().notNull(),
}, (table) => [
  index("idx_report_pages_user").on(table.userId),
]);

export type ReportPage = typeof reportPages.$inferSelect;
export type InsertReportPage = typeof reportPages.$inferInsert;

// Report blocks - individual components within a report page
export const reportBlocks = pgTable("report_blocks", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  pageId: varchar("page_id").notNull().references(() => reportPages.id, { onDelete: "cascade" }),
  kind: varchar("kind").$type<ReportBlockKind>().notNull(),
  title: varchar("title"),
  position: jsonb("position").$type<{ row: number; col: number; width: number; height: number }>().notNull(),
  config: jsonb("config").$type<ReportBlockConfig>().notNull(),
  createdAt: timestamp("created_at").defaultNow().notNull(),
  updatedAt: timestamp("updated_at").defaultNow().notNull(),
}, (table) => [
  index("idx_report_blocks_page").on(table.pageId),
]);

export type ReportBlock = typeof reportBlocks.$inferSelect;
export type InsertReportBlock = typeof reportBlocks.$inferInsert;

// Sub-join configuration for nested joins (e.g., bookings -> addresses -> districts)
export interface SubJoinConfig {
  table: string; // The table to join from the first join table
  on: [string, string]; // [fromColumn, toColumn] - column in first join table to column in this table
  type?: "inner" | "left"; // Join type, defaults to "left"
}

// Join configuration for report blocks
export interface JoinConfig {
  table: string; // The table to join (e.g., "public.vendors")
  on: [string, string]; // [fromColumn, toColumn] - e.g., ["vendor_id", "id"]
  type?: "inner" | "left"; // Join type, defaults to "left"
  columns?: string[]; // Columns to select from joined table
  subJoin?: SubJoinConfig; // Optional nested join from this joined table to another table
}

// Report block configuration types
export interface TableBlockConfig {
  database: string;
  table: string;
  columns: string[];
  filters: { column: string; operator: string; value: string | string[] }[];
  orderBy?: { column: string; direction: "asc" | "desc" };
  rowLimit: number;
  join?: JoinConfig;
}

export interface ChartBlockConfig {
  database: string;
  table: string;
  chartType: ChartType;
  xColumn: string;
  yColumn: string;
  aggregateFunction?: AggregateFunction;
  groupBy?: string;
  filters: { column: string; operator: string; value: string }[];
  rowLimit: number;
}

export interface MetricBlockConfig {
  database: string;
  table: string;
  column: string;
  aggregateFunction: AggregateFunction;
  filters: { column: string; operator: string; value: string }[];
  label?: string;
  format?: "number" | "currency" | "percentage";
}

export interface TextBlockConfig {
  content: string;
}

export type ReportBlockConfig = TableBlockConfig | ChartBlockConfig | MetricBlockConfig | TextBlockConfig;

// AI chat sessions for report building
export const reportChatSessions = pgTable("report_chat_sessions", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  pageId: varchar("page_id").notNull().references(() => reportPages.id, { onDelete: "cascade" }),
  messages: jsonb("messages").$type<ChatMessage[]>().default([]).notNull(),
  createdAt: timestamp("created_at").defaultNow().notNull(),
  updatedAt: timestamp("updated_at").defaultNow().notNull(),
}, (table) => [
  index("idx_report_chat_sessions_page").on(table.pageId),
]);

export type ReportChatSession = typeof reportChatSessions.$inferSelect;
export type InsertReportChatSession = typeof reportChatSessions.$inferInsert;

export interface ChatMessage {
  role: "user" | "assistant" | "system";
  content: string;
  timestamp: string;
}
