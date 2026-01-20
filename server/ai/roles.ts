import type { ColumnStats } from "./data-dictionary";

export type SemanticRole = 
  | "event_time.created"
  | "event_time.updated"
  | "event_time.scheduled"
  | "event_time.completed"
  | "event_time.deleted"
  | "actor.customer"
  | "actor.vendor"
  | "actor.user"
  | "status.lifecycle"
  | "status.payment"
  | "money.amount"
  | "money.total"
  | "identifier.primary"
  | "identifier.foreign"
  | "location.address"
  | "location.city"
  | "location.state"
  | "location.zip"
  | "contact.email"
  | "contact.phone";

interface RoleMapping {
  role: SemanticRole;
  patterns: RegExp[];
  dataTypes?: string[];
}

const ROLE_MAPPINGS: RoleMapping[] = [
  { role: "event_time.created", patterns: [/^created[_]?at$/i, /^date[_]?created$/i, /^creation[_]?date$/i, /^inserted[_]?at$/i], dataTypes: ["timestamp", "date"] },
  { role: "event_time.updated", patterns: [/^updated[_]?at$/i, /^modified[_]?at$/i, /^last[_]?modified$/i, /^changed[_]?at$/i], dataTypes: ["timestamp", "date"] },
  { role: "event_time.scheduled", patterns: [/^scheduled[_]?at$/i, /^scheduled[_]?for$/i, /^appointment[_]?(time|date)?$/i, /^booking[_]?(time|date)?$/i, /^start[_]?(time|date)?$/i], dataTypes: ["timestamp", "date"] },
  { role: "event_time.completed", patterns: [/^completed[_]?at$/i, /^finished[_]?at$/i, /^done[_]?at$/i, /^end[_]?(time|date)?$/i], dataTypes: ["timestamp", "date"] },
  { role: "event_time.deleted", patterns: [/^deleted[_]?at$/i, /^removed[_]?at$/i, /^archived[_]?at$/i], dataTypes: ["timestamp", "date"] },
  { role: "actor.customer", patterns: [/^customer[_]?id$/i, /^client[_]?id$/i, /^buyer[_]?id$/i], dataTypes: ["integer", "varchar", "uuid"] },
  { role: "actor.vendor", patterns: [/^vendor[_]?id$/i, /^supplier[_]?id$/i, /^provider[_]?id$/i, /^merchant[_]?id$/i], dataTypes: ["integer", "varchar", "uuid"] },
  { role: "actor.user", patterns: [/^user[_]?id$/i, /^member[_]?id$/i, /^account[_]?id$/i, /^owner[_]?id$/i], dataTypes: ["integer", "varchar", "uuid"] },
  { role: "status.lifecycle", patterns: [/^status$/i, /^state$/i, /^lifecycle[_]?status$/i, /^order[_]?status$/i, /^booking[_]?status$/i], dataTypes: ["varchar", "text", "USER-DEFINED"] },
  { role: "status.payment", patterns: [/^payment[_]?status$/i, /^paid[_]?status$/i, /^billing[_]?status$/i], dataTypes: ["varchar", "text", "USER-DEFINED"] },
  { role: "money.amount", patterns: [/^amount$/i, /^price$/i, /^cost$/i, /^fee$/i, /^charge$/i, /^rate$/i], dataTypes: ["numeric", "decimal", "money", "double precision", "real"] },
  { role: "money.total", patterns: [/^total$/i, /^grand[_]?total$/i, /^subtotal$/i, /^sum$/i], dataTypes: ["numeric", "decimal", "money", "double precision", "real"] },
  { role: "identifier.primary", patterns: [/^id$/i, /^pk$/i], dataTypes: ["integer", "serial", "bigserial", "uuid", "varchar"] },
  { role: "identifier.foreign", patterns: [/[_]id$/i], dataTypes: ["integer", "varchar", "uuid"] },
  { role: "location.address", patterns: [/^address$/i, /^street$/i, /^address[_]?line$/i], dataTypes: ["varchar", "text"] },
  { role: "location.city", patterns: [/^city$/i, /^town$/i, /^municipality$/i], dataTypes: ["varchar", "text"] },
  { role: "location.state", patterns: [/^state$/i, /^province$/i, /^region$/i], dataTypes: ["varchar", "text"] },
  { role: "location.zip", patterns: [/^zip$/i, /^zip[_]?code$/i, /^postal[_]?code$/i, /^postcode$/i], dataTypes: ["varchar", "text"] },
  { role: "contact.email", patterns: [/email$/i, /^e[_]?mail$/i], dataTypes: ["varchar", "text"] },
  { role: "contact.phone", patterns: [/phone$/i, /^tel$/i, /^telephone$/i, /^mobile$/i, /^cell$/i], dataTypes: ["varchar", "text"] },
];

export interface ColumnRoleAssignment {
  column: string;
  dataType: string;
  role: SemanticRole | null;
  confidence: "high" | "medium" | "low";
}

export function inferColumnRole(columnName: string, dataType: string): ColumnRoleAssignment {
  for (const mapping of ROLE_MAPPINGS) {
    const patternMatch = mapping.patterns.some(p => p.test(columnName));
    const typeMatch = !mapping.dataTypes || mapping.dataTypes.some(t => dataType.toLowerCase().includes(t.toLowerCase()));

    if (patternMatch && typeMatch) {
      return { column: columnName, dataType, role: mapping.role, confidence: "high" };
    }

    if (patternMatch) {
      return { column: columnName, dataType, role: mapping.role, confidence: "medium" };
    }
  }

  return { column: columnName, dataType, role: null, confidence: "low" };
}

export function inferAllColumnRoles(columns: Array<{ name: string; dataType: string }>): ColumnRoleAssignment[] {
  return columns.map(col => inferColumnRole(col.name, col.dataType));
}

export function getDateColumns(columns: ColumnStats[]): ColumnStats[] {
  return columns.filter(c => c.dataType.includes("timestamp") || c.dataType.includes("date"));
}

export function getBestDateColumn(columns: ColumnStats[], preferenceHints: string[] = []): { column: string; reason: string } | null {
  const dateColumns = getDateColumns(columns);
  if (dateColumns.length === 0) return null;
  if (dateColumns.length === 1) return { column: dateColumns[0].name, reason: "only date column" };

  for (const hint of preferenceHints) {
    const hintLower = hint.toLowerCase();
    for (const col of dateColumns) {
      if (col.name.toLowerCase().includes(hintLower)) {
        return { column: col.name, reason: `matched hint "${hint}"` };
      }
    }
  }

  const roleAssignments = dateColumns.map(c => inferColumnRole(c.name, c.dataType));

  const priorityOrder: SemanticRole[] = [
    "event_time.scheduled",
    "event_time.created",
    "event_time.completed",
    "event_time.updated",
    "event_time.deleted",
  ];

  for (const priority of priorityOrder) {
    const match = roleAssignments.find(r => r.role === priority && r.confidence === "high");
    if (match) {
      return { column: match.column, reason: `inferred role: ${priority}` };
    }
  }

  const withData = dateColumns.filter(c => c.dateRange && c.nullRate < 0.5);
  if (withData.length > 0) {
    const sorted = withData.sort((a, b) => a.nullRate - b.nullRate);
    return { column: sorted[0].name, reason: "lowest null rate among date columns" };
  }

  return { column: dateColumns[0].name, reason: "first date column" };
}

export function resolveSemanticReference(
  query: string,
  columns: ColumnStats[]
): { resolvedColumn: string | null; type: "date" | "status" | "actor" | null; needsClarification: boolean; options?: string[] } {
  const queryLower = query.toLowerCase();

  if (/\b(scheduled|appointment|booking|service\s*date)\b/i.test(queryLower)) {
    const scheduled = columns.find(c => /scheduled|appointment|booking|start/i.test(c.name) && (c.dataType.includes("timestamp") || c.dataType.includes("date")));
    if (scheduled) return { resolvedColumn: scheduled.name, type: "date", needsClarification: false };
  }

  if (/\b(created|added|registered)\b/i.test(queryLower)) {
    const created = columns.find(c => /created|inserted/i.test(c.name) && (c.dataType.includes("timestamp") || c.dataType.includes("date")));
    if (created) return { resolvedColumn: created.name, type: "date", needsClarification: false };
  }

  if (/\b(updated|modified|changed)\b/i.test(queryLower)) {
    const updated = columns.find(c => /updated|modified/i.test(c.name) && (c.dataType.includes("timestamp") || c.dataType.includes("date")));
    if (updated) return { resolvedColumn: updated.name, type: "date", needsClarification: false };
  }

  if (/\b(completed|finished|done)\b/i.test(queryLower)) {
    const completed = columns.find(c => /completed|finished|done|end/i.test(c.name) && (c.dataType.includes("timestamp") || c.dataType.includes("date")));
    if (completed) return { resolvedColumn: completed.name, type: "date", needsClarification: false };
  }

  const dateColumns = getDateColumns(columns);
  if (dateColumns.length > 1) {
    if (/\b(yesterday|today|this\s+(week|month|year)|last\s+(week|month|year)|(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)|20\d{2})\b/i.test(queryLower)) {
      return {
        resolvedColumn: null,
        type: "date",
        needsClarification: true,
        options: dateColumns.map(c => c.name),
      };
    }
  } else if (dateColumns.length === 1) {
    return { resolvedColumn: dateColumns[0].name, type: "date", needsClarification: false };
  }

  return { resolvedColumn: null, type: null, needsClarification: false };
}

export function formatRolesForPrompt(columns: ColumnStats[]): string {
  const roles = inferAllColumnRoles(columns.map(c => ({ name: c.name, dataType: c.dataType })));
  const highConfidence = roles.filter(r => r.role && r.confidence === "high");

  if (highConfidence.length === 0) return "";

  const lines: string[] = ["\nDetected Column Roles:"];
  for (const role of highConfidence) {
    lines.push(`  - ${role.column}: ${role.role}`);
  }
  return lines.join("\n");
}
