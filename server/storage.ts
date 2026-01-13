import * as fs from "fs";
import * as path from "path";
import type { FilterDefinition, FilterHistoryEntry, ActiveFilter } from "@shared/schema";

const FILTERS_FILE = path.join(process.cwd(), "filters.json");
const TABLE_SETTINGS_FILE = path.join(process.cwd(), "table_settings.json");
const FILTER_HISTORY_FILE = path.join(process.cwd(), "filter_history.json");

const MAX_HISTORY_PER_TABLE = 5;

export interface FiltersConfig {
  [tableKey: string]: FilterDefinition[];
}

export interface TableSettingsEntry {
  isVisible: boolean;
  displayName: string | null;
  hiddenColumns?: string[];
}

export interface TableSettingsConfig {
  [key: string]: TableSettingsEntry;
}

export interface IStorage {
  getFilters(table: string): Promise<FilterDefinition[]>;
  setFilters(table: string, filters: FilterDefinition[]): Promise<void>;
  getAllFilters(): Promise<FiltersConfig>;
  getTableSettings(database: string, tableName: string): Promise<TableSettingsEntry | null>;
  setTableSettings(database: string, tableName: string, settings: TableSettingsEntry): Promise<void>;
  getAllTableSettings(): Promise<TableSettingsConfig>;
  getFilterHistory(userId: string, database: string, table: string): Promise<FilterHistoryEntry[]>;
  saveFilterHistory(userId: string, database: string, table: string, filters: ActiveFilter[]): Promise<FilterHistoryEntry>;
  deleteFilterHistory(id: string, userId: string): Promise<boolean>;
}

function readFiltersFile(): FiltersConfig {
  try {
    if (fs.existsSync(FILTERS_FILE)) {
      const content = fs.readFileSync(FILTERS_FILE, "utf-8");
      return JSON.parse(content);
    }
  } catch (err) {
    console.error("Error reading filters.json:", err);
  }
  return {};
}

function writeFiltersFile(config: FiltersConfig): void {
  try {
    fs.writeFileSync(FILTERS_FILE, JSON.stringify(config, null, 2));
  } catch (err) {
    console.error("Error writing filters.json:", err);
    throw err;
  }
}

function readTableSettingsFile(): TableSettingsConfig {
  try {
    if (fs.existsSync(TABLE_SETTINGS_FILE)) {
      const content = fs.readFileSync(TABLE_SETTINGS_FILE, "utf-8");
      return JSON.parse(content);
    }
  } catch (err) {
    console.error("Error reading table_settings.json:", err);
  }
  return {};
}

function writeTableSettingsFile(config: TableSettingsConfig): void {
  try {
    fs.writeFileSync(TABLE_SETTINGS_FILE, JSON.stringify(config, null, 2));
  } catch (err) {
    console.error("Error writing table_settings.json:", err);
    throw err;
  }
}

function readFilterHistoryFile(): FilterHistoryEntry[] {
  try {
    if (fs.existsSync(FILTER_HISTORY_FILE)) {
      const content = fs.readFileSync(FILTER_HISTORY_FILE, "utf-8");
      return JSON.parse(content);
    }
  } catch (err) {
    console.error("Error reading filter_history.json:", err);
  }
  return [];
}

function writeFilterHistoryFile(entries: FilterHistoryEntry[]): void {
  try {
    fs.writeFileSync(FILTER_HISTORY_FILE, JSON.stringify(entries, null, 2));
  } catch (err) {
    console.error("Error writing filter_history.json:", err);
    throw err;
  }
}

function generateId(): string {
  return Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
}

function filtersEqual(a: ActiveFilter[], b: ActiveFilter[]): boolean {
  if (a.length !== b.length) return false;
  const sortedA = [...a].sort((x, y) => x.column.localeCompare(y.column));
  const sortedB = [...b].sort((x, y) => x.column.localeCompare(y.column));
  return sortedA.every((f, i) => 
    f.column === sortedB[i].column && 
    f.operator === sortedB[i].operator && 
    f.value === sortedB[i].value
  );
}

export class MemStorage implements IStorage {
  private filters: FiltersConfig;
  private tableSettings: TableSettingsConfig;
  private filterHistory: FilterHistoryEntry[];

  constructor() {
    this.filters = readFiltersFile();
    this.tableSettings = readTableSettingsFile();
    this.filterHistory = readFilterHistoryFile();
  }

  async getFilters(table: string): Promise<FilterDefinition[]> {
    return this.filters[table] || [];
  }

  async setFilters(table: string, filters: FilterDefinition[]): Promise<void> {
    this.filters[table] = filters;
    writeFiltersFile(this.filters);
  }

  async getAllFilters(): Promise<FiltersConfig> {
    return this.filters;
  }

  async getTableSettings(database: string, tableName: string): Promise<TableSettingsEntry | null> {
    const key = `${database}:${tableName}`;
    return this.tableSettings[key] || null;
  }

  async setTableSettings(database: string, tableName: string, settings: TableSettingsEntry): Promise<void> {
    const key = `${database}:${tableName}`;
    this.tableSettings[key] = settings;
    writeTableSettingsFile(this.tableSettings);
  }

  async getAllTableSettings(): Promise<TableSettingsConfig> {
    return this.tableSettings;
  }

  async getFilterHistory(userId: string, database: string, table: string): Promise<FilterHistoryEntry[]> {
    return this.filterHistory
      .filter(e => e.userId === userId && e.database === database && e.table === table)
      .sort((a, b) => new Date(b.lastUsedAt).getTime() - new Date(a.lastUsedAt).getTime())
      .slice(0, MAX_HISTORY_PER_TABLE);
  }

  async saveFilterHistory(userId: string, database: string, table: string, filters: ActiveFilter[]): Promise<FilterHistoryEntry> {
    if (filters.length === 0) {
      throw new Error("Cannot save empty filter history");
    }

    const existingIndex = this.filterHistory.findIndex(
      e => e.userId === userId && e.database === database && e.table === table && filtersEqual(e.filters, filters)
    );

    const now = new Date().toISOString();

    if (existingIndex !== -1) {
      this.filterHistory[existingIndex].lastUsedAt = now;
      writeFilterHistoryFile(this.filterHistory);
      return this.filterHistory[existingIndex];
    }

    const newEntry: FilterHistoryEntry = {
      id: generateId(),
      userId,
      database,
      table,
      filters,
      lastUsedAt: now,
    };

    this.filterHistory.push(newEntry);

    const userTableEntries = this.filterHistory
      .filter(e => e.userId === userId && e.database === database && e.table === table)
      .sort((a, b) => new Date(b.lastUsedAt).getTime() - new Date(a.lastUsedAt).getTime());

    if (userTableEntries.length > MAX_HISTORY_PER_TABLE) {
      const toRemove = userTableEntries.slice(MAX_HISTORY_PER_TABLE);
      this.filterHistory = this.filterHistory.filter(e => !toRemove.some(r => r.id === e.id));
    }

    writeFilterHistoryFile(this.filterHistory);
    return newEntry;
  }

  async deleteFilterHistory(id: string, userId: string): Promise<boolean> {
    const index = this.filterHistory.findIndex(e => e.id === id && e.userId === userId);
    if (index === -1) return false;
    this.filterHistory.splice(index, 1);
    writeFilterHistoryFile(this.filterHistory);
    return true;
  }
}

export const storage = new MemStorage();
