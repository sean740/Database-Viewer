import * as fs from "fs";
import * as path from "path";
import type { FilterDefinition } from "@shared/schema";

const FILTERS_FILE = path.join(process.cwd(), "filters.json");
const TABLE_SETTINGS_FILE = path.join(process.cwd(), "table_settings.json");

export interface FiltersConfig {
  [tableKey: string]: FilterDefinition[];
}

export interface TableSettingsEntry {
  isVisible: boolean;
  displayName: string | null;
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

export class MemStorage implements IStorage {
  private filters: FiltersConfig;
  private tableSettings: TableSettingsConfig;

  constructor() {
    this.filters = readFiltersFile();
    this.tableSettings = readTableSettingsFile();
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
}

export const storage = new MemStorage();
