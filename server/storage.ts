import * as fs from "fs";
import * as path from "path";
import type { FilterDefinition } from "@shared/schema";

const FILTERS_FILE = path.join(process.cwd(), "filters.json");

export interface FiltersConfig {
  [tableKey: string]: FilterDefinition[];
}

export interface IStorage {
  getFilters(table: string): Promise<FilterDefinition[]>;
  setFilters(table: string, filters: FilterDefinition[]): Promise<void>;
  getAllFilters(): Promise<FiltersConfig>;
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

export class MemStorage implements IStorage {
  private filters: FiltersConfig;

  constructor() {
    this.filters = readFiltersFile();
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
}

export const storage = new MemStorage();
