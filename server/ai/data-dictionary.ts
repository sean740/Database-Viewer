import { Pool } from "pg";

export interface ColumnStats {
  name: string;
  dataType: string;
  nullRate: number;
  dateRange?: { min: string; max: string };
  numericRange?: { min: number; max: number; p50?: number; p95?: number };
  topValues?: Array<{ value: string; count: number }>;
  totalRows?: number;
}

export interface TableDataDictionary {
  database: string;
  schema: string;
  table: string;
  columns: ColumnStats[];
  fetchedAt: number;
  totalRows: number;
}

const dataDictionaryCache = new Map<string, TableDataDictionary>();
const CACHE_TTL_MS = 15 * 60 * 1000;
const MAX_COLUMNS_HEAVY_SAMPLING = 15;
const QUERY_TIMEOUT_MS = 5000;
const LOW_CARDINALITY_THRESHOLD = 100;

function getCacheKey(database: string, schema: string, table: string): string {
  return `${database}:${schema}.${table}`;
}

async function queryWithTimeout<T>(pool: Pool, sql: string, params: any[] = [], timeoutMs = QUERY_TIMEOUT_MS): Promise<T | null> {
  return new Promise((resolve) => {
    const timeoutId = setTimeout(() => resolve(null), timeoutMs);
    pool.query(sql, params)
      .then((result) => {
        clearTimeout(timeoutId);
        resolve(result as T);
      })
      .catch(() => {
        clearTimeout(timeoutId);
        resolve(null);
      });
  });
}

export async function getTableDataDictionary(
  pool: Pool,
  database: string,
  schema: string,
  table: string,
  forceRefresh = false
): Promise<TableDataDictionary | null> {
  const cacheKey = getCacheKey(database, schema, table);
  const cached = dataDictionaryCache.get(cacheKey);

  if (!forceRefresh && cached && Date.now() - cached.fetchedAt < CACHE_TTL_MS) {
    return cached;
  }

  try {
    const columnsResult = await pool.query(`
      SELECT column_name, data_type, is_nullable
      FROM information_schema.columns
      WHERE table_schema = $1 AND table_name = $2
      ORDER BY ordinal_position
    `, [schema, table]);

    if (columnsResult.rows.length === 0) return null;

    const countResult = await queryWithTimeout<{ rows: Array<{ count: string }> }>(
      pool,
      `SELECT COUNT(*) as count FROM "${schema}"."${table}"`,
      [],
      3000
    );
    const totalRows = countResult?.rows[0]?.count ? parseInt(countResult.rows[0].count, 10) : 0;

    const columns: ColumnStats[] = [];
    let heavySamplingCount = 0;

    for (const col of columnsResult.rows) {
      const colName = col.column_name;
      const dataType = col.data_type;
      const isNullable = col.is_nullable === "YES";

      const stat: ColumnStats = {
        name: colName,
        dataType,
        nullRate: 0,
      };

      if (heavySamplingCount >= MAX_COLUMNS_HEAVY_SAMPLING) {
        columns.push(stat);
        continue;
      }

      try {
        if (isNullable && totalRows > 0) {
          const nullResult = await queryWithTimeout<{ rows: Array<{ null_count: string }> }>(
            pool,
            `SELECT COUNT(*) FILTER (WHERE "${colName}" IS NULL) as null_count FROM "${schema}"."${table}"`,
            [],
            2000
          );
          if (nullResult?.rows[0]) {
            stat.nullRate = parseInt(nullResult.rows[0].null_count, 10) / totalRows;
          }
        }

        if (dataType.includes("timestamp") || dataType.includes("date")) {
          heavySamplingCount++;
          const rangeResult = await queryWithTimeout<{ rows: Array<{ min_val: string; max_val: string }> }>(
            pool,
            `SELECT MIN("${colName}")::text as min_val, MAX("${colName}")::text as max_val 
             FROM "${schema}"."${table}" WHERE "${colName}" IS NOT NULL`,
            [],
            3000
          );
          if (rangeResult?.rows[0]?.min_val) {
            stat.dateRange = {
              min: rangeResult.rows[0].min_val,
              max: rangeResult.rows[0].max_val,
            };
          }
        }

        else if (dataType.includes("int") || dataType.includes("numeric") || dataType.includes("decimal") || dataType === "double precision" || dataType === "real") {
          heavySamplingCount++;
          const numericResult = await queryWithTimeout<{ rows: Array<{ min_val: number; max_val: number; p50: number; p95: number }> }>(
            pool,
            `SELECT 
              MIN("${colName}")::float as min_val,
              MAX("${colName}")::float as max_val,
              PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "${colName}")::float as p50,
              PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY "${colName}")::float as p95
             FROM "${schema}"."${table}" WHERE "${colName}" IS NOT NULL`,
            [],
            3000
          );
          if (numericResult?.rows[0]) {
            stat.numericRange = {
              min: numericResult.rows[0].min_val,
              max: numericResult.rows[0].max_val,
              p50: numericResult.rows[0].p50,
              p95: numericResult.rows[0].p95,
            };
          }
        }

        else if (dataType.includes("character") || dataType.includes("text") || dataType === "USER-DEFINED") {
          heavySamplingCount++;
          const distinctCountResult = await queryWithTimeout<{ rows: Array<{ count: string }> }>(
            pool,
            `SELECT COUNT(DISTINCT "${colName}") as count FROM "${schema}"."${table}" WHERE "${colName}" IS NOT NULL`,
            [],
            2000
          );
          const distinctCount = distinctCountResult?.rows[0]?.count ? parseInt(distinctCountResult.rows[0].count, 10) : Infinity;

          if (distinctCount <= LOW_CARDINALITY_THRESHOLD) {
            const topValuesResult = await queryWithTimeout<{ rows: Array<{ val: string; count: string }> }>(
              pool,
              `SELECT "${colName}"::text as val, COUNT(*)::int as count 
               FROM "${schema}"."${table}" 
               WHERE "${colName}" IS NOT NULL 
               GROUP BY "${colName}" 
               ORDER BY count DESC 
               LIMIT 15`,
              [],
              3000
            );
            if (topValuesResult?.rows) {
              stat.topValues = topValuesResult.rows.map(r => ({
                value: r.val,
                count: parseInt(r.count, 10),
              }));
            }
          }
        }
      } catch {
        // Skip column on error
      }

      columns.push(stat);
    }

    const dictionary: TableDataDictionary = {
      database,
      schema,
      table,
      columns,
      fetchedAt: Date.now(),
      totalRows,
    };

    dataDictionaryCache.set(cacheKey, dictionary);
    return dictionary;
  } catch (err) {
    console.error(`Error fetching data dictionary for ${schema}.${table}:`, err);
    return null;
  }
}

export function formatDataDictionaryForPrompt(dictionary: TableDataDictionary): string {
  const lines: string[] = [];
  lines.push(`Table: ${dictionary.schema}.${dictionary.table} (${dictionary.totalRows.toLocaleString()} rows)`);
  lines.push("\nColumns:");

  for (const col of dictionary.columns) {
    let colLine = `  - ${col.name} (${col.dataType})`;

    if (col.nullRate > 0.5) {
      colLine += ` [${Math.round(col.nullRate * 100)}% null]`;
    }

    if (col.dateRange) {
      colLine += ` [range: ${col.dateRange.min.split("T")[0]} to ${col.dateRange.max.split("T")[0]}]`;
    }

    if (col.numericRange) {
      colLine += ` [range: ${col.numericRange.min} - ${col.numericRange.max}]`;
    }

    if (col.topValues && col.topValues.length > 0) {
      const topVals = col.topValues.slice(0, 8).map(v => `"${v.value}"`).join(", ");
      colLine += ` [values: ${topVals}${col.topValues.length > 8 ? ", ..." : ""}]`;
    }

    lines.push(colLine);
  }

  return lines.join("\n");
}

export function clearDataDictionaryCache(database?: string, schema?: string, table?: string): void {
  if (database && schema && table) {
    dataDictionaryCache.delete(getCacheKey(database, schema, table));
  } else {
    dataDictionaryCache.clear();
  }
}
