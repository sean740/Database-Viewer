interface CacheEntry<T> {
  data: T;
  timestamp: number;
  expiresAt: number;
}

interface DashboardCacheStore {
  [key: string]: CacheEntry<any>;
}

const cache: DashboardCacheStore = {};

const ONE_HOUR_MS = 60 * 60 * 1000;
const ONE_WEEK_MS = 7 * 24 * 60 * 60 * 1000;

export function getCacheKey(
  dashboardType: "marketing" | "operations",
  database: string,
  periodType: string,
  periodIdentifier?: string,
  zones?: string[]
): string {
  const zonesKey = zones && zones.length > 0 ? zones.sort().join(",") : "all";
  return `${dashboardType}:${database}:${periodType}:${periodIdentifier || "all"}:${zonesKey}`;
}

export function isCurrentPeriod(periodStart: string, periodType: "weekly" | "monthly"): boolean {
  const now = new Date();
  const periodStartDate = new Date(periodStart);
  
  if (periodType === "monthly") {
    return (
      periodStartDate.getFullYear() === now.getFullYear() &&
      periodStartDate.getMonth() === now.getMonth()
    );
  } else {
    const weekAgo = new Date(now.getTime() - 7 * 24 * 60 * 60 * 1000);
    return periodStartDate >= weekAgo;
  }
}

export function getCacheDuration(isCurrentPeriod: boolean): number {
  return isCurrentPeriod ? ONE_HOUR_MS : ONE_WEEK_MS;
}

export function getFromCache<T>(key: string): T | null {
  const entry = cache[key];
  if (!entry) {
    return null;
  }
  
  const now = Date.now();
  if (now > entry.expiresAt) {
    delete cache[key];
    return null;
  }
  
  return entry.data as T;
}

export function setInCache<T>(key: string, data: T, durationMs: number): void {
  const now = Date.now();
  cache[key] = {
    data,
    timestamp: now,
    expiresAt: now + durationMs,
  };
}

export function invalidateCache(pattern?: string): number {
  let count = 0;
  if (pattern) {
    for (const key of Object.keys(cache)) {
      if (key.includes(pattern)) {
        delete cache[key];
        count++;
      }
    }
  } else {
    count = Object.keys(cache).length;
    for (const key of Object.keys(cache)) {
      delete cache[key];
    }
  }
  return count;
}

export function getCacheStats(): { entries: number; keys: string[] } {
  return {
    entries: Object.keys(cache).length,
    keys: Object.keys(cache),
  };
}
