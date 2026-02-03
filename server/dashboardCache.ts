interface CacheEntry<T> {
  data: T;
  timestamp: number;
  expiresAt: number;
  lastAccessed: number;
}

interface DashboardCacheStore {
  [key: string]: CacheEntry<any>;
}

const cache: DashboardCacheStore = {};

const ONE_HOUR_MS = 60 * 60 * 1000;
const ONE_WEEK_MS = 7 * 24 * 60 * 60 * 1000;
const MAX_CACHE_ENTRIES = 100;

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

export function getCacheDuration(isCurrentPeriod: boolean): number {
  return isCurrentPeriod ? ONE_HOUR_MS : ONE_WEEK_MS;
}

function evictOldestEntry(): void {
  const keys = Object.keys(cache);
  if (keys.length === 0) return;

  let oldestKey = keys[0];
  let oldestTime = cache[oldestKey].lastAccessed;

  for (const key of keys) {
    if (cache[key].lastAccessed < oldestTime) {
      oldestTime = cache[key].lastAccessed;
      oldestKey = key;
    }
  }

  delete cache[oldestKey];
  console.log(`[Cache EVICT] Removed oldest entry: ${oldestKey}`);
}

function cleanupExpiredEntries(): void {
  const now = Date.now();
  const keys = Object.keys(cache);
  let removed = 0;

  for (const key of keys) {
    if (cache[key].expiresAt < now) {
      delete cache[key];
      removed++;
    }
  }

  if (removed > 0) {
    console.log(`[Cache CLEANUP] Removed ${removed} expired entries`);
  }
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
  
  entry.lastAccessed = now;
  return entry.data as T;
}

export function setInCache<T>(key: string, data: T, durationMs: number): void {
  cleanupExpiredEntries();
  
  if (Object.keys(cache).length >= MAX_CACHE_ENTRIES && !cache[key]) {
    evictOldestEntry();
  }
  
  const now = Date.now();
  cache[key] = {
    data,
    timestamp: now,
    expiresAt: now + durationMs,
    lastAccessed: now,
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
