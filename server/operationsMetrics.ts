import { Pool } from "pg";

export interface OperationsMetricSpec {
  id: string;
  name: string;
  category: "Network Management" | "Supply Management";
  formula: string;
  sourceTable: string;
  sourceTables?: string[];
  description: string;
  format?: "number" | "percent" | "decimal" | "rating";
  getDrilldownQuery: (periodStart: string, periodEnd: string) => {
    sql: string;
    params: unknown[];
    columns: string[];
  };
}

export const OPERATIONS_METRIC_SPECS: Record<string, OperationsMetricSpec> = {
  // Network Management Metrics
  bookingsCompleted: {
    id: "bookingsCompleted",
    name: "Bookings Completed",
    category: "Network Management",
    formula: "COUNT(*) FROM bookings WHERE date_due >= [period_start] AND date_due < [period_end] AND status = 'done'",
    sourceTable: "bookings",
    description: "Count of bookings with date_due in the period that have status = 'done'",
    format: "number",
    getDrilldownQuery: (periodStart, periodEnd) => ({
      sql: `SELECT id, user_id, vendor_id, created_at, status, price, margin, date_due 
            FROM public.bookings 
            WHERE date_due >= $1 AND date_due < $2 AND status = 'done'
            ORDER BY date_due DESC`,
      params: [periodStart, periodEnd],
      columns: ["id", "user_id", "vendor_id", "created_at", "status", "price", "margin", "date_due"],
    }),
  },

  emergencies: {
    id: "emergencies",
    name: "Emergencies",
    category: "Network Management",
    formula: "COUNT(*) FROM vendor_emergencies WHERE bookings_count > 0 AND created_at >= [period_start] AND created_at < [period_end]",
    sourceTable: "vendor_emergencies",
    description: "Count of vendor emergencies with bookings_count greater than 0 during the period",
    format: "number",
    getDrilldownQuery: (periodStart, periodEnd) => ({
      sql: `SELECT id, vendor_id, bookings_count, created_at, updated_at
            FROM public.vendor_emergencies 
            WHERE bookings_count > 0 AND created_at >= $1 AND created_at < $2
            ORDER BY created_at DESC`,
      params: [periodStart, periodEnd],
      columns: ["id", "vendor_id", "bookings_count", "created_at", "updated_at"],
    }),
  },

  deliveryRate: {
    id: "deliveryRate",
    name: "Delivery Rate",
    category: "Network Management",
    formula: "(Bookings Completed - Cancellations for reasons 4,5,6,7,8,9,17,18) / Bookings Completed * 100",
    sourceTable: "bookings",
    sourceTables: ["bookings", "cancelled_bookings"],
    description: "Percentage of completed bookings not cancelled for vendor-related reasons (4,5,6,7,8,9,17,18)",
    format: "percent",
    getDrilldownQuery: (periodStart, periodEnd) => ({
      sql: `SELECT b.id, b.vendor_id, b.status, b.date_due,
                   cb.cancel_reason_id,
                   CASE WHEN cb.cancel_reason_id IN (4,5,6,7,8,9,17,18) THEN 'Vendor Cancellation' ELSE 'Completed' END as delivery_status
            FROM public.bookings b
            LEFT JOIN public.cancelled_bookings cb ON cb.booking_id = b.id
            WHERE b.date_due >= $1 AND b.date_due < $2 AND b.status = 'done'
            ORDER BY b.date_due DESC`,
      params: [periodStart, periodEnd],
      columns: ["id", "vendor_id", "status", "date_due", "cancel_reason_id", "delivery_status"],
    }),
  },

  defectPercent: {
    id: "defectPercent",
    name: "Defect %",
    category: "Network Management",
    formula: "COUNT(DISTINCT rescheduling_requests with vendor-related reasons) / Bookings Completed * 100",
    sourceTable: "rescheduling_requests",
    sourceTables: ["rescheduling_requests", "bookings"],
    description: "Percentage of unique rescheduling requests with vendor_no_availabilities, vendor_emergency, vendor_no_show, or overbooking reasons",
    format: "percent",
    getDrilldownQuery: (periodStart, periodEnd) => ({
      sql: `SELECT id, booking_id, reason, accepted_at
            FROM public.rescheduling_requests 
            WHERE accepted_at >= $1 AND accepted_at < $2
              AND reason IN ('vendor_no_availabilities', 'vendor_emergency', 'vendor_no_show', 'overbooking')
            ORDER BY accepted_at DESC`,
      params: [periodStart, periodEnd],
      columns: ["id", "booking_id", "reason", "accepted_at"],
    }),
  },

  overbookedPercent: {
    id: "overbookedPercent",
    name: "Overbooked %",
    category: "Network Management",
    formula: "COUNT(bookings where overbooked = true) / Bookings Created * 100",
    sourceTable: "bookings",
    description: "Percentage of bookings created during the period with overbooked = true",
    format: "percent",
    getDrilldownQuery: (periodStart, periodEnd) => ({
      sql: `SELECT id, user_id, vendor_id, created_at, status, overbooked, date_due
            FROM public.bookings 
            WHERE created_at >= $1 AND created_at < $2 AND overbooked = true
            ORDER BY created_at DESC`,
      params: [periodStart, periodEnd],
      columns: ["id", "user_id", "vendor_id", "created_at", "status", "overbooked", "date_due"],
    }),
  },

  avgRating: {
    id: "avgRating",
    name: "Rating",
    category: "Network Management",
    formula: "AVG(rating) FROM booking_ratings WHERE created_at >= [period_start] AND created_at < [period_end]",
    sourceTable: "booking_ratings",
    description: "Average rating from all booking ratings received during the period",
    format: "rating",
    getDrilldownQuery: (periodStart, periodEnd) => ({
      sql: `SELECT id, booking_id, rating, comment, created_at
            FROM public.booking_ratings 
            WHERE created_at >= $1 AND created_at < $2 AND rating IS NOT NULL
            ORDER BY created_at DESC`,
      params: [periodStart, periodEnd],
      columns: ["id", "booking_id", "rating", "comment", "created_at"],
    }),
  },

  responseRate: {
    id: "responseRate",
    name: "Response Rate",
    category: "Network Management",
    formula: "COUNT(bookings with rating) / Bookings Completed * 100",
    sourceTable: "booking_ratings",
    sourceTables: ["booking_ratings", "bookings"],
    description: "Percentage of completed bookings that received a rating",
    format: "percent",
    getDrilldownQuery: (periodStart, periodEnd) => ({
      sql: `SELECT b.id, b.user_id, b.vendor_id, b.date_due, b.status,
                   br.rating,
                   CASE WHEN br.id IS NOT NULL THEN 'Has Rating' ELSE 'No Rating' END as rating_status
            FROM public.bookings b
            LEFT JOIN public.booking_ratings br ON br.booking_id = b.id
            WHERE b.date_due >= $1 AND b.date_due < $2 AND b.status = 'done'
            ORDER BY rating_status, b.date_due DESC`,
      params: [periodStart, periodEnd],
      columns: ["id", "user_id", "vendor_id", "date_due", "status", "rating", "rating_status"],
    }),
  },

  stripeMargin: {
    id: "stripeMargin",
    name: "Margin",
    category: "Network Management",
    formula: "Stripe Net Volume / Stripe Gross Volume * 100",
    sourceTable: "stripe",
    description: "Net Volume as a percentage of Gross Volume from Stripe (platform margin)",
    format: "percent",
    getDrilldownQuery: (periodStart, periodEnd) => ({
      sql: `SELECT 'See Stripe Dashboard for details' as note`,
      params: [],
      columns: ["note"],
    }),
  },

  // Supply Management Metrics
  activeVendors: {
    id: "activeVendors",
    name: "Active Vendors",
    category: "Supply Management",
    formula: "COUNT(DISTINCT vendor_id) FROM bookings WHERE status = 'done' AND date_due >= [period_start] AND date_due < [period_end]",
    sourceTable: "bookings",
    description: "Count of unique vendors that completed at least one booking during the period",
    format: "number",
    getDrilldownQuery: (periodStart, periodEnd) => ({
      sql: `SELECT DISTINCT v.id, v.first_name, v.last_name, v.email, v.vendor_level_id,
                   COUNT(b.id) as bookings_completed
            FROM public.vendors v
            INNER JOIN public.bookings b ON b.vendor_id = v.id
            WHERE b.date_due >= $1 AND b.date_due < $2 AND b.status = 'done'
            GROUP BY v.id, v.first_name, v.last_name, v.email, v.vendor_level_id
            ORDER BY bookings_completed DESC`,
      params: [periodStart, periodEnd],
      columns: ["id", "first_name", "last_name", "email", "vendor_level_id", "bookings_completed"],
    }),
  },

  vendorLevelCounts: {
    id: "vendorLevelCounts",
    name: "Vendor Level Counts",
    category: "Supply Management",
    formula: "GROUP BY vendor_level_id for active vendors during the period",
    sourceTable: "vendors",
    sourceTables: ["vendors", "vendor_levels", "bookings"],
    description: "Count of active vendors at each dispatch level during the period",
    format: "number",
    getDrilldownQuery: (periodStart, periodEnd) => ({
      sql: `SELECT vl.id as level_id, vl.name as level_name, COUNT(DISTINCT v.id) as vendor_count
            FROM public.vendor_levels vl
            LEFT JOIN public.vendors v ON v.vendor_level_id = vl.id
            LEFT JOIN public.bookings b ON b.vendor_id = v.id AND b.date_due >= $1 AND b.date_due < $2 AND b.status = 'done'
            WHERE b.id IS NOT NULL
            GROUP BY vl.id, vl.name
            ORDER BY vl.id`,
      params: [periodStart, periodEnd],
      columns: ["level_id", "level_name", "vendor_count"],
    }),
  },

  newVendors: {
    id: "newVendors",
    name: "New Vendors",
    category: "Supply Management",
    formula: "COUNT vendors with starting_date in period, step='finished', and at least 1 completed booking",
    sourceTable: "vendor_onboardings",
    sourceTables: ["vendor_onboardings", "vendors", "bookings"],
    description: "Vendors activated during the period (step='finished', starting_date in period) who have completed at least 1 booking",
    format: "number",
    getDrilldownQuery: (periodStart, periodEnd) => ({
      sql: `SELECT DISTINCT v.id, v.first_name, v.last_name, v.email, vo.starting_date, vo.step
            FROM public.vendors v
            INNER JOIN public.vendor_onboardings vo ON vo.vendor_id = v.id
            WHERE vo.step = 'finished' 
              AND vo.starting_date >= $1 AND vo.starting_date < $2
              AND EXISTS (SELECT 1 FROM public.bookings b WHERE b.vendor_id = v.id AND b.status = 'done')
            ORDER BY vo.starting_date DESC`,
      params: [periodStart, periodEnd],
      columns: ["id", "first_name", "last_name", "email", "starting_date", "step"],
    }),
  },

  dismissedVendors: {
    id: "dismissedVendors",
    name: "Dismissed Vendors",
    category: "Supply Management",
    formula: "COUNT vendors with status='dismissed' updated during the period",
    sourceTable: "vendors",
    description: "Vendors with status 'dismissed' whose record was updated during the period",
    format: "number",
    getDrilldownQuery: (periodStart, periodEnd) => ({
      sql: `SELECT id, first_name, last_name, email, status, updated_at
            FROM public.vendors 
            WHERE status = 'dismissed' AND updated_at >= $1 AND updated_at < $2
            ORDER BY updated_at DESC`,
      params: [periodStart, periodEnd],
      columns: ["id", "first_name", "last_name", "email", "status", "updated_at"],
    }),
  },

  scheduledHours: {
    id: "scheduledHours",
    name: "Scheduled Hours",
    category: "Supply Management",
    formula: "SUM(total_minutes_effective) / 60 for non-dismissed vendors with a booking in last 30 days",
    sourceTable: "vendor_schedules",
    sourceTables: ["vendor_schedules", "vendors", "bookings"],
    description: "Total scheduled hours from vendor_schedules for non-dismissed vendors who completed at least one job in the last 30 days",
    format: "number",
    getDrilldownQuery: (periodStart, periodEnd) => ({
      sql: `SELECT vs.vendor_id, v.first_name, v.last_name, v.status, 
                   vs.total_minutes_effective, ROUND(vs.total_minutes_effective / 60.0, 2) as hours_effective
            FROM public.vendor_schedules vs
            INNER JOIN public.vendors v ON v.id = vs.vendor_id
            WHERE v.status != 'dismissed'
              AND EXISTS (
                SELECT 1 FROM public.bookings b 
                WHERE b.vendor_id = v.id 
                  AND b.status = 'done' 
                  AND b.date_due >= ($2::timestamp - interval '30 days')
                  AND b.date_due < $2
              )
            ORDER BY vs.total_minutes_effective DESC`,
      params: [periodStart, periodEnd],
      columns: ["vendor_id", "first_name", "last_name", "status", "total_minutes_effective", "hours_effective"],
    }),
  },

  utilization: {
    id: "utilization",
    name: "Utilization",
    category: "Supply Management",
    formula: "SUM(total_minutes_worked) / SUM(total_minutes_effective) * 100",
    sourceTable: "vendor_schedules",
    description: "Total minutes worked divided by total minutes effective (scheduled), as a percentage",
    format: "percent",
    getDrilldownQuery: (periodStart, periodEnd) => ({
      sql: `SELECT vs.vendor_id, v.first_name, v.last_name,
                   vs.total_minutes_worked, vs.total_minutes_effective,
                   CASE WHEN vs.total_minutes_effective > 0 
                        THEN ROUND((vs.total_minutes_worked::numeric / vs.total_minutes_effective) * 100, 2) 
                        ELSE 0 END as utilization_pct
            FROM public.vendor_schedules vs
            INNER JOIN public.vendors v ON v.id = vs.vendor_id
            WHERE v.status != 'dismissed'
              AND EXISTS (
                SELECT 1 FROM public.bookings b 
                WHERE b.vendor_id = v.id 
                  AND b.status = 'done' 
                  AND b.date_due >= ($2::timestamp - interval '30 days')
                  AND b.date_due < $2
              )
            ORDER BY utilization_pct DESC`,
      params: [periodStart, periodEnd],
      columns: ["vendor_id", "first_name", "last_name", "total_minutes_worked", "total_minutes_effective", "utilization_pct"],
    }),
  },
};

export function getOperationsMetricSpec(metricId: string): OperationsMetricSpec | undefined {
  return OPERATIONS_METRIC_SPECS[metricId];
}

export function getAllOperationsMetricSpecs(): OperationsMetricSpec[] {
  return Object.values(OPERATIONS_METRIC_SPECS);
}

export function getOperationsMetricCategories(): string[] {
  return ["Network Management", "Supply Management"];
}

export interface OperationsPeriodMetrics {
  periodLabel: string;
  periodStart: string;
  periodEnd: string;
  periodType: "weekly" | "monthly";
  metrics: {
    bookingsCompleted: number;
    emergencies: number;
    deliveryRate: number;
    defectPercent: number;
    overbookedPercent: number;
    avgRating: number;
    responseRate: number;
    stripeMargin: number;
    activeVendors: number;
    vendorLevelCounts: Record<string, number>;
    newVendors: number;
    dismissedVendors: number;
    scheduledHours: number;
    utilization: number;
  };
  variance?: Record<string, number | null>;
}

export async function calculateOperationsMetrics(
  pool: Pool,
  periodStart: string,
  periodEnd: string,
  stripeMetrics?: { grossVolume: number; netVolume: number } | null
): Promise<OperationsPeriodMetrics["metrics"]> {
  
  // Bookings Completed
  const bookingsCompletedResult = await pool.query(
    `SELECT COUNT(*) as count FROM public.bookings 
     WHERE date_due >= $1 AND date_due < $2 AND status = 'done'`,
    [periodStart, periodEnd]
  );
  const bookingsCompleted = parseInt(bookingsCompletedResult.rows[0]?.count || "0");

  // Bookings Created (for percentage calculations)
  const bookingsCreatedResult = await pool.query(
    `SELECT COUNT(*) as count FROM public.bookings 
     WHERE created_at >= $1 AND created_at < $2`,
    [periodStart, periodEnd]
  );
  const bookingsCreated = parseInt(bookingsCreatedResult.rows[0]?.count || "0");

  // Emergencies (with bookings_count > 0)
  const emergenciesResult = await pool.query(
    `SELECT COUNT(*) as count FROM public.vendor_emergencies 
     WHERE bookings_count > 0 AND created_at >= $1 AND created_at < $2`,
    [periodStart, periodEnd]
  );
  const emergencies = parseInt(emergenciesResult.rows[0]?.count || "0");

  // Delivery Rate: cancelled for vendor reasons vs completed
  const cancellationsResult = await pool.query(
    `SELECT COUNT(*) as count FROM public.cancelled_bookings cb
     INNER JOIN public.bookings b ON b.id = cb.booking_id
     WHERE b.date_due >= $1 AND b.date_due < $2
       AND cb.cancel_reason_id IN (4, 5, 6, 7, 8, 9, 17, 18)`,
    [periodStart, periodEnd]
  );
  const vendorCancellations = parseInt(cancellationsResult.rows[0]?.count || "0");
  const deliveryRate = bookingsCompleted > 0 
    ? ((bookingsCompleted - vendorCancellations) / bookingsCompleted) * 100 
    : 0;

  // Defect %: unique rescheduling requests with vendor-related reasons
  const defectsResult = await pool.query(
    `SELECT COUNT(DISTINCT id) as count FROM public.rescheduling_requests 
     WHERE accepted_at >= $1 AND accepted_at < $2
       AND reason IN ('vendor_no_availabilities', 'vendor_emergency', 'vendor_no_show', 'overbooking')`,
    [periodStart, periodEnd]
  );
  const defects = parseInt(defectsResult.rows[0]?.count || "0");
  const defectPercent = bookingsCompleted > 0 ? (defects / bookingsCompleted) * 100 : 0;

  // Overbooked %
  const overbookedResult = await pool.query(
    `SELECT COUNT(*) as count FROM public.bookings 
     WHERE created_at >= $1 AND created_at < $2 AND overbooked = true`,
    [periodStart, periodEnd]
  );
  const overbooked = parseInt(overbookedResult.rows[0]?.count || "0");
  const overbookedPercent = bookingsCreated > 0 ? (overbooked / bookingsCreated) * 100 : 0;

  // Average Rating
  const ratingResult = await pool.query(
    `SELECT AVG(rating) as avg_rating FROM public.booking_ratings 
     WHERE created_at >= $1 AND created_at < $2 AND rating IS NOT NULL`,
    [periodStart, periodEnd]
  );
  const avgRating = parseFloat(ratingResult.rows[0]?.avg_rating || "0");

  // Response Rate
  const ratingsCountResult = await pool.query(
    `SELECT COUNT(DISTINCT br.booking_id) as count 
     FROM public.booking_ratings br
     INNER JOIN public.bookings b ON b.id = br.booking_id
     WHERE b.date_due >= $1 AND b.date_due < $2 AND b.status = 'done'`,
    [periodStart, periodEnd]
  );
  const ratingsCount = parseInt(ratingsCountResult.rows[0]?.count || "0");
  const responseRate = bookingsCompleted > 0 ? (ratingsCount / bookingsCompleted) * 100 : 0;

  // Stripe Margin
  const stripeMargin = stripeMetrics && stripeMetrics.grossVolume > 0
    ? (stripeMetrics.netVolume / stripeMetrics.grossVolume) * 100
    : 0;

  // Active Vendors
  const activeVendorsResult = await pool.query(
    `SELECT COUNT(DISTINCT vendor_id) as count FROM public.bookings 
     WHERE date_due >= $1 AND date_due < $2 AND status = 'done'`,
    [periodStart, periodEnd]
  );
  const activeVendors = parseInt(activeVendorsResult.rows[0]?.count || "0");

  // Vendor Level Counts
  const vendorLevelCountsResult = await pool.query(
    `SELECT vl.name as level_name, COUNT(DISTINCT v.id) as vendor_count
     FROM public.vendor_levels vl
     LEFT JOIN public.vendors v ON v.vendor_level_id = vl.id
     LEFT JOIN public.bookings b ON b.vendor_id = v.id AND b.date_due >= $1 AND b.date_due < $2 AND b.status = 'done'
     WHERE b.id IS NOT NULL
     GROUP BY vl.id, vl.name
     ORDER BY vl.id`,
    [periodStart, periodEnd]
  );
  const vendorLevelCounts: Record<string, number> = {};
  for (const row of vendorLevelCountsResult.rows) {
    vendorLevelCounts[row.level_name || `Level ${row.level_id}`] = parseInt(row.vendor_count || "0");
  }

  // New Vendors (activated during period with at least 1 booking ever)
  const newVendorsResult = await pool.query(
    `SELECT COUNT(DISTINCT v.id) as count
     FROM public.vendors v
     INNER JOIN public.vendor_onboardings vo ON vo.vendor_id = v.id
     WHERE vo.step = 'finished' 
       AND vo.starting_date >= $1 AND vo.starting_date < $2
       AND EXISTS (SELECT 1 FROM public.bookings b WHERE b.vendor_id = v.id AND b.status = 'done')`,
    [periodStart, periodEnd]
  );
  const newVendors = parseInt(newVendorsResult.rows[0]?.count || "0");

  // Dismissed Vendors
  const dismissedVendorsResult = await pool.query(
    `SELECT COUNT(*) as count FROM public.vendors 
     WHERE status = 'dismissed' AND updated_at >= $1 AND updated_at < $2`,
    [periodStart, periodEnd]
  );
  const dismissedVendors = parseInt(dismissedVendorsResult.rows[0]?.count || "0");

  // Scheduled Hours (for non-dismissed vendors with job in last 30 days)
  const scheduledHoursResult = await pool.query(
    `SELECT SUM(vs.total_minutes_effective) as total_minutes
     FROM public.vendor_schedules vs
     INNER JOIN public.vendors v ON v.id = vs.vendor_id
     WHERE v.status != 'dismissed'
       AND EXISTS (
         SELECT 1 FROM public.bookings b 
         WHERE b.vendor_id = v.id 
           AND b.status = 'done' 
           AND b.date_due >= ($2::timestamp - interval '30 days')
           AND b.date_due < $2
       )`,
    [periodStart, periodEnd]
  );
  const totalMinutesEffective = parseFloat(scheduledHoursResult.rows[0]?.total_minutes || "0");
  const scheduledHours = totalMinutesEffective / 60;

  // Utilization
  const utilizationResult = await pool.query(
    `SELECT SUM(vs.total_minutes_worked) as worked, SUM(vs.total_minutes_effective) as effective
     FROM public.vendor_schedules vs
     INNER JOIN public.vendors v ON v.id = vs.vendor_id
     WHERE v.status != 'dismissed'
       AND EXISTS (
         SELECT 1 FROM public.bookings b 
         WHERE b.vendor_id = v.id 
           AND b.status = 'done' 
           AND b.date_due >= ($2::timestamp - interval '30 days')
           AND b.date_due < $2
       )`,
    [periodStart, periodEnd]
  );
  const totalMinutesWorked = parseFloat(utilizationResult.rows[0]?.worked || "0");
  const totalMinutesEffectiveUtil = parseFloat(utilizationResult.rows[0]?.effective || "0");
  const utilization = totalMinutesEffectiveUtil > 0 
    ? (totalMinutesWorked / totalMinutesEffectiveUtil) * 100 
    : 0;

  return {
    bookingsCompleted,
    emergencies,
    deliveryRate,
    defectPercent,
    overbookedPercent,
    avgRating,
    responseRate,
    stripeMargin,
    activeVendors,
    vendorLevelCounts,
    newVendors,
    dismissedVendors,
    scheduledHours,
    utilization,
  };
}

export function calculateOperationsVariance(
  current: OperationsPeriodMetrics["metrics"],
  previous: OperationsPeriodMetrics["metrics"] | null
): Record<string, number | null> {
  if (!previous) {
    return {};
  }

  const percentMetrics = ["deliveryRate", "defectPercent", "overbookedPercent", "responseRate", "stripeMargin", "utilization"];
  
  const variance: Record<string, number | null> = {};
  
  for (const key of Object.keys(current) as (keyof typeof current)[]) {
    if (key === "vendorLevelCounts") continue;
    
    const currVal = current[key] as number;
    const prevVal = previous[key] as number;
    
    if (percentMetrics.includes(key)) {
      variance[key] = currVal - prevVal;
    } else {
      variance[key] = prevVal !== 0 ? ((currVal - prevVal) / prevVal) * 100 : (currVal > 0 ? 100 : 0);
    }
  }

  return variance;
}
