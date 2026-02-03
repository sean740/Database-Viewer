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
    name: "Emergency Rate",
    category: "Network Management",
    formula: "(COUNT(*) FROM vendor_emergencies WHERE bookings_count > 0) / Bookings Completed * 100",
    sourceTable: "vendor_emergencies",
    description: "Percentage of vendor emergencies (with bookings_count > 0) relative to bookings completed during the period",
    format: "percent",
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
    formula: "(COUNT(DISTINCT rescheduling_requests with vendor-related reasons) + COUNT(cancelled bookings with cancellation_reason 4,5,6,7,8,9,17,18)) / Bookings Completed * 100",
    sourceTable: "rescheduling_requests",
    sourceTables: ["rescheduling_requests", "bookings"],
    description: "Percentage of defects: vendor-related rescheduling requests plus cancellations with reason codes 4,5,6,7,8,9,17,18",
    format: "percent",
    getDrilldownQuery: (periodStart, periodEnd) => ({
      sql: `WITH all_defects AS (
              SELECT 'rescheduling' as defect_type, rr.id, rr.booking_id, rr.reason as reason_detail, rr.requested_at as event_date
              FROM public.rescheduling_requests rr
              WHERE rr.requested_at >= $1 AND rr.requested_at < $2
                AND rr.reason IN ('vendor_no_availabilities', 'vendor_emergency', 'vendor_no_show', 'overbooking')
              UNION ALL
              SELECT 'cancellation' as defect_type, cb.id, cb.booking_id, cb.cancel_reason_id::text as reason_detail, b.date_due as event_date
              FROM public.cancelled_bookings cb
              INNER JOIN public.bookings b ON b.id = cb.booking_id
              WHERE b.date_due >= $1 AND b.date_due < $2
                AND cb.cancel_reason_id IN (4,5,6,7,8,9,17,18)
            )
            SELECT DISTINCT ON (booking_id) defect_type, id, booking_id, reason_detail, event_date
            FROM all_defects
            ORDER BY booking_id, event_date DESC`,
      params: [periodStart, periodEnd],
      columns: ["defect_type", "id", "booking_id", "reason_detail", "event_date"],
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
    formula: "Not available (vendor_levels table does not exist)",
    sourceTable: "vendors",
    sourceTables: ["vendors"],
    description: "Count of active vendors at each dispatch level during the period (currently unavailable)",
    format: "number",
    getDrilldownQuery: () => ({
      sql: `SELECT 'No data' as info`,
      params: [],
      columns: ["info"],
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

// Helper to build booking-based zone filter (via address -> district)
function buildBookingZoneFilter(
  bookingAlias: string = 'b',
  paramOffset: number = 3,
  selectedZones: string[]
): { clause: string; params: string[] } {
  if (selectedZones.length === 0) {
    return { clause: '', params: [] };
  }
  const placeholders = selectedZones.map((_, i) => `$${paramOffset + i}`).join(', ');
  return {
    clause: `
      AND ${bookingAlias}.address_id IN (
        SELECT addr.id FROM public.addresses addr
        INNER JOIN public.districts d ON d.id = addr.district_id
        WHERE d.abbreviation IN (${placeholders})
      )
    `,
    params: selectedZones
  };
}

// Helper to build vendor-based zone filter (via vendors.washos_zone)
function buildVendorZoneFilter(
  vendorAlias: string = 'v',
  paramOffset: number = 3,
  selectedZones: string[]
): { clause: string; params: string[] } {
  if (selectedZones.length === 0) {
    return { clause: '', params: [] };
  }
  const placeholders = selectedZones.map((_, i) => `$${paramOffset + i}`).join(', ');
  return {
    clause: ` AND ${vendorAlias}.washos_zone IN (${placeholders})`,
    params: selectedZones
  };
}

export async function calculateOperationsMetrics(
  pool: Pool,
  periodStart: string,
  periodEnd: string,
  stripeMetrics?: { grossVolume: number; netVolume: number } | null,
  selectedZones: string[] = []
): Promise<OperationsPeriodMetrics["metrics"]> {
  
  // Bookings Completed (zone-filtered via address -> district)
  const bookingZoneFilter = buildBookingZoneFilter('b', 3, selectedZones);
  const bookingsCompletedResult = await pool.query(
    `SELECT COUNT(*) as count FROM public.bookings b
     WHERE b.date_due >= $1 AND b.date_due < $2 AND b.status = 'done'${bookingZoneFilter.clause}`,
    [periodStart, periodEnd, ...bookingZoneFilter.params]
  );
  const bookingsCompleted = parseInt(bookingsCompletedResult.rows[0]?.count || "0");

  // Bookings Created (for percentage calculations) - zone-filtered
  const bookingsCreatedZoneFilter = buildBookingZoneFilter('b', 3, selectedZones);
  const bookingsCreatedResult = await pool.query(
    `SELECT COUNT(*) as count FROM public.bookings b
     WHERE b.created_at >= $1 AND b.created_at < $2${bookingsCreatedZoneFilter.clause}`,
    [periodStart, periodEnd, ...bookingsCreatedZoneFilter.params]
  );
  const bookingsCreated = parseInt(bookingsCreatedResult.rows[0]?.count || "0");

  // Emergencies % (with bookings_count > 0) - zone-filtered via vendor
  const emergencyVendorZoneFilter = buildVendorZoneFilter('v', 3, selectedZones);
  const emergenciesResult = await pool.query(
    `SELECT COUNT(*) as count FROM public.vendor_emergencies ve
     INNER JOIN public.vendors v ON v.id = ve.vendor_id
     WHERE ve.bookings_count > 0 AND ve.created_at >= $1 AND ve.created_at < $2${emergencyVendorZoneFilter.clause}`,
    [periodStart, periodEnd, ...emergencyVendorZoneFilter.params]
  );
  const emergenciesCount = parseInt(emergenciesResult.rows[0]?.count || "0");
  const emergencies = bookingsCompleted > 0 ? (emergenciesCount / bookingsCompleted) * 100 : 0;

  // Delivery Rate: cancelled for vendor reasons vs completed - zone-filtered
  const cancellationZoneFilter = buildBookingZoneFilter('b', 3, selectedZones);
  const cancellationsResult = await pool.query(
    `SELECT COUNT(*) as count FROM public.cancelled_bookings cb
     INNER JOIN public.bookings b ON b.id = cb.booking_id
     WHERE b.date_due >= $1 AND b.date_due < $2
       AND cb.cancel_reason_id IN (4, 5, 6, 7, 8, 9, 17, 18)${cancellationZoneFilter.clause}`,
    [periodStart, periodEnd, ...cancellationZoneFilter.params]
  );
  const vendorCancellations = parseInt(cancellationsResult.rows[0]?.count || "0");
  const deliveryRate = bookingsCompleted > 0 
    ? ((bookingsCompleted - vendorCancellations) / bookingsCompleted) * 100 
    : 0;

  // Defect %: rescheduling requests with vendor-related reasons + cancellations with specific reasons
  // Zone-filtered via booking address
  const defectZoneFilter = buildBookingZoneFilter('b', 3, selectedZones);
  const defectsResult = await pool.query(
    `SELECT COUNT(DISTINCT booking_id) as count FROM (
       SELECT rr.booking_id FROM public.rescheduling_requests rr
       INNER JOIN public.bookings b ON b.id = rr.booking_id
       WHERE rr.requested_at >= $1 AND rr.requested_at < $2
         AND rr.reason IN ('vendor_no_availabilities', 'vendor_emergency', 'vendor_no_show', 'overbooking')${defectZoneFilter.clause}
       UNION ALL
       SELECT cb.booking_id FROM public.cancelled_bookings cb
       INNER JOIN public.bookings b ON b.id = cb.booking_id
       WHERE b.date_due >= $1 AND b.date_due < $2
         AND cb.cancel_reason_id IN (4,5,6,7,8,9,17,18)${defectZoneFilter.clause}
     ) all_defects`,
    [periodStart, periodEnd, ...defectZoneFilter.params]
  );
  const totalDefects = parseInt(defectsResult.rows[0]?.count || "0");
  const defectPercent = bookingsCompleted > 0 ? (totalDefects / bookingsCompleted) * 100 : 0;

  // Overbooked % - zone-filtered
  const overbookedZoneFilter = buildBookingZoneFilter('b', 3, selectedZones);
  const overbookedResult = await pool.query(
    `SELECT COUNT(*) as count FROM public.bookings b
     WHERE b.created_at >= $1 AND b.created_at < $2 AND b.overbooked = true${overbookedZoneFilter.clause}`,
    [periodStart, periodEnd, ...overbookedZoneFilter.params]
  );
  const overbooked = parseInt(overbookedResult.rows[0]?.count || "0");
  const overbookedPercent = bookingsCreated > 0 ? (overbooked / bookingsCreated) * 100 : 0;

  // Average Rating - zone-filtered via booking
  const ratingZoneFilter = buildBookingZoneFilter('b', 3, selectedZones);
  const ratingResult = await pool.query(
    `SELECT AVG(br.rating) as avg_rating FROM public.booking_ratings br
     INNER JOIN public.bookings b ON b.id = br.booking_id
     WHERE br.created_at >= $1 AND br.created_at < $2 AND br.rating IS NOT NULL${ratingZoneFilter.clause}`,
    [periodStart, periodEnd, ...ratingZoneFilter.params]
  );
  const avgRating = parseFloat(ratingResult.rows[0]?.avg_rating || "0");

  // Response Rate - zone-filtered
  const responseZoneFilter = buildBookingZoneFilter('b', 3, selectedZones);
  const ratingsCountResult = await pool.query(
    `SELECT COUNT(DISTINCT br.booking_id) as count 
     FROM public.booking_ratings br
     INNER JOIN public.bookings b ON b.id = br.booking_id
     WHERE b.date_due >= $1 AND b.date_due < $2 AND b.status = 'done'${responseZoneFilter.clause}`,
    [periodStart, periodEnd, ...responseZoneFilter.params]
  );
  const ratingsCount = parseInt(ratingsCountResult.rows[0]?.count || "0");
  const responseRate = bookingsCompleted > 0 ? (ratingsCount / bookingsCompleted) * 100 : 0;

  // Stripe Margin
  const stripeMargin = stripeMetrics && stripeMetrics.grossVolume > 0
    ? (stripeMetrics.netVolume / stripeMetrics.grossVolume) * 100
    : 0;

  // Active Vendors - zone-filtered via vendor
  const activeVendorZoneFilter = buildVendorZoneFilter('v', 3, selectedZones);
  const activeVendorsResult = await pool.query(
    `SELECT COUNT(DISTINCT b.vendor_id) as count FROM public.bookings b
     INNER JOIN public.vendors v ON v.id = b.vendor_id
     WHERE b.date_due >= $1 AND b.date_due < $2 AND b.status = 'done'${activeVendorZoneFilter.clause}`,
    [periodStart, periodEnd, ...activeVendorZoneFilter.params]
  );
  const activeVendors = parseInt(activeVendorsResult.rows[0]?.count || "0");

  // Vendor Level Counts - simplified (vendor_levels table doesn't exist)
  // Just return an empty object for now since the table doesn't exist
  const vendorLevelCounts: Record<string, number> = {};

  // New Vendors (activated during period with at least 1 booking ever) - zone-filtered
  const newVendorZoneFilter = buildVendorZoneFilter('v', 3, selectedZones);
  const newVendorsResult = await pool.query(
    `SELECT COUNT(DISTINCT v.id) as count
     FROM public.vendors v
     INNER JOIN public.vendor_onboardings vo ON vo.vendor_id = v.id
     WHERE vo.step = 'finished' 
       AND vo.starting_date >= $1 AND vo.starting_date < $2
       AND EXISTS (SELECT 1 FROM public.bookings b WHERE b.vendor_id = v.id AND b.status = 'done')${newVendorZoneFilter.clause}`,
    [periodStart, periodEnd, ...newVendorZoneFilter.params]
  );
  const newVendors = parseInt(newVendorsResult.rows[0]?.count || "0");

  // Dismissed Vendors - zone-filtered
  const dismissedVendorZoneFilter = buildVendorZoneFilter('v', 3, selectedZones);
  const dismissedVendorsResult = await pool.query(
    `SELECT COUNT(*) as count FROM public.vendors v
     WHERE v.status = 'dismissed' AND v.updated_at >= $1 AND v.updated_at < $2${dismissedVendorZoneFilter.clause}`,
    [periodStart, periodEnd, ...dismissedVendorZoneFilter.params]
  );
  const dismissedVendors = parseInt(dismissedVendorsResult.rows[0]?.count || "0");

  // Scheduled Hours (for non-dismissed vendors with job in last 30 days) - zone-filtered
  const scheduledHoursVendorZoneFilter = buildVendorZoneFilter('v', 2, selectedZones);
  const scheduledHoursResult = await pool.query(
    `SELECT SUM(vs.total_minutes_effective) as total_minutes
     FROM public.vendor_schedules vs
     INNER JOIN public.vendors v ON v.id = vs.vendor_id
     WHERE v.status != 'dismissed'
       AND EXISTS (
         SELECT 1 FROM public.bookings b 
         WHERE b.vendor_id = v.id 
           AND b.status = 'done' 
           AND b.date_due >= ($1::timestamp - interval '30 days')
           AND b.date_due < $1
       )${scheduledHoursVendorZoneFilter.clause}`,
    [periodEnd, ...scheduledHoursVendorZoneFilter.params]
  );
  const totalMinutesEffective = parseFloat(scheduledHoursResult.rows[0]?.total_minutes || "0");
  const scheduledHours = totalMinutesEffective / 60;

  // Utilization - zone-filtered
  const utilizationVendorZoneFilter = buildVendorZoneFilter('v', 2, selectedZones);
  const utilizationResult = await pool.query(
    `SELECT SUM(vs.total_minutes_worked) as worked, SUM(vs.total_minutes_effective) as effective
     FROM public.vendor_schedules vs
     INNER JOIN public.vendors v ON v.id = vs.vendor_id
     WHERE v.status != 'dismissed'
       AND EXISTS (
         SELECT 1 FROM public.bookings b 
         WHERE b.vendor_id = v.id 
           AND b.status = 'done' 
           AND b.date_due >= ($1::timestamp - interval '30 days')
           AND b.date_due < $1
       )${utilizationVendorZoneFilter.clause}`,
    [periodEnd, ...utilizationVendorZoneFilter.params]
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
