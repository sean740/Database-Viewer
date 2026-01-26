import { Pool } from "pg";

export interface MetricSpec {
  id: string;
  name: string;
  category: "Bookings" | "Revenue" | "Users" | "Membership";
  formula: string;
  sourceTable: string;
  sourceTables?: string[];
  description: string;
  getDrilldownQuery: (weekStart: string, weekEnd: string) => {
    sql: string;
    params: unknown[];
    columns: string[];
  };
  subSources?: {
    id: string;
    name: string;
    getDrilldownQuery: (weekStart: string, weekEnd: string) => {
      sql: string;
      params: unknown[];
      columns: string[];
    };
  }[];
}

export const METRIC_SPECS: Record<string, MetricSpec> = {
  bookingsCreated: {
    id: "bookingsCreated",
    name: "Bookings Created",
    category: "Bookings",
    formula: "COUNT(*) FROM bookings WHERE created_at >= [week_start] AND created_at < [week_end]",
    sourceTable: "bookings",
    description: "Count of all bookings where created_at falls within the week (Monday 00:00:00 PST to next Monday 00:00:00 PST, exclusive)",
    getDrilldownQuery: (weekStart, weekEnd) => ({
      sql: `SELECT id, user_id, created_at, status, price, date_due 
            FROM public.bookings 
            WHERE created_at >= $1 AND created_at < $2
            ORDER BY created_at DESC`,
      params: [weekStart, weekEnd],
      columns: ["id", "user_id", "created_at", "status", "price", "date_due"],
    }),
  },

  bookingsDue: {
    id: "bookingsDue",
    name: "Bookings Due",
    category: "Bookings",
    formula: "COUNT(*) FROM bookings WHERE date_due >= [week_start] AND date_due < [week_end]",
    sourceTable: "bookings",
    description: "Count of all bookings scheduled to be completed (date_due) within the week",
    getDrilldownQuery: (weekStart, weekEnd) => ({
      sql: `SELECT id, user_id, created_at, status, price, date_due 
            FROM public.bookings 
            WHERE date_due >= $1 AND date_due < $2
            ORDER BY date_due DESC`,
      params: [weekStart, weekEnd],
      columns: ["id", "user_id", "created_at", "status", "price", "date_due"],
    }),
  },

  bookingsCompleted: {
    id: "bookingsCompleted",
    name: "Bookings Completed",
    category: "Bookings",
    formula: "COUNT(*) FROM bookings WHERE date_due >= [week_start] AND date_due < [week_end] AND status = 'done'",
    sourceTable: "bookings",
    description: "Count of bookings with date_due in the week that have status = 'done'",
    getDrilldownQuery: (weekStart, weekEnd) => ({
      sql: `SELECT id, user_id, created_at, status, price, margin, date_due 
            FROM public.bookings 
            WHERE date_due >= $1 AND date_due < $2 AND status = 'done'
            ORDER BY date_due DESC`,
      params: [weekStart, weekEnd],
      columns: ["id", "user_id", "created_at", "status", "price", "margin", "date_due"],
    }),
  },

  avgPerDay: {
    id: "avgPerDay",
    name: "Avg Bookings Per Day",
    category: "Bookings",
    formula: "(Bookings Completed) / 7",
    sourceTable: "bookings",
    description: "The number of completed bookings divided by 7 days in the week",
    getDrilldownQuery: (weekStart, weekEnd) => ({
      sql: `SELECT id, user_id, created_at, status, price, margin, date_due 
            FROM public.bookings 
            WHERE date_due >= $1 AND date_due < $2 AND status = 'done'
            ORDER BY date_due DESC`,
      params: [weekStart, weekEnd],
      columns: ["id", "user_id", "created_at", "status", "price", "margin", "date_due"],
    }),
  },

  conversion: {
    id: "conversion",
    name: "Conversion (Done/Due)",
    category: "Bookings",
    formula: "(Bookings Completed / Bookings Due) * 100",
    sourceTable: "bookings",
    description: "Percentage of bookings due in the week that were completed (status = 'done')",
    getDrilldownQuery: (weekStart, weekEnd) => ({
      sql: `SELECT id, user_id, created_at, status, price, date_due,
                   CASE WHEN status = 'done' THEN 'Completed' ELSE 'Not Completed' END as completion_status
            FROM public.bookings 
            WHERE date_due >= $1 AND date_due < $2
            ORDER BY status, date_due DESC`,
      params: [weekStart, weekEnd],
      columns: ["id", "user_id", "created_at", "status", "price", "date_due", "completion_status"],
    }),
  },

  avgBookingPrice: {
    id: "avgBookingPrice",
    name: "Avg Booking Price",
    category: "Revenue",
    formula: "AVG(price) FROM bookings WHERE date_due >= [week_start] AND date_due < [week_end] AND status = 'done'",
    sourceTable: "bookings",
    description: "Average price of completed bookings (status = 'done') with date_due in the week",
    getDrilldownQuery: (weekStart, weekEnd) => ({
      sql: `SELECT id, user_id, price, margin, date_due, status
            FROM public.bookings 
            WHERE date_due >= $1 AND date_due < $2 AND status = 'done'
            ORDER BY price DESC`,
      params: [weekStart, weekEnd],
      columns: ["id", "user_id", "price", "margin", "date_due", "status"],
    }),
  },

  totalRevenue: {
    id: "totalRevenue",
    name: "Total Revenue",
    category: "Revenue",
    formula: "Booking Revenue + Subscription Fees + Customer Fees + Tips + Credit Packs - Refunds - Stripe Fees",
    sourceTable: "bookings",
    sourceTables: ["bookings", "subscription_invoices", "subscriptions", "customer_fees", "booking_tips", "user_credits_transactions", "credits_packs", "booking_refunds"],
    description: `Total Revenue is calculated as:
- Booking Revenue: SUM(price) from completed bookings (status='done', date_due in week)
- Subscription Fees: SUM of fees from paid subscription_invoices (updated_at in week), where price_plan_id 11=$96, 10=$9.99, others=$0
- Customer Fees: SUM(amount) from customer_fees (created_at in week)
- Tips: SUM(tip_amount) from booking_tips (created_at in week)
- Credit Packs: SUM(pay_amount) from credits_packs joined with user_credits_transactions (type_id=16, created_at in week)
- Refunds: SUM(total) from booking_refunds (created_at in week) - SUBTRACTED
- Stripe Fees: SUM(stripe_fee) from completed bookings - SUBTRACTED`,
    getDrilldownQuery: (weekStart, weekEnd) => ({
      sql: `SELECT id, user_id, price, margin, stripe_fee, date_due, status
            FROM public.bookings 
            WHERE date_due >= $1 AND date_due < $2 AND status = 'done'
            ORDER BY price DESC`,
      params: [weekStart, weekEnd],
      columns: ["id", "user_id", "price", "margin", "stripe_fee", "date_due", "status"],
    }),
    subSources: [
      {
        id: "bookingRevenue",
        name: "Booking Revenue",
        getDrilldownQuery: (weekStart, weekEnd) => ({
          sql: `SELECT id, user_id, price, margin, stripe_fee, date_due, status
                FROM public.bookings 
                WHERE date_due >= $1 AND date_due < $2 AND status = 'done'
                ORDER BY price DESC`,
          params: [weekStart, weekEnd],
          columns: ["id", "user_id", "price", "margin", "stripe_fee", "date_due", "status"],
        }),
      },
      {
        id: "subscriptionFees",
        name: "Subscription Fees",
        getDrilldownQuery: (weekStart, weekEnd) => ({
          sql: `SELECT DISTINCT ON (si.subscription_id) si.id, si.subscription_id, s.user_id, s.price_plan_id, si.status, s.status as subscription_status, si.updated_at,
                       CASE 
                         WHEN s.price_plan_id = 11 THEN 96.00
                         WHEN s.price_plan_id = 10 THEN 9.99
                         ELSE 0
                       END as fee_amount
                FROM public.subscription_invoices si
                INNER JOIN public.subscriptions s ON s.id = si.subscription_id
                WHERE si.updated_at >= $1 AND si.updated_at < $2
                  AND si.status = 'paid'
                  AND s.status != 'trialing'
                ORDER BY si.subscription_id, si.updated_at DESC`,
          params: [weekStart, weekEnd],
          columns: ["id", "subscription_id", "user_id", "price_plan_id", "status", "subscription_status", "updated_at", "fee_amount"],
        }),
      },
      {
        id: "customerFees",
        name: "Customer Fees",
        getDrilldownQuery: (weekStart, weekEnd) => ({
          sql: `SELECT id, amount, charge_id, waived, created_at
                FROM public.customer_fees
                WHERE created_at >= $1 AND created_at < $2
                  AND (waived IS NULL OR waived != true)
                  AND charge_id IS NOT NULL AND charge_id != ''
                ORDER BY created_at DESC`,
          params: [weekStart, weekEnd],
          columns: ["id", "amount", "charge_id", "waived", "created_at"],
        }),
      },
      {
        id: "tips",
        name: "Tips",
        getDrilldownQuery: (weekStart, weekEnd) => ({
          sql: `SELECT id, booking_id, tip_amount, vendor_amount, (tip_amount - vendor_amount) as profit, created_at
                FROM public.booking_tips
                WHERE created_at >= $1 AND created_at < $2
                ORDER BY created_at DESC`,
          params: [weekStart, weekEnd],
          columns: ["id", "booking_id", "tip_amount", "vendor_amount", "profit", "created_at"],
        }),
      },
      {
        id: "creditPacks",
        name: "Credit Packs",
        getDrilldownQuery: (weekStart, weekEnd) => ({
          sql: `SELECT DISTINCT ON (uct.id) uct.id, uct.user_id, uct.amount as credits_received, cp.pay_amount, uct.created_at
                FROM public.user_credits_transactions uct
                INNER JOIN public.credits_packs cp ON uct.amount = cp.get_amount
                WHERE uct.created_at >= $1 AND uct.created_at < $2
                  AND uct.user_credits_transaction_type_id = 16
                ORDER BY uct.id, uct.created_at DESC`,
          params: [weekStart, weekEnd],
          columns: ["id", "user_id", "credits_received", "pay_amount", "created_at"],
        }),
      },
      {
        id: "refunds",
        name: "Refunds (Subtracted)",
        getDrilldownQuery: (weekStart, weekEnd) => ({
          sql: `SELECT id, booking_id, total, created_at
                FROM public.booking_refunds
                WHERE created_at >= $1 AND created_at < $2
                ORDER BY created_at DESC`,
          params: [weekStart, weekEnd],
          columns: ["id", "booking_id", "total", "created_at"],
        }),
      },
      {
        id: "stripeFees",
        name: "Stripe Fees (Subtracted)",
        getDrilldownQuery: (weekStart, weekEnd) => ({
          sql: `SELECT id, user_id, price, stripe_fee, date_due
                FROM public.bookings
                WHERE date_due >= $1 AND date_due < $2 AND status = 'done' AND stripe_fee > 0
                ORDER BY stripe_fee DESC`,
          params: [weekStart, weekEnd],
          columns: ["id", "user_id", "price", "stripe_fee", "date_due"],
        }),
      },
    ],
  },

  totalProfit: {
    id: "totalProfit",
    name: "Gross Profit",
    category: "Revenue",
    formula: "Booking Margin + Subscription Fees + Customer Fees + Tip Profit - Refunds",
    sourceTable: "bookings",
    sourceTables: ["bookings", "subscription_invoices", "customer_fees", "booking_tips", "booking_refunds"],
    description: `Gross Profit is calculated as:
- Booking Margin: SUM(margin) from completed bookings (status='done', date_due in week)
- Subscription Fees: 100% margin (price_plan_id 11=$96, 10=$9.99)
- Customer Fees: 100% margin - SUM(amount) from customer_fees
- Tip Profit: SUM(tip_amount - vendor_amount) from booking_tips
- Refunds: SUM(total) from booking_refunds - SUBTRACTED`,
    getDrilldownQuery: (weekStart, weekEnd) => ({
      sql: `SELECT id, user_id, price, margin, date_due, status
            FROM public.bookings 
            WHERE date_due >= $1 AND date_due < $2 AND status = 'done'
            ORDER BY margin DESC`,
      params: [weekStart, weekEnd],
      columns: ["id", "user_id", "price", "margin", "date_due", "status"],
    }),
    subSources: [
      {
        id: "bookingMargin",
        name: "Booking Margin",
        getDrilldownQuery: (weekStart, weekEnd) => ({
          sql: `SELECT id, user_id, price, margin, date_due, status
                FROM public.bookings 
                WHERE date_due >= $1 AND date_due < $2 AND status = 'done'
                ORDER BY margin DESC`,
          params: [weekStart, weekEnd],
          columns: ["id", "user_id", "price", "margin", "date_due", "status"],
        }),
      },
      {
        id: "subscriptionFees",
        name: "Subscription Fees (100% margin)",
        getDrilldownQuery: (weekStart, weekEnd) => ({
          sql: `SELECT DISTINCT ON (si.subscription_id) si.id, si.subscription_id, s.user_id, s.price_plan_id, si.status, s.status as subscription_status, si.updated_at,
                       CASE 
                         WHEN s.price_plan_id = 11 THEN 96.00
                         WHEN s.price_plan_id = 10 THEN 9.99
                         ELSE 0
                       END as fee_amount
                FROM public.subscription_invoices si
                INNER JOIN public.subscriptions s ON s.id = si.subscription_id
                WHERE si.updated_at >= $1 AND si.updated_at < $2
                  AND si.status = 'paid'
                  AND s.status != 'trialing'
                ORDER BY si.subscription_id, si.updated_at DESC`,
          params: [weekStart, weekEnd],
          columns: ["id", "subscription_id", "user_id", "price_plan_id", "status", "subscription_status", "updated_at", "fee_amount"],
        }),
      },
      {
        id: "customerFees",
        name: "Customer Fees (100% margin)",
        getDrilldownQuery: (weekStart, weekEnd) => ({
          sql: `SELECT id, amount, charge_id, waived, created_at
                FROM public.customer_fees
                WHERE created_at >= $1 AND created_at < $2
                  AND (waived IS NULL OR waived != true)
                  AND charge_id IS NOT NULL AND charge_id != ''
                ORDER BY created_at DESC`,
          params: [weekStart, weekEnd],
          columns: ["id", "amount", "charge_id", "waived", "created_at"],
        }),
      },
      {
        id: "tipProfit",
        name: "Tip Profit",
        getDrilldownQuery: (weekStart, weekEnd) => ({
          sql: `SELECT id, booking_id, tip_amount, vendor_amount, (tip_amount - vendor_amount) as profit, created_at
                FROM public.booking_tips
                WHERE created_at >= $1 AND created_at < $2
                ORDER BY profit DESC`,
          params: [weekStart, weekEnd],
          columns: ["id", "booking_id", "tip_amount", "vendor_amount", "profit", "created_at"],
        }),
      },
      {
        id: "refunds",
        name: "Refunds (Subtracted)",
        getDrilldownQuery: (weekStart, weekEnd) => ({
          sql: `SELECT id, booking_id, total, created_at
                FROM public.booking_refunds
                WHERE created_at >= $1 AND created_at < $2
                ORDER BY created_at DESC`,
          params: [weekStart, weekEnd],
          columns: ["id", "booking_id", "total", "created_at"],
        }),
      },
    ],
  },

  marginPercent: {
    id: "marginPercent",
    name: "Margin %",
    category: "Revenue",
    formula: "(Gross Profit / Total Revenue) * 100",
    sourceTable: "bookings",
    description: "Gross profit as a percentage of total revenue",
    getDrilldownQuery: (weekStart, weekEnd) => ({
      sql: `SELECT id, user_id, price, margin, 
                   CASE WHEN price > 0 THEN ROUND((margin / price) * 100, 2) ELSE 0 END as margin_pct,
                   date_due
            FROM public.bookings 
            WHERE date_due >= $1 AND date_due < $2 AND status = 'done'
            ORDER BY margin_pct DESC`,
      params: [weekStart, weekEnd],
      columns: ["id", "user_id", "price", "margin", "margin_pct", "date_due"],
    }),
  },

  signups: {
    id: "signups",
    name: "Sign Ups",
    category: "Users",
    formula: "COUNT(*) FROM users WHERE created_at >= [week_start] AND created_at < [week_end]",
    sourceTable: "users",
    description: "Count of new user registrations with created_at in the week",
    getDrilldownQuery: (weekStart, weekEnd) => ({
      sql: `SELECT id, email, first_name, last_name, created_at
            FROM public.users 
            WHERE created_at >= $1 AND created_at < $2
            ORDER BY created_at DESC`,
      params: [weekStart, weekEnd],
      columns: ["id", "email", "first_name", "last_name", "created_at"],
    }),
  },

  newUsersWithBookings: {
    id: "newUsersWithBookings",
    name: "New Users (w/ Booking)",
    category: "Users",
    formula: "COUNT(DISTINCT users) WHERE users.created_at IN week AND EXISTS(booking for that user ever)",
    sourceTable: "users",
    sourceTables: ["users", "bookings"],
    description: "Users who signed up during the week AND have made at least one booking ever (at any time, not just this week)",
    getDrilldownQuery: (weekStart, weekEnd) => ({
      sql: `SELECT DISTINCT u.id, u.email, u.first_name, u.last_name, u.created_at,
                   (SELECT COUNT(*) FROM public.bookings b WHERE b.user_id = u.id) as total_bookings
            FROM public.users u
            INNER JOIN public.bookings b ON b.user_id = u.id
            WHERE u.created_at >= $1 AND u.created_at < $2
            ORDER BY u.created_at DESC`,
      params: [weekStart, weekEnd],
      columns: ["id", "email", "first_name", "last_name", "created_at", "total_bookings"],
    }),
  },

  newUserConversion: {
    id: "newUserConversion",
    name: "New User Conversion",
    category: "Users",
    formula: "(New Users w/ Booking / Sign Ups) * 100",
    sourceTable: "users",
    description: "Percentage of new signups that have made at least one booking ever",
    getDrilldownQuery: (weekStart, weekEnd) => ({
      sql: `SELECT u.id, u.email, u.first_name, u.last_name, u.created_at,
                   CASE WHEN EXISTS(SELECT 1 FROM public.bookings b WHERE b.user_id = u.id) 
                        THEN 'Has Booking' ELSE 'No Booking' END as booking_status,
                   (SELECT COUNT(*) FROM public.bookings b WHERE b.user_id = u.id) as total_bookings
            FROM public.users u
            WHERE u.created_at >= $1 AND u.created_at < $2
            ORDER BY booking_status, u.created_at DESC`,
      params: [weekStart, weekEnd],
      columns: ["id", "email", "first_name", "last_name", "created_at", "booking_status", "total_bookings"],
    }),
  },

  subscriptionRevenue: {
    id: "subscriptionRevenue",
    name: "Subscription Revenue",
    category: "Membership",
    formula: "Subscription Booking Revenue + Subscription Fees",
    sourceTable: "bookings",
    sourceTables: ["bookings", "subscription_usages", "subscription_invoices", "subscriptions"],
    description: `Subscription Revenue includes:
- Subscription Booking Revenue: SUM(price) from unique completed bookings linked to subscription_usages
- Subscription Fees: SUM of fees from paid subscription_invoices (price_plan_id 11=$96, 10=$9.99)`,
    getDrilldownQuery: (weekStart, weekEnd) => ({
      sql: `SELECT DISTINCT b.id, b.user_id, b.price, b.margin, b.date_due, b.status
            FROM public.bookings b
            INNER JOIN public.subscription_usages su ON su.booking_id = b.id
            WHERE b.date_due >= $1 AND b.date_due < $2 AND b.status = 'done'
            ORDER BY b.date_due DESC`,
      params: [weekStart, weekEnd],
      columns: ["id", "user_id", "price", "margin", "date_due", "status"],
    }),
  },

  subscriptionFees: {
    id: "subscriptionFees",
    name: "Subscription Fees",
    category: "Membership",
    formula: "SUM(fee) FROM subscription_invoices WHERE invoice.status='paid' AND subscription.status!='trialing' AND updated_at IN week (one per subscription_id), fee = $96 if price_plan_id=11, $9.99 if price_plan_id=10",
    sourceTable: "subscription_invoices",
    sourceTables: ["subscription_invoices", "subscriptions"],
    description: "Sum of subscription fees from paid invoices updated during the week, counting only one invoice per subscription to avoid duplicates. Excludes subscriptions in 'trialing' status (first month free). Price plan 11 = $96, plan 10 = $9.99",
    getDrilldownQuery: (weekStart, weekEnd) => ({
      sql: `SELECT DISTINCT ON (si.subscription_id) si.id, si.subscription_id, s.user_id, s.price_plan_id, si.status, s.status as subscription_status, si.updated_at,
                   CASE 
                     WHEN s.price_plan_id = 11 THEN 96.00
                     WHEN s.price_plan_id = 10 THEN 9.99
                     ELSE 0
                   END as fee_amount
            FROM public.subscription_invoices si
            INNER JOIN public.subscriptions s ON s.id = si.subscription_id
            WHERE si.updated_at >= $1 AND si.updated_at < $2
              AND si.status = 'paid'
              AND s.status != 'trialing'
            ORDER BY si.subscription_id, si.updated_at DESC`,
      params: [weekStart, weekEnd],
      columns: ["id", "subscription_id", "user_id", "price_plan_id", "status", "subscription_status", "updated_at", "fee_amount"],
    }),
  },

  memberBookings: {
    id: "memberBookings",
    name: "Member Bookings",
    category: "Membership",
    formula: "COUNT(DISTINCT bookings) WHERE status='done' AND date_due IN week AND linked to subscription_usages",
    sourceTable: "bookings",
    sourceTables: ["bookings", "subscription_usages"],
    description: "Count of unique completed bookings linked to subscription usages, with date_due in the week",
    getDrilldownQuery: (weekStart, weekEnd) => ({
      sql: `SELECT DISTINCT b.id, b.user_id, b.price, b.margin, b.date_due, b.status, su.subscription_id
            FROM public.bookings b
            INNER JOIN public.subscription_usages su ON su.booking_id = b.id
            WHERE b.date_due >= $1 AND b.date_due < $2 AND b.status = 'done'
            ORDER BY b.date_due DESC`,
      params: [weekStart, weekEnd],
      columns: ["id", "user_id", "price", "margin", "date_due", "status", "subscription_id"],
    }),
  },

  membershipRevenuePercent: {
    id: "membershipRevenuePercent",
    name: "% Revenue from Members",
    category: "Membership",
    formula: "(Subscription Revenue / Total Revenue) * 100",
    sourceTable: "bookings",
    description: "Subscription revenue as a percentage of total revenue for the week",
    getDrilldownQuery: (weekStart, weekEnd) => ({
      sql: `SELECT DISTINCT b.id, b.user_id, b.price, b.margin, b.date_due, b.status
            FROM public.bookings b
            INNER JOIN public.subscription_usages su ON su.booking_id = b.id
            WHERE b.date_due >= $1 AND b.date_due < $2 AND b.status = 'done'
            ORDER BY b.date_due DESC`,
      params: [weekStart, weekEnd],
      columns: ["id", "user_id", "price", "margin", "date_due", "status"],
    }),
  },

  newSubscriptions: {
    id: "newSubscriptions",
    name: "New Memberships",
    category: "Membership",
    formula: "COUNT(*) FROM subscriptions WHERE created_at >= [week_start] AND created_at < [week_end]",
    sourceTable: "subscriptions",
    description: "Count of new subscription signups with created_at in the week",
    getDrilldownQuery: (weekStart, weekEnd) => ({
      sql: `SELECT s.id, s.user_id, s.price_plan_id, s.created_at, s.status
            FROM public.subscriptions s
            WHERE s.created_at >= $1 AND s.created_at < $2
            ORDER BY s.created_at DESC`,
      params: [weekStart, weekEnd],
      columns: ["id", "user_id", "price_plan_id", "created_at", "status"],
    }),
  },
};

export function getMetricSpec(metricId: string): MetricSpec | undefined {
  return METRIC_SPECS[metricId];
}

export function getAllMetricSpecs(): MetricSpec[] {
  return Object.values(METRIC_SPECS);
}

export function getMetricCategories(): string[] {
  return ["Bookings", "Revenue", "Users", "Membership"];
}
