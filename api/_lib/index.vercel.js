var __defProp = Object.defineProperty;
var __getOwnPropNames = Object.getOwnPropertyNames;
var __esm = (fn, res) => function __init() {
  return fn && (res = (0, fn[__getOwnPropNames(fn)[0]])(fn = 0)), res;
};
var __export = (target, all) => {
  for (var name in all)
    __defProp(target, name, { get: all[name], enumerable: true });
};

// server/weeklyMetrics.ts
var weeklyMetrics_exports = {};
__export(weeklyMetrics_exports, {
  METRIC_SPECS: () => METRIC_SPECS,
  getAllMetricSpecs: () => getAllMetricSpecs,
  getMetricCategories: () => getMetricCategories,
  getMetricSpec: () => getMetricSpec
});
function getMetricSpec(metricId) {
  return METRIC_SPECS[metricId];
}
function getAllMetricSpecs() {
  return Object.values(METRIC_SPECS);
}
function getMetricCategories() {
  return ["Bookings", "Revenue", "Users", "Membership"];
}
var METRIC_SPECS;
var init_weeklyMetrics = __esm({
  "server/weeklyMetrics.ts"() {
    "use strict";
    METRIC_SPECS = {
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
          columns: ["id", "user_id", "created_at", "status", "price", "date_due"]
        })
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
          columns: ["id", "user_id", "created_at", "status", "price", "date_due"]
        })
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
          columns: ["id", "user_id", "created_at", "status", "price", "margin", "date_due"]
        })
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
          columns: ["id", "user_id", "created_at", "status", "price", "margin", "date_due"]
        })
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
          columns: ["id", "user_id", "created_at", "status", "price", "date_due", "completion_status"]
        })
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
          columns: ["id", "user_id", "price", "margin", "date_due", "status"]
        })
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
          columns: ["id", "user_id", "price", "margin", "stripe_fee", "date_due", "status"]
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
              columns: ["id", "user_id", "price", "margin", "stripe_fee", "date_due", "status"]
            })
          },
          {
            id: "subscriptionFees",
            name: "Subscription Fees (Invoices)",
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
              columns: ["id", "subscription_id", "user_id", "price_plan_id", "status", "subscription_status", "updated_at", "fee_amount"]
            })
          },
          {
            id: "cancellationFees",
            name: "Cancellation Fees ($59 each)",
            getDrilldownQuery: (weekStart, weekEnd) => ({
              sql: `SELECT id, user_id, status, cancellation_fee_charge_id, updated_at, 59.00 as fee_amount
                FROM public.subscriptions
                WHERE updated_at >= $1 AND updated_at < $2
                  AND cancellation_fee_charge_id IS NOT NULL
                  AND cancellation_fee_charge_id != ''
                ORDER BY updated_at DESC`,
              params: [weekStart, weekEnd],
              columns: ["id", "user_id", "status", "cancellation_fee_charge_id", "updated_at", "fee_amount"]
            })
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
              columns: ["id", "amount", "charge_id", "waived", "created_at"]
            })
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
              columns: ["id", "booking_id", "tip_amount", "vendor_amount", "profit", "created_at"]
            })
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
              columns: ["id", "user_id", "credits_received", "pay_amount", "created_at"]
            })
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
              columns: ["id", "booking_id", "total", "created_at"]
            })
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
              columns: ["id", "user_id", "price", "stripe_fee", "date_due"]
            })
          }
        ]
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
          columns: ["id", "user_id", "price", "margin", "date_due", "status"]
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
              columns: ["id", "user_id", "price", "margin", "date_due", "status"]
            })
          },
          {
            id: "subscriptionFees",
            name: "Subscription Fees (100% margin, Invoices)",
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
              columns: ["id", "subscription_id", "user_id", "price_plan_id", "status", "subscription_status", "updated_at", "fee_amount"]
            })
          },
          {
            id: "cancellationFees",
            name: "Cancellation Fees (100% margin, $59 each)",
            getDrilldownQuery: (weekStart, weekEnd) => ({
              sql: `SELECT id, user_id, status, cancellation_fee_charge_id, updated_at, 59.00 as fee_amount
                FROM public.subscriptions
                WHERE updated_at >= $1 AND updated_at < $2
                  AND cancellation_fee_charge_id IS NOT NULL
                  AND cancellation_fee_charge_id != ''
                ORDER BY updated_at DESC`,
              params: [weekStart, weekEnd],
              columns: ["id", "user_id", "status", "cancellation_fee_charge_id", "updated_at", "fee_amount"]
            })
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
              columns: ["id", "amount", "charge_id", "waived", "created_at"]
            })
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
              columns: ["id", "booking_id", "tip_amount", "vendor_amount", "profit", "created_at"]
            })
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
              columns: ["id", "booking_id", "total", "created_at"]
            })
          }
        ]
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
          columns: ["id", "user_id", "price", "margin", "margin_pct", "date_due"]
        })
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
          columns: ["id", "email", "first_name", "last_name", "created_at"]
        })
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
          columns: ["id", "email", "first_name", "last_name", "created_at", "total_bookings"]
        })
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
          columns: ["id", "email", "first_name", "last_name", "created_at", "booking_status", "total_bookings"]
        })
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
          columns: ["id", "user_id", "price", "margin", "date_due", "status"]
        })
      },
      subscriptionFees: {
        id: "subscriptionFees",
        name: "Subscription Fees",
        category: "Membership",
        formula: "Invoice Fees (one per subscription: $96 for plan 11, $9.99 for plan 10) + Cancellation Fees ($59 each for subscriptions with cancellation_fee_charge_id)",
        sourceTable: "subscription_invoices",
        sourceTables: ["subscription_invoices", "subscriptions"],
        description: "Sum of subscription fees including: (1) Paid invoice fees updated during the week (one per subscription, excludes trialing), (2) Cancellation fees ($59) for subscriptions with valid cancellation_fee_charge_id updated during the week.",
        subSources: [
          {
            id: "invoiceFees",
            name: "Invoice Fees",
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
              columns: ["id", "subscription_id", "user_id", "price_plan_id", "status", "subscription_status", "updated_at", "fee_amount"]
            })
          },
          {
            id: "cancellationFees",
            name: "Cancellation Fees ($59 each)",
            getDrilldownQuery: (weekStart, weekEnd) => ({
              sql: `SELECT id, user_id, status, cancellation_fee_charge_id, updated_at, 59.00 as fee_amount
                FROM public.subscriptions
                WHERE updated_at >= $1 AND updated_at < $2
                  AND cancellation_fee_charge_id IS NOT NULL
                  AND cancellation_fee_charge_id != ''
                ORDER BY updated_at DESC`,
              params: [weekStart, weekEnd],
              columns: ["id", "user_id", "status", "cancellation_fee_charge_id", "updated_at", "fee_amount"]
            })
          }
        ],
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
          columns: ["id", "subscription_id", "user_id", "price_plan_id", "status", "subscription_status", "updated_at", "fee_amount"]
        })
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
          columns: ["id", "user_id", "price", "margin", "date_due", "status", "subscription_id"]
        })
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
          columns: ["id", "user_id", "price", "margin", "date_due", "status"]
        })
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
          columns: ["id", "user_id", "price_plan_id", "created_at", "status"]
        })
      }
    };
  }
});

// server/index.vercel.ts
import "dotenv/config";
import express from "express";

// server/routes.ts
import { Pool as Pool2 } from "pg";
import rateLimit from "express-rate-limit";
import { eq as eq2, and, desc, count } from "drizzle-orm";

// server/storage.ts
import * as fs from "fs";
import * as path from "path";
var FILTERS_FILE = path.join(process.cwd(), "filters.json");
var TABLE_SETTINGS_FILE = path.join(process.cwd(), "table_settings.json");
var FILTER_HISTORY_FILE = path.join(process.cwd(), "filter_history.json");
var MAX_HISTORY_PER_TABLE = 5;
function readFiltersFile() {
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
function writeFiltersFile(config) {
  try {
    fs.writeFileSync(FILTERS_FILE, JSON.stringify(config, null, 2));
  } catch (err) {
    console.error("Error writing filters.json:", err);
    throw err;
  }
}
function readTableSettingsFile() {
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
function writeTableSettingsFile(config) {
  try {
    fs.writeFileSync(TABLE_SETTINGS_FILE, JSON.stringify(config, null, 2));
  } catch (err) {
    console.error("Error writing table_settings.json:", err);
    throw err;
  }
}
function readFilterHistoryFile() {
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
function writeFilterHistoryFile(entries) {
  try {
    fs.writeFileSync(FILTER_HISTORY_FILE, JSON.stringify(entries, null, 2));
  } catch (err) {
    console.error("Error writing filter_history.json:", err);
    throw err;
  }
}
function generateId() {
  return Math.random().toString(36).substring(2, 15) + Math.random().toString(36).substring(2, 15);
}
function filtersEqual(a, b) {
  if (a.length !== b.length) return false;
  const sortedA = [...a].sort((x, y) => x.column.localeCompare(y.column));
  const sortedB = [...b].sort((x, y) => x.column.localeCompare(y.column));
  return sortedA.every(
    (f, i) => f.column === sortedB[i].column && f.operator === sortedB[i].operator && f.value === sortedB[i].value
  );
}
var MemStorage = class {
  filters;
  tableSettings;
  filterHistory;
  constructor() {
    this.filters = readFiltersFile();
    this.tableSettings = readTableSettingsFile();
    this.filterHistory = readFilterHistoryFile();
  }
  async getFilters(table) {
    return this.filters[table] || [];
  }
  async setFilters(table, filters) {
    this.filters[table] = filters;
    writeFiltersFile(this.filters);
  }
  async getAllFilters() {
    return this.filters;
  }
  async getTableSettings(database, tableName) {
    const key = `${database}:${tableName}`;
    return this.tableSettings[key] || null;
  }
  async setTableSettings(database, tableName, settings) {
    const key = `${database}:${tableName}`;
    this.tableSettings[key] = settings;
    writeTableSettingsFile(this.tableSettings);
  }
  async getAllTableSettings() {
    return this.tableSettings;
  }
  async getFilterHistory(userId, database, table) {
    return this.filterHistory.filter((e) => e.userId === userId && e.database === database && e.table === table).sort((a, b) => new Date(b.lastUsedAt).getTime() - new Date(a.lastUsedAt).getTime()).slice(0, MAX_HISTORY_PER_TABLE);
  }
  async saveFilterHistory(userId, database, table, filters) {
    if (filters.length === 0) {
      throw new Error("Cannot save empty filter history");
    }
    const existingIndex = this.filterHistory.findIndex(
      (e) => e.userId === userId && e.database === database && e.table === table && filtersEqual(e.filters, filters)
    );
    const now = (/* @__PURE__ */ new Date()).toISOString();
    if (existingIndex !== -1) {
      this.filterHistory[existingIndex].lastUsedAt = now;
      writeFilterHistoryFile(this.filterHistory);
      return this.filterHistory[existingIndex];
    }
    const newEntry = {
      id: generateId(),
      userId,
      database,
      table,
      filters,
      lastUsedAt: now
    };
    this.filterHistory.push(newEntry);
    const userTableEntries = this.filterHistory.filter((e) => e.userId === userId && e.database === database && e.table === table).sort((a, b) => new Date(b.lastUsedAt).getTime() - new Date(a.lastUsedAt).getTime());
    if (userTableEntries.length > MAX_HISTORY_PER_TABLE) {
      const toRemove = userTableEntries.slice(MAX_HISTORY_PER_TABLE);
      this.filterHistory = this.filterHistory.filter((e) => !toRemove.some((r) => r.id === e.id));
    }
    writeFilterHistoryFile(this.filterHistory);
    return newEntry;
  }
  async deleteFilterHistory(id, userId) {
    const index2 = this.filterHistory.findIndex((e) => e.id === id && e.userId === userId);
    if (index2 === -1) return false;
    this.filterHistory.splice(index2, 1);
    writeFilterHistoryFile(this.filterHistory);
    return true;
  }
};
var storage = new MemStorage();

// server/db.ts
import { drizzle } from "drizzle-orm/node-postgres";
import pg from "pg";
var { Pool } = pg;
if (!process.env.DATABASE_URL) {
  throw new Error("DATABASE_URL is not set");
}
var pool = new Pool({
  connectionString: process.env.DATABASE_URL
});
var db = drizzle(pool);

// server/stripeClient.ts
import Stripe from "stripe";
var connectionSettings;
async function getCredentials() {
  if (process.env.STRIPE_SECRET_KEY) {
    console.log("[STRIPE] Using STRIPE_SECRET_KEY from environment");
    return {
      publishableKey: process.env.STRIPE_PUBLISHABLE_KEY || "",
      secretKey: process.env.STRIPE_SECRET_KEY
    };
  }
  const hostname = process.env.REPLIT_CONNECTORS_HOSTNAME;
  const xReplitToken = process.env.REPL_IDENTITY ? "repl " + process.env.REPL_IDENTITY : process.env.WEB_REPL_RENEWAL ? "depl " + process.env.WEB_REPL_RENEWAL : null;
  if (!xReplitToken) {
    throw new Error("X_REPLIT_TOKEN not found for repl/depl");
  }
  const connectorName = "stripe";
  const isProduction = process.env.REPLIT_DEPLOYMENT === "1";
  const targetEnvironment = isProduction ? "production" : "development";
  const url = new URL(`https://${hostname}/api/v2/connection`);
  url.searchParams.set("include_secrets", "true");
  url.searchParams.set("connector_names", connectorName);
  url.searchParams.set("environment", targetEnvironment);
  const response = await fetch(url.toString(), {
    headers: {
      "Accept": "application/json",
      "X_REPLIT_TOKEN": xReplitToken
    }
  });
  const data = await response.json();
  connectionSettings = data.items?.[0];
  if (!connectionSettings || (!connectionSettings.settings.publishable || !connectionSettings.settings.secret)) {
    throw new Error(`Stripe ${targetEnvironment} connection not found`);
  }
  console.log("[STRIPE] Using Replit connector");
  return {
    publishableKey: connectionSettings.settings.publishable,
    secretKey: connectionSettings.settings.secret
  };
}
async function getStripeClient() {
  const { secretKey } = await getCredentials();
  return new Stripe(secretKey, {
    apiVersion: "2025-11-17.clover"
  });
}
async function getStripeMetricsForWeek(startTimestamp, endTimestamp) {
  const stripe = await getStripeClient();
  let grossVolume = 0;
  let netVolume = 0;
  let refunds = 0;
  let disputes = 0;
  let transactionCount = 0;
  let refundCount = 0;
  let disputeCount = 0;
  let totalBalanceChange = 0;
  let totalPayouts = 0;
  let totalStripeFees = 0;
  console.log(`[STRIPE DEBUG] Querying balance transactions from ${new Date(startTimestamp * 1e3).toISOString()} to ${new Date(endTimestamp * 1e3).toISOString()}`);
  let hasMore = true;
  let startingAfter;
  let totalFetched = 0;
  while (hasMore) {
    const balanceTransactions = await stripe.balanceTransactions.list({
      created: {
        gte: startTimestamp,
        lt: endTimestamp
      },
      limit: 100,
      expand: ["data.source"],
      ...startingAfter ? { starting_after: startingAfter } : {}
    });
    totalFetched += balanceTransactions.data.length;
    console.log(`[STRIPE DEBUG] Fetched ${balanceTransactions.data.length} transactions (total: ${totalFetched}), has_more: ${balanceTransactions.has_more}`);
    for (const txn of balanceTransactions.data) {
      totalBalanceChange += txn.net;
      if (txn.type === "charge" || txn.type === "payment") {
        grossVolume += txn.amount;
        transactionCount++;
      } else if (txn.type === "refund") {
        refunds += Math.abs(txn.amount);
        refundCount++;
      } else if (txn.type === "payout") {
        totalPayouts += txn.net;
        console.log(`[STRIPE DEBUG] Payout to bank: ${txn.id}, net: ${txn.net / 100}`);
      } else if (txn.type === "transfer") {
        const source = txn.source;
        const isConnectedAccountTransfer = source && typeof source === "object" && source.object === "transfer" && source.destination && typeof source.destination === "string" && source.destination.startsWith("acct_");
        if (isConnectedAccountTransfer) {
          console.log(`[STRIPE DEBUG] Connected account transfer: ${txn.id}, net: ${txn.net / 100}, destination: ${source.destination}`);
        } else {
          console.log(`[STRIPE DEBUG] Non-connected transfer: ${txn.id}, net: ${txn.net / 100}, destination: ${source?.destination || "none"}`);
        }
      } else if (txn.type === "application_fee") {
        console.log(`[STRIPE DEBUG] Application fee: ${txn.id}, net: ${txn.net / 100}`);
      } else if (txn.type === "stripe_fee") {
        totalStripeFees += txn.net;
        console.log(`[STRIPE DEBUG] Stripe fee: ${txn.id}, net: ${txn.net / 100}`);
      }
    }
    hasMore = balanceTransactions.has_more;
    if (hasMore && balanceTransactions.data.length > 0) {
      startingAfter = balanceTransactions.data[balanceTransactions.data.length - 1].id;
    }
  }
  netVolume = totalBalanceChange - totalPayouts - totalStripeFees;
  console.log(`[STRIPE DEBUG] Total balance change: ${totalBalanceChange / 100}, Total payouts: ${totalPayouts / 100}, Total Stripe fees: ${totalStripeFees / 100}, Net Volume: ${netVolume / 100}`);
  let disputeHasMore = true;
  let disputeStartingAfter;
  while (disputeHasMore) {
    const listParams = {
      created: {
        gte: startTimestamp,
        lt: endTimestamp
      },
      limit: 100
    };
    if (disputeStartingAfter) {
      listParams.starting_after = disputeStartingAfter;
    }
    const disputesList = await stripe.disputes.list(listParams);
    for (const dispute of disputesList.data) {
      disputes += dispute.amount;
      disputeCount++;
    }
    disputeHasMore = disputesList.has_more;
    if (disputeHasMore && disputesList.data.length > 0) {
      disputeStartingAfter = disputesList.data[disputesList.data.length - 1].id;
    }
  }
  return {
    grossVolume: grossVolume / 100,
    netVolume: netVolume / 100,
    refunds: refunds / 100,
    disputes: disputes / 100,
    transactionCount,
    refundCount,
    disputeCount
  };
}
async function checkStripeConnection() {
  try {
    const stripe = await getStripeClient();
    await stripe.balance.retrieve();
    return true;
  } catch (error) {
    console.error("Stripe connection check failed:", error);
    return false;
  }
}

// server/operationsMetrics.ts
var OPERATIONS_METRIC_SPECS = {
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
      columns: ["id", "user_id", "vendor_id", "created_at", "status", "price", "margin", "date_due"]
    })
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
      columns: ["id", "vendor_id", "bookings_count", "created_at", "updated_at"]
    })
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
      columns: ["id", "vendor_id", "status", "date_due", "cancel_reason_id", "delivery_status"]
    })
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
      columns: ["defect_type", "id", "booking_id", "reason_detail", "event_date"]
    })
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
      columns: ["id", "user_id", "vendor_id", "created_at", "status", "overbooked", "date_due"]
    })
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
      columns: ["id", "booking_id", "rating", "comment", "created_at"]
    })
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
      columns: ["id", "user_id", "vendor_id", "date_due", "status", "rating", "rating_status"]
    })
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
      columns: ["note"]
    })
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
      columns: ["id", "first_name", "last_name", "email", "vendor_level_id", "bookings_completed"]
    })
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
      columns: ["info"]
    })
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
      columns: ["id", "first_name", "last_name", "email", "starting_date", "step"]
    })
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
      columns: ["id", "first_name", "last_name", "email", "status", "updated_at"]
    })
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
      columns: ["vendor_id", "first_name", "last_name", "status", "total_minutes_effective", "hours_effective"]
    })
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
      columns: ["vendor_id", "first_name", "last_name", "total_minutes_worked", "total_minutes_effective", "utilization_pct"]
    })
  }
};
function getOperationsMetricSpec(metricId) {
  return OPERATIONS_METRIC_SPECS[metricId];
}
function getAllOperationsMetricSpecs() {
  return Object.values(OPERATIONS_METRIC_SPECS);
}
function buildBookingZoneFilter(bookingAlias = "b", paramOffset = 3, selectedZones) {
  if (selectedZones.length === 0) {
    return { clause: "", params: [] };
  }
  const placeholders = selectedZones.map((_, i) => `$${paramOffset + i}`).join(", ");
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
function buildVendorZoneFilter(vendorAlias = "v", paramOffset = 3, selectedZones) {
  if (selectedZones.length === 0) {
    return { clause: "", params: [] };
  }
  const placeholders = selectedZones.map((_, i) => `$${paramOffset + i}`).join(", ");
  return {
    clause: ` AND ${vendorAlias}.washos_zone IN (${placeholders})`,
    params: selectedZones
  };
}
async function calculateOperationsMetrics(pool2, periodStart, periodEnd, stripeMetrics, selectedZones = []) {
  const bookingZoneFilter = buildBookingZoneFilter("b", 3, selectedZones);
  const bookingsCompletedResult = await pool2.query(
    `SELECT COUNT(*) as count FROM public.bookings b
     WHERE b.date_due >= $1 AND b.date_due < $2 AND b.status = 'done'${bookingZoneFilter.clause}`,
    [periodStart, periodEnd, ...bookingZoneFilter.params]
  );
  const bookingsCompleted = parseInt(bookingsCompletedResult.rows[0]?.count || "0");
  const bookingsCreatedZoneFilter = buildBookingZoneFilter("b", 3, selectedZones);
  const bookingsCreatedResult = await pool2.query(
    `SELECT COUNT(*) as count FROM public.bookings b
     WHERE b.created_at >= $1 AND b.created_at < $2${bookingsCreatedZoneFilter.clause}`,
    [periodStart, periodEnd, ...bookingsCreatedZoneFilter.params]
  );
  const bookingsCreated = parseInt(bookingsCreatedResult.rows[0]?.count || "0");
  const emergencyVendorZoneFilter = buildVendorZoneFilter("v", 3, selectedZones);
  const emergenciesResult = await pool2.query(
    `SELECT COUNT(*) as count FROM public.vendor_emergencies ve
     INNER JOIN public.vendors v ON v.id = ve.vendor_id
     WHERE ve.bookings_count > 0 AND ve.created_at >= $1 AND ve.created_at < $2${emergencyVendorZoneFilter.clause}`,
    [periodStart, periodEnd, ...emergencyVendorZoneFilter.params]
  );
  const emergenciesCount = parseInt(emergenciesResult.rows[0]?.count || "0");
  const emergencies = bookingsCompleted > 0 ? emergenciesCount / bookingsCompleted * 100 : 0;
  const cancellationZoneFilter = buildBookingZoneFilter("b", 3, selectedZones);
  const cancellationsResult = await pool2.query(
    `SELECT COUNT(*) as count FROM public.cancelled_bookings cb
     INNER JOIN public.bookings b ON b.id = cb.booking_id
     WHERE b.date_due >= $1 AND b.date_due < $2
       AND cb.cancel_reason_id IN (4, 5, 6, 7, 8, 9, 17, 18)${cancellationZoneFilter.clause}`,
    [periodStart, periodEnd, ...cancellationZoneFilter.params]
  );
  const vendorCancellations = parseInt(cancellationsResult.rows[0]?.count || "0");
  const deliveryRate = bookingsCompleted > 0 ? (bookingsCompleted - vendorCancellations) / bookingsCompleted * 100 : 0;
  const defectZoneFilter = buildBookingZoneFilter("b", 3, selectedZones);
  const defectsResult = await pool2.query(
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
  const defectPercent = bookingsCompleted > 0 ? totalDefects / bookingsCompleted * 100 : 0;
  const overbookedZoneFilter = buildBookingZoneFilter("b", 3, selectedZones);
  const overbookedResult = await pool2.query(
    `SELECT COUNT(*) as count FROM public.bookings b
     WHERE b.created_at >= $1 AND b.created_at < $2 AND b.overbooked = true${overbookedZoneFilter.clause}`,
    [periodStart, periodEnd, ...overbookedZoneFilter.params]
  );
  const overbooked = parseInt(overbookedResult.rows[0]?.count || "0");
  const overbookedPercent = bookingsCreated > 0 ? overbooked / bookingsCreated * 100 : 0;
  const ratingZoneFilter = buildBookingZoneFilter("b", 3, selectedZones);
  const ratingResult = await pool2.query(
    `SELECT AVG(br.rating) as avg_rating FROM public.booking_ratings br
     INNER JOIN public.bookings b ON b.id = br.booking_id
     WHERE br.created_at >= $1 AND br.created_at < $2 AND br.rating IS NOT NULL${ratingZoneFilter.clause}`,
    [periodStart, periodEnd, ...ratingZoneFilter.params]
  );
  const avgRating = parseFloat(ratingResult.rows[0]?.avg_rating || "0");
  const responseZoneFilter = buildBookingZoneFilter("b", 3, selectedZones);
  const ratingsCountResult = await pool2.query(
    `SELECT COUNT(DISTINCT br.booking_id) as count 
     FROM public.booking_ratings br
     INNER JOIN public.bookings b ON b.id = br.booking_id
     WHERE b.date_due >= $1 AND b.date_due < $2 AND b.status = 'done'${responseZoneFilter.clause}`,
    [periodStart, periodEnd, ...responseZoneFilter.params]
  );
  const ratingsCount = parseInt(ratingsCountResult.rows[0]?.count || "0");
  const responseRate = bookingsCompleted > 0 ? ratingsCount / bookingsCompleted * 100 : 0;
  const stripeMargin = stripeMetrics && stripeMetrics.grossVolume > 0 ? stripeMetrics.netVolume / stripeMetrics.grossVolume * 100 : 0;
  const activeVendorZoneFilter = buildVendorZoneFilter("v", 3, selectedZones);
  const activeVendorsResult = await pool2.query(
    `SELECT COUNT(DISTINCT b.vendor_id) as count FROM public.bookings b
     INNER JOIN public.vendors v ON v.id = b.vendor_id
     WHERE b.date_due >= $1 AND b.date_due < $2 AND b.status = 'done'${activeVendorZoneFilter.clause}`,
    [periodStart, periodEnd, ...activeVendorZoneFilter.params]
  );
  const activeVendors = parseInt(activeVendorsResult.rows[0]?.count || "0");
  const vendorLevelCounts = {};
  const newVendorZoneFilter = buildVendorZoneFilter("v", 3, selectedZones);
  const newVendorsResult = await pool2.query(
    `SELECT COUNT(DISTINCT v.id) as count
     FROM public.vendors v
     INNER JOIN public.vendor_onboardings vo ON vo.vendor_id = v.id
     WHERE vo.step = 'finished' 
       AND vo.starting_date >= $1 AND vo.starting_date < $2
       AND EXISTS (SELECT 1 FROM public.bookings b WHERE b.vendor_id = v.id AND b.status = 'done')${newVendorZoneFilter.clause}`,
    [periodStart, periodEnd, ...newVendorZoneFilter.params]
  );
  const newVendors = parseInt(newVendorsResult.rows[0]?.count || "0");
  const dismissedVendorZoneFilter = buildVendorZoneFilter("v", 3, selectedZones);
  const dismissedVendorsResult = await pool2.query(
    `SELECT COUNT(*) as count FROM public.vendors v
     WHERE v.status = 'dismissed' AND v.updated_at >= $1 AND v.updated_at < $2${dismissedVendorZoneFilter.clause}`,
    [periodStart, periodEnd, ...dismissedVendorZoneFilter.params]
  );
  const dismissedVendors = parseInt(dismissedVendorsResult.rows[0]?.count || "0");
  const scheduledHoursVendorZoneFilter = buildVendorZoneFilter("v", 2, selectedZones);
  const scheduledHoursResult = await pool2.query(
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
  const utilizationVendorZoneFilter = buildVendorZoneFilter("v", 2, selectedZones);
  const utilizationResult = await pool2.query(
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
  const utilization = totalMinutesEffectiveUtil > 0 ? totalMinutesWorked / totalMinutesEffectiveUtil * 100 : 0;
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
    utilization
  };
}
function calculateOperationsVariance(current, previous) {
  if (!previous) {
    return {};
  }
  const percentMetrics = ["deliveryRate", "defectPercent", "overbookedPercent", "responseRate", "stripeMargin", "utilization"];
  const variance = {};
  for (const key of Object.keys(current)) {
    if (key === "vendorLevelCounts") continue;
    const currVal = current[key];
    const prevVal = previous[key];
    if (percentMetrics.includes(key)) {
      variance[key] = currVal - prevVal;
    } else {
      variance[key] = prevVal !== 0 ? (currVal - prevVal) / prevVal * 100 : currVal > 0 ? 100 : 0;
    }
  }
  return variance;
}

// server/dashboardCache.ts
var cache = {};
var ONE_HOUR_MS = 60 * 60 * 1e3;
var ONE_WEEK_MS = 7 * 24 * 60 * 60 * 1e3;
var MAX_CACHE_ENTRIES = 100;
function getCacheKey(dashboardType, database, periodType, periodIdentifier, zones) {
  const zonesKey = zones && zones.length > 0 ? zones.sort().join(",") : "all";
  return `${dashboardType}:${database}:${periodType}:${periodIdentifier || "all"}:${zonesKey}`;
}
function getCacheDuration(isCurrentPeriod) {
  return isCurrentPeriod ? ONE_HOUR_MS : ONE_WEEK_MS;
}
function evictOldestEntry() {
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
function cleanupExpiredEntries() {
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
function getFromCache(key) {
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
  return entry.data;
}
function setInCache(key, data, durationMs) {
  cleanupExpiredEntries();
  if (Object.keys(cache).length >= MAX_CACHE_ENTRIES && !cache[key]) {
    evictOldestEntry();
  }
  const now = Date.now();
  cache[key] = {
    data,
    timestamp: now,
    expiresAt: now + durationMs,
    lastAccessed: now
  };
}

// server/replit_integrations/auth/replitAuth.ts
import passport from "passport";
import { Strategy as LocalStrategy } from "passport-local";
import session from "express-session";
import connectPg from "connect-pg-simple";
import bcrypt from "bcryptjs";

// shared/models/auth.ts
import { sql } from "drizzle-orm";
import { boolean, index, jsonb, pgTable, text, timestamp, varchar, integer } from "drizzle-orm/pg-core";
var sessions = pgTable(
  "sessions",
  {
    sid: varchar("sid").primaryKey(),
    sess: jsonb("sess").notNull(),
    expire: timestamp("expire").notNull()
  },
  (table) => [index("IDX_session_expire").on(table.expire)]
);
var users = pgTable("users", {
  id: integer("id").primaryKey().generatedAlwaysAsIdentity(),
  email: varchar("email").unique().notNull(),
  password: varchar("password_digest"),
  firstName: varchar("first_name"),
  lastName: varchar("last_name"),
  profileImageUrl: varchar("profile_image_url"),
  role: varchar("role").$type().default("external_customer").notNull(),
  isActive: boolean("is_active").default(true).notNull(),
  createdAt: timestamp("created_at").defaultNow(),
  updatedAt: timestamp("updated_at").defaultNow()
});
var tableGrants = pgTable("table_grants", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  userId: integer("user_id").notNull().references(() => users.id, { onDelete: "cascade" }),
  database: varchar("database").notNull(),
  tableName: varchar("table_name").notNull(),
  grantedBy: integer("granted_by").notNull().references(() => users.id),
  grantedAt: timestamp("granted_at").defaultNow()
});
var auditLogs = pgTable("audit_logs", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  userId: varchar("user_id").notNull(),
  userEmail: varchar("user_email").notNull(),
  action: varchar("action").notNull(),
  database: varchar("database"),
  tableName: varchar("table_name"),
  details: text("details"),
  ipAddress: varchar("ip_address"),
  timestamp: timestamp("timestamp").defaultNow().notNull()
}, (table) => [
  index("idx_audit_logs_user").on(table.userId),
  index("idx_audit_logs_timestamp").on(table.timestamp)
]);
var reportPages = pgTable("report_pages", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  userId: varchar("user_id").notNull().references(() => users.id, { onDelete: "cascade" }),
  title: varchar("title").notNull(),
  description: text("description"),
  isArchived: boolean("is_archived").default(false).notNull(),
  createdAt: timestamp("created_at").defaultNow().notNull(),
  updatedAt: timestamp("updated_at").defaultNow().notNull()
}, (table) => [
  index("idx_report_pages_user").on(table.userId)
]);
var reportBlocks = pgTable("report_blocks", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  pageId: varchar("page_id").notNull().references(() => reportPages.id, { onDelete: "cascade" }),
  kind: varchar("kind").$type().notNull(),
  title: varchar("title"),
  position: jsonb("position").$type().notNull(),
  config: jsonb("config").$type().notNull(),
  createdAt: timestamp("created_at").defaultNow().notNull(),
  updatedAt: timestamp("updated_at").defaultNow().notNull()
}, (table) => [
  index("idx_report_blocks_page").on(table.pageId)
]);
var reportChatSessions = pgTable("report_chat_sessions", {
  id: varchar("id").primaryKey().default(sql`gen_random_uuid()`),
  pageId: varchar("page_id").notNull().references(() => reportPages.id, { onDelete: "cascade" }),
  messages: jsonb("messages").$type().default([]).notNull(),
  createdAt: timestamp("created_at").defaultNow().notNull(),
  updatedAt: timestamp("updated_at").defaultNow().notNull()
}, (table) => [
  index("idx_report_chat_sessions_page").on(table.pageId)
]);

// server/replit_integrations/auth/storage.ts
import { eq } from "drizzle-orm";
var AuthStorage = class {
  async getUser(id) {
    const [user] = await db.select().from(users).where(eq(users.id, id));
    return user;
  }
  async getUserByEmail(email) {
    const [user] = await db.select().from(users).where(eq(users.email, email));
    return user;
  }
  async createUser(userData) {
    const [user] = await db.insert(users).values(userData).returning();
    return user;
  }
  async upsertUser(userData) {
    const [user] = await db.insert(users).values(userData).onConflictDoUpdate({
      target: users.id,
      set: {
        ...userData,
        updatedAt: /* @__PURE__ */ new Date()
      }
    }).returning();
    return user;
  }
};
var authStorage = new AuthStorage();

// server/replit_integrations/auth/replitAuth.ts
function getSession() {
  const sessionTtl = 7 * 24 * 60 * 60 * 1e3;
  const pgStore = connectPg(session);
  const sessionStore = new pgStore({
    conString: process.env.DATABASE_URL,
    createTableIfMissing: false,
    ttl: sessionTtl,
    tableName: "sessions"
  });
  return session({
    secret: process.env.SESSION_SECRET,
    store: sessionStore,
    resave: false,
    saveUninitialized: false,
    proxy: true,
    cookie: {
      httpOnly: true,
      secure: process.env.NODE_ENV === "production",
      sameSite: "lax",
      maxAge: sessionTtl
    }
  });
}
async function setupAuth(app2) {
  app2.set("trust proxy", 1);
  app2.use(getSession());
  app2.use(passport.initialize());
  app2.use(passport.session());
  passport.use(
    new LocalStrategy(
      { usernameField: "email", passwordField: "password" },
      async (email, password, done) => {
        try {
          const normalizedEmail = email.toLowerCase().trim();
          const user = await authStorage.getUserByEmail(normalizedEmail);
          if (!user || !user.password || !user.isActive) {
            return done(null, false, { message: "Invalid email or password" });
          }
          const isMatch = await bcrypt.compare(password, user.password);
          if (!isMatch) {
            return done(null, false, { message: "Invalid email or password" });
          }
          return done(null, user);
        } catch (error) {
          return done(error);
        }
      }
    )
  );
  passport.serializeUser((user, cb) => cb(null, user.id));
  passport.deserializeUser(async (id, cb) => {
    try {
      const user = await authStorage.getUser(id);
      cb(null, user || null);
    } catch (error) {
      cb(error);
    }
  });
  app2.post("/api/login", (req, res, next) => {
    passport.authenticate("local", (err, user, info) => {
      if (err) {
        return res.status(500).json({ message: "Login failed" });
      }
      if (!user) {
        return res.status(401).json({ message: info?.message || "Invalid credentials" });
      }
      req.logIn(user, (err2) => {
        if (err2) {
          return res.status(500).json({ message: "Login failed" });
        }
        const { password, ...userWithoutPassword } = user;
        return res.json({ user: userWithoutPassword });
      });
    })(req, res, next);
  });
  app2.post("/api/logout", (req, res) => {
    req.logout((err) => {
      if (err) {
        return res.status(500).json({ message: "Logout failed" });
      }
      req.session.destroy((err2) => {
        if (err2) {
          return res.status(500).json({ message: "Logout failed" });
        }
        res.clearCookie("connect.sid");
        return res.json({ message: "Logged out successfully" });
      });
    });
  });
}
var isAuthenticated = async (req, res, next) => {
  if (!req.isAuthenticated() || !req.user) {
    return res.status(401).json({ message: "Unauthorized" });
  }
  return next();
};

// server/replit_integrations/auth/routes.ts
function registerAuthRoutes(app2) {
  app2.get("/api/auth/user", isAuthenticated, async (req, res) => {
    try {
      const user = req.user;
      if (!user) {
        return res.status(401).json({ message: "Not authenticated" });
      }
      const { password, ...userWithoutPassword } = user;
      res.json(userWithoutPassword);
    } catch (error) {
      console.error("Error fetching user:", error);
      res.status(500).json({ message: "Failed to fetch user" });
    }
  });
  app2.get("/api/auth/me", async (req, res) => {
    try {
      if (!req.isAuthenticated() || !req.user) {
        return res.json({ user: null });
      }
      const user = req.user;
      const { password, ...userWithoutPassword } = user;
      res.json({ user: userWithoutPassword });
    } catch (error) {
      console.error("Error fetching user:", error);
      res.status(500).json({ message: "Failed to fetch user" });
    }
  });
}

// shared/schema.ts
import { z } from "zod";
var databaseConnectionSchema = z.object({
  name: z.string(),
  url: z.string()
});
var tableInfoSchema = z.object({
  schema: z.string(),
  name: z.string(),
  fullName: z.string(),
  // schema.table
  displayName: z.string().nullable().optional(),
  isVisible: z.boolean().optional()
});
var columnInfoSchema = z.object({
  name: z.string(),
  dataType: z.string(),
  isNullable: z.boolean(),
  isPrimaryKey: z.boolean()
});
var filterOperatorSchema = z.enum(["eq", "contains", "gt", "gte", "lt", "lte", "between", "in"]);
var filterDefinitionSchema = z.object({
  id: z.string(),
  name: z.string(),
  column: z.string(),
  operator: filterOperatorSchema
});
var activeFilterSchema = z.object({
  column: z.string(),
  operator: filterOperatorSchema,
  value: z.string()
});
var queryRequestSchema = z.object({
  database: z.string(),
  table: z.string(),
  page: z.number().int().positive().default(1),
  filters: z.array(activeFilterSchema).optional()
});
var queryResponseSchema = z.object({
  rows: z.array(z.record(z.unknown())),
  totalCount: z.number(),
  page: z.number(),
  pageSize: z.number(),
  totalPages: z.number()
});
var nlqRequestSchema = z.object({
  database: z.string(),
  table: z.string().optional(),
  query: z.string(),
  context: z.string().optional()
});
var nlqActionSchema = z.enum(["clarify", "plan", "suggest"]);
var timeframeSchema = z.object({
  start: z.string(),
  end: z.string(),
  timezone: z.string().default("America/Los_Angeles"),
  mode: z.enum(["rolling", "calendar"]).optional()
});
var nlqFilterSchema = z.object({
  column: z.string(),
  op: filterOperatorSchema,
  value: z.union([z.string(), z.array(z.string())])
});
var nlqExplainSchema = z.object({
  table: z.string(),
  resolvedDateColumn: z.string().nullable().optional(),
  timeframe: timeframeSchema.nullable().optional(),
  filtersApplied: z.array(z.object({
    column: z.string(),
    operator: z.string(),
    value: z.union([z.string(), z.array(z.string())]),
    interpretation: z.string().optional()
  })).optional(),
  sortApplied: z.object({
    column: z.string(),
    direction: z.enum(["asc", "desc"])
  }).nullable().optional(),
  page: z.number().optional(),
  limit: z.number().optional()
});
var nlqSuggestionSchema = z.object({
  description: z.string(),
  filters: z.array(nlqFilterSchema).optional(),
  chartType: z.string().optional()
});
var nlqPlanSchema = z.object({
  action: nlqActionSchema.optional().default("plan"),
  table: z.string(),
  page: z.number().default(1),
  filters: z.array(z.object({
    column: z.string(),
    op: filterOperatorSchema,
    value: z.union([z.string(), z.array(z.string())])
  })),
  questions: z.array(z.string()).optional(),
  suggestions: z.array(nlqSuggestionSchema).optional(),
  explain: nlqExplainSchema.optional(),
  needsClarification: z.boolean().optional(),
  clarificationQuestion: z.string().optional(),
  ambiguousColumns: z.array(z.string()).optional(),
  summary: z.string().optional()
});
var smartFollowupIssueSchema = z.enum([
  "value_mismatch",
  "case_mismatch",
  "date_out_of_range",
  "null_column",
  "synonym_mismatch",
  "typo",
  "unknown"
]);
var smartFollowupChangeSchema = z.object({
  filterIndex: z.number(),
  column: z.string(),
  currentValue: z.string(),
  suggestedValue: z.string().optional(),
  suggestedOperator: z.string().optional(),
  reason: z.string()
});
var smartFollowupResponseSchema = z.object({
  likelyIssue: smartFollowupIssueSchema,
  suggestedChanges: z.array(smartFollowupChangeSchema),
  questions: z.array(z.string()).optional(),
  evidence: z.object({
    sampledValues: z.record(z.string(), z.array(z.string())).optional(),
    dateRanges: z.record(z.string(), z.object({ min: z.string(), max: z.string() })).optional()
  }).optional(),
  clarificationQuestion: z.string().optional(),
  suggestedFilters: z.array(nlqFilterSchema).optional(),
  summary: z.string().optional()
});
var filtersConfigSchema = z.record(z.string(), z.array(filterDefinitionSchema));
var filterHistoryEntrySchema = z.object({
  id: z.string(),
  userId: z.string(),
  database: z.string(),
  table: z.string(),
  filters: z.array(activeFilterSchema),
  lastUsedAt: z.string()
  // ISO timestamp
});

// server/ai/openai.ts
import OpenAI from "openai";
var openaiClient = null;
function getOpenAIClient() {
  if (openaiClient) return openaiClient;
  const apiKey = process.env.AI_INTEGRATIONS_OPENAI_API_KEY;
  const baseURL = process.env.AI_INTEGRATIONS_OPENAI_BASE_URL;
  if (!apiKey || !baseURL) {
    return null;
  }
  openaiClient = new OpenAI({
    apiKey,
    baseURL
  });
  return openaiClient;
}
var AI_CONFIG = {
  nlq: {
    model: "gpt-4o",
    temperature: 0.1,
    maxTokens: 1200
  },
  smartFollowup: {
    model: "gpt-4o",
    temperature: 0.3,
    maxTokens: 800
  },
  reportChat: {
    model: "gpt-4o",
    temperature: 0.5,
    maxTokens: 1200
  }
};

// server/ai/data-dictionary.ts
var dataDictionaryCache = /* @__PURE__ */ new Map();
var CACHE_TTL_MS = 15 * 60 * 1e3;
var MAX_COLUMNS_HEAVY_SAMPLING = 15;
var QUERY_TIMEOUT_MS = 5e3;
var LOW_CARDINALITY_THRESHOLD = 100;
function getCacheKey2(database, schema, table) {
  return `${database}:${schema}.${table}`;
}
async function queryWithTimeout(pool2, sql2, params = [], timeoutMs = QUERY_TIMEOUT_MS) {
  return new Promise((resolve) => {
    const timeoutId = setTimeout(() => resolve(null), timeoutMs);
    pool2.query(sql2, params).then((result) => {
      clearTimeout(timeoutId);
      resolve(result);
    }).catch(() => {
      clearTimeout(timeoutId);
      resolve(null);
    });
  });
}
async function getTableDataDictionary(pool2, database, schema, table, forceRefresh = false) {
  const cacheKey = getCacheKey2(database, schema, table);
  const cached = dataDictionaryCache.get(cacheKey);
  if (!forceRefresh && cached && Date.now() - cached.fetchedAt < CACHE_TTL_MS) {
    return cached;
  }
  try {
    const columnsResult = await pool2.query(`
      SELECT column_name, data_type, is_nullable
      FROM information_schema.columns
      WHERE table_schema = $1 AND table_name = $2
      ORDER BY ordinal_position
    `, [schema, table]);
    if (columnsResult.rows.length === 0) return null;
    const countResult = await queryWithTimeout(
      pool2,
      `SELECT COUNT(*) as count FROM "${schema}"."${table}"`,
      [],
      3e3
    );
    const totalRows = countResult?.rows[0]?.count ? parseInt(countResult.rows[0].count, 10) : 0;
    const columns = [];
    let heavySamplingCount = 0;
    for (const col of columnsResult.rows) {
      const colName = col.column_name;
      const dataType = col.data_type;
      const isNullable = col.is_nullable === "YES";
      const stat = {
        name: colName,
        dataType,
        nullRate: 0
      };
      if (heavySamplingCount >= MAX_COLUMNS_HEAVY_SAMPLING) {
        columns.push(stat);
        continue;
      }
      try {
        if (isNullable && totalRows > 0) {
          const nullResult = await queryWithTimeout(
            pool2,
            `SELECT COUNT(*) FILTER (WHERE "${colName}" IS NULL) as null_count FROM "${schema}"."${table}"`,
            [],
            2e3
          );
          if (nullResult?.rows[0]) {
            stat.nullRate = parseInt(nullResult.rows[0].null_count, 10) / totalRows;
          }
        }
        if (dataType.includes("timestamp") || dataType.includes("date")) {
          heavySamplingCount++;
          const rangeResult = await queryWithTimeout(
            pool2,
            `SELECT MIN("${colName}")::text as min_val, MAX("${colName}")::text as max_val 
             FROM "${schema}"."${table}" WHERE "${colName}" IS NOT NULL`,
            [],
            3e3
          );
          if (rangeResult?.rows[0]?.min_val) {
            stat.dateRange = {
              min: rangeResult.rows[0].min_val,
              max: rangeResult.rows[0].max_val
            };
          }
        } else if (dataType.includes("int") || dataType.includes("numeric") || dataType.includes("decimal") || dataType === "double precision" || dataType === "real") {
          heavySamplingCount++;
          const numericResult = await queryWithTimeout(
            pool2,
            `SELECT 
              MIN("${colName}")::float as min_val,
              MAX("${colName}")::float as max_val,
              PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY "${colName}")::float as p50,
              PERCENTILE_CONT(0.95) WITHIN GROUP (ORDER BY "${colName}")::float as p95
             FROM "${schema}"."${table}" WHERE "${colName}" IS NOT NULL`,
            [],
            3e3
          );
          if (numericResult?.rows[0]) {
            stat.numericRange = {
              min: numericResult.rows[0].min_val,
              max: numericResult.rows[0].max_val,
              p50: numericResult.rows[0].p50,
              p95: numericResult.rows[0].p95
            };
          }
        } else if (dataType.includes("character") || dataType.includes("text") || dataType === "USER-DEFINED") {
          heavySamplingCount++;
          const distinctCountResult = await queryWithTimeout(
            pool2,
            `SELECT COUNT(DISTINCT "${colName}") as count FROM "${schema}"."${table}" WHERE "${colName}" IS NOT NULL`,
            [],
            2e3
          );
          const distinctCount = distinctCountResult?.rows[0]?.count ? parseInt(distinctCountResult.rows[0].count, 10) : Infinity;
          if (distinctCount <= LOW_CARDINALITY_THRESHOLD) {
            const topValuesResult = await queryWithTimeout(
              pool2,
              `SELECT "${colName}"::text as val, COUNT(*)::int as count 
               FROM "${schema}"."${table}" 
               WHERE "${colName}" IS NOT NULL 
               GROUP BY "${colName}" 
               ORDER BY count DESC 
               LIMIT 15`,
              [],
              3e3
            );
            if (topValuesResult?.rows) {
              stat.topValues = topValuesResult.rows.map((r) => ({
                value: r.val,
                count: parseInt(r.count, 10)
              }));
            }
          }
        }
      } catch {
      }
      columns.push(stat);
    }
    const dictionary = {
      database,
      schema,
      table,
      columns,
      fetchedAt: Date.now(),
      totalRows
    };
    dataDictionaryCache.set(cacheKey, dictionary);
    return dictionary;
  } catch (err) {
    console.error(`Error fetching data dictionary for ${schema}.${table}:`, err);
    return null;
  }
}
function formatDataDictionaryForPrompt(dictionary) {
  const lines = [];
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
      const topVals = col.topValues.slice(0, 8).map((v) => `"${v.value}"`).join(", ");
      colLine += ` [values: ${topVals}${col.topValues.length > 8 ? ", ..." : ""}]`;
    }
    lines.push(colLine);
  }
  return lines.join("\n");
}

// server/ai/roles.ts
var ROLE_MAPPINGS = [
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
  { role: "contact.phone", patterns: [/phone$/i, /^tel$/i, /^telephone$/i, /^mobile$/i, /^cell$/i], dataTypes: ["varchar", "text"] }
];
function inferColumnRole(columnName, dataType) {
  for (const mapping of ROLE_MAPPINGS) {
    const patternMatch = mapping.patterns.some((p) => p.test(columnName));
    const typeMatch = !mapping.dataTypes || mapping.dataTypes.some((t) => dataType.toLowerCase().includes(t.toLowerCase()));
    if (patternMatch && typeMatch) {
      return { column: columnName, dataType, role: mapping.role, confidence: "high" };
    }
    if (patternMatch) {
      return { column: columnName, dataType, role: mapping.role, confidence: "medium" };
    }
  }
  return { column: columnName, dataType, role: null, confidence: "low" };
}
function inferAllColumnRoles(columns) {
  return columns.map((col) => inferColumnRole(col.name, col.dataType));
}
function getDateColumns(columns) {
  return columns.filter((c) => c.dataType.includes("timestamp") || c.dataType.includes("date"));
}
function resolveSemanticReference(query, columns) {
  const queryLower = query.toLowerCase();
  if (/\b(scheduled|appointment|booking|service\s*date)\b/i.test(queryLower)) {
    const scheduled = columns.find((c) => /scheduled|appointment|booking|start/i.test(c.name) && (c.dataType.includes("timestamp") || c.dataType.includes("date")));
    if (scheduled) return { resolvedColumn: scheduled.name, type: "date", needsClarification: false };
  }
  if (/\b(created|added|registered)\b/i.test(queryLower)) {
    const created = columns.find((c) => /created|inserted/i.test(c.name) && (c.dataType.includes("timestamp") || c.dataType.includes("date")));
    if (created) return { resolvedColumn: created.name, type: "date", needsClarification: false };
  }
  if (/\b(updated|modified|changed)\b/i.test(queryLower)) {
    const updated = columns.find((c) => /updated|modified/i.test(c.name) && (c.dataType.includes("timestamp") || c.dataType.includes("date")));
    if (updated) return { resolvedColumn: updated.name, type: "date", needsClarification: false };
  }
  if (/\b(completed|finished|done)\b/i.test(queryLower)) {
    const completed = columns.find((c) => /completed|finished|done|end/i.test(c.name) && (c.dataType.includes("timestamp") || c.dataType.includes("date")));
    if (completed) return { resolvedColumn: completed.name, type: "date", needsClarification: false };
  }
  const dateColumns = getDateColumns(columns);
  if (dateColumns.length > 1) {
    if (/\b(yesterday|today|this\s+(week|month|year)|last\s+(week|month|year)|(jan|feb|mar|apr|may|jun|jul|aug|sep|oct|nov|dec)|20\d{2})\b/i.test(queryLower)) {
      return {
        resolvedColumn: null,
        type: "date",
        needsClarification: true,
        options: dateColumns.map((c) => c.name)
      };
    }
  } else if (dateColumns.length === 1) {
    return { resolvedColumn: dateColumns[0].name, type: "date", needsClarification: false };
  }
  return { resolvedColumn: null, type: null, needsClarification: false };
}
function formatRolesForPrompt(columns) {
  const roles = inferAllColumnRoles(columns.map((c) => ({ name: c.name, dataType: c.dataType })));
  const highConfidence = roles.filter((r) => r.role && r.confidence === "high");
  if (highConfidence.length === 0) return "";
  const lines = ["\nDetected Column Roles:"];
  for (const role of highConfidence) {
    lines.push(`  - ${role.column}: ${role.role}`);
  }
  return lines.join("\n");
}

// server/ai/prompts.ts
function getPacificDateString() {
  const today = /* @__PURE__ */ new Date();
  const pacificFormatter = new Intl.DateTimeFormat("en-CA", {
    timeZone: "America/Los_Angeles",
    year: "numeric",
    month: "2-digit",
    day: "2-digit"
  });
  return pacificFormatter.format(today);
}
function buildNLQSystemPrompt(params) {
  const { table, dictionary, columns, dateColumns, context } = params;
  const todayStr = getPacificDateString();
  const columnInfo = dictionary ? formatDataDictionaryForPrompt(dictionary) : columns.map((c) => `  - ${c.name} (${c.dataType})`).join("\n");
  const rolesInfo = dictionary ? formatRolesForPrompt(dictionary.columns) : "";
  return `You are a helpful assistant that converts natural language queries into structured query plans for a database viewer.

IMPORTANT: Today's date is ${todayStr} (Pacific Time - PST/PDT). Use this for any relative date references like "yesterday", "last week", "this month", etc. All date/time queries should be interpreted in Pacific Time.

The user is querying the table: ${table}

${columnInfo}
${rolesInfo}

You must return ONLY a valid JSON object with this structure:
{
  "action": "plan" | "clarify" | "suggest",
  "table": "${table}",
  "page": 1,
  "filters": [
    {"column": "column_name", "op": "operator", "value": "filter_value"}
  ],
  "questions": ["question1", "question2"],
  "suggestions": [
    {"description": "description of suggested analysis", "filters": [...]}
  ],
  "explain": {
    "table": "${table}",
    "resolvedDateColumn": "column_name or null",
    "timeframe": {"start": "YYYY-MM-DD", "end": "YYYY-MM-DD", "timezone": "America/Los_Angeles"},
    "filtersApplied": [{"column": "...", "operator": "...", "value": "...", "interpretation": "..."}],
    "sortApplied": null,
    "page": 1,
    "limit": 50
  },
  "summary": "A brief description of what this filter does"
}

ACTION TYPES:
- "plan": You understand the query and can create filters. Include filters, explain, and summary.
- "clarify": The query is ambiguous. Include 1-3 questions to clarify. Do NOT guess.
- "suggest": User asked for recommendations. Include 2-5 suggestions for useful analyses.

Valid operators are:
- eq: equals (exact match)
- contains: substring match (case-insensitive)
- gt: greater than
- gte: greater than or equal to
- lt: less than
- lte: less than or equal to
- between: range filter (value should be array like ["2024-01-01", "2024-12-31"])
- in: match any value in a list (value should be array like ["value1", "value2", "value3"]) - USE THIS when user provides a list of specific values to match

IMPORTANT RULES:
1. Always use the table "${table}" - do not change it
2. Only use columns that exist in the column list above
3. For date queries with multiple date columns (${dateColumns.join(", ") || "none"}):
   - If user says "scheduled", "appointment", "booking" -> use scheduled_at/appointment_at if available
   - If user says "created", "added", "registered" -> use created_at if available
   - If user says "updated", "modified" -> use updated_at if available
   - If still ambiguous, set action: "clarify" and ask which date column to use
4. For date ranges like "last week", "this month", "2024", use the "between" operator with [start, end] array
5. Always include the "explain" object when action is "plan"
6. Only return valid JSON, no explanation or markdown

${context ? `Previous context from conversation:
${context}
` : ""}`;
}
function buildSmartFollowupPrompt(params) {
  const { table, filters, samplingInfo, context } = params;
  const todayStr = getPacificDateString();
  return `You are a helpful assistant that helps users find data when their query returns no results.

Today's date is ${todayStr} (Pacific Time - PST/PDT).

The user's query on table "${table}" returned 0 results with these filters:
${filters.map((f) => `- ${f.column} ${f.op} "${f.value}"`).join("\n")}

Here's what we found when sampling the actual data:
${samplingInfo}

ANALYZE the mismatch and return ONLY a valid JSON object:
{
  "likelyIssue": "value_mismatch" | "case_mismatch" | "date_out_of_range" | "null_column" | "synonym_mismatch" | "typo" | "unknown",
  "suggestedChanges": [
    {
      "filterIndex": 0,
      "column": "column_name",
      "currentValue": "what user searched",
      "suggestedValue": "corrected value",
      "suggestedOperator": "contains",
      "reason": "explanation"
    }
  ],
  "questions": ["optional clarifying questions"],
  "evidence": {
    "sampledValues": {"column_name": ["actual", "values"]},
    "dateRanges": {"date_column": {"min": "2023-01-01", "max": "2024-12-31"}}
  },
  "clarificationQuestion": "Main question to ask user",
  "suggestedFilters": [
    {"column": "...", "op": "...", "value": "..."}
  ],
  "summary": "Brief explanation of the issue"
}

RECOVERY STRATEGIES (apply in order):
1. Case/spacing mismatch: Suggest "contains" operator instead of "eq"
2. Synonym mismatch: If user searched "completed" but data has "done", suggest the actual value
3. Typo: Use Levenshtein-like matching to find similar values
4. Date out of range: If date filter is outside actual range, suggest valid range
5. Null column: If the filtered column is mostly null, suggest alternative column
6. Remove filter: Suggest removing one filter to isolate the culprit

${context ? `Previous context:
${context}
` : ""}`;
}

// server/ai/validators.ts
import { z as z2 } from "zod";
var nlqActionSchema2 = z2.enum(["clarify", "plan", "suggest"]);
var timeframeSchema2 = z2.object({
  start: z2.string(),
  end: z2.string(),
  timezone: z2.string().default("America/Los_Angeles"),
  mode: z2.enum(["rolling", "calendar"]).optional()
});
var nlqFilterSchema2 = z2.object({
  column: z2.string(),
  op: z2.enum(["eq", "contains", "gt", "gte", "lt", "lte", "between"]),
  value: z2.union([z2.string(), z2.array(z2.string())])
});
var nlqExplainSchema2 = z2.object({
  table: z2.string(),
  resolvedDateColumn: z2.string().nullable().optional(),
  timeframe: timeframeSchema2.nullable().optional(),
  filtersApplied: z2.array(z2.object({
    column: z2.string(),
    operator: z2.string(),
    value: z2.union([z2.string(), z2.array(z2.string())]),
    interpretation: z2.string().optional()
  })).optional(),
  sortApplied: z2.object({
    column: z2.string(),
    direction: z2.enum(["asc", "desc"])
  }).nullable().optional(),
  page: z2.number().optional(),
  limit: z2.number().optional()
});
var nlqResponseSchema = z2.object({
  action: nlqActionSchema2,
  table: z2.string(),
  page: z2.number().default(1),
  filters: z2.array(nlqFilterSchema2).default([]),
  questions: z2.array(z2.string()).optional(),
  suggestions: z2.array(z2.object({
    description: z2.string(),
    filters: z2.array(nlqFilterSchema2).optional(),
    chartType: z2.string().optional()
  })).optional(),
  explain: nlqExplainSchema2.optional(),
  summary: z2.string().optional(),
  needsClarification: z2.boolean().optional(),
  clarificationQuestion: z2.string().optional(),
  ambiguousColumns: z2.array(z2.string()).optional()
});
var smartFollowupIssueSchema2 = z2.enum([
  "value_mismatch",
  "case_mismatch",
  "date_out_of_range",
  "null_column",
  "synonym_mismatch",
  "typo",
  "unknown"
]);
var smartFollowupResponseSchema2 = z2.object({
  likelyIssue: smartFollowupIssueSchema2,
  suggestedChanges: z2.array(z2.object({
    filterIndex: z2.number(),
    column: z2.string(),
    currentValue: z2.string(),
    suggestedValue: z2.string().optional(),
    suggestedOperator: z2.string().optional(),
    reason: z2.string()
  })),
  questions: z2.array(z2.string()).optional(),
  evidence: z2.object({
    sampledValues: z2.record(z2.string(), z2.array(z2.string())).optional(),
    dateRanges: z2.record(z2.string(), z2.object({ min: z2.string(), max: z2.string() })).optional()
  }).optional(),
  clarificationQuestion: z2.string().optional(),
  suggestedFilters: z2.array(nlqFilterSchema2).optional(),
  summary: z2.string().optional()
});
var reportBlockActionSchema = z2.object({
  kind: z2.enum(["table", "chart", "metric", "text"]),
  title: z2.string(),
  config: z2.record(z2.unknown())
});
var reportChatResponseSchema = z2.object({
  action: z2.enum(["clarify", "create_block", "create_blocks", "modify_block", "delete_block", "explain", "none"]),
  questions: z2.array(z2.string()).optional(),
  block: reportBlockActionSchema.optional(),
  blocks: z2.array(reportBlockActionSchema).optional(),
  blockId: z2.string().optional(),
  explanation: z2.string().optional()
});
function extractJSON(text2) {
  const cleaned = text2.replace(/```json\n?|\n?```/g, "").trim();
  const jsonMatch = cleaned.match(/\{[\s\S]*\}/);
  if (jsonMatch) {
    return jsonMatch[0];
  }
  return null;
}
async function parseAndValidateNLQResponse(content, table, validColumns, retryFn) {
  const jsonStr = extractJSON(content);
  if (!jsonStr) {
    if (retryFn) {
      const retryContent = await retryFn();
      return parseAndValidateNLQResponse(retryContent, table, validColumns);
    }
    return { success: false, error: "Failed to extract JSON from response" };
  }
  let parsed;
  try {
    parsed = JSON.parse(jsonStr);
  } catch {
    if (retryFn) {
      const retryContent = await retryFn();
      return parseAndValidateNLQResponse(retryContent, table, validColumns);
    }
    return { success: false, error: "Failed to parse JSON response" };
  }
  const result = nlqResponseSchema.safeParse(parsed);
  if (!result.success) {
    if (retryFn) {
      const retryContent = await retryFn();
      return parseAndValidateNLQResponse(retryContent, table, validColumns);
    }
    return { success: false, error: `Invalid response schema: ${result.error.message}` };
  }
  const data = result.data;
  data.table = table;
  if (data.action === "plan" && data.filters) {
    for (const filter of data.filters) {
      if (validColumns.length > 0 && !validColumns.includes(filter.column)) {
        return { success: false, error: `Invalid column: ${filter.column}` };
      }
    }
  }
  return { success: true, data };
}
async function parseAndValidateSmartFollowupResponse(content, retryFn) {
  const jsonStr = extractJSON(content);
  if (!jsonStr) {
    if (retryFn) {
      const retryContent = await retryFn();
      return parseAndValidateSmartFollowupResponse(retryContent);
    }
    return { success: false, error: "Failed to extract JSON from response" };
  }
  let parsed;
  try {
    parsed = JSON.parse(jsonStr);
  } catch {
    if (retryFn) {
      const retryContent = await retryFn();
      return parseAndValidateSmartFollowupResponse(retryContent);
    }
    return { success: false, error: "Failed to parse JSON response" };
  }
  const result = smartFollowupResponseSchema2.safeParse(parsed);
  if (!result.success) {
    if (retryFn) {
      const retryContent = await retryFn();
      return parseAndValidateSmartFollowupResponse(retryContent);
    }
    return { success: false, error: `Invalid response schema: ${result.error.message}` };
  }
  return { success: true, data: result.data };
}

// server/routes.ts
var PAGE_SIZE = 50;
var IDENTIFIER_REGEX = /^[a-zA-Z_][a-zA-Z0-9_]*$/;
function isValidIdentifier(name) {
  return IDENTIFIER_REGEX.test(name) && name.length <= 128;
}
function validateIdentifier(name, type) {
  if (!isValidIdentifier(name)) {
    throw new Error(`Invalid ${type} identifier: ${name}`);
  }
}
function getDatabaseConnections() {
  const urlsEnv = process.env.DATABASE_URLS;
  if (!urlsEnv) {
    return [];
  }
  const trimmed = urlsEnv.trim();
  if (trimmed.startsWith("[")) {
    try {
      const parsed = JSON.parse(trimmed);
      if (!Array.isArray(parsed)) {
        console.error("DATABASE_URLS JSON must be an array");
        return [];
      }
      return parsed.filter((db2) => db2.name && db2.url);
    } catch (err) {
      console.error("Failed to parse DATABASE_URLS as JSON:", err);
      return [];
    }
  }
  if (trimmed.startsWith("postgres://") || trimmed.startsWith("postgresql://")) {
    return [{ name: "Default", url: trimmed }];
  }
  console.error("DATABASE_URLS must be a JSON array or a valid postgres:// connection string");
  return [];
}
var pools = /* @__PURE__ */ new Map();
function getPool(dbName) {
  const existing = pools.get(dbName);
  if (existing) return existing;
  const dbs = getDatabaseConnections();
  const db2 = dbs.find((d) => d.name === dbName);
  if (!db2) {
    throw new Error(`Database not found: ${dbName}`);
  }
  const sslConfig = process.env.DB_SSL_REJECT_UNAUTHORIZED === "false" ? { rejectUnauthorized: false } : { rejectUnauthorized: true };
  const pool2 = new Pool2({
    connectionString: db2.url,
    max: 5,
    idleTimeoutMillis: 3e4,
    ssl: sslConfig
  });
  pools.set(dbName, pool2);
  return pool2;
}
function convertPSTDateToUTC(dateStr, isEndOfRange = false) {
  const dateOnlyMatch = dateStr.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (dateOnlyMatch) {
    const [, year, month, day] = dateOnlyMatch;
    if (isEndOfRange) {
      const date = /* @__PURE__ */ new Date(`${year}-${month}-${day}T00:00:00-08:00`);
      date.setDate(date.getDate() + 1);
      return date.toISOString();
    } else {
      const date = /* @__PURE__ */ new Date(`${year}-${month}-${day}T00:00:00-08:00`);
      return date.toISOString();
    }
  }
  const dateTimeMatch = dateStr.match(/^(\d{4})-(\d{2})-(\d{2})\s+(\d{2}):(\d{2}):(\d{2})$/);
  if (dateTimeMatch) {
    const [, year, month, day, hour, minute, second] = dateTimeMatch;
    if (hour === "00" && minute === "00" && second === "00") {
      const date = /* @__PURE__ */ new Date(`${year}-${month}-${day}T00:00:00-08:00`);
      return date.toISOString();
    } else if (hour === "23" && minute === "59" && second === "59") {
      const date = /* @__PURE__ */ new Date(`${year}-${month}-${day}T00:00:00-08:00`);
      date.setDate(date.getDate() + 1);
      return date.toISOString();
    } else {
      const date = /* @__PURE__ */ new Date(`${year}-${month}-${day}T${hour}:${minute}:${second}-08:00`);
      return date.toISOString();
    }
  }
  return dateStr;
}
function getOperatorSQL(operator, paramIndex) {
  switch (operator) {
    case "eq":
      return { sql: `= $${paramIndex}` };
    case "contains":
      return {
        sql: `ILIKE $${paramIndex}`,
        transform: (v) => `%${v}%`
      };
    case "gt":
      return { sql: `> $${paramIndex}` };
    case "gte":
      return { sql: `>= $${paramIndex}` };
    case "lt":
      return { sql: `< $${paramIndex}` };
    case "lte":
      return { sql: `<= $${paramIndex}` };
    case "between":
      return {
        sql: `BETWEEN $${paramIndex} AND $${paramIndex + 1}`,
        paramCount: 2
      };
    case "in":
      return {
        sql: `= ANY($${paramIndex})`,
        paramCount: 1
      };
    default:
      throw new Error(`Invalid operator: ${operator}`);
  }
}
function addFilterToQuery(f, params, whereClauses) {
  validateIdentifier(f.column, "column");
  const opInfo = getOperatorSQL(f.operator, params.length + 1);
  whereClauses.push(`"${f.column}" ${opInfo.sql}`);
  if (f.operator === "between" && Array.isArray(f.value)) {
    const startValue = convertPSTDateToUTC(f.value[0], false);
    const endValue = convertPSTDateToUTC(f.value[1], true);
    params.push(startValue, endValue);
  } else if (f.operator === "in" && Array.isArray(f.value)) {
    params.push(f.value);
  } else if (["gt", "gte", "lt", "lte", "eq"].includes(f.operator) && typeof f.value === "string") {
    const dateMatch = f.value.match(/^\d{4}-\d{2}-\d{2}$/);
    if (dateMatch) {
      const converted = convertPSTDateToUTC(f.value, f.operator === "lte" || f.operator === "lt");
      params.push(opInfo.transform ? opInfo.transform(converted) : converted);
    } else {
      params.push(opInfo.transform ? opInfo.transform(f.value) : f.value);
    }
  } else {
    params.push(opInfo.transform ? opInfo.transform(f.value) : f.value);
  }
}
function addFilterToQueryWithAlias(f, params, whereClauses) {
  const opInfo = getOperatorSQL(f.operator, params.length + 1);
  whereClauses.push(`${f.column} ${opInfo.sql}`);
  if (f.operator === "between" && Array.isArray(f.value)) {
    const startValue = convertPSTDateToUTC(f.value[0], false);
    const endValue = convertPSTDateToUTC(f.value[1], true);
    params.push(startValue, endValue);
  } else if (f.operator === "in" && Array.isArray(f.value)) {
    params.push(f.value);
  } else if (["gt", "gte", "lt", "lte", "eq"].includes(f.operator) && typeof f.value === "string") {
    const dateMatch = f.value.match(/^\d{4}-\d{2}-\d{2}$/);
    if (dateMatch) {
      const converted = convertPSTDateToUTC(f.value, f.operator === "lte" || f.operator === "lt");
      params.push(opInfo.transform ? opInfo.transform(converted) : converted);
    } else {
      params.push(opInfo.transform ? opInfo.transform(f.value) : f.value);
    }
  } else {
    params.push(opInfo.transform ? opInfo.transform(f.value) : f.value);
  }
}
function requireRole(...allowedRoles) {
  return async (req, res, next) => {
    const userId = req.user?.id;
    if (!userId) {
      return res.status(401).json({ error: "Unauthorized" });
    }
    const user = await authStorage.getUser(userId);
    if (!user || !user.isActive) {
      return res.status(403).json({ error: "Account is inactive" });
    }
    if (!allowedRoles.includes(user.role)) {
      return res.status(403).json({ error: "Insufficient permissions" });
    }
    req.currentUser = user;
    next();
  };
}
async function getAllowedTables(userId) {
  const grants = await db.select().from(tableGrants).where(eq2(tableGrants.userId, userId));
  return grants.map((g) => `${g.database}:${g.tableName}`);
}
function parseTableName(tableName) {
  if (tableName.includes(".")) {
    const parts = tableName.split(".");
    if (parts.length !== 2) return null;
    const [schema, table] = parts;
    if (!isValidIdentifier(schema) || !isValidIdentifier(table)) return null;
    return { schema, table };
  } else {
    if (!isValidIdentifier(tableName)) return null;
    return { schema: "public", table: tableName };
  }
}
async function validateTableAccess(dbName, tableName, user, options = {}) {
  try {
    const parsed = parseTableName(tableName);
    if (!parsed) {
      return { valid: false, error: "Invalid table name" };
    }
    const pool2 = getPool(dbName);
    const tableResult = await pool2.query(`
      SELECT table_name FROM information_schema.tables 
      WHERE table_schema = $1 AND table_name = $2
    `, [parsed.schema, parsed.table]);
    if (tableResult.rows.length === 0) {
      return { valid: false, error: "Table not found" };
    }
    if (user.role === "external_customer") {
      const allowedTables = await getAllowedTables(user.id);
      if (!allowedTables.includes(`${dbName}:${parsed.schema}.${parsed.table}`)) {
        return { valid: false, error: "Access denied to this table" };
      }
    }
    if (!options.bypassVisibility && user.role !== "admin") {
      const allSettings = await storage.getAllTableSettings();
      const settingsKey = `${dbName}:${parsed.schema}.${parsed.table}`;
      const tableSettings = allSettings[settingsKey];
      if (tableSettings && tableSettings.isVisible === false) {
        return { valid: false, error: "Access denied to this table" };
      }
    }
    return { valid: true, parsedTable: parsed };
  } catch (err) {
    console.error("Error validating table access:", err);
    return { valid: false, error: "Failed to validate table access" };
  }
}
function levenshteinDistance(a, b) {
  const matrix = [];
  const aLower = a.toLowerCase();
  const bLower = b.toLowerCase();
  for (let i = 0; i <= bLower.length; i++) {
    matrix[i] = [i];
  }
  for (let j = 0; j <= aLower.length; j++) {
    matrix[0][j] = j;
  }
  for (let i = 1; i <= bLower.length; i++) {
    for (let j = 1; j <= aLower.length; j++) {
      if (bLower.charAt(i - 1) === aLower.charAt(j - 1)) {
        matrix[i][j] = matrix[i - 1][j - 1];
      } else {
        matrix[i][j] = Math.min(
          matrix[i - 1][j - 1] + 1,
          matrix[i][j - 1] + 1,
          matrix[i - 1][j] + 1
        );
      }
    }
  }
  return matrix[bLower.length][aLower.length];
}
function findSimilarColumns(target, columns, maxSuggestions = 3) {
  const normalizedTarget = target.toLowerCase().replace(/_/g, "");
  const scored = columns.map((col) => {
    const normalizedCol = col.toLowerCase().replace(/_/g, "");
    const distance = levenshteinDistance(normalizedTarget, normalizedCol);
    const maxLen = Math.max(normalizedTarget.length, normalizedCol.length);
    const similarity = maxLen > 0 ? 1 - distance / maxLen : 0;
    return { col, similarity, distance };
  });
  return scored.filter((s) => s.similarity > 0.4).sort((a, b) => b.similarity - a.similarity).slice(0, maxSuggestions).map((s) => s.col);
}
async function validateColumns(dbName, tableName, columns) {
  try {
    if (columns.length === 0) return { valid: true };
    const parsed = parseTableName(tableName);
    if (!parsed) {
      return { valid: false, error: "Invalid table name" };
    }
    for (const col of columns) {
      if (!isValidIdentifier(col)) {
        return { valid: false, error: `Invalid column name: ${col}` };
      }
    }
    const pool2 = getPool(dbName);
    const columnResult = await pool2.query(`
      SELECT column_name FROM information_schema.columns 
      WHERE table_schema = $1 AND table_name = $2
    `, [parsed.schema, parsed.table]);
    const existingColumnsArray = columnResult.rows.map((r) => r.column_name);
    const existingColumns = new Set(existingColumnsArray);
    for (const col of columns) {
      if (!existingColumns.has(col)) {
        const suggestions = findSimilarColumns(col, existingColumnsArray);
        const suggestionText = suggestions.length > 0 ? ` Did you mean: ${suggestions.map((s) => `'${s}'`).join(", ")}?` : "";
        return { valid: false, error: `Column not found: ${col}.${suggestionText}` };
      }
    }
    return { valid: true };
  } catch (err) {
    console.error("Error validating columns:", err);
    return { valid: false, error: "Failed to validate columns" };
  }
}
async function validateBlockConfig(config, kind, user, options = {}) {
  if (kind === "text") {
    return { valid: true };
  }
  if (!config.database || !config.table) {
    return { valid: false, error: "Database and table are required" };
  }
  const dbs = getDatabaseConnections();
  if (!dbs.find((d) => d.name === config.database)) {
    return { valid: false, error: "Database not found" };
  }
  const tableValidation = await validateTableAccess(config.database, config.table, user, options);
  if (!tableValidation.valid) {
    return tableValidation;
  }
  let joinTableColumns = [];
  let mainTableColumns = [];
  const mainParsed = parseTableName(config.table);
  if (mainParsed) {
    const pool2 = getPool(config.database);
    const mainColResult = await pool2.query(`
      SELECT column_name FROM information_schema.columns 
      WHERE table_schema = $1 AND table_name = $2
    `, [mainParsed.schema, mainParsed.table]);
    mainTableColumns = mainColResult.rows.map((r) => r.column_name);
  }
  let subJoinTableColumns = [];
  if (config.join?.table) {
    const joinTableValidation = await validateTableAccess(config.database, config.join.table, user, options);
    if (!joinTableValidation.valid) {
      return { valid: false, error: `Join table access error: ${joinTableValidation.error}` };
    }
    if (!config.join.on || config.join.on.length !== 2) {
      return { valid: false, error: "Join 'on' must specify two columns [fromColumn, toColumn]" };
    }
    const [fromCol, toCol] = config.join.on;
    if (!mainTableColumns.includes(fromCol)) {
      return { valid: false, error: `Join column '${fromCol}' not found in main table` };
    }
    const joinParsed = parseTableName(config.join.table);
    if (joinParsed) {
      const pool2 = getPool(config.database);
      const joinColResult = await pool2.query(`
        SELECT column_name FROM information_schema.columns 
        WHERE table_schema = $1 AND table_name = $2
      `, [joinParsed.schema, joinParsed.table]);
      joinTableColumns = joinColResult.rows.map((r) => r.column_name);
      if (!joinTableColumns.includes(toCol)) {
        return { valid: false, error: `Join column '${toCol}' not found in joined table` };
      }
      if (config.join.subJoin?.table) {
        const subJoinTableValidation = await validateTableAccess(config.database, config.join.subJoin.table, user, options);
        if (!subJoinTableValidation.valid) {
          return { valid: false, error: `SubJoin table access error: ${subJoinTableValidation.error}` };
        }
        if (!config.join.subJoin.on || config.join.subJoin.on.length !== 2) {
          return { valid: false, error: "SubJoin 'on' must specify two columns [fromColumn, toColumn]" };
        }
        const [subFromCol, subToCol] = config.join.subJoin.on;
        if (!joinTableColumns.includes(subFromCol)) {
          return { valid: false, error: `SubJoin column '${subFromCol}' not found in join table` };
        }
        const subJoinParsed = parseTableName(config.join.subJoin.table);
        if (subJoinParsed) {
          const subJoinColResult = await pool2.query(`
            SELECT column_name FROM information_schema.columns 
            WHERE table_schema = $1 AND table_name = $2
          `, [subJoinParsed.schema, subJoinParsed.table]);
          subJoinTableColumns = subJoinColResult.rows.map((r) => r.column_name);
          if (!subJoinTableColumns.includes(subToCol)) {
            return { valid: false, error: `SubJoin column '${subToCol}' not found in subJoin table` };
          }
        }
      }
    }
  }
  const validateDottedColumn = (col) => {
    const parts = col.split(".");
    if (parts.length !== 2) {
      return { valid: false, error: `Invalid column reference '${col}': must be 'alias.column' format with exactly one dot` };
    }
    const [prefix, colName] = parts;
    if (!colName || !isValidIdentifier(colName)) {
      return { valid: false, error: `Invalid column name in '${col}'` };
    }
    if (!config.join) {
      return { valid: false, error: `Cannot use join column reference '${col}' without a join configuration` };
    }
    const isSubJoin = prefix.includes("_") || prefix.toLowerCase().includes("district") || prefix.toLowerCase().includes("sub");
    return { valid: true, colName, isSubJoin };
  };
  const columnsToValidate = [];
  const joinColumnsToValidate = [];
  const subJoinColumnsToValidate = [];
  if (kind === "table" && config.columns?.length > 0) {
    for (const col of config.columns) {
      if (col.includes(".")) {
        const result = validateDottedColumn(col);
        if (!result.valid) {
          return { valid: false, error: result.error };
        }
        if (result.isSubJoin && config.join?.subJoin) {
          subJoinColumnsToValidate.push(result.colName);
        } else {
          joinColumnsToValidate.push(result.colName);
        }
      } else {
        columnsToValidate.push(col);
      }
    }
  }
  if (kind === "table" && config.orderBy?.column) {
    if (config.orderBy.column.includes(".")) {
      const result = validateDottedColumn(config.orderBy.column);
      if (!result.valid) {
        return { valid: false, error: result.error };
      }
      if (result.isSubJoin && config.join?.subJoin) {
        subJoinColumnsToValidate.push(result.colName);
      } else {
        joinColumnsToValidate.push(result.colName);
      }
    } else {
      columnsToValidate.push(config.orderBy.column);
    }
  }
  if (kind === "chart") {
    const validateChartColumn = (col) => {
      if (col.includes(".")) {
        const result = validateDottedColumn(col);
        if (!result.valid) {
          return { valid: false, error: result.error };
        }
        if (result.isSubJoin && config.join?.subJoin) {
          subJoinColumnsToValidate.push(result.colName);
        } else {
          joinColumnsToValidate.push(result.colName);
        }
      } else {
        columnsToValidate.push(col);
      }
      return { valid: true };
    };
    if (config.xColumn) {
      const result = validateChartColumn(config.xColumn);
      if (!result.valid) return { valid: false, error: result.error };
    }
    if (config.yColumn) {
      const result = validateChartColumn(config.yColumn);
      if (!result.valid) return { valid: false, error: result.error };
    }
    const dateGroupByValues = ["month", "year", "day", "week", "quarter"];
    if (config.groupBy && !dateGroupByValues.includes(config.groupBy.toLowerCase())) {
      const result = validateChartColumn(config.groupBy);
      if (!result.valid) return { valid: false, error: result.error };
    }
  }
  if (kind === "metric" && config.column) {
    if (config.column.includes(".")) {
      const result = validateDottedColumn(config.column);
      if (!result.valid) {
        return { valid: false, error: result.error };
      }
      if (result.isSubJoin && config.join?.subJoin) {
        subJoinColumnsToValidate.push(result.colName);
      } else {
        joinColumnsToValidate.push(result.colName);
      }
    } else {
      columnsToValidate.push(config.column);
    }
  }
  if (config.filters?.length > 0) {
    for (const f of config.filters) {
      if (f.column) {
        if (f.column.includes(".")) {
          const result = validateDottedColumn(f.column);
          if (!result.valid) {
            return { valid: false, error: result.error };
          }
          if (result.isSubJoin && config.join?.subJoin) {
            subJoinColumnsToValidate.push(result.colName);
          } else {
            joinColumnsToValidate.push(result.colName);
          }
        } else {
          columnsToValidate.push(f.column);
        }
      }
      const validOps = ["eq", "contains", "gt", "gte", "lt", "lte", "between", "in"];
      if (!validOps.includes(f.operator)) {
        return { valid: false, error: `Invalid filter operator: ${f.operator}` };
      }
    }
  }
  const columnValidation = await validateColumns(config.database, config.table, columnsToValidate);
  if (!columnValidation.valid) {
    return columnValidation;
  }
  if (joinColumnsToValidate.length > 0 && joinTableColumns.length > 0) {
    for (const col of joinColumnsToValidate) {
      if (!joinTableColumns.includes(col)) {
        const suggestions = findSimilarColumns(col, joinTableColumns);
        const suggestionText = suggestions.length > 0 ? ` Did you mean: ${suggestions.map((s) => `'${s}'`).join(", ")}?` : "";
        console.log(`[DEBUG] Join validation failed - Looking for column '${col}' in joined table '${config.join?.table}'. Available columns: [${joinTableColumns.join(", ")}]`);
        return { valid: false, error: `Column not found in joined table: ${col}.${suggestionText}` };
      }
    }
  }
  if (subJoinColumnsToValidate.length > 0 && subJoinTableColumns.length > 0) {
    for (const col of subJoinColumnsToValidate) {
      if (!subJoinTableColumns.includes(col)) {
        const suggestions = findSimilarColumns(col, subJoinTableColumns);
        const suggestionText = suggestions.length > 0 ? ` Did you mean: ${suggestions.map((s) => `'${s}'`).join(", ")}?` : "";
        console.log(`[DEBUG] SubJoin validation failed - Looking for column '${col}' in subJoin table '${config.join?.subJoin?.table}'. Available columns: [${subJoinTableColumns.join(", ")}]`);
        return { valid: false, error: `Column not found in subJoin table: ${col}.${suggestionText}` };
      }
    }
  }
  if (config.aggregateFunction) {
    const validAggs = ["count", "sum", "avg", "min", "max"];
    if (!validAggs.includes(config.aggregateFunction.toLowerCase())) {
      return { valid: false, error: `Invalid aggregate function: ${config.aggregateFunction}` };
    }
  }
  if (kind === "chart" && config.chartType) {
    const validChartTypes = ["bar", "line", "pie", "area"];
    if (!validChartTypes.includes(config.chartType)) {
      return { valid: false, error: `Invalid chart type: ${config.chartType}` };
    }
  }
  return { valid: true };
}
async function logAudit(entry) {
  try {
    await db.insert(auditLogs).values({
      userId: entry.userId,
      userEmail: entry.userEmail,
      action: entry.action,
      database: entry.database || null,
      tableName: entry.table || null,
      details: entry.details || null,
      ipAddress: entry.ip || null
    });
    console.log(`[AUDIT] ${(/* @__PURE__ */ new Date()).toISOString()} | User: ${entry.userEmail} | Action: ${entry.action} | DB: ${entry.database || "-"} | Table: ${entry.table || "-"} | ${entry.details || ""}`);
  } catch (err) {
    console.error("Failed to write audit log:", err);
  }
}
async function registerRoutes(httpServer, app2) {
  await setupAuth(app2);
  registerAuthRoutes(app2);
  const generalLimiter = rateLimit({
    windowMs: 60 * 1e3,
    // 1 minute
    max: 100,
    message: { error: "Too many requests, please try again later" },
    standardHeaders: true,
    legacyHeaders: false
  });
  const authLimiter = rateLimit({
    windowMs: 60 * 1e3,
    // 1 minute
    max: 10,
    message: { error: "Too many login attempts, please try again later" },
    standardHeaders: true,
    legacyHeaders: false
  });
  const exportLimiter = rateLimit({
    windowMs: 60 * 1e3,
    // 1 minute
    max: 10,
    message: { error: "Too many export requests, please try again later" },
    standardHeaders: true,
    legacyHeaders: false
  });
  const nlqLimiter = rateLimit({
    windowMs: 60 * 1e3,
    // 1 minute
    max: 20,
    message: { error: "Too many AI query requests, please try again later" },
    standardHeaders: true,
    legacyHeaders: false
  });
  app2.use("/api/", generalLimiter);
  app2.use("/api/auth/login", authLimiter);
  app2.use("/api/auth/register", authLimiter);
  app2.use("/api/export", exportLimiter);
  app2.use("/api/nlq", nlqLimiter);
  app2.get("/api/admin/users", isAuthenticated, requireRole("admin"), async (req, res) => {
    try {
      const allUsers = await db.select().from(users);
      res.json(allUsers);
    } catch (err) {
      console.error("Error fetching users:", err);
      res.status(500).json({ error: "Failed to fetch users" });
    }
  });
  app2.patch("/api/admin/users/:userId", isAuthenticated, requireRole("admin"), async (req, res) => {
    try {
      const { userId } = req.params;
      const { role, isActive, firstName, lastName, email, password } = req.body;
      const updates = { updatedAt: /* @__PURE__ */ new Date() };
      if (role && ["admin", "washos_user", "external_customer"].includes(role)) {
        updates.role = role;
      }
      if (typeof isActive === "boolean") {
        updates.isActive = isActive;
      }
      if (typeof firstName === "string") {
        updates.firstName = firstName.trim();
      }
      if (typeof lastName === "string") {
        updates.lastName = lastName.trim();
      }
      if (typeof email === "string") {
        const normalizedEmail = email.toLowerCase().trim();
        const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
        if (!emailRegex.test(normalizedEmail)) {
          return res.status(400).json({ error: "Invalid email format" });
        }
        const existingUser = await db.select().from(users).where(eq2(users.email, normalizedEmail)).limit(1);
        if (existingUser.length > 0 && existingUser[0].id !== userId) {
          return res.status(400).json({ error: "Email already in use" });
        }
        updates.email = normalizedEmail;
      }
      if (typeof password === "string" && password.length > 0) {
        if (password.length < 4) {
          return res.status(400).json({ error: "Password must be at least 4 characters" });
        }
        const bcrypt2 = await import("bcryptjs");
        updates.password = await bcrypt2.hash(password, 10);
      }
      const [updated] = await db.update(users).set(updates).where(eq2(users.id, userId)).returning();
      if (!updated) {
        return res.status(404).json({ error: "User not found" });
      }
      res.json(updated);
    } catch (err) {
      console.error("Error updating user:", err);
      res.status(500).json({ error: "Failed to update user" });
    }
  });
  app2.post("/api/admin/users", isAuthenticated, requireRole("admin"), async (req, res) => {
    try {
      const { email, password, firstName, lastName, role } = req.body;
      if (!email || !password) {
        return res.status(400).json({ error: "Email and password are required" });
      }
      const normalizedEmail = email.toLowerCase().trim();
      const emailRegex = /^[^\s@]+@[^\s@]+\.[^\s@]+$/;
      if (!emailRegex.test(normalizedEmail)) {
        return res.status(400).json({ error: "Invalid email format" });
      }
      if (password.length < 4) {
        return res.status(400).json({ error: "Password must be at least 4 characters" });
      }
      const existingUser = await authStorage.getUserByEmail(normalizedEmail);
      if (existingUser) {
        return res.status(400).json({ error: "Email already exists" });
      }
      const bcrypt2 = await import("bcryptjs");
      const hashedPassword = await bcrypt2.hash(password, 10);
      const validRole = ["admin", "washos_user", "external_customer"].includes(role) ? role : "external_customer";
      const [newUser] = await db.insert(users).values({
        email: normalizedEmail,
        password: hashedPassword,
        firstName: firstName?.trim(),
        lastName: lastName?.trim(),
        role: validRole
      }).returning();
      const { password: _, ...userWithoutPassword } = newUser;
      res.json(userWithoutPassword);
    } catch (err) {
      console.error("Error creating user:", err);
      res.status(500).json({ error: "Failed to create user" });
    }
  });
  app2.delete("/api/admin/users/:userId", isAuthenticated, requireRole("admin"), async (req, res) => {
    try {
      const { userId } = req.params;
      const currentUserId = req.user?.id;
      if (userId === currentUserId) {
        return res.status(400).json({ error: "Cannot delete your own account" });
      }
      const [deleted] = await db.delete(users).where(eq2(users.id, userId)).returning();
      if (!deleted) {
        return res.status(404).json({ error: "User not found" });
      }
      res.json({ message: "User deleted" });
    } catch (err) {
      console.error("Error deleting user:", err);
      res.status(500).json({ error: "Failed to delete user" });
    }
  });
  app2.get("/api/admin/grants/:userId", isAuthenticated, requireRole("admin", "washos_user"), async (req, res) => {
    try {
      const { userId } = req.params;
      const grants = await db.select().from(tableGrants).where(eq2(tableGrants.userId, userId));
      res.json(grants);
    } catch (err) {
      console.error("Error fetching grants:", err);
      res.status(500).json({ error: "Failed to fetch grants" });
    }
  });
  app2.post("/api/admin/grants", isAuthenticated, requireRole("admin", "washos_user"), async (req, res) => {
    try {
      const { userId, database, tableName } = req.body;
      const grantedBy = req.user?.id;
      if (!userId || !database || !tableName) {
        return res.status(400).json({ error: "userId, database, and tableName are required" });
      }
      const [grant] = await db.insert(tableGrants).values({
        userId,
        database,
        tableName,
        grantedBy
      }).returning();
      res.json(grant);
    } catch (err) {
      console.error("Error creating grant:", err);
      res.status(500).json({ error: "Failed to create grant" });
    }
  });
  app2.delete("/api/admin/grants/:grantId", isAuthenticated, requireRole("admin", "washos_user"), async (req, res) => {
    try {
      const { grantId } = req.params;
      await db.delete(tableGrants).where(eq2(tableGrants.id, grantId));
      res.json({ success: true });
    } catch (err) {
      console.error("Error deleting grant:", err);
      res.status(500).json({ error: "Failed to delete grant" });
    }
  });
  app2.get("/api/admin/audit-logs", isAuthenticated, requireRole("admin"), async (req, res) => {
    try {
      const { limit = "100", offset = "0", userId, action } = req.query;
      const limitNum = Math.min(parseInt(limit) || 100, 1e3);
      const offsetNum = parseInt(offset) || 0;
      const conditions = [];
      if (userId) {
        conditions.push(eq2(auditLogs.userId, userId));
      }
      if (action) {
        conditions.push(eq2(auditLogs.action, action));
      }
      let countQuery = db.select({ count: count() }).from(auditLogs);
      if (conditions.length > 0) {
        countQuery = countQuery.where(and(...conditions));
      }
      const [{ count: total }] = await countQuery;
      let logsQuery = db.select().from(auditLogs);
      if (conditions.length > 0) {
        logsQuery = logsQuery.where(and(...conditions));
      }
      const logs = await logsQuery.orderBy(desc(auditLogs.timestamp)).limit(limitNum).offset(offsetNum);
      res.json({
        logs,
        total: Number(total),
        limit: limitNum,
        offset: offsetNum
      });
    } catch (err) {
      console.error("Error fetching audit logs:", err);
      res.status(500).json({ error: "Failed to fetch audit logs" });
    }
  });
  app2.get("/api/admin/table-settings", isAuthenticated, requireRole("admin"), async (req, res) => {
    try {
      const settings = await storage.getAllTableSettings();
      res.json(settings);
    } catch (err) {
      console.error("Error fetching table settings:", err);
      res.status(500).json({ error: "Failed to fetch table settings" });
    }
  });
  app2.get("/api/table-settings", isAuthenticated, async (req, res) => {
    try {
      const settings = await storage.getAllTableSettings();
      res.json(settings);
    } catch (err) {
      console.error("Error fetching table settings:", err);
      res.status(500).json({ error: "Failed to fetch table settings" });
    }
  });
  app2.post("/api/admin/table-settings", isAuthenticated, requireRole("admin"), async (req, res) => {
    try {
      const { database, tableName, isVisible, displayName, hiddenColumns } = req.body;
      if (!database || !tableName) {
        return res.status(400).json({ error: "database and tableName are required" });
      }
      await storage.setTableSettings(database, tableName, {
        isVisible: isVisible !== false,
        displayName: displayName || null,
        hiddenColumns: Array.isArray(hiddenColumns) ? hiddenColumns : void 0
      });
      res.json({ success: true });
    } catch (err) {
      console.error("Error updating table settings:", err);
      res.status(500).json({ error: "Failed to update table settings" });
    }
  });
  app2.get("/api/auth/me", isAuthenticated, async (req, res) => {
    try {
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (!user) {
        return res.status(404).json({ error: "User not found" });
      }
      res.json(user);
    } catch (err) {
      console.error("Error fetching current user:", err);
      res.status(500).json({ error: "Failed to fetch user" });
    }
  });
  app2.get("/api/databases", isAuthenticated, (req, res) => {
    try {
      const dbs = getDatabaseConnections();
      res.json(dbs.map((db2) => ({ name: db2.name })));
    } catch (err) {
      console.error("Error getting databases:", err);
      res.status(500).json({ error: "Failed to get databases" });
    }
  });
  app2.get("/api/tables/:database", isAuthenticated, async (req, res) => {
    try {
      const { database } = req.params;
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (!user || !user.isActive) {
        return res.status(403).json({ error: "Account is inactive" });
      }
      const pool2 = getPool(database);
      const allTableSettings = await storage.getAllTableSettings();
      const result = await pool2.query(`
        SELECT table_schema, table_name
        FROM information_schema.tables
        WHERE table_type = 'BASE TABLE'
          AND table_schema NOT IN ('pg_catalog', 'information_schema')
        ORDER BY table_schema, table_name
      `);
      let tables = result.rows.map((row) => {
        const fullName = `${row.table_schema}.${row.table_name}`;
        const settingsKey = `${database}:${fullName}`;
        const settings = allTableSettings[settingsKey];
        return {
          schema: row.table_schema,
          name: row.table_name,
          fullName,
          displayName: settings?.displayName || null,
          isVisible: settings?.isVisible !== false
        };
      });
      if (user.role === "external_customer") {
        const allowedTables = await getAllowedTables(userId);
        tables = tables.filter((t) => allowedTables.includes(`${database}:${t.fullName}`));
      }
      if (user.role !== "admin") {
        tables = tables.filter((t) => t.isVisible !== false);
      }
      res.json(tables);
    } catch (err) {
      console.error("Error getting tables:", err);
      res.status(500).json({
        error: err instanceof Error ? err.message : "Failed to get tables"
      });
    }
  });
  app2.get(
    "/api/columns/:database/:fullTable",
    isAuthenticated,
    async (req, res) => {
      try {
        const { database, fullTable } = req.params;
        const userId = req.user?.id;
        const user = await authStorage.getUser(userId);
        if (user?.role === "external_customer") {
          const allowedTables = await getAllowedTables(userId);
          if (!allowedTables.includes(`${database}:${fullTable}`)) {
            return res.status(403).json({ error: "You don't have access to this table" });
          }
        }
        const [schema, table] = fullTable.split(".");
        if (!schema || !table) {
          return res.status(400).json({ error: "Invalid table name format. Expected schema.table" });
        }
        validateIdentifier(schema, "schema");
        validateIdentifier(table, "table");
        const pool2 = getPool(database);
        const columnsResult = await pool2.query(
          `
          SELECT column_name, data_type, is_nullable
          FROM information_schema.columns
          WHERE table_schema = $1 AND table_name = $2
          ORDER BY ordinal_position
        `,
          [schema, table]
        );
        const pkResult = await pool2.query(
          `
          SELECT a.attname
          FROM pg_index i
          JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
          WHERE i.indrelid = ($1 || '.' || $2)::regclass
            AND i.indisprimary
        `,
          [schema, table]
        );
        const pkColumns = new Set(pkResult.rows.map((r) => r.attname));
        const columns = columnsResult.rows.map((row) => ({
          name: row.column_name,
          dataType: row.data_type,
          isNullable: row.is_nullable === "YES",
          isPrimaryKey: pkColumns.has(row.column_name)
        }));
        res.json(columns);
      } catch (err) {
        console.error("Error getting columns:", err);
        res.status(500).json({
          error: err instanceof Error ? err.message : "Failed to get columns"
        });
      }
    }
  );
  app2.get("/api/filters/:table", isAuthenticated, async (req, res) => {
    try {
      const { table } = req.params;
      const filters = await storage.getFilters(table);
      res.json(filters);
    } catch (err) {
      console.error("Error getting filters:", err);
      res.status(500).json({ error: "Failed to get filters" });
    }
  });
  app2.post("/api/filters", isAuthenticated, requireRole("admin"), async (req, res) => {
    try {
      const { table, filters } = req.body;
      if (!table || !Array.isArray(filters)) {
        return res.status(400).json({ error: "Invalid request body" });
      }
      await storage.setFilters(table, filters);
      res.json({ success: true });
    } catch (err) {
      console.error("Error saving filters:", err);
      res.status(500).json({ error: "Failed to save filters" });
    }
  });
  app2.get("/api/filters/history/:database/:table", isAuthenticated, async (req, res) => {
    try {
      const userId = req.user?.id;
      const { database, table } = req.params;
      if (!userId) {
        return res.status(401).json({ error: "Not authenticated" });
      }
      const history = await storage.getFilterHistory(userId, database, table);
      res.json(history);
    } catch (err) {
      console.error("Error getting filter history:", err);
      res.status(500).json({ error: "Failed to get filter history" });
    }
  });
  app2.post("/api/filters/history", isAuthenticated, async (req, res) => {
    try {
      const userId = req.user?.id;
      const { database, table, filters } = req.body;
      if (!userId) {
        return res.status(401).json({ error: "Not authenticated" });
      }
      if (!database || !table || !Array.isArray(filters) || filters.length === 0) {
        return res.status(400).json({ error: "Database, table, and non-empty filters are required" });
      }
      const entry = await storage.saveFilterHistory(userId, database, table, filters);
      res.json(entry);
    } catch (err) {
      console.error("Error saving filter history:", err);
      res.status(500).json({ error: "Failed to save filter history" });
    }
  });
  app2.delete("/api/filters/history/:id", isAuthenticated, async (req, res) => {
    try {
      const userId = req.user?.id;
      const { id } = req.params;
      if (!userId) {
        return res.status(401).json({ error: "Not authenticated" });
      }
      const deleted = await storage.deleteFilterHistory(id, userId);
      if (!deleted) {
        return res.status(404).json({ error: "Filter history entry not found" });
      }
      res.json({ success: true });
    } catch (err) {
      console.error("Error deleting filter history:", err);
      res.status(500).json({ error: "Failed to delete filter history" });
    }
  });
  app2.post("/api/rows", isAuthenticated, async (req, res) => {
    try {
      const { database, table, page = 1, filters = [], sort } = req.body;
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (user?.role === "external_customer") {
        const allowedTables = await getAllowedTables(userId);
        if (!allowedTables.includes(`${database}:${table}`)) {
          return res.status(403).json({ error: "You don't have access to this table" });
        }
      }
      if (!database || !table) {
        return res.status(400).json({ error: "Database and table are required" });
      }
      const [schema, tableName] = table.split(".");
      validateIdentifier(schema, "schema");
      validateIdentifier(tableName, "table");
      const pool2 = getPool(database);
      const columnsResult = await pool2.query(
        `
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
      `,
        [schema, tableName]
      );
      const validColumns = new Set(columnsResult.rows.map((r) => r.column_name));
      const pkResult = await pool2.query(
        `
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = ($1 || '.' || $2)::regclass
          AND i.indisprimary
        ORDER BY a.attnum
      `,
        [schema, tableName]
      );
      let orderByClause;
      if (sort && Array.isArray(sort) && sort.length > 0) {
        const sortParts = [];
        for (const sortItem of sort) {
          if (sortItem.column && validColumns.has(sortItem.column)) {
            validateIdentifier(sortItem.column, "sort column");
            const direction = sortItem.direction === "desc" ? "DESC" : "ASC";
            sortParts.push(`"${sortItem.column}" ${direction}`);
          }
        }
        if (sortParts.length > 0) {
          orderByClause = sortParts.join(", ");
        } else {
          orderByClause = pkResult.rows.length > 0 ? `"${pkResult.rows[0].attname}"` : "ctid";
        }
      } else {
        orderByClause = pkResult.rows.length > 0 ? `"${pkResult.rows[0].attname}"` : "ctid";
      }
      const whereClauses = [];
      const params = [];
      let paramIndex = 1;
      for (const filter of filters) {
        validateIdentifier(filter.column, "column");
        if (!validColumns.has(filter.column)) {
          return res.status(400).json({ error: `Invalid column: ${filter.column}` });
        }
        const op = getOperatorSQL(filter.operator, paramIndex);
        whereClauses.push(`"${filter.column}" ${op.sql}`);
        if (filter.operator === "between" && Array.isArray(filter.value)) {
          const startValue = convertPSTDateToUTC(filter.value[0], false);
          const endValue = convertPSTDateToUTC(filter.value[1], true);
          params.push(startValue, endValue);
          paramIndex += 2;
        } else if (["gt", "gte", "lt", "lte", "eq"].includes(filter.operator) && typeof filter.value === "string") {
          const dateMatch = filter.value.match(/^\d{4}-\d{2}-\d{2}$/);
          if (dateMatch) {
            const converted = convertPSTDateToUTC(filter.value, filter.operator === "lte" || filter.operator === "lt");
            params.push(op.transform ? op.transform(converted) : converted);
          } else {
            params.push(op.transform ? op.transform(filter.value) : filter.value);
          }
          paramIndex++;
        } else {
          params.push(op.transform ? op.transform(filter.value) : filter.value);
          paramIndex++;
        }
      }
      const whereSQL = whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";
      const countQuery = `SELECT COUNT(*) as count FROM "${schema}"."${tableName}" ${whereSQL}`;
      const countResult = await pool2.query(countQuery, params);
      const totalCount = parseInt(countResult.rows[0].count, 10);
      const totalPages = Math.max(1, Math.ceil(totalCount / PAGE_SIZE));
      const safePage = Math.min(Math.max(1, page), totalPages);
      const offset = (safePage - 1) * PAGE_SIZE;
      const dataQuery = `
        SELECT * FROM "${schema}"."${tableName}"
        ${whereSQL}
        ORDER BY ${orderByClause}
        LIMIT ${PAGE_SIZE}
        OFFSET ${offset}
      `;
      const dataResult = await pool2.query(dataQuery, params);
      logAudit({
        userId,
        userEmail: user?.email || "unknown",
        action: "VIEW_DATA",
        database,
        table,
        details: `Viewed page ${safePage} of ${totalPages} (${dataResult.rows.length} rows)${filters.length > 0 ? `, ${filters.length} filters applied` : ""}`,
        ip: req.ip || req.socket.remoteAddress
      });
      res.json({
        rows: dataResult.rows,
        totalCount,
        page: safePage,
        pageSize: PAGE_SIZE,
        totalPages
      });
    } catch (err) {
      console.error("Error fetching rows:", err);
      res.status(500).json({
        error: err instanceof Error ? err.message : "Failed to fetch rows"
      });
    }
  });
  app2.post("/api/export/check", isAuthenticated, async (req, res) => {
    try {
      const { database, table, filters } = req.body;
      if (!database || !table) {
        return res.status(400).json({ error: "Database and table are required" });
      }
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (user?.role === "external_customer") {
        const allowedTables = await getAllowedTables(userId);
        if (!allowedTables.includes(`${database}:${table}`)) {
          return res.status(403).json({ error: "You don't have access to this table" });
        }
      }
      const [schema, tableName] = table.split(".");
      validateIdentifier(schema, "schema");
      validateIdentifier(tableName, "table");
      const pool2 = getPool(database);
      const activeFilters = filters || [];
      const columnsResult = await pool2.query(
        `SELECT column_name FROM information_schema.columns WHERE table_schema = $1 AND table_name = $2`,
        [schema, tableName]
      );
      const validColumns = new Set(columnsResult.rows.map((r) => r.column_name));
      const whereClauses = [];
      const params = [];
      let paramIndex = 1;
      for (const filter of activeFilters) {
        validateIdentifier(filter.column, "column");
        if (!validColumns.has(filter.column)) {
          return res.status(400).json({ error: `Invalid column: ${filter.column}` });
        }
        const op = getOperatorSQL(filter.operator, paramIndex);
        whereClauses.push(`"${filter.column}" ${op.sql}`);
        if (filter.operator === "between" && Array.isArray(filter.value)) {
          const startValue = convertPSTDateToUTC(filter.value[0], false);
          const endValue = convertPSTDateToUTC(filter.value[1], true);
          params.push(startValue, endValue);
          paramIndex += 2;
        } else if (["gt", "gte", "lt", "lte", "eq"].includes(filter.operator) && typeof filter.value === "string") {
          const dateMatch = filter.value.match(/^\d{4}-\d{2}-\d{2}$/);
          if (dateMatch) {
            const converted = convertPSTDateToUTC(filter.value, filter.operator === "lte" || filter.operator === "lt");
            params.push(op.transform ? op.transform(converted) : converted);
          } else {
            params.push(op.transform ? op.transform(filter.value) : filter.value);
          }
          paramIndex++;
        } else {
          params.push(op.transform ? op.transform(filter.value) : filter.value);
          paramIndex++;
        }
      }
      const whereSQL = whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";
      const countQuery = `SELECT COUNT(*) as count FROM "${schema}"."${tableName}" ${whereSQL}`;
      const countResult = await pool2.query(countQuery, params);
      const totalCount = parseInt(countResult.rows[0].count, 10);
      const isAdmin = user?.role === "admin";
      const maxRowsForRole = isAdmin ? 5e4 : 1e4;
      const warningThreshold = 2e3;
      res.json({
        totalCount,
        isAdmin,
        maxRowsForRole,
        warningThreshold,
        canExport: totalCount <= maxRowsForRole,
        needsWarning: totalCount > warningThreshold,
        exceedsLimit: totalCount > maxRowsForRole
      });
    } catch (err) {
      console.error("Error checking export:", err);
      res.status(500).json({
        error: err instanceof Error ? err.message : "Failed to check export"
      });
    }
  });
  app2.get("/api/export", isAuthenticated, async (req, res) => {
    try {
      const { database, table, page = "1", filters: filtersJson, exportAll } = req.query;
      if (!database || !table) {
        return res.status(400).json({ error: "Database and table are required" });
      }
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (user?.role === "external_customer") {
        const allowedTables = await getAllowedTables(userId);
        if (!allowedTables.includes(`${database}:${table}`)) {
          return res.status(403).json({ error: "You don't have access to this table" });
        }
      }
      const [schema, tableName] = table.split(".");
      validateIdentifier(schema, "schema");
      validateIdentifier(tableName, "table");
      const pool2 = getPool(database);
      let filters = [];
      if (filtersJson) {
        try {
          filters = JSON.parse(filtersJson);
        } catch {
          return res.status(400).json({ error: "Invalid filters format" });
        }
      }
      const columnsResult = await pool2.query(
        `
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = $1 AND table_name = $2
        ORDER BY ordinal_position
      `,
        [schema, tableName]
      );
      const columnNames = columnsResult.rows.map((r) => r.column_name);
      const validColumns = new Set(columnNames);
      const pkResult = await pool2.query(
        `
        SELECT a.attname
        FROM pg_index i
        JOIN pg_attribute a ON a.attrelid = i.indrelid AND a.attnum = ANY(i.indkey)
        WHERE i.indrelid = ($1 || '.' || $2)::regclass
          AND i.indisprimary
        ORDER BY a.attnum
      `,
        [schema, tableName]
      );
      const orderByColumn = pkResult.rows.length > 0 ? `"${pkResult.rows[0].attname}"` : "ctid";
      const whereClauses = [];
      const params = [];
      let paramIndex = 1;
      for (const filter of filters) {
        validateIdentifier(filter.column, "column");
        if (!validColumns.has(filter.column)) {
          return res.status(400).json({ error: `Invalid column: ${filter.column}` });
        }
        const op = getOperatorSQL(filter.operator, paramIndex);
        whereClauses.push(`"${filter.column}" ${op.sql}`);
        if (filter.operator === "between" && Array.isArray(filter.value)) {
          const startValue = convertPSTDateToUTC(filter.value[0], false);
          const endValue = convertPSTDateToUTC(filter.value[1], true);
          params.push(startValue, endValue);
          paramIndex += 2;
        } else if (["gt", "gte", "lt", "lte", "eq"].includes(filter.operator) && typeof filter.value === "string") {
          const dateMatch = filter.value.match(/^\d{4}-\d{2}-\d{2}$/);
          if (dateMatch) {
            const converted = convertPSTDateToUTC(filter.value, filter.operator === "lte" || filter.operator === "lt");
            params.push(op.transform ? op.transform(converted) : converted);
          } else {
            params.push(op.transform ? op.transform(filter.value) : filter.value);
          }
          paramIndex++;
        } else {
          params.push(op.transform ? op.transform(filter.value) : filter.value);
          paramIndex++;
        }
      }
      const whereSQL = whereClauses.length > 0 ? `WHERE ${whereClauses.join(" AND ")}` : "";
      const isExportAll = exportAll === "true";
      const isAdmin = user?.role === "admin";
      const maxRowsForRole = isAdmin ? 5e4 : 1e4;
      let exportTotalCount = 0;
      if (isExportAll) {
        const countQuery = `SELECT COUNT(*) as count FROM "${schema}"."${tableName}" ${whereSQL}`;
        const countResult = await pool2.query(countQuery, params);
        exportTotalCount = parseInt(countResult.rows[0].count, 10);
        if (exportTotalCount > maxRowsForRole) {
          return res.status(403).json({
            error: isAdmin ? `Export exceeds maximum limit of ${maxRowsForRole.toLocaleString()} rows` : `Export exceeds your limit of ${maxRowsForRole.toLocaleString()} rows. Please contact an administrator for larger exports.`
          });
        }
      }
      const pageNum = Math.max(1, parseInt(page, 10) || 1);
      const offset = (pageNum - 1) * PAGE_SIZE;
      let dataQuery;
      let filename;
      if (isExportAll) {
        dataQuery = `
          SELECT * FROM "${schema}"."${tableName}"
          ${whereSQL}
          ORDER BY ${orderByColumn} ASC
          LIMIT ${exportTotalCount}
        `;
        filename = `${tableName}_export.csv`;
      } else {
        dataQuery = `
          SELECT * FROM "${schema}"."${tableName}"
          ${whereSQL}
          ORDER BY ${orderByColumn} ASC
          LIMIT ${PAGE_SIZE}
          OFFSET ${offset}
        `;
        filename = `${tableName}_page${pageNum}.csv`;
      }
      res.setHeader("Content-Type", "text/csv");
      res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
      const escapeCSV = (value) => {
        if (value === null || value === void 0) return "";
        const str = typeof value === "object" ? JSON.stringify(value) : String(value);
        if (str.includes(",") || str.includes('"') || str.includes("\n")) {
          return `"${str.replace(/"/g, '""')}"`;
        }
        return str;
      };
      res.write(columnNames.map(escapeCSV).join(",") + "\n");
      if (isExportAll) {
        const client = await pool2.connect();
        const cursorName = `export_cursor_${Date.now()}`;
        let transactionStarted = false;
        try {
          await client.query("BEGIN");
          transactionStarted = true;
          await client.query(`DECLARE ${cursorName} CURSOR FOR ${dataQuery}`, params);
          const batchSize = 1e3;
          let hasMore = true;
          while (hasMore) {
            const batchResult = await client.query(`FETCH ${batchSize} FROM ${cursorName}`);
            if (batchResult.rows.length === 0) {
              hasMore = false;
            } else {
              for (const row of batchResult.rows) {
                res.write(columnNames.map((col) => escapeCSV(row[col])).join(",") + "\n");
              }
            }
          }
          await client.query(`CLOSE ${cursorName}`);
          await client.query("COMMIT");
        } catch (streamError) {
          if (transactionStarted) {
            try {
              await client.query("ROLLBACK");
            } catch (rollbackError) {
              console.error("Error rolling back export transaction:", rollbackError);
            }
          }
          throw streamError;
        } finally {
          client.release();
        }
      } else {
        const dataResult = await pool2.query(dataQuery, params);
        for (const row of dataResult.rows) {
          res.write(columnNames.map((col) => escapeCSV(row[col])).join(",") + "\n");
        }
      }
      logAudit({
        userId,
        userEmail: user?.email || "unknown",
        action: isExportAll ? "EXPORT_ALL" : "EXPORT_PAGE",
        database,
        table,
        details: isExportAll ? `Exported ${exportTotalCount} rows${filters.length > 0 ? `, ${filters.length} filters applied` : ""}` : `Exported page ${pageNum}${filters.length > 0 ? `, ${filters.length} filters applied` : ""}`,
        ip: req.ip || req.socket.remoteAddress
      });
      res.end();
    } catch (err) {
      console.error("Error exporting CSV:", err);
      if (!res.headersSent) {
        res.status(500).json({
          error: err instanceof Error ? err.message : "Failed to export CSV"
        });
      }
    }
  });
  app2.get("/api/nlq/status", isAuthenticated, (req, res) => {
    const client = getOpenAIClient();
    res.json({ enabled: client !== null });
  });
  app2.post("/api/nlq", isAuthenticated, async (req, res) => {
    try {
      const { database, query, table: currentTable, context } = req.body;
      const client = getOpenAIClient();
      if (!client) {
        return res.status(400).json({ error: "Natural language queries are not enabled" });
      }
      if (!query || typeof query !== "string") {
        return res.status(400).json({ error: "Query is required" });
      }
      if (!currentTable) {
        return res.status(400).json({ error: "Please select a table first" });
      }
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (user?.role === "external_customer") {
        const allowedTables = await getAllowedTables(userId);
        if (!allowedTables.includes(`${database}:${currentTable}`)) {
          return res.status(403).json({ error: "You don't have access to this table" });
        }
      }
      const pool2 = getPool(database);
      const [schema, tableName] = currentTable.split(".");
      const dictionary = await getTableDataDictionary(pool2, database, schema, tableName);
      let columnsWithTypes = [];
      if (dictionary) {
        columnsWithTypes = dictionary.columns.map((c) => ({ name: c.name, dataType: c.dataType }));
      } else {
        const columnsResult = await pool2.query(
          `SELECT column_name, data_type FROM information_schema.columns 
           WHERE table_schema = $1 AND table_name = $2 ORDER BY ordinal_position`,
          [schema, tableName]
        );
        columnsWithTypes = columnsResult.rows.map((r) => ({
          name: r.column_name,
          dataType: r.data_type
        }));
      }
      const dateColumnNames = columnsWithTypes.filter((c) => c.dataType.includes("date") || c.dataType.includes("timestamp")).map((c) => c.name);
      const semanticResolution = dictionary ? resolveSemanticReference(query, dictionary.columns) : { resolvedColumn: null, type: null, needsClarification: false };
      const systemPrompt = buildNLQSystemPrompt({
        table: currentTable,
        dictionary,
        columns: columnsWithTypes,
        dateColumns: dateColumnNames,
        context
      });
      const makeRequest = async () => {
        const response = await client.chat.completions.create({
          model: AI_CONFIG.nlq.model,
          messages: [
            { role: "system", content: systemPrompt },
            { role: "user", content: query }
          ],
          max_completion_tokens: AI_CONFIG.nlq.maxTokens,
          temperature: AI_CONFIG.nlq.temperature
        });
        return response.choices[0]?.message?.content || "{}";
      };
      const content = await makeRequest();
      const validColumns = columnsWithTypes.map((c) => c.name);
      const parseResult = await parseAndValidateNLQResponse(
        content,
        currentTable,
        validColumns,
        makeRequest
      );
      if (!parseResult.success) {
        return res.status(400).json({ error: parseResult.error });
      }
      const plan = parseResult.data;
      plan.table = currentTable;
      plan.page = plan.page || 1;
      plan.action = plan.action || "plan";
      if (plan.action === "clarify" && semanticResolution.needsClarification && semanticResolution.options) {
        plan.questions = plan.questions || [];
        if (plan.questions.length === 0) {
          plan.questions.push(`Which date column should I use? Available options: ${semanticResolution.options.join(", ")}`);
        }
        plan.ambiguousColumns = semanticResolution.options;
      }
      res.json(plan);
    } catch (err) {
      console.error("Error processing NLQ:", err);
      res.status(500).json({
        error: err instanceof Error ? err.message : "Failed to process query"
      });
    }
  });
  app2.post("/api/nlq/smart-followup", isAuthenticated, async (req, res) => {
    try {
      const { database, table: currentTable, filters, context } = req.body;
      const client = getOpenAIClient();
      if (!client) {
        return res.status(400).json({ error: "Natural language queries are not enabled" });
      }
      if (!currentTable || !database || !filters || !Array.isArray(filters)) {
        return res.status(400).json({ error: "Missing required parameters" });
      }
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (user?.role === "external_customer") {
        const allowedTables = await getAllowedTables(userId);
        if (!allowedTables.includes(`${database}:${currentTable}`)) {
          return res.status(403).json({ error: "You don't have access to this table" });
        }
      }
      const pool2 = getPool(database);
      const [schema, tableName] = currentTable.split(".");
      const dictionary = await getTableDataDictionary(pool2, database, schema, tableName);
      const columnTypes = {};
      if (dictionary) {
        dictionary.columns.forEach((c) => {
          columnTypes[c.name] = c.dataType;
        });
      } else {
        const columnsResult = await pool2.query(
          `SELECT column_name, data_type FROM information_schema.columns 
           WHERE table_schema = $1 AND table_name = $2`,
          [schema, tableName]
        );
        columnsResult.rows.forEach((r) => {
          columnTypes[r.column_name] = r.data_type;
        });
      }
      const TIMEOUT_MS = 3e3;
      const MAX_DISTINCT_VALUES = 15;
      const columnSamples = [];
      const queryWithTimeout2 = async (queryStr) => {
        return new Promise((resolve, reject) => {
          const timeoutId = setTimeout(() => reject(new Error("Timeout")), TIMEOUT_MS);
          pool2.query(queryStr).then((result) => {
            clearTimeout(timeoutId);
            resolve(result);
          }).catch((err) => {
            clearTimeout(timeoutId);
            reject(err);
          });
        });
      };
      for (const filter of filters) {
        const colName = filter.column;
        const dataType = columnTypes[colName];
        if (!dataType) continue;
        const sample = {
          column: colName,
          dataType,
          filteredValue: Array.isArray(filter.value) ? filter.value.join(" - ") : filter.value,
          operator: filter.op
        };
        try {
          if (dictionary) {
            const colStats = dictionary.columns.find((c) => c.name === colName);
            if (colStats?.topValues) {
              sample.actualValues = colStats.topValues.map((v) => v.value);
            } else if (colStats?.dateRange) {
              sample.dateRange = colStats.dateRange;
            }
          }
          if (!sample.actualValues && !sample.dateRange) {
            if (dataType.includes("character") || dataType.includes("text") || dataType === "USER-DEFINED") {
              const result = await queryWithTimeout2(`
                SELECT DISTINCT "${colName}" as val FROM "${schema}"."${tableName}" 
                WHERE "${colName}" IS NOT NULL ORDER BY "${colName}" LIMIT ${MAX_DISTINCT_VALUES}
              `);
              sample.actualValues = result.rows.map((r) => String(r.val));
            } else if (dataType.includes("date") || dataType.includes("timestamp")) {
              const result = await queryWithTimeout2(`
                SELECT MIN("${colName}")::text as min_val, MAX("${colName}")::text as max_val 
                FROM "${schema}"."${tableName}" WHERE "${colName}" IS NOT NULL
              `);
              if (result.rows[0]) {
                sample.dateRange = { min: result.rows[0].min_val, max: result.rows[0].max_val };
              }
            }
          }
        } catch (err) {
          console.log(`Skipping column ${colName} sampling:`, err);
        }
        columnSamples.push(sample);
      }
      let samplingInfo = "";
      for (const sample of columnSamples) {
        if (sample.actualValues && sample.actualValues.length > 0) {
          samplingInfo += `
- Column "${sample.column}" (${sample.dataType}): User searched for "${sample.filteredValue}" using operator "${sample.operator}"`;
          samplingInfo += `
  Actual values in database: ${sample.actualValues.map((v) => `"${v}"`).join(", ")}`;
        } else if (sample.dateRange) {
          samplingInfo += `
- Column "${sample.column}" (${sample.dataType}): User searched for "${sample.filteredValue}" using operator "${sample.operator}"`;
          samplingInfo += `
  Date range in database: from ${sample.dateRange.min} to ${sample.dateRange.max}`;
        }
      }
      if (!samplingInfo) {
        return res.json({
          likelyIssue: "unknown",
          suggestedChanges: [],
          clarificationQuestion: "No results found for your query. Try adjusting your filter values or broadening your search.",
          summary: "Unable to sample column values to provide suggestions."
        });
      }
      const systemPrompt = buildSmartFollowupPrompt({
        table: currentTable,
        filters: filters.map((f) => ({ column: f.column, op: f.op, value: Array.isArray(f.value) ? f.value.join(" - ") : f.value })),
        samplingInfo,
        context
      });
      const makeRequest = async () => {
        const response = await client.chat.completions.create({
          model: AI_CONFIG.smartFollowup.model,
          messages: [
            { role: "system", content: systemPrompt },
            { role: "user", content: "Help me understand why my query returned no results and suggest alternatives." }
          ],
          max_completion_tokens: AI_CONFIG.smartFollowup.maxTokens,
          temperature: AI_CONFIG.smartFollowup.temperature
        });
        return response.choices[0]?.message?.content || "{}";
      };
      const content = await makeRequest();
      const parseResult = await parseAndValidateSmartFollowupResponse(content, makeRequest);
      if (!parseResult.success) {
        return res.json({
          likelyIssue: "unknown",
          suggestedChanges: [],
          clarificationQuestion: "I couldn't analyze your query. Try adjusting your filter values.",
          summary: parseResult.error
        });
      }
      res.json(parseResult.data);
    } catch (err) {
      console.error("Error processing smart followup:", err);
      res.status(500).json({
        error: err instanceof Error ? err.message : "Failed to process followup"
      });
    }
  });
  const reportLimiter = rateLimit({
    windowMs: 60 * 1e3,
    max: 30,
    message: { error: "Too many report requests, please try again later" },
    standardHeaders: true,
    legacyHeaders: false
  });
  const reportAILimiter = rateLimit({
    windowMs: 60 * 1e3,
    max: 15,
    message: { error: "Too many AI requests, please try again later" },
    standardHeaders: true,
    legacyHeaders: false
  });
  app2.get("/api/reports/pages", isAuthenticated, reportLimiter, async (req, res) => {
    try {
      const userId = req.user?.id;
      if (!userId) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      const pages = await db.select().from(reportPages).where(and(eq2(reportPages.userId, userId), eq2(reportPages.isArchived, false))).orderBy(desc(reportPages.updatedAt));
      res.json(pages);
    } catch (err) {
      console.error("Error fetching report pages:", err);
      res.status(500).json({ error: "Failed to fetch report pages" });
    }
  });
  app2.post("/api/reports/pages", isAuthenticated, reportLimiter, async (req, res) => {
    try {
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (!userId || !user) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      const { title, description } = req.body;
      if (!title || typeof title !== "string" || title.trim().length === 0) {
        return res.status(400).json({ error: "Title is required" });
      }
      const [newPage] = await db.insert(reportPages).values({
        userId,
        title: title.trim(),
        description: description?.trim() || null
      }).returning();
      await logAudit({
        userId,
        userEmail: user.email,
        action: "REPORT_CREATE",
        details: `Created report page: ${newPage.title}`,
        ip: req.ip || req.socket.remoteAddress
      });
      res.status(201).json(newPage);
    } catch (err) {
      console.error("Error creating report page:", err);
      res.status(500).json({ error: "Failed to create report page" });
    }
  });
  app2.get("/api/reports/pages/:id", isAuthenticated, reportLimiter, async (req, res) => {
    try {
      const userId = req.user?.id;
      if (!userId) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      const { id } = req.params;
      const [page] = await db.select().from(reportPages).where(and(eq2(reportPages.id, id), eq2(reportPages.userId, userId)));
      if (!page) {
        return res.status(404).json({ error: "Report page not found" });
      }
      const blocks = await db.select().from(reportBlocks).where(eq2(reportBlocks.pageId, id));
      res.json({ ...page, blocks });
    } catch (err) {
      console.error("Error fetching report page:", err);
      res.status(500).json({ error: "Failed to fetch report page" });
    }
  });
  app2.patch("/api/reports/pages/:id", isAuthenticated, reportLimiter, async (req, res) => {
    try {
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (!userId || !user) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      const { id } = req.params;
      const { title, description } = req.body;
      const [existing] = await db.select().from(reportPages).where(and(eq2(reportPages.id, id), eq2(reportPages.userId, userId)));
      if (!existing) {
        return res.status(404).json({ error: "Report page not found" });
      }
      const updates = {
        updatedAt: /* @__PURE__ */ new Date()
      };
      if (title !== void 0) updates.title = title.trim();
      if (description !== void 0) updates.description = description?.trim() || null;
      const [updated] = await db.update(reportPages).set(updates).where(eq2(reportPages.id, id)).returning();
      await logAudit({
        userId,
        userEmail: user.email,
        action: "REPORT_UPDATE",
        details: `Updated report page: ${updated.title}`,
        ip: req.ip || req.socket.remoteAddress
      });
      res.json(updated);
    } catch (err) {
      console.error("Error updating report page:", err);
      res.status(500).json({ error: "Failed to update report page" });
    }
  });
  app2.delete("/api/reports/pages/:id", isAuthenticated, reportLimiter, async (req, res) => {
    try {
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (!userId || !user) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      const { id } = req.params;
      const [existing] = await db.select().from(reportPages).where(and(eq2(reportPages.id, id), eq2(reportPages.userId, userId)));
      if (!existing) {
        return res.status(404).json({ error: "Report page not found" });
      }
      await db.update(reportPages).set({ isArchived: true, updatedAt: /* @__PURE__ */ new Date() }).where(eq2(reportPages.id, id));
      await logAudit({
        userId,
        userEmail: user.email,
        action: "REPORT_DELETE",
        details: `Archived report page: ${existing.title}`,
        ip: req.ip || req.socket.remoteAddress
      });
      res.json({ success: true });
    } catch (err) {
      console.error("Error deleting report page:", err);
      res.status(500).json({ error: "Failed to delete report page" });
    }
  });
  app2.post("/api/reports/pages/:pageId/blocks", isAuthenticated, reportLimiter, async (req, res) => {
    try {
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (!userId || !user) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      const { pageId } = req.params;
      const { kind, title, position, config } = req.body;
      const [page] = await db.select().from(reportPages).where(and(eq2(reportPages.id, pageId), eq2(reportPages.userId, userId)));
      if (!page) {
        return res.status(404).json({ error: "Report page not found" });
      }
      const validKinds = ["table", "chart", "metric", "text"];
      if (!validKinds.includes(kind)) {
        return res.status(400).json({ error: "Invalid block kind" });
      }
      const validation = await validateBlockConfig(config, kind, user, { bypassVisibility: true });
      if (!validation.valid) {
        return res.status(400).json({ error: validation.error });
      }
      const [newBlock] = await db.insert(reportBlocks).values({
        pageId,
        kind,
        title: title || null,
        position: position || { row: 0, col: 0, width: 6, height: 4 },
        config
      }).returning();
      await db.update(reportPages).set({ updatedAt: /* @__PURE__ */ new Date() }).where(eq2(reportPages.id, pageId));
      await logAudit({
        userId,
        userEmail: user.email,
        action: "REPORT_BLOCK_CREATE",
        details: `Created ${kind} block in report: ${page.title}`,
        ip: req.ip || req.socket.remoteAddress
      });
      res.status(201).json(newBlock);
    } catch (err) {
      console.error("Error creating report block:", err);
      res.status(500).json({ error: "Failed to create report block" });
    }
  });
  app2.patch("/api/reports/blocks/:id", isAuthenticated, reportLimiter, async (req, res) => {
    try {
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (!userId || !user) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      const { id } = req.params;
      const { title, position, config } = req.body;
      const [block] = await db.select().from(reportBlocks).where(eq2(reportBlocks.id, id));
      if (!block) {
        return res.status(404).json({ error: "Block not found" });
      }
      const [page] = await db.select().from(reportPages).where(and(eq2(reportPages.id, block.pageId), eq2(reportPages.userId, userId)));
      if (!page) {
        return res.status(404).json({ error: "Report page not found" });
      }
      if (config) {
        const validation = await validateBlockConfig(config, block.kind, user, { bypassVisibility: true });
        if (!validation.valid) {
          return res.status(400).json({ error: validation.error });
        }
      }
      const updates = {
        updatedAt: /* @__PURE__ */ new Date()
      };
      if (title !== void 0) updates.title = title;
      if (position !== void 0) updates.position = position;
      if (config !== void 0) updates.config = config;
      const [updated] = await db.update(reportBlocks).set(updates).where(eq2(reportBlocks.id, id)).returning();
      await db.update(reportPages).set({ updatedAt: /* @__PURE__ */ new Date() }).where(eq2(reportPages.id, block.pageId));
      res.json(updated);
    } catch (err) {
      console.error("Error updating report block:", err);
      res.status(500).json({ error: "Failed to update report block" });
    }
  });
  app2.delete("/api/reports/blocks/:id", isAuthenticated, reportLimiter, async (req, res) => {
    try {
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (!userId || !user) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      const { id } = req.params;
      const [block] = await db.select().from(reportBlocks).where(eq2(reportBlocks.id, id));
      if (!block) {
        return res.status(404).json({ error: "Block not found" });
      }
      const [page] = await db.select().from(reportPages).where(and(eq2(reportPages.id, block.pageId), eq2(reportPages.userId, userId)));
      if (!page) {
        return res.status(404).json({ error: "Report page not found" });
      }
      await db.delete(reportBlocks).where(eq2(reportBlocks.id, id));
      await db.update(reportPages).set({ updatedAt: /* @__PURE__ */ new Date() }).where(eq2(reportPages.id, block.pageId));
      await logAudit({
        userId,
        userEmail: user.email,
        action: "REPORT_BLOCK_DELETE",
        details: `Deleted ${block.kind} block from report: ${page.title}`,
        ip: req.ip || req.socket.remoteAddress
      });
      res.json({ success: true });
    } catch (err) {
      console.error("Error deleting report block:", err);
      res.status(500).json({ error: "Failed to delete report block" });
    }
  });
  const REPORT_BLOCK_PAGE_SIZE = 50;
  app2.post("/api/reports/blocks/:id/run", isAuthenticated, reportLimiter, async (req, res) => {
    try {
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (!userId || !user) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      const { id } = req.params;
      const { page: pageNum = 1, exportAll = false } = req.body;
      const currentPage = Math.max(1, parseInt(pageNum) || 1);
      const MAX_EXPORT_ROWS = 1e4;
      const [block] = await db.select().from(reportBlocks).where(eq2(reportBlocks.id, id));
      if (!block) {
        return res.status(404).json({ error: "Block not found" });
      }
      const [page] = await db.select().from(reportPages).where(and(eq2(reportPages.id, block.pageId), eq2(reportPages.userId, userId)));
      if (!page) {
        return res.status(404).json({ error: "Report page not found" });
      }
      if (block.kind === "text") {
        return res.json({ type: "text", content: block.config.content });
      }
      const config = block.config;
      const validation = await validateBlockConfig(config, block.kind, user, { bypassVisibility: true });
      if (!validation.valid) {
        const statusCode = validation.error?.includes("Access denied") ? 403 : 400;
        return res.status(statusCode).json({ error: validation.error });
      }
      const pool2 = getPool(config.database);
      const parsedTable = parseTableName(config.table);
      if (!parsedTable) {
        return res.status(400).json({ error: "Invalid table name" });
      }
      const tableRef = `"${parsedTable.schema}"."${parsedTable.table}"`;
      let query;
      let params = [];
      if (block.kind === "table") {
        const tableConfig = config;
        const mainAlias = "t1";
        const joinAlias = "t2";
        const subJoinAlias = "t3";
        const isSubJoinColumn = (prefix) => {
          return prefix.includes("_") || prefix.toLowerCase().includes("district") || prefix.toLowerCase().includes("sub");
        };
        let columns;
        if (tableConfig.columns?.length > 0) {
          columns = tableConfig.columns.map((c) => {
            if (c.includes(".")) {
              const [prefix, colName] = c.split(".");
              validateIdentifier(colName, "column");
              if (isSubJoinColumn(prefix) && tableConfig.join?.subJoin) {
                return `${subJoinAlias}."${colName}" AS "${c.replace(".", "_")}"`;
              } else {
                return `${joinAlias}."${colName}" AS "${c.replace(".", "_")}"`;
              }
            } else {
              validateIdentifier(c, "column");
              return `${mainAlias}."${c}"`;
            }
          }).join(", ");
        } else {
          columns = `${mainAlias}.*`;
        }
        query = `SELECT ${columns} FROM ${tableRef} AS ${mainAlias}`;
        if (tableConfig.join?.table) {
          const joinParsed = parseTableName(tableConfig.join.table);
          if (!joinParsed) {
            return res.status(400).json({ error: "Invalid join table name" });
          }
          const joinTableRef = `"${joinParsed.schema}"."${joinParsed.table}"`;
          const joinType = tableConfig.join.type === "inner" ? "INNER JOIN" : "LEFT JOIN";
          const [fromCol, toCol] = tableConfig.join.on;
          validateIdentifier(fromCol, "column");
          validateIdentifier(toCol, "column");
          query += ` ${joinType} ${joinTableRef} AS ${joinAlias} ON ${mainAlias}."${fromCol}" = ${joinAlias}."${toCol}"`;
          if (tableConfig.join.subJoin?.table) {
            const subJoinParsed = parseTableName(tableConfig.join.subJoin.table);
            if (!subJoinParsed) {
              return res.status(400).json({ error: "Invalid subJoin table name" });
            }
            const subJoinTableRef = `"${subJoinParsed.schema}"."${subJoinParsed.table}"`;
            const subJoinType = tableConfig.join.subJoin.type === "inner" ? "INNER JOIN" : "LEFT JOIN";
            const [subFromCol, subToCol] = tableConfig.join.subJoin.on;
            validateIdentifier(subFromCol, "column");
            validateIdentifier(subToCol, "column");
            query += ` ${subJoinType} ${subJoinTableRef} AS ${subJoinAlias} ON ${joinAlias}."${subFromCol}" = ${subJoinAlias}."${subToCol}"`;
          }
        }
        if (tableConfig.filters?.length > 0) {
          const whereClauses = [];
          tableConfig.filters.forEach((f) => {
            let columnRef;
            if (f.column.includes(".")) {
              const [prefix, colName] = f.column.split(".");
              validateIdentifier(colName, "column");
              if (isSubJoinColumn(prefix) && tableConfig.join?.subJoin) {
                columnRef = `${subJoinAlias}."${colName}"`;
              } else {
                columnRef = `${joinAlias}."${colName}"`;
              }
            } else {
              validateIdentifier(f.column, "column");
              columnRef = `${mainAlias}."${f.column}"`;
            }
            const filterWithAlias = { ...f, column: columnRef };
            addFilterToQueryWithAlias(filterWithAlias, params, whereClauses);
          });
          query += ` WHERE ${whereClauses.join(" AND ")}`;
        }
        let baseQuery = `FROM ${tableRef} AS ${mainAlias}`;
        if (tableConfig.join?.table) {
          const joinParsed = parseTableName(tableConfig.join.table);
          if (joinParsed) {
            const joinTableRef = `"${joinParsed.schema}"."${joinParsed.table}"`;
            const joinType = tableConfig.join.type === "inner" ? "INNER JOIN" : "LEFT JOIN";
            const [fromCol, toCol] = tableConfig.join.on;
            baseQuery += ` ${joinType} ${joinTableRef} AS ${joinAlias} ON ${mainAlias}."${fromCol}" = ${joinAlias}."${toCol}"`;
            if (tableConfig.join.subJoin?.table) {
              const subJoinParsed = parseTableName(tableConfig.join.subJoin.table);
              if (subJoinParsed) {
                const subJoinTableRef = `"${subJoinParsed.schema}"."${subJoinParsed.table}"`;
                const subJoinType = tableConfig.join.subJoin.type === "inner" ? "INNER JOIN" : "LEFT JOIN";
                const [subFromCol, subToCol] = tableConfig.join.subJoin.on;
                baseQuery += ` ${subJoinType} ${subJoinTableRef} AS ${subJoinAlias} ON ${joinAlias}."${subFromCol}" = ${subJoinAlias}."${subToCol}"`;
              }
            }
          }
        }
        if (tableConfig.filters?.length > 0) {
          baseQuery += ` WHERE ${query.split(" WHERE ")[1]?.split(" ORDER BY ")[0] || "1=1"}`;
        }
        const countResult = await pool2.query(`SELECT COUNT(*) as count ${baseQuery}`, params);
        const totalCount = parseInt(countResult.rows[0]?.count || "0");
        const totalPages = Math.max(1, Math.ceil(totalCount / REPORT_BLOCK_PAGE_SIZE));
        const safePage = Math.min(Math.max(1, currentPage), totalPages);
        const offset = (safePage - 1) * REPORT_BLOCK_PAGE_SIZE;
        if (tableConfig.orderBy && typeof tableConfig.orderBy === "object" && !Array.isArray(tableConfig.orderBy) && tableConfig.orderBy.column) {
          let orderColumnRef;
          if (tableConfig.orderBy.column.includes(".")) {
            const [prefix, colName] = tableConfig.orderBy.column.split(".");
            validateIdentifier(colName, "column");
            if (isSubJoinColumn(prefix) && tableConfig.join?.subJoin) {
              orderColumnRef = `${subJoinAlias}."${colName}"`;
            } else {
              orderColumnRef = `${joinAlias}."${colName}"`;
            }
          } else {
            validateIdentifier(tableConfig.orderBy.column, "column");
            orderColumnRef = `${mainAlias}."${tableConfig.orderBy.column}"`;
          }
          query += ` ORDER BY ${orderColumnRef} ${tableConfig.orderBy.direction === "desc" ? "DESC" : "ASC"}`;
        }
        if (exportAll) {
          query += ` LIMIT ${MAX_EXPORT_ROWS}`;
        } else {
          query += ` LIMIT ${REPORT_BLOCK_PAGE_SIZE} OFFSET ${offset}`;
        }
        const result = await pool2.query(query, params);
        await logAudit({
          userId,
          userEmail: user.email,
          action: exportAll ? "REPORT_EXPORT" : "REPORT_QUERY",
          database: config.database,
          table: config.table,
          details: exportAll ? `Table block export: ${result.rows.length} rows${tableConfig.join ? ` (joined with ${tableConfig.join.table})` : ""}` : `Table block query: page ${safePage} of ${totalPages} (${result.rows.length} rows)${tableConfig.join ? ` (joined with ${tableConfig.join.table})` : ""}`,
          ip: req.ip || req.socket.remoteAddress
        });
        res.json({
          type: "table",
          rows: result.rows,
          rowCount: result.rows.length,
          totalCount,
          page: exportAll ? 1 : safePage,
          pageSize: exportAll ? result.rows.length : REPORT_BLOCK_PAGE_SIZE,
          totalPages: exportAll ? 1 : totalPages
        });
      } else if (block.kind === "chart") {
        const chartConfig = config;
        validateIdentifier(chartConfig.xColumn, "column");
        validateIdentifier(chartConfig.yColumn, "column");
        let selectPart;
        if (chartConfig.aggregateFunction && chartConfig.groupBy) {
          const aggFunc = chartConfig.aggregateFunction.toUpperCase();
          if (!["COUNT", "SUM", "AVG", "MIN", "MAX"].includes(aggFunc)) {
            return res.status(400).json({ error: "Invalid aggregate function" });
          }
          const dateGroupByValues = ["month", "year", "day", "week", "quarter"];
          const isDateGroupBy = dateGroupByValues.includes(chartConfig.groupBy.toLowerCase());
          let groupByExpr;
          let labelExpr;
          if (isDateGroupBy) {
            const dateCol = `"${chartConfig.xColumn}"`;
            const dateColPST = `(${dateCol} AT TIME ZONE 'UTC' AT TIME ZONE 'America/Los_Angeles')`;
            const datePart = chartConfig.groupBy.toLowerCase();
            if (datePart === "month") {
              labelExpr = `TO_CHAR(${dateColPST}, 'YYYY-MM')`;
              groupByExpr = labelExpr;
            } else if (datePart === "year") {
              labelExpr = `TO_CHAR(${dateColPST}, 'YYYY')`;
              groupByExpr = labelExpr;
            } else if (datePart === "day") {
              labelExpr = `TO_CHAR(${dateColPST}, 'YYYY-MM-DD')`;
              groupByExpr = labelExpr;
            } else if (datePart === "week") {
              labelExpr = `TO_CHAR(${dateColPST}, 'IYYY-IW')`;
              groupByExpr = labelExpr;
            } else if (datePart === "quarter") {
              labelExpr = `TO_CHAR(${dateColPST}, 'YYYY-"Q"Q')`;
              groupByExpr = labelExpr;
            } else {
              labelExpr = dateCol;
              groupByExpr = dateCol;
            }
          } else {
            validateIdentifier(chartConfig.groupBy, "column");
            labelExpr = `"${chartConfig.groupBy}"`;
            groupByExpr = `"${chartConfig.groupBy}"`;
          }
          selectPart = `${labelExpr} as label, ${aggFunc}("${chartConfig.yColumn}") as value`;
          query = `SELECT ${selectPart} FROM ${tableRef}`;
          if (chartConfig.filters?.length > 0) {
            const whereClauses = [];
            chartConfig.filters.forEach((f) => {
              addFilterToQuery(f, params, whereClauses);
            });
            query += ` WHERE ${whereClauses.join(" AND ")}`;
          }
          query += ` GROUP BY ${groupByExpr} ORDER BY ${groupByExpr} LIMIT 500`;
        } else {
          selectPart = `"${chartConfig.xColumn}" as label, "${chartConfig.yColumn}" as value`;
          query = `SELECT ${selectPart} FROM ${tableRef}`;
          if (chartConfig.filters?.length > 0) {
            const whereClauses = [];
            chartConfig.filters.forEach((f) => {
              addFilterToQuery(f, params, whereClauses);
            });
            query += ` WHERE ${whereClauses.join(" AND ")}`;
          }
          query += ` LIMIT 500`;
        }
        const result = await pool2.query(query, params);
        await logAudit({
          userId,
          userEmail: user.email,
          action: "REPORT_QUERY",
          database: config.database,
          table: config.table,
          details: `Chart block query: ${result.rows.length} data points`,
          ip: req.ip || req.socket.remoteAddress
        });
        res.json({
          type: "chart",
          chartType: chartConfig.chartType,
          data: result.rows
        });
      } else if (block.kind === "metric") {
        const metricConfig = config;
        const aggFunc = metricConfig.aggregateFunction.toUpperCase();
        if (!["COUNT", "SUM", "AVG", "MIN", "MAX"].includes(aggFunc)) {
          return res.status(400).json({ error: "Invalid aggregate function" });
        }
        const mainAlias = "m";
        const joinAlias = "joined";
        let fromClause = `${tableRef} AS ${mainAlias}`;
        let columnRef;
        if (metricConfig.column.includes(".")) {
          const parts = metricConfig.column.split(".");
          if (parts.length !== 2) {
            return res.status(400).json({ error: "Invalid column reference format" });
          }
          const [prefix, colName] = parts;
          validateIdentifier(colName, "column");
          if (!metricConfig.join) {
            return res.status(400).json({ error: "Cannot use dotted column reference without join config" });
          }
          columnRef = `${joinAlias}."${colName}"`;
        } else {
          validateIdentifier(metricConfig.column, "column");
          columnRef = `${mainAlias}."${metricConfig.column}"`;
        }
        if (metricConfig.join?.table) {
          const joinConfig = metricConfig.join;
          const joinParsed = parseTableName(joinConfig.table);
          if (!joinParsed) {
            return res.status(400).json({ error: "Invalid join table name" });
          }
          const joinTableRef = `"${joinParsed.schema}"."${joinParsed.table}"`;
          const joinType = joinConfig.type === "inner" ? "INNER JOIN" : "LEFT JOIN";
          const [fromCol, toCol] = joinConfig.on;
          validateIdentifier(fromCol, "column");
          validateIdentifier(toCol, "column");
          fromClause += ` ${joinType} ${joinTableRef} AS ${joinAlias} ON ${mainAlias}."${fromCol}" = ${joinAlias}."${toCol}"`;
        }
        query = `SELECT ${aggFunc}(${columnRef}) as value FROM ${fromClause}`;
        if (metricConfig.filters?.length > 0) {
          const whereClauses = [];
          metricConfig.filters.forEach((f) => {
            const filterCol = f.column.includes(".") ? `${f.column.split(".")[0]}."${f.column.split(".")[1]}"` : `${mainAlias}."${f.column}"`;
            const paramIndex = params.length + 1;
            if (f.operator === "eq") {
              whereClauses.push(`${filterCol} = $${paramIndex}`);
              params.push(f.value);
            } else if (f.operator === "contains") {
              whereClauses.push(`${filterCol}::text ILIKE $${paramIndex}`);
              params.push(`%${f.value}%`);
            } else if (f.operator === "gt") {
              whereClauses.push(`${filterCol} > $${paramIndex}`);
              params.push(f.value);
            } else if (f.operator === "gte") {
              whereClauses.push(`${filterCol} >= $${paramIndex}`);
              params.push(f.value);
            } else if (f.operator === "lt") {
              whereClauses.push(`${filterCol} < $${paramIndex}`);
              params.push(f.value);
            } else if (f.operator === "lte") {
              whereClauses.push(`${filterCol} <= $${paramIndex}`);
              params.push(f.value);
            } else if (f.operator === "between" && Array.isArray(f.value) && f.value.length === 2) {
              whereClauses.push(`${filterCol} >= $${paramIndex} AND ${filterCol} <= $${paramIndex + 1}`);
              params.push(f.value[0], f.value[1]);
            } else if (f.operator === "in" && Array.isArray(f.value)) {
              whereClauses.push(`${filterCol} = ANY($${paramIndex})`);
              params.push(f.value);
            }
          });
          query += ` WHERE ${whereClauses.join(" AND ")}`;
        }
        const result = await pool2.query(query, params);
        await logAudit({
          userId,
          userEmail: user.email,
          action: "REPORT_QUERY",
          database: config.database,
          table: config.table,
          details: `Metric block query: ${metricConfig.aggregateFunction}(${metricConfig.column})`,
          ip: req.ip || req.socket.remoteAddress
        });
        res.json({
          type: "metric",
          value: result.rows[0]?.value || 0,
          label: metricConfig.label || `${metricConfig.aggregateFunction}(${metricConfig.column})`,
          format: metricConfig.format || "number"
        });
      }
    } catch (err) {
      console.error("Error running report block:", err);
      res.status(500).json({ error: err instanceof Error ? err.message : "Failed to run report block" });
    }
  });
  app2.get("/api/reports/pages/:pageId/chat", isAuthenticated, async (req, res) => {
    try {
      const userId = req.user?.id;
      if (!userId) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      const { pageId } = req.params;
      const [page] = await db.select().from(reportPages).where(and(eq2(reportPages.id, pageId), eq2(reportPages.userId, userId)));
      if (!page) {
        return res.status(404).json({ error: "Report page not found" });
      }
      const [session2] = await db.select().from(reportChatSessions).where(eq2(reportChatSessions.pageId, pageId));
      res.json({ messages: session2?.messages || [] });
    } catch (err) {
      console.error("Error fetching chat history:", err);
      res.status(500).json({ error: "Failed to fetch chat history" });
    }
  });
  app2.post("/api/reports/ai/chat", isAuthenticated, reportAILimiter, async (req, res) => {
    try {
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (!userId || !user) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      const { pageId, message } = req.body;
      if (!pageId || !message) {
        return res.status(400).json({ error: "pageId and message are required" });
      }
      const [page] = await db.select().from(reportPages).where(and(eq2(reportPages.id, pageId), eq2(reportPages.userId, userId)));
      if (!page) {
        return res.status(404).json({ error: "Report page not found" });
      }
      const client = getOpenAIClient();
      if (!client) {
        return res.status(503).json({ error: "AI service not available" });
      }
      const dbs = getDatabaseConnections();
      let availableTablesWithColumns = [];
      for (const dbConn of dbs) {
        try {
          const pool2 = getPool(dbConn.name);
          const tableResult = await pool2.query(`
            SELECT table_schema as schema, table_name as name
            FROM information_schema.tables
            WHERE table_schema = 'public' AND table_type = 'BASE TABLE'
            ORDER BY table_name
          `);
          const allSettings = await storage.getAllTableSettings();
          let tables = [];
          const allowedTables = user.role === "external_customer" ? await getAllowedTables(userId) : null;
          for (const t of tableResult.rows) {
            const fullName = `${t.schema}.${t.name}`;
            const settingsKey = `${dbConn.name}:${fullName}`;
            const isVisible = allSettings[settingsKey]?.isVisible ?? true;
            if (allowedTables && !allowedTables.includes(`${dbConn.name}:${fullName}`)) continue;
            const columnResult = await pool2.query(`
              SELECT column_name FROM information_schema.columns
              WHERE table_schema = $1 AND table_name = $2
              ORDER BY ordinal_position
            `, [t.schema, t.name]);
            tables.push({
              schema: t.schema,
              name: t.name,
              fullName,
              displayName: allSettings[settingsKey]?.displayName || null,
              isVisible,
              columns: columnResult.rows.map((c) => c.column_name)
            });
          }
          availableTablesWithColumns.push({ database: dbConn.name, tables });
        } catch (err) {
          console.error(`Error fetching tables for ${dbConn.name}:`, err);
        }
      }
      const blocks = await db.select().from(reportBlocks).where(eq2(reportBlocks.pageId, pageId));
      let [session2] = await db.select().from(reportChatSessions).where(eq2(reportChatSessions.pageId, pageId));
      if (!session2) {
        [session2] = await db.insert(reportChatSessions).values({ pageId, messages: [] }).returning();
      }
      const messages = session2.messages || [];
      messages.push({
        role: "user",
        content: message,
        timestamp: (/* @__PURE__ */ new Date()).toISOString()
      });
      const today = /* @__PURE__ */ new Date();
      const pacificFormatter = new Intl.DateTimeFormat("en-CA", {
        timeZone: "America/Los_Angeles",
        year: "numeric",
        month: "2-digit",
        day: "2-digit"
      });
      const todayStr = pacificFormatter.format(today);
      const systemPrompt = `You are a helpful report building assistant. You help users create custom reports with tables, charts, and metrics.

IMPORTANT: Today's date is ${todayStr} (Pacific Time - PST/PDT). Use this for any relative date references like "yesterday", "last week", "this month", etc. All date/time queries should be interpreted in Pacific Time.

IMPORTANT: You MUST only use the exact column names listed below. Do NOT guess or invent column names.

AVAILABLE TABLES AND COLUMNS:
${availableTablesWithColumns.map(
        (db2) => `Database: ${db2.database}
${db2.tables.map(
          (t) => `  - ${t.displayName || t.name} (${t.fullName})
    Columns: ${t.columns.slice(0, 20).join(", ")}${t.columns.length > 20 ? ` (and ${t.columns.length - 20} more)` : ""}`
        ).join("\n")}`
      ).join("\n\n")}

CURRENT REPORT: "${page.title}"
CURRENT BLOCKS: ${blocks.length === 0 ? "None yet" : blocks.map((b) => `${b.kind}: ${b.title || "Untitled"}`).join(", ")}

You can help users by:
1. Suggesting which tables to use for their reporting needs
2. Recommending chart types (bar, line, pie, area) for their data
3. Explaining what metrics (count, sum, avg, min, max) would be useful
4. Helping structure their reports

When the user wants to add a block, respond with a JSON action in this format:
{
  "action": "create_block",
  "block": {
    "kind": "table|chart|metric|text",
    "title": "Block title",
    "config": { ... config based on kind ... }
  },
  "explanation": "Why this block is useful"
}

To create MULTIPLE blocks at once (for comparisons), use this format:
{
  "action": "create_blocks",
  "blocks": [
    { "kind": "table", "title": "Period 1", "config": { ... } },
    { "kind": "table", "title": "Period 2", "config": { ... } }
  ],
  "explanation": "Why these blocks are useful for comparison"
}

For table blocks, config should have: database, table, columns (array of exact column names from above), filters (array), orderBy, rowLimit
For table blocks with JOINS (to pull data from related tables):
- Add a "join" object with: table (the related table like "public.vendors"), on (array of two column names [fromColumn, toColumn] like ["vendor_id", "id"])
- For columns from the joined table, prefix with "joined." like "joined.email" or "joined.first_name"
- CRITICAL: When using joins, you MUST use the EXACT column names from the joined table as listed in AVAILABLE TABLES above. For example, if the vendors table has "first_name" and "last_name" columns, use "joined.first_name" and "joined.last_name" (NOT "joined.firstname" or "joined.lastName")
- Example join config: { "table": "public.vendors", "on": ["vendor_id", "id"] }
- Check the column list for the joined table before constructing joined.column_name references

For NESTED JOINS (sub-joins) when you need to traverse through two tables:
- Add a "subJoin" object inside the "join" object to join from the first joined table to a third table
- subJoin has: table (the third table like "public.districts"), on (array of [fromColumnInJoinTable, toColumnInSubJoinTable])
- For columns from the sub-joined table, use prefix "joined_" followed by the table name, like "joined_districts.name" or "joined_district.name"
- Example: To get district name from bookings -> addresses -> districts:
  { "join": { "table": "public.addresses", "on": ["address_id", "id"], "subJoin": { "table": "public.districts", "on": ["district_id", "id"] } } }
  Then use columns like "joined_district.name" to get the district name (NOT "label" - use the EXACT column name from districts table which is "name")
- CRITICAL: Always check the AVAILABLE TABLES list above for the exact column names in the sub-joined table

For chart blocks, config should have: database, table, chartType, xColumn (the date/timestamp column to group by), yColumn (the column to aggregate), aggregateFunction, groupBy (can be a column name OR one of: "month", "year", "day", "week", "quarter" for date-based grouping), filters, rowLimit
For metric blocks, config should have: database, table, column, aggregateFunction, filters, label, format

FILTER OPERATORS - ONLY USE THESE EXACT VALUES (no others allowed):
- "eq" for equals (use this for exact matches of a SINGLE value)
- "in" for matching ANY of multiple values (use this when user provides a LIST of values like multiple email addresses, IDs, or names)
- "contains" for text contains/partial match
- "gt" for greater than
- "gte" for greater than or equal
- "lt" for less than
- "lte" for less than or equal
- "between" for date ranges (value must be array of two dates like ["2025-01-01", "2025-12-31"])

\u26A0\uFE0F CRITICAL - LIST FILTERING: When a user provides a LIST of values (multiple emails, IDs, names, etc.), you MUST use ONE filter with the "in" operator and an array of ALL values.
CORRECT: {"column": "email", "operator": "in", "value": ["a@test.com", "b@test.com", "c@test.com"]}
WRONG: Multiple filters with "eq" on the same column - this creates AND logic and returns ZERO results!
IMPORTANT: Do NOT use "like", "!=", or any other operators not listed above.

CRITICAL DATE RANGE COMPARISONS: When the user asks to compare TWO different date ranges (e.g., "Jan 5-11 vs Jan 12-18"), you MUST create TWO SEPARATE blocks - one for each date range. This is because all filters are combined with AND logic, so putting two "between" filters on the same column in one block will return zero results (a date cannot be in two non-overlapping ranges simultaneously). For comparisons, create separate blocks like:
- Block 1: "Bookings: Jan 5-11, 2026" with filter between ["2026-01-05", "2026-01-11"]
- Block 2: "Bookings: Jan 12-18, 2026" with filter between ["2026-01-12", "2026-01-18"]
This allows side-by-side comparison of the two periods.

CRITICAL: Only use column names that are listed above. For date-based grouping, use groupBy: "month" (or year/day/week/quarter) with xColumn set to the date column like "created_at".

If you're just providing information or need clarification, respond with plain text.
IMPORTANT: If you cannot create a block because the request is unclear, you don't have enough information, or you're unsure which columns/tables to use, you MUST ask the user a clarifying question. Never leave the user without a response - either create a block OR ask a specific question to help you understand what they need.
Always be helpful and explain your suggestions in simple terms.`;
      const response = await client.chat.completions.create({
        model: "gpt-4o",
        messages: [
          { role: "system", content: systemPrompt },
          ...messages.slice(-10).map((m) => ({ role: m.role, content: m.content }))
        ],
        max_completion_tokens: 1e3,
        temperature: 0.7
      });
      let assistantMessage = response.choices[0]?.message?.content || "I'm sorry, I couldn't process that request.";
      messages.push({
        role: "assistant",
        content: assistantMessage,
        timestamp: (/* @__PURE__ */ new Date()).toISOString()
      });
      await db.update(reportChatSessions).set({ messages, updatedAt: /* @__PURE__ */ new Date() }).where(eq2(reportChatSessions.id, session2.id));
      await logAudit({
        userId,
        userEmail: user.email,
        action: "REPORT_AI_CHAT",
        details: `AI chat in report: ${page.title}`,
        ip: req.ip || req.socket.remoteAddress
      });
      let action = null;
      let validatedAction = null;
      let displayMessage = assistantMessage;
      try {
        const jsonMatch = assistantMessage.match(/\{[\s\S]*"action"[\s\S]*\}/);
        if (jsonMatch) {
          action = JSON.parse(jsonMatch[0]);
          displayMessage = assistantMessage.replace(jsonMatch[0], "").trim();
          displayMessage = displayMessage.replace(/```json\s*/g, "").replace(/```\s*/g, "").trim();
          const validKinds = ["table", "chart", "metric", "text"];
          const validateSingleBlock = async (block) => {
            if (!validKinds.includes(block.kind)) return null;
            const config = {
              ...block.config,
              database: block.config?.database || "Default",
              rowLimit: Math.min(block.config?.rowLimit || 500, 1e4),
              filters: block.config?.filters || []
            };
            const validation = await validateBlockConfig(config, block.kind, user, { bypassVisibility: true });
            if (validation.valid) {
              return {
                kind: block.kind,
                title: block.title || `${block.kind} block`,
                config
              };
            } else {
              console.log(`[SECURITY] AI suggested invalid block config: ${validation.error}`);
              return { error: validation.error };
            }
          };
          if (action?.action === "create_block" && action?.block) {
            const result = await validateSingleBlock(action.block);
            if (result && !("error" in result)) {
              validatedAction = {
                action: "create_block",
                block: result,
                explanation: action.explanation || ""
              };
            } else if (result && "error" in result) {
              displayMessage += `

**Note:** I wasn't able to create this block because: ${result.error}. Please try rephrasing your request or ask me which columns are available in the table you want to use.`;
            }
          }
          if (action?.action === "create_blocks" && Array.isArray(action?.blocks)) {
            const validatedBlocks = [];
            const errors = [];
            for (const block of action.blocks) {
              const result = await validateSingleBlock(block);
              if (result && !("error" in result)) {
                validatedBlocks.push(result);
              } else if (result && "error" in result && result.error) {
                errors.push(result.error);
              }
            }
            if (validatedBlocks.length > 0) {
              validatedAction = {
                action: "create_blocks",
                blocks: validatedBlocks,
                explanation: action.explanation || ""
              };
            }
            if (errors.length > 0) {
              displayMessage += `

**Note:** Some blocks could not be created: ${errors.join("; ")}. Please try rephrasing your request.`;
            }
          }
        }
      } catch {
      }
      let finalMessage = displayMessage;
      if (!finalMessage && validatedAction?.explanation) {
        finalMessage = validatedAction.explanation;
      } else if (validatedAction?.explanation && !finalMessage.includes(validatedAction.explanation)) {
        finalMessage = finalMessage ? `${finalMessage}

${validatedAction.explanation}` : validatedAction.explanation;
      }
      res.json({
        message: finalMessage || "I've created the block for you.",
        action: validatedAction
      });
    } catch (err) {
      console.error("Error in AI chat:", err);
      res.status(500).json({ error: "Failed to process AI request" });
    }
  });
  app2.get("/api/zones/:database", isAuthenticated, async (req, res) => {
    try {
      const { database } = req.params;
      const pool2 = getPool(database);
      const result = await pool2.query(`
        SELECT DISTINCT abbreviation as zone
        FROM public.districts
        WHERE abbreviation IS NOT NULL AND abbreviation != ''
        ORDER BY abbreviation
      `);
      res.json(result.rows.map((r) => r.zone));
    } catch (err) {
      console.error("Error fetching zones:", err);
      res.status(500).json({ error: "Failed to fetch zones" });
    }
  });
  app2.get("/api/weekly-performance/:database", isAuthenticated, async (req, res) => {
    try {
      const { database } = req.params;
      const periodType = req.query.periodType || "weekly";
      const zonesParam = req.query.zones;
      const forceRefresh = req.query.refresh === "true";
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (!user) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      const selectedZones = zonesParam ? zonesParam.split(",").filter((z3) => z3.trim()) : [];
      const cacheKey = getCacheKey("marketing", database, periodType, void 0, selectedZones);
      if (!forceRefresh) {
        const cachedData = getFromCache(cacheKey);
        if (cachedData) {
          console.log(`[Cache HIT] Marketing dashboard: ${cacheKey}`);
          return res.json({ ...cachedData, fromCache: true });
        }
      }
      console.log(`[Cache MISS] Marketing dashboard: ${cacheKey}${forceRefresh ? " (force refresh)" : ""}`);
      const pool2 = getPool(database);
      const buildZoneFilter = (bookingAlias = "b", paramOffset = 2) => {
        if (selectedZones.length === 0) {
          return { clause: "", params: [] };
        }
        const placeholders = selectedZones.map((_, i) => `$${paramOffset + i}`).join(", ");
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
      };
      const periods = [];
      const now = /* @__PURE__ */ new Date();
      const getPSTOffset = (date) => {
        const month = date.getMonth();
        const day = date.getDate();
        if (month > 2 && month < 10) {
          return "-07:00";
        } else if (month < 2 || month > 10) {
          return "-08:00";
        } else if (month === 2) {
          const secondSunday = 14 - new Date(date.getFullYear(), 2, 1).getDay();
          return day >= secondSunday ? "-07:00" : "-08:00";
        } else {
          const firstSunday = 7 - new Date(date.getFullYear(), 10, 1).getDay();
          if (firstSunday === 7) return day >= 7 ? "-08:00" : "-07:00";
          return day >= firstSunday ? "-08:00" : "-07:00";
        }
      };
      if (periodType === "monthly") {
        for (let i = 0; i < 12; i++) {
          const monthStart = new Date(now.getFullYear(), now.getMonth() - i, 1);
          const monthEnd = new Date(now.getFullYear(), now.getMonth() - i + 1, 1);
          if (monthStart > now) continue;
          const startOffset = getPSTOffset(monthStart);
          const endOffset = getPSTOffset(monthEnd);
          const startDateStr = `${monthStart.getFullYear()}-${String(monthStart.getMonth() + 1).padStart(2, "0")}-01T00:00:00${startOffset}`;
          const endDateStr = `${monthEnd.getFullYear()}-${String(monthEnd.getMonth() + 1).padStart(2, "0")}-01T00:00:00${endOffset}`;
          const label = monthStart.toLocaleDateString("en-US", { month: "long", year: "numeric" });
          periods.push({
            startUTC: new Date(startDateStr).toISOString(),
            endUTC: new Date(endDateStr).toISOString(),
            label
          });
        }
      } else {
        let currentYear = 2025;
        let currentMonth = 11;
        let currentDay = 29;
        while (true) {
          const weekStartDate = new Date(currentYear, currentMonth, currentDay);
          const startOffset = getPSTOffset(weekStartDate);
          const startDateStr = `${currentYear}-${String(currentMonth + 1).padStart(2, "0")}-${String(currentDay).padStart(2, "0")}T00:00:00${startOffset}`;
          const weekStartUTC = new Date(startDateStr).toISOString();
          const weekEndDate = new Date(currentYear, currentMonth, currentDay + 7);
          const endYear = weekEndDate.getFullYear();
          const endMonth = weekEndDate.getMonth();
          const endDay = weekEndDate.getDate();
          const endOffset = getPSTOffset(weekEndDate);
          const endDateStr = `${endYear}-${String(endMonth + 1).padStart(2, "0")}-${String(endDay).padStart(2, "0")}T00:00:00${endOffset}`;
          const weekEndUTC = new Date(endDateStr).toISOString();
          if (new Date(weekStartUTC) > now) break;
          const labelEndDate = new Date(currentYear, currentMonth, currentDay + 6);
          const startMonthLabel = weekStartDate.toLocaleDateString("en-US", { month: "short" });
          const startDayLabel = weekStartDate.getDate();
          const endMonthLabel = labelEndDate.toLocaleDateString("en-US", { month: "short" });
          const endDayLabel = labelEndDate.getDate();
          const label = startMonthLabel === endMonthLabel ? `${startMonthLabel} ${startDayLabel} - ${endDayLabel}` : `${startMonthLabel} ${startDayLabel} - ${endMonthLabel} ${endDayLabel}`;
          periods.push({ startUTC: weekStartUTC, endUTC: weekEndUTC, label });
          const nextMonday = new Date(currentYear, currentMonth, currentDay + 7);
          currentYear = nextMonday.getFullYear();
          currentMonth = nextMonday.getMonth();
          currentDay = nextMonday.getDate();
        }
        periods.reverse();
      }
      const periodsData = [];
      for (const period of periods) {
        const periodStartUTC = period.startUTC;
        const periodEndUTC = period.endUTC;
        const zoneFilter = buildZoneFilter("b", 3);
        const zoneFilterNoAlias = buildZoneFilter("public.bookings", 3);
        const baseParams = [periodStartUTC, periodEndUTC];
        const paramsWithZones = [...baseParams, ...zoneFilter.params];
        const [
          bookingsCreatedResult,
          bookingsDueResult,
          bookingsCompletedResult,
          revenueResult,
          signupsResult,
          newUsersWithBookingsResult,
          subscriptionRevenueResult,
          subscriptionFeesResult,
          memberBookingsResult,
          newSubscriptionsResult,
          memberBookingsRevenueResult,
          customerFeesResult,
          tipsResult,
          creditPacksResult,
          refundsResult
        ] = await Promise.all([
          // 1. Bookings Created (created_at in week)
          pool2.query(`
            SELECT COUNT(*) as count 
            FROM public.bookings b
            WHERE b.created_at >= $1 AND b.created_at < $2
            ${zoneFilter.clause}
          `, paramsWithZones),
          // 2. Bookings Due (date_due in week)
          pool2.query(`
            SELECT COUNT(*) as count 
            FROM public.bookings b
            WHERE b.date_due >= $1 AND b.date_due < $2
            ${zoneFilter.clause}
          `, paramsWithZones),
          // 3. Bookings Completed (date_due in week AND status = 'done')
          pool2.query(`
            SELECT COUNT(*) as count 
            FROM public.bookings b
            WHERE b.date_due >= $1 AND b.date_due < $2 AND b.status = 'done'
            ${zoneFilter.clause}
          `, paramsWithZones),
          // 6, 7, 8: Revenue metrics for completed bookings (including stripe fees)
          pool2.query(`
            SELECT 
              COALESCE(AVG(b.price), 0) as avg_price,
              COALESCE(SUM(b.price), 0) as total_revenue,
              COALESCE(SUM(b.margin), 0) as total_profit,
              COALESCE(SUM(b.stripe_fee), 0) as total_stripe_fees
            FROM public.bookings b
            WHERE b.date_due >= $1 AND b.date_due < $2 AND b.status = 'done'
            ${zoneFilter.clause}
          `, paramsWithZones),
          // 10. Sign Ups (new users created_at in week) - NOT zone-filtered (users don't have zones)
          pool2.query(`
            SELECT COUNT(*) as count 
            FROM public.users 
            WHERE created_at >= $1 AND created_at < $2
          `, baseParams),
          // 11. New Users who have any booking (signed up in week AND have at least one booking ever)
          // Zone filter applies to the booking join
          pool2.query(`
            SELECT COUNT(DISTINCT u.id) as count 
            FROM public.users u
            INNER JOIN public.bookings b ON b.user_id = u.id
            WHERE u.created_at >= $1 AND u.created_at < $2
            ${zoneFilter.clause}
          `, paramsWithZones),
          // 13. Subscription Revenue and Margin (price and margin of UNIQUE completed bookings with date_due in week, linked to subscription_usages)
          pool2.query(`
            SELECT 
              COALESCE(SUM(price), 0) as total_revenue,
              COALESCE(SUM(margin), 0) as total_margin
            FROM (
              SELECT DISTINCT b.id, b.price, b.margin
              FROM public.bookings b
              INNER JOIN public.subscription_usages su ON su.booking_id = b.id
              WHERE b.date_due >= $1 AND b.date_due < $2
                AND b.status = 'done'
                ${zoneFilter.clause}
            ) unique_bookings
          `, paramsWithZones).catch(() => ({ rows: [{ total_revenue: 0, total_margin: 0 }] })),
          // 13b. Subscription Fees (paid subscription_invoices updated in week, with price based on price_plan_id)
          // Use DISTINCT ON to avoid double-counting when same subscription has multiple invoices
          // Exclude subscriptions with status='trialing' (first month free)
          // Also includes $59 cancellation fees for subscriptions with valid cancellation_fee_charge_id
          // Note: Subscription fees are NOT zone-filtered (subscriptions don't have zones directly)
          pool2.query(`
            SELECT COALESCE(
              (SELECT SUM(fee_amount) FROM (
                SELECT DISTINCT ON (si.subscription_id)
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
                ORDER BY si.subscription_id, si.updated_at DESC
              ) invoice_fees), 0) +
              COALESCE(
                (SELECT COUNT(*) * 59.00
                FROM public.subscriptions
                WHERE updated_at >= $1 AND updated_at < $2
                  AND cancellation_fee_charge_id IS NOT NULL 
                  AND cancellation_fee_charge_id != ''
              ), 0) as total
          `, baseParams).catch(() => ({ rows: [{ total: 0 }] })),
          // 14. Member Bookings (unique completed bookings with date_due in week, linked to subscription_usages)
          pool2.query(`
            SELECT COUNT(DISTINCT b.id) as count
            FROM public.bookings b
            INNER JOIN public.subscription_usages su ON su.booking_id = b.id
            WHERE b.date_due >= $1 AND b.date_due < $2
              AND b.status = 'done'
              ${zoneFilter.clause}
          `, paramsWithZones).catch(() => ({ rows: [{ count: 0 }] })),
          // 16. New Membership Signups - NOT zone-filtered (subscriptions don't have zones)
          pool2.query(`
            SELECT COUNT(*) as count
            FROM public.subscriptions
            WHERE created_at >= $1 AND created_at < $2
          `, baseParams).catch(() => ({ rows: [{ count: 0 }] })),
          // Revenue from member bookings (for % calculation) - UNIQUE bookings only
          pool2.query(`
            SELECT COALESCE(SUM(price), 0) as total
            FROM (
              SELECT DISTINCT b.id, b.price
              FROM public.bookings b
              INNER JOIN public.subscription_usages su ON su.booking_id = b.id
              WHERE b.date_due >= $1 AND b.date_due < $2 AND b.status = 'done'
                ${zoneFilter.clause}
            ) unique_bookings
          `, paramsWithZones).catch(() => ({ rows: [{ total: 0 }] })),
          // Customer fees charged in the week (exclude waived fees and those without charge_id) - NOT zone-filtered
          pool2.query(`
            SELECT COALESCE(SUM(amount), 0) as total
            FROM public.customer_fees
            WHERE created_at >= $1 AND created_at < $2
              AND (waived IS NULL OR waived != true)
              AND charge_id IS NOT NULL AND charge_id != ''
          `, baseParams).catch(() => ({ rows: [{ total: 0 }] })),
          // Tips from booking_tips where tip was created in the week
          // Zone filter via the linked booking
          pool2.query(`
            SELECT 
              COALESCE(SUM(bt.tip_amount), 0) as tip_revenue,
              COALESCE(SUM(bt.tip_amount - bt.vendor_amount), 0) as tip_profit
            FROM public.booking_tips bt
            ${selectedZones.length > 0 ? "INNER JOIN public.bookings b ON b.id = bt.booking_id" : ""}
            WHERE bt.created_at >= $1 AND bt.created_at < $2
            ${zoneFilter.clause}
          `, paramsWithZones).catch(() => ({ rows: [{ tip_revenue: 0, tip_profit: 0 }] })),
          // Credit packs purchased in the week - NOT zone-filtered (credit packs don't have zones)
          pool2.query(`
            SELECT COALESCE(SUM(pay_amount), 0) as total
            FROM (
              SELECT DISTINCT ON (uct.id) uct.id, cp.pay_amount
              FROM public.user_credits_transactions uct
              INNER JOIN public.credits_packs cp ON uct.amount = cp.get_amount
              WHERE uct.created_at >= $1 AND uct.created_at < $2
                AND uct.user_credits_transaction_type_id = 16
            ) unique_transactions
          `, baseParams).catch(() => ({ rows: [{ total: 0 }] })),
          // Refunds from booking_refunds (created_at in week) - subtract from total revenue
          // Zone filter via the linked booking
          pool2.query(`
            SELECT COALESCE(SUM(br.total), 0) as total
            FROM public.booking_refunds br
            ${selectedZones.length > 0 ? "INNER JOIN public.bookings b ON b.id = br.booking_id" : ""}
            WHERE br.created_at >= $1 AND br.created_at < $2
            ${zoneFilter.clause}
          `, paramsWithZones).catch(() => ({ rows: [{ total: 0 }] }))
        ]);
        const bookingsCreated = parseInt(bookingsCreatedResult.rows[0]?.count || "0");
        const bookingsDue = parseInt(bookingsDueResult.rows[0]?.count || "0");
        const bookingsCompleted = parseInt(bookingsCompletedResult.rows[0]?.count || "0");
        const avgPerDay = bookingsCompleted / 7;
        const conversion = bookingsDue > 0 ? bookingsCompleted / bookingsDue * 100 : 0;
        const avgBookingPrice = parseFloat(revenueResult.rows[0]?.avg_price || "0");
        const bookingRevenue = parseFloat(revenueResult.rows[0]?.total_revenue || "0");
        const bookingProfit = parseFloat(revenueResult.rows[0]?.total_profit || "0");
        const stripeFees = parseFloat(revenueResult.rows[0]?.total_stripe_fees || "0");
        const subscriptionBookingRevenue = parseFloat(subscriptionRevenueResult.rows[0]?.total_revenue || "0");
        const subscriptionBookingProfit = parseFloat(subscriptionRevenueResult.rows[0]?.total_margin || "0");
        const subscriptionFees = parseFloat(subscriptionFeesResult.rows[0]?.total || "0");
        const customerFees = parseFloat(customerFeesResult.rows[0]?.total || "0");
        const tipRevenue = parseFloat(tipsResult.rows[0]?.tip_revenue || "0");
        const tipProfit = parseFloat(tipsResult.rows[0]?.tip_profit || "0");
        const creditPackRevenue = parseFloat(creditPacksResult.rows[0]?.total || "0");
        const refundsTotal = parseFloat(refundsResult.rows[0]?.total || "0");
        const subscriptionRevenue = subscriptionBookingRevenue + subscriptionFees;
        const totalRevenue = bookingRevenue + subscriptionFees + customerFees + tipRevenue + creditPackRevenue - refundsTotal - stripeFees;
        console.log(`[REVENUE DEBUG] ${period.label}: Booking=$${bookingRevenue.toFixed(2)}, SubFees=$${subscriptionFees.toFixed(2)}, CustFees=$${customerFees.toFixed(2)}, Tips=$${tipRevenue.toFixed(2)}, CreditPacks=$${creditPackRevenue.toFixed(2)}, Refunds=$${refundsTotal.toFixed(2)}, StripeFees=$${stripeFees.toFixed(2)}, TOTAL=$${totalRevenue.toFixed(2)}`);
        const totalProfit = bookingProfit + subscriptionFees + customerFees + tipProfit - refundsTotal;
        const marginPercent = totalRevenue > 0 ? totalProfit / totalRevenue * 100 : 0;
        const signups = parseInt(signupsResult.rows[0]?.count || "0");
        const newUsersWithBookings = parseInt(newUsersWithBookingsResult.rows[0]?.count || "0");
        const newUserConversion = signups > 0 ? newUsersWithBookings / signups * 100 : 0;
        const memberBookings = parseInt(memberBookingsResult.rows[0]?.count || "0");
        const newSubscriptions = parseInt(newSubscriptionsResult.rows[0]?.count || "0");
        const memberBookingsRevenue = parseFloat(memberBookingsRevenueResult.rows[0]?.total || "0");
        const membershipRevenuePercent = totalRevenue > 0 ? subscriptionRevenue / totalRevenue * 100 : 0;
        periodsData.push({
          periodLabel: period.label,
          periodStart: periodStartUTC,
          periodEnd: periodEndUTC,
          metrics: {
            bookingsCreated,
            bookingsDue,
            bookingsCompleted,
            avgPerDay: Math.round(avgPerDay * 100) / 100,
            conversion: Math.round(conversion * 100) / 100,
            avgBookingPrice: Math.round(avgBookingPrice * 100) / 100,
            totalRevenue: Math.round(totalRevenue * 100) / 100,
            totalProfit: Math.round(totalProfit * 100) / 100,
            marginPercent: Math.round(marginPercent * 100) / 100,
            signups,
            newUsersWithBookings,
            newUserConversion: Math.round(newUserConversion * 100) / 100,
            subscriptionRevenue: Math.round(subscriptionRevenue * 100) / 100,
            subscriptionFees: Math.round(subscriptionFees * 100) / 100,
            memberBookings,
            membershipRevenuePercent: Math.round(membershipRevenuePercent * 100) / 100,
            newSubscriptions
          }
        });
      }
      const periodsDataWithVariance = periodsData.map((periodItem, index2) => {
        if (periodType === "monthly") {
          if (index2 === periodsData.length - 1) {
            return { ...periodItem, variance: null };
          }
          const prev = periodsData[index2 + 1].metrics;
          const curr = periodItem.metrics;
          const calcVariance = (current, previous) => {
            if (previous === 0) return current > 0 ? 100 : 0;
            return Math.round((current - previous) / previous * 100 * 100) / 100;
          };
          return {
            ...periodItem,
            variance: {
              bookingsCreated: calcVariance(curr.bookingsCreated, prev.bookingsCreated),
              bookingsDue: calcVariance(curr.bookingsDue, prev.bookingsDue),
              bookingsCompleted: calcVariance(curr.bookingsCompleted, prev.bookingsCompleted),
              avgPerDay: calcVariance(curr.avgPerDay, prev.avgPerDay),
              conversion: Math.round((curr.conversion - prev.conversion) * 100) / 100,
              avgBookingPrice: calcVariance(curr.avgBookingPrice, prev.avgBookingPrice),
              totalRevenue: calcVariance(curr.totalRevenue, prev.totalRevenue),
              totalProfit: calcVariance(curr.totalProfit, prev.totalProfit),
              marginPercent: Math.round((curr.marginPercent - prev.marginPercent) * 100) / 100,
              signups: calcVariance(curr.signups, prev.signups),
              newUsersWithBookings: calcVariance(curr.newUsersWithBookings, prev.newUsersWithBookings),
              newUserConversion: Math.round((curr.newUserConversion - prev.newUserConversion) * 100) / 100,
              subscriptionRevenue: calcVariance(curr.subscriptionRevenue, prev.subscriptionRevenue),
              subscriptionFees: calcVariance(curr.subscriptionFees, prev.subscriptionFees),
              memberBookings: calcVariance(curr.memberBookings, prev.memberBookings),
              membershipRevenuePercent: Math.round((curr.membershipRevenuePercent - prev.membershipRevenuePercent) * 100) / 100,
              newSubscriptions: calcVariance(curr.newSubscriptions, prev.newSubscriptions)
            }
          };
        } else {
          if (index2 === 0) {
            return { ...periodItem, variance: null };
          }
          const prev = periodsData[index2 - 1].metrics;
          const curr = periodItem.metrics;
          const calcVariance = (current, previous) => {
            if (previous === 0) return current > 0 ? 100 : 0;
            return Math.round((current - previous) / previous * 100 * 100) / 100;
          };
          return {
            ...periodItem,
            variance: {
              bookingsCreated: calcVariance(curr.bookingsCreated, prev.bookingsCreated),
              bookingsDue: calcVariance(curr.bookingsDue, prev.bookingsDue),
              bookingsCompleted: calcVariance(curr.bookingsCompleted, prev.bookingsCompleted),
              avgPerDay: calcVariance(curr.avgPerDay, prev.avgPerDay),
              conversion: Math.round((curr.conversion - prev.conversion) * 100) / 100,
              avgBookingPrice: calcVariance(curr.avgBookingPrice, prev.avgBookingPrice),
              totalRevenue: calcVariance(curr.totalRevenue, prev.totalRevenue),
              totalProfit: calcVariance(curr.totalProfit, prev.totalProfit),
              marginPercent: Math.round((curr.marginPercent - prev.marginPercent) * 100) / 100,
              signups: calcVariance(curr.signups, prev.signups),
              newUsersWithBookings: calcVariance(curr.newUsersWithBookings, prev.newUsersWithBookings),
              newUserConversion: Math.round((curr.newUserConversion - prev.newUserConversion) * 100) / 100,
              subscriptionRevenue: calcVariance(curr.subscriptionRevenue, prev.subscriptionRevenue),
              subscriptionFees: calcVariance(curr.subscriptionFees, prev.subscriptionFees),
              memberBookings: calcVariance(curr.memberBookings, prev.memberBookings),
              membershipRevenuePercent: Math.round((curr.membershipRevenuePercent - prev.membershipRevenuePercent) * 100) / 100,
              newSubscriptions: calcVariance(curr.newSubscriptions, prev.newSubscriptions)
            }
          };
        }
      });
      if (periodType !== "monthly") {
        periodsDataWithVariance.reverse();
      }
      await logAudit({
        userId,
        userEmail: user.email,
        action: "VIEW_WEEKLY_PERFORMANCE",
        database,
        table: void 0,
        ip: req.ip || void 0,
        details: `Viewed ${periodsDataWithVariance.length} ${periodType === "monthly" ? "months" : "weeks"}`
      });
      const responseData = {
        periods: periodsDataWithVariance,
        periodType,
        generatedAt: (/* @__PURE__ */ new Date()).toISOString(),
        selectedZones: selectedZones.length > 0 ? selectedZones : null
      };
      const cacheDuration = getCacheDuration(true);
      setInCache(cacheKey, responseData, cacheDuration);
      console.log(`[Cache SET] Marketing dashboard: ${cacheKey} (expires in ${cacheDuration / 6e4} minutes)`);
      res.json({ ...responseData, fromCache: false });
    } catch (err) {
      console.error("Error fetching weekly performance:", err);
      res.status(500).json({ error: "Failed to fetch weekly performance data" });
    }
  });
  app2.post("/api/weekly-performance/:database/chat", isAuthenticated, reportAILimiter, async (req, res) => {
    try {
      const { database } = req.params;
      const { message, dashboardData, selectedWeek, periodType = "weekly" } = req.body;
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (!user) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      if (!message) {
        return res.status(400).json({ error: "Message is required" });
      }
      const client = getOpenAIClient();
      if (!client) {
        return res.status(503).json({ error: "AI service not available" });
      }
      const canDrillDown = user.role === "admin" || user.role === "washos_user";
      const { METRIC_SPECS: METRIC_SPECS2, getAllMetricSpecs: getAllMetricSpecs2 } = await Promise.resolve().then(() => (init_weeklyMetrics(), weeklyMetrics_exports));
      const currentYear = (/* @__PURE__ */ new Date()).getFullYear();
      const periodLabel = periodType === "monthly" ? "month" : "week";
      const periodsLabel = periodType === "monthly" ? "months" : "weeks";
      const periods = dashboardData?.periods || dashboardData?.weeks || [];
      const availablePeriodsContext = periods.length > 0 ? `Available ${periodsLabel} with their EXACT date ranges (use these dates for drill-down):
${periods.map(
        (p) => `- "${p.periodLabel || p.weekLabel}": periodStart="${p.periodStart || p.weekStart}", periodEnd="${p.periodEnd || p.weekEnd}"`
      ).join("\n")}` : "";
      const metricsContext = periods.length > 0 ? `The dashboard currently shows ${periods.length} ${periodsLabel} of data.
          
The most recent ${periodLabel} (${periods[0]?.periodLabel || periods[0]?.weekLabel || "Current"}) has these metrics:
${JSON.stringify(periods[0]?.metrics || {}, null, 2)}

${periods[0]?.variance ? `${periodType === "monthly" ? "Month-over-month" : "Week-over-week"} variance (% change, or percentage point change for rates):
${JSON.stringify(periods[0].variance, null, 2)}` : ""}

${periods.length > 1 ? `Previous ${periodLabel} (${periods[1]?.periodLabel || periods[1]?.weekLabel}) metrics:
${JSON.stringify(periods[1]?.metrics || {}, null, 2)}` : ""}
` : "No dashboard data is currently loaded.";
      const metricSpecsList = getAllMetricSpecs2().map(
        (m) => `- ${m.name} (id: ${m.id}): ${m.description}
  Formula: ${m.formula}${m.subSources ? `
  Sub-sources: ${m.subSources.map((s) => s.name).join(", ")}` : ""}`
      ).join("\n\n");
      const selectedPeriodLabel = selectedWeek?.periodLabel || selectedWeek?.weekLabel;
      const selectedPeriodStart = selectedWeek?.periodStart || selectedWeek?.weekStart;
      const selectedPeriodEnd = selectedWeek?.periodEnd || selectedWeek?.weekEnd;
      const systemPrompt = `You are an AI assistant for the WashOS Marketing Performance Dashboard. Your role is to help users understand and analyze their business metrics.

IMPORTANT: The current year is ${currentYear}. When users mention dates like "Jan 12-18", they mean ${currentYear}, NOT any other year.

DASHBOARD CONTEXT:
The Marketing Performance Dashboard tracks these key metrics ${periodType === "monthly" ? "month over month" : "week over week"} (Pacific Time):

METRIC DEFINITIONS AND FORMULAS:
${metricSpecsList}

CURRENT DATA:
${metricsContext}

${selectedPeriodLabel ? `SELECTED ${periodType === "monthly" ? "MONTH" : "WEEK"}: ${selectedPeriodLabel} (${selectedPeriodStart} to ${selectedPeriodEnd})` : ""}

${availablePeriodsContext}

CRITICAL: When calling get_metric_rows, you MUST use the EXACT periodStart and periodEnd values from the available ${periodsLabel} list above. Look up the ${periodLabel} label the user mentions and use its corresponding periodStart and periodEnd values. Do NOT make up date values.

${canDrillDown ? `DRILL-DOWN CAPABILITY:
You have access to tools to fetch the actual database rows that make up each metric.
When users ask "what went into this number", "show me the details", "export the data", or want to see the underlying rows, use the get_metric_rows tool.
When users ask "how is this calculated", use the get_metric_details tool for the exact formula.
For revenue metrics with multiple sources (totalRevenue, totalProfit), you can drill down into specific sources like bookingRevenue, subscriptionFees, tips, refunds, etc.

IMPORTANT FOR BREAKDOWNS: When a user asks to "break down" a metric with sub-sources, you should make SEPARATE calls to get_metric_rows for EACH sub-source. For example:
- subscriptionFees has sub-sources: "invoiceFees" (recurring invoice payments) and "cancellationFees" ($59 cancellation fees)
- To break down subscription fees, call get_metric_rows TWICE: once with subSourceId="invoiceFees" and once with subSourceId="cancellationFees"
- Then present both results to show the complete breakdown.` : "Note: Drill-down to underlying data rows is not available for this user role."}

INSTRUCTIONS:
1. Answer questions about the metrics, trends, and performance
2. Help users understand what the numbers mean and provide insights
3. Compare ${periodsLabel} when relevant data is available
4. Explain variances and what might be driving changes
5. Be concise but informative
6. Format numbers appropriately (currency with $, percentages with %, etc.)
7. When discussing variance, positive changes are generally good for revenue/bookings/users metrics
${canDrillDown ? "8. When users want to see underlying data, use the tools to fetch and display it" : ""}`;
      const tools = canDrillDown ? [
        {
          type: "function",
          function: {
            name: "get_metric_details",
            description: "Get the exact calculation formula and description for a specific metric",
            parameters: {
              type: "object",
              properties: {
                metricId: {
                  type: "string",
                  description: "The metric ID (e.g., totalRevenue, bookingsCompleted, signups)",
                  enum: Object.keys(METRIC_SPECS2)
                }
              },
              required: ["metricId"]
            }
          }
        },
        {
          type: "function",
          function: {
            name: "get_metric_rows",
            description: "Fetch the actual database rows that contribute to a metric for a specific week. Returns up to 50 rows as a preview with a CSV download option for full data.",
            parameters: {
              type: "object",
              properties: {
                metricId: {
                  type: "string",
                  description: "The metric ID (e.g., totalRevenue, bookingsCompleted)",
                  enum: Object.keys(METRIC_SPECS2)
                },
                subSourceId: {
                  type: "string",
                  description: "For metrics with multiple sources (like totalRevenue), optionally specify a sub-source (e.g., bookingRevenue, subscriptionFees, tips, refunds, creditPacks, stripeFees)"
                },
                weekStart: {
                  type: "string",
                  description: "ISO date string for the start of the week (UTC)"
                },
                weekEnd: {
                  type: "string",
                  description: "ISO date string for the end of the week (UTC)"
                }
              },
              required: ["metricId", "weekStart", "weekEnd"]
            }
          }
        }
      ] : void 0;
      const messages = [
        { role: "system", content: systemPrompt },
        { role: "user", content: message }
      ];
      let response = await client.chat.completions.create({
        model: AI_CONFIG.reportChat.model,
        messages,
        tools,
        temperature: 0.7,
        max_tokens: 2e3
      });
      let assistantMessage = response.choices[0]?.message;
      const toolResults = [];
      while (assistantMessage?.tool_calls && assistantMessage.tool_calls.length > 0) {
        messages.push(assistantMessage);
        for (const toolCall of assistantMessage.tool_calls) {
          const tc = toolCall;
          const args = JSON.parse(tc.function.arguments);
          let result;
          if (tc.function.name === "get_metric_details") {
            const spec = METRIC_SPECS2[args.metricId];
            if (spec) {
              result = {
                name: spec.name,
                category: spec.category,
                formula: spec.formula,
                description: spec.description,
                sourceTables: spec.sourceTables || [spec.sourceTable],
                subSources: spec.subSources?.map((s) => ({ id: s.id, name: s.name }))
              };
            } else {
              result = { error: "Metric not found" };
            }
          } else if (tc.function.name === "get_metric_rows") {
            const pool2 = getPool(database);
            const spec = METRIC_SPECS2[args.metricId];
            if (!spec) {
              result = { error: "Metric not found" };
            } else {
              let queryConfig;
              if (args.subSourceId && spec.subSources) {
                const subSource = spec.subSources.find((s) => s.id === args.subSourceId);
                if (subSource) {
                  queryConfig = subSource.getDrilldownQuery(args.weekStart, args.weekEnd);
                }
              }
              if (!queryConfig) {
                queryConfig = spec.getDrilldownQuery(args.weekStart, args.weekEnd);
              }
              try {
                const queryResult = await pool2.query(
                  queryConfig.sql + " LIMIT 50",
                  queryConfig.params
                );
                const countSql = `SELECT COUNT(*) as total FROM (${queryConfig.sql}) as subq`;
                const countResult = await pool2.query(countSql, queryConfig.params);
                const totalCount = parseInt(countResult.rows[0]?.total || "0");
                result = {
                  metricName: spec.name,
                  subSource: args.subSourceId,
                  columns: queryConfig.columns,
                  rows: queryResult.rows,
                  totalCount,
                  previewCount: queryResult.rows.length,
                  hasMore: totalCount > 50,
                  csvExportAvailable: totalCount > 0
                };
                toolResults.push({
                  metricId: args.metricId,
                  subSourceId: args.subSourceId,
                  weekStart: args.weekStart,
                  weekEnd: args.weekEnd,
                  ...result
                });
                await logAudit({
                  userId,
                  userEmail: user.email,
                  action: "WEEKLY_PERFORMANCE_DRILLDOWN",
                  database,
                  details: `Metric: ${spec.name}${args.subSourceId ? ` (${args.subSourceId})` : ""}, ${totalCount} rows`,
                  ip: req.ip || void 0
                });
              } catch (queryErr) {
                console.error("Drilldown query error:", queryErr);
                result = { error: "Failed to fetch data" };
              }
            }
          }
          messages.push({
            role: "tool",
            tool_call_id: toolCall.id,
            content: JSON.stringify(result)
          });
        }
        response = await client.chat.completions.create({
          model: AI_CONFIG.reportChat.model,
          messages,
          tools,
          temperature: 0.7,
          max_tokens: 2e3
        });
        assistantMessage = response.choices[0]?.message;
      }
      const finalMessage = assistantMessage?.content || "I apologize, but I couldn't generate a response. Please try again.";
      await logAudit({
        userId,
        userEmail: user.email,
        action: "WEEKLY_PERFORMANCE_AI_CHAT",
        database,
        details: `AI chat message: ${message.substring(0, 100)}...`,
        ip: req.ip || void 0
      });
      res.json({
        message: finalMessage,
        drilldownData: toolResults.length > 0 ? toolResults : void 0
      });
    } catch (err) {
      console.error("Error in weekly performance AI chat:", err);
      res.status(500).json({ error: "Failed to process AI request" });
    }
  });
  app2.get("/api/weekly-performance/:database/drilldown-export", isAuthenticated, exportLimiter, async (req, res) => {
    try {
      const { database } = req.params;
      const { metricId, subSourceId, weekStart, weekEnd } = req.query;
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (!user) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      if (user.role !== "admin" && user.role !== "washos_user") {
        return res.status(403).json({ error: "Export not available for your role" });
      }
      if (!metricId || !weekStart || !weekEnd) {
        return res.status(400).json({ error: "Missing required parameters" });
      }
      const { METRIC_SPECS: METRIC_SPECS2 } = await Promise.resolve().then(() => (init_weeklyMetrics(), weeklyMetrics_exports));
      const spec = METRIC_SPECS2[metricId];
      if (!spec) {
        return res.status(400).json({ error: "Invalid metric" });
      }
      let queryConfig;
      if (subSourceId && spec.subSources) {
        const subSource = spec.subSources.find((s) => s.id === subSourceId);
        if (subSource) {
          queryConfig = subSource.getDrilldownQuery(weekStart, weekEnd);
        }
      }
      if (!queryConfig) {
        queryConfig = spec.getDrilldownQuery(weekStart, weekEnd);
      }
      const pool2 = getPool(database);
      const result = await pool2.query(
        queryConfig.sql + " LIMIT 10000",
        queryConfig.params
      );
      const headers = queryConfig.columns.join(",");
      const rows = result.rows.map(
        (row) => queryConfig.columns.map((col) => {
          const val = row[col];
          if (val === null || val === void 0) return "";
          const strVal = String(val);
          if (strVal.includes(",") || strVal.includes('"') || strVal.includes("\n")) {
            return `"${strVal.replace(/"/g, '""')}"`;
          }
          return strVal;
        }).join(",")
      );
      const csv = [headers, ...rows].join("\n");
      await logAudit({
        userId,
        userEmail: user.email,
        action: "WEEKLY_PERFORMANCE_DRILLDOWN_EXPORT",
        database,
        details: `Metric: ${spec.name}${subSourceId ? ` (${subSourceId})` : ""}, ${result.rows.length} rows exported`,
        ip: req.ip || void 0
      });
      res.setHeader("Content-Type", "text/csv");
      res.setHeader("Content-Disposition", `attachment; filename="${spec.id}_${subSourceId || "all"}_drilldown.csv"`);
      res.send(csv);
    } catch (err) {
      console.error("Error exporting drilldown data:", err);
      res.status(500).json({ error: "Failed to export data" });
    }
  });
  app2.get("/api/reports/templates", isAuthenticated, async (req, res) => {
    const templates = [
      {
        id: "booking-summary",
        name: "Booking Summary",
        description: "Overview of bookings with status breakdown",
        blocks: [
          { kind: "metric", title: "Total Bookings", config: { table: "bookings", column: "id", aggregateFunction: "count" } },
          { kind: "chart", title: "Bookings by Status", config: { table: "bookings", chartType: "pie", xColumn: "status", yColumn: "id", aggregateFunction: "count", groupBy: "status" } }
        ]
      },
      {
        id: "customer-metrics",
        name: "Customer Metrics",
        description: "Key customer statistics and trends",
        blocks: [
          { kind: "metric", title: "Total Customers", config: { table: "users", column: "id", aggregateFunction: "count" } },
          { kind: "table", title: "Recent Customers", config: { table: "users", columns: ["email", "first_name", "created_at"], orderBy: { column: "created_at", direction: "desc" }, rowLimit: 10 } }
        ]
      }
    ];
    res.json(templates);
  });
  app2.get("/api/stripe-metrics", isAuthenticated, async (req, res) => {
    try {
      const { weekStart, weekEnd } = req.query;
      if (!weekStart || !weekEnd) {
        return res.status(400).json({ error: "weekStart and weekEnd query parameters required" });
      }
      const stripeConnected = await checkStripeConnection();
      if (!stripeConnected) {
        return res.status(503).json({
          error: "Stripe not connected",
          message: "Please connect your Stripe account to view financial metrics"
        });
      }
      const startDate = new Date(weekStart);
      const endDate = new Date(weekEnd);
      const startTimestamp = Math.floor(startDate.getTime() / 1e3);
      const endTimestamp = Math.floor(endDate.getTime() / 1e3);
      const metrics = await getStripeMetricsForWeek(startTimestamp, endTimestamp);
      res.json({
        weekStart,
        weekEnd,
        metrics
      });
    } catch (error) {
      console.error("Stripe metrics error:", error);
      res.status(500).json({ error: "Failed to fetch Stripe metrics", message: error.message });
    }
  });
  app2.get("/api/stripe-status", isAuthenticated, async (req, res) => {
    try {
      const connected = await checkStripeConnection();
      res.json({ connected });
    } catch (error) {
      res.json({ connected: false });
    }
  });
  app2.get("/api/operations-performance/:database", isAuthenticated, async (req, res) => {
    try {
      const { database } = req.params;
      const periodType = req.query.periodType || "weekly";
      const forceRefresh = req.query.refresh === "true";
      const zonesParam = req.query.zones;
      const selectedZones = zonesParam ? zonesParam.split(",").filter(Boolean) : [];
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (!user) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      const zonesKey = selectedZones.length > 0 ? `_zones:${selectedZones.sort().join(",")}` : "";
      const cacheKey = getCacheKey("operations", database, periodType) + zonesKey;
      if (!forceRefresh) {
        const cachedData = getFromCache(cacheKey);
        if (cachedData) {
          console.log(`[Cache HIT] Operations dashboard: ${cacheKey}`);
          return res.json({ ...cachedData, fromCache: true });
        }
      }
      console.log(`[Cache MISS] Operations dashboard: ${cacheKey}${forceRefresh ? " (force refresh)" : ""}`);
      const pool2 = getPool(database);
      const getPSTOffset = (date) => {
        const month = date.getMonth();
        if (month > 2 && month < 10) {
          return "-07:00";
        } else if (month < 2 || month > 10) {
          return "-08:00";
        } else if (month === 2) {
          const day = date.getDate();
          const dayOfWeek = date.getDay();
          const secondSunday = 14 - (new Date(date.getFullYear(), 2, 1).getDay() || 7);
          if (day >= secondSunday) return "-07:00";
          return "-08:00";
        } else {
          const day = date.getDate();
          const firstSunday = 7 - (new Date(date.getFullYear(), 10, 1).getDay() || 7);
          if (day >= firstSunday) return "-08:00";
          return "-07:00";
        }
      };
      const periods = [];
      const now = /* @__PURE__ */ new Date();
      if (periodType === "monthly") {
        for (let i = 0; i < 12; i++) {
          const monthStart = new Date(now.getFullYear(), now.getMonth() - i, 1);
          const monthEnd = new Date(now.getFullYear(), now.getMonth() - i + 1, 1);
          const startOffset = getPSTOffset(monthStart);
          const endOffset = getPSTOffset(monthEnd);
          const startUTC = (/* @__PURE__ */ new Date(`${monthStart.getFullYear()}-${String(monthStart.getMonth() + 1).padStart(2, "0")}-01T00:00:00${startOffset}`)).toISOString();
          const endUTC = (/* @__PURE__ */ new Date(`${monthEnd.getFullYear()}-${String(monthEnd.getMonth() + 1).padStart(2, "0")}-01T00:00:00${endOffset}`)).toISOString();
          const monthLabel = monthStart.toLocaleDateString("en-US", { month: "short", year: "numeric" });
          periods.push({ startUTC, endUTC, label: monthLabel });
        }
      } else {
        const startDate = /* @__PURE__ */ new Date("2025-12-29T00:00:00-08:00");
        let currentMonday = new Date(now);
        currentMonday.setDate(currentMonday.getDate() - (currentMonday.getDay() + 6) % 7);
        currentMonday.setHours(0, 0, 0, 0);
        let weekStart = startDate;
        while (weekStart <= currentMonday) {
          const weekEnd = new Date(weekStart);
          weekEnd.setDate(weekEnd.getDate() + 7);
          const startOffset = getPSTOffset(weekStart);
          const endOffset = getPSTOffset(weekEnd);
          const year = weekStart.getFullYear();
          const month = String(weekStart.getMonth() + 1).padStart(2, "0");
          const day = String(weekStart.getDate()).padStart(2, "0");
          const startUTC = (/* @__PURE__ */ new Date(`${year}-${month}-${day}T00:00:00${startOffset}`)).toISOString();
          const endYear = weekEnd.getFullYear();
          const endMonth = String(weekEnd.getMonth() + 1).padStart(2, "0");
          const endDay = String(weekEnd.getDate()).padStart(2, "0");
          const endUTC = (/* @__PURE__ */ new Date(`${endYear}-${endMonth}-${endDay}T00:00:00${endOffset}`)).toISOString();
          const weekLabel = `${weekStart.toLocaleDateString("en-US", { month: "short", day: "numeric" })} - ${new Date(weekEnd.getTime() - 864e5).toLocaleDateString("en-US", { month: "short", day: "numeric", year: "numeric" })}`;
          periods.push({ startUTC, endUTC, label: weekLabel });
          weekStart = weekEnd;
        }
      }
      if (periodType === "weekly") {
        periods.reverse();
      }
      const limitedPeriods = periods.slice(0, periodType === "monthly" ? 12 : 52);
      const results = [];
      for (let i = 0; i < limitedPeriods.length; i++) {
        const period = limitedPeriods[i];
        const metrics = await calculateOperationsMetrics(pool2, period.startUTC, period.endUTC, null, selectedZones);
        let variance = {};
        if (i < limitedPeriods.length - 1) {
          const prevPeriod = limitedPeriods[i + 1];
          const prevMetrics = await calculateOperationsMetrics(pool2, prevPeriod.startUTC, prevPeriod.endUTC, null, selectedZones);
          variance = calculateOperationsVariance(metrics, prevMetrics);
        }
        results.push({
          periodLabel: period.label,
          periodStart: period.startUTC,
          periodEnd: period.endUTC,
          periodType,
          metrics,
          variance
        });
      }
      const responseData = {
        periods: results,
        stripeConnected: false,
        periodType
      };
      const cacheDuration = getCacheDuration(true);
      setInCache(cacheKey, responseData, cacheDuration);
      console.log(`[Cache SET] Operations dashboard: ${cacheKey} (expires in ${cacheDuration / 6e4} minutes)`);
      res.json({ ...responseData, fromCache: false });
    } catch (error) {
      console.error("Operations performance error:", error);
      res.status(500).json({ error: "Failed to fetch operations metrics", message: error.message });
    }
  });
  app2.post("/api/operations-performance/:database/chat", isAuthenticated, reportAILimiter, async (req, res) => {
    try {
      const { database } = req.params;
      const { message, dashboardData, selectedPeriod, periodType } = req.body;
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (!user) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      if (!message) {
        return res.status(400).json({ error: "Message is required" });
      }
      const client = getOpenAIClient();
      if (!client) {
        return res.status(503).json({ error: "AI service not available" });
      }
      const pool2 = getPool(database);
      const canDrillDown = user.role === "admin" || user.role === "washos_user";
      const currentYear = (/* @__PURE__ */ new Date()).getFullYear();
      const availablePeriodsContext = dashboardData?.periods?.length > 0 ? `Available ${periodType === "monthly" ? "months" : "weeks"} with their EXACT date ranges (use these dates for drill-down):
${dashboardData.periods.map(
        (p) => `- "${p.periodLabel}": periodStart="${p.periodStart}", periodEnd="${p.periodEnd}"`
      ).join("\n")}` : "";
      const metricsContext = dashboardData?.periods?.length > 0 ? `The dashboard currently shows ${dashboardData.periods.length} ${periodType === "monthly" ? "months" : "weeks"} of data.
          
The most recent ${periodType === "monthly" ? "month" : "week"} (${dashboardData.periods[0]?.periodLabel || "Current"}) has these metrics:
${JSON.stringify(dashboardData.periods[0]?.metrics || {}, null, 2)}

${dashboardData.periods[0]?.variance ? `Period-over-period variance (% change, or percentage point change for rates):
${JSON.stringify(dashboardData.periods[0].variance, null, 2)}` : ""}

${dashboardData.periods.length > 1 ? `Previous ${periodType === "monthly" ? "month" : "week"} (${dashboardData.periods[1]?.periodLabel}) metrics:
${JSON.stringify(dashboardData.periods[1]?.metrics || {}, null, 2)}` : ""}
` : "No dashboard data is currently loaded.";
      const metricSpecsList = getAllOperationsMetricSpecs().map(
        (m) => `- ${m.name} (id: ${m.id}): ${m.description}
  Formula: ${m.formula}
  Category: ${m.category}`
      ).join("\n\n");
      const periodLabel = periodType === "monthly" ? "month" : "week";
      const periodsLabel = periodType === "monthly" ? "months" : "weeks";
      const systemPrompt = `You are an AI assistant for the WashOS Operations Performance Dashboard. Your role is to help users understand and analyze their operations metrics.

IMPORTANT: The current year is ${currentYear}. When users mention dates, they mean ${currentYear}, NOT any other year.

CURRENT VIEW MODE: ${periodType.toUpperCase()} - You are currently viewing ${periodsLabel}. Always refer to time periods as "${periodsLabel}" not "${periodType === "monthly" ? "weeks" : "months"}". When users ask about "this period" or "last period", they mean ${periodsLabel}.

DASHBOARD CONTEXT:
The Operations Performance Dashboard tracks these key metrics for network management and supply management:

METRIC DEFINITIONS AND FORMULAS:
${metricSpecsList}

CURRENT DATA:
${metricsContext}

${selectedPeriod ? `SELECTED ${periodLabel.toUpperCase()}: ${selectedPeriod.periodLabel} (${selectedPeriod.periodStart} to ${selectedPeriod.periodEnd})` : ""}

${availablePeriodsContext}

CRITICAL: When calling get_metric_rows, you MUST use the EXACT periodStart and periodEnd values from the available ${periodsLabel} list above. Look up the ${periodLabel} label the user mentions and use its corresponding periodStart and periodEnd values. Do NOT make up date values.

${canDrillDown ? `DRILL-DOWN CAPABILITY:
You have access to tools to fetch the actual database rows that make up each metric.
When users ask "what went into this number", "show me the details", "export the data", or want to see the underlying rows, use the get_metric_rows tool.
When users ask "how is this calculated", use the get_metric_details tool for the exact formula.` : "Note: Drill-down to underlying data rows is not available for this user role."}

INSTRUCTIONS:
1. Answer questions about the metrics, trends, and performance
2. Help users understand what the numbers mean and provide insights
3. Compare ${periodsLabel} when relevant data is available
4. Explain variances and what might be driving changes
5. Be concise but informative
6. Format numbers appropriately (percentages with %, counts as whole numbers, ratings to 2 decimal places)
7. For operations metrics, lower is generally better for: Emergencies, Defect %, Overbooked %, Dismissed Vendors
   Higher is generally better for: Delivery Rate, Rating, Response Rate, Margin, Active Vendors, New Vendors, Utilization
8. ALWAYS use "${periodsLabel}" terminology when referring to time periods, never use "${periodType === "monthly" ? "weeks" : "months"}"
${canDrillDown ? "9. When users want to see underlying data, use the tools to fetch and display it" : ""}`;
      const tools = canDrillDown ? [
        {
          type: "function",
          function: {
            name: "get_metric_details",
            description: "Get the exact calculation formula and description for a specific operations metric",
            parameters: {
              type: "object",
              properties: {
                metricId: {
                  type: "string",
                  description: "The metric ID (e.g., bookingsCompleted, emergencies, deliveryRate)",
                  enum: Object.keys(OPERATIONS_METRIC_SPECS)
                }
              },
              required: ["metricId"]
            }
          }
        },
        {
          type: "function",
          function: {
            name: "get_metric_rows",
            description: "Fetch the actual database rows that contribute to a metric for a specific period. Returns up to 50 rows as a preview with a CSV download option for full data.",
            parameters: {
              type: "object",
              properties: {
                metricId: {
                  type: "string",
                  description: "The metric ID (e.g., bookingsCompleted, emergencies, deliveryRate)",
                  enum: Object.keys(OPERATIONS_METRIC_SPECS)
                },
                periodStart: {
                  type: "string",
                  description: "ISO date string for the start of the period (UTC)"
                },
                periodEnd: {
                  type: "string",
                  description: "ISO date string for the end of the period (UTC)"
                }
              },
              required: ["metricId", "periodStart", "periodEnd"]
            }
          }
        }
      ] : void 0;
      const messages = [
        { role: "system", content: systemPrompt },
        { role: "user", content: message }
      ];
      let response = await client.chat.completions.create({
        model: AI_CONFIG.reportChat.model,
        messages,
        tools,
        temperature: 0.7,
        max_tokens: 2e3
      });
      let assistantMessage = response.choices[0]?.message;
      const toolResults = [];
      while (assistantMessage?.tool_calls && assistantMessage.tool_calls.length > 0) {
        messages.push(assistantMessage);
        for (const toolCall of assistantMessage.tool_calls) {
          let result;
          if (toolCall.function.name === "get_metric_details") {
            const args = JSON.parse(toolCall.function.arguments);
            const spec = getOperationsMetricSpec(args.metricId);
            if (spec) {
              result = {
                name: spec.name,
                category: spec.category,
                formula: spec.formula,
                description: spec.description,
                sourceTable: spec.sourceTable,
                sourceTables: spec.sourceTables
              };
            } else {
              result = { error: "Unknown metric" };
            }
          } else if (toolCall.function.name === "get_metric_rows") {
            const args = JSON.parse(toolCall.function.arguments);
            const spec = getOperationsMetricSpec(args.metricId);
            if (!spec) {
              result = { error: "Unknown metric" };
            } else {
              const queryConfig = spec.getDrilldownQuery(args.periodStart, args.periodEnd);
              try {
                const queryResult = await pool2.query(
                  queryConfig.sql + " LIMIT 50",
                  queryConfig.params
                );
                const countSql = `SELECT COUNT(*) as total FROM (${queryConfig.sql}) as subq`;
                const countResult = await pool2.query(countSql, queryConfig.params);
                const totalCount = parseInt(countResult.rows[0]?.total || "0");
                result = {
                  metricName: spec.name,
                  columns: queryConfig.columns,
                  rows: queryResult.rows,
                  totalCount,
                  previewCount: queryResult.rows.length,
                  hasMore: totalCount > 50,
                  csvExportAvailable: totalCount > 0
                };
                toolResults.push({
                  metricId: args.metricId,
                  periodStart: args.periodStart,
                  periodEnd: args.periodEnd,
                  ...result
                });
                await logAudit({
                  userId,
                  userEmail: user.email,
                  action: "OPERATIONS_PERFORMANCE_DRILLDOWN",
                  database,
                  details: `Metric: ${spec.name}, ${totalCount} rows`,
                  ip: req.ip || void 0
                });
              } catch (queryErr) {
                console.error("Drilldown query error:", queryErr);
                result = { error: "Failed to fetch data" };
              }
            }
          }
          messages.push({
            role: "tool",
            tool_call_id: toolCall.id,
            content: JSON.stringify(result)
          });
        }
        response = await client.chat.completions.create({
          model: AI_CONFIG.reportChat.model,
          messages,
          tools,
          temperature: 0.7,
          max_tokens: 2e3
        });
        assistantMessage = response.choices[0]?.message;
      }
      const finalMessage = assistantMessage?.content || "I apologize, but I couldn't generate a response. Please try again.";
      await logAudit({
        userId,
        userEmail: user.email,
        action: "OPERATIONS_PERFORMANCE_AI_CHAT",
        database,
        details: `AI chat message: ${message.substring(0, 100)}...`,
        ip: req.ip || void 0
      });
      res.json({
        message: finalMessage,
        drilldownData: toolResults.length > 0 ? toolResults : void 0
      });
    } catch (err) {
      console.error("Error in operations performance AI chat:", err);
      res.status(500).json({ error: "Failed to process AI request" });
    }
  });
  app2.get("/api/operations-performance/:database/drilldown-export", isAuthenticated, exportLimiter, async (req, res) => {
    try {
      const { database } = req.params;
      const { metricId, periodStart, periodEnd } = req.query;
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (!user) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      if (user.role !== "admin" && user.role !== "washos_user") {
        return res.status(403).json({ error: "Export not available for this user role" });
      }
      if (!metricId || !periodStart || !periodEnd) {
        return res.status(400).json({ error: "Missing required parameters" });
      }
      const spec = getOperationsMetricSpec(metricId);
      if (!spec) {
        return res.status(400).json({ error: "Unknown metric" });
      }
      const pool2 = getPool(database);
      const queryConfig = spec.getDrilldownQuery(periodStart, periodEnd);
      const result = await pool2.query(
        queryConfig.sql + " LIMIT 10000",
        queryConfig.params
      );
      const headers = queryConfig.columns.join(",");
      const rows = result.rows.map(
        (row) => queryConfig.columns.map((col) => {
          const val = row[col];
          if (val === null || val === void 0) return "";
          const str = String(val);
          if (str.includes(",") || str.includes('"') || str.includes("\n")) {
            return `"${str.replace(/"/g, '""')}"`;
          }
          return str;
        }).join(",")
      ).join("\n");
      const csv = headers + "\n" + rows;
      await logAudit({
        userId,
        userEmail: user.email,
        action: "OPERATIONS_DRILLDOWN_EXPORT",
        database,
        details: `Metric: ${spec.name}, Exported ${result.rows.length} rows`,
        ip: req.ip || void 0
      });
      res.setHeader("Content-Type", "text/csv");
      res.setHeader("Content-Disposition", `attachment; filename="${metricId}_${periodStart}_to_${periodEnd}.csv"`);
      res.send(csv);
    } catch (err) {
      console.error("Error in operations drilldown export:", err);
      res.status(500).json({ error: "Failed to export data" });
    }
  });
  app2.get("/api/operations-performance/:database/zone-comparison", isAuthenticated, async (req, res) => {
    try {
      const { database } = req.params;
      const { metricId, periodStart, periodEnd, prevPeriodStart, prevPeriodEnd } = req.query;
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (!user) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      if (!metricId || !periodStart || !periodEnd) {
        return res.status(400).json({ error: "Missing required parameters: metricId, periodStart, periodEnd" });
      }
      const metricSpec = getOperationsMetricSpec(metricId);
      if (!metricSpec) {
        return res.status(400).json({ error: `Invalid metricId: ${metricId}. Use a valid operations metric ID.` });
      }
      const pool2 = getPool(database);
      const cacheKey = `zone-comparison:${database}:${metricId}:${periodStart}:${periodEnd}:${prevPeriodStart || ""}:${prevPeriodEnd || ""}`;
      const cachedData = getFromCache(cacheKey);
      if (cachedData) {
        await logAudit({
          userId,
          userEmail: user.email,
          action: "VIEW_ZONE_COMPARISON",
          database,
          details: `Metric: ${metricId}, Period: ${periodStart} to ${periodEnd} (cached)`,
          ip: req.ip || void 0
        });
        return res.json({ ...cachedData, fromCache: true });
      }
      const zonesResult = await pool2.query(
        `SELECT DISTINCT abbreviation FROM public.districts WHERE abbreviation IS NOT NULL ORDER BY abbreviation`
      );
      const allZones = zonesResult.rows.map((r) => r.abbreviation);
      const zoneMetricsPromises = allZones.map(async (zone) => {
        const [currentMetrics, prevMetrics] = await Promise.all([
          calculateOperationsMetrics(pool2, periodStart, periodEnd, null, [zone]),
          prevPeriodStart && prevPeriodEnd ? calculateOperationsMetrics(pool2, prevPeriodStart, prevPeriodEnd, null, [zone]) : Promise.resolve(null)
        ]);
        const metricKey = metricId;
        const currentValue = currentMetrics[metricKey];
        if (typeof currentValue !== "number") {
          return null;
        }
        let variance = null;
        if (prevMetrics) {
          const prevValue = prevMetrics[metricKey];
          if (typeof prevValue === "number" && prevValue !== 0) {
            variance = Math.round((currentValue - prevValue) / prevValue * 100 * 100) / 100;
          } else if (typeof prevValue === "number") {
            variance = currentValue > 0 ? 100 : 0;
          }
        }
        return {
          zone,
          value: currentValue,
          variance
        };
      });
      const zoneResults = (await Promise.all(zoneMetricsPromises)).filter(
        (r) => r !== null
      );
      zoneResults.sort((a, b) => b.value - a.value);
      const allZonesMetrics = await calculateOperationsMetrics(
        pool2,
        periodStart,
        periodEnd,
        null,
        []
      );
      const allZonesValue = allZonesMetrics[metricId];
      let allZonesVariance = null;
      if (prevPeriodStart && prevPeriodEnd) {
        const prevAllZonesMetrics = await calculateOperationsMetrics(
          pool2,
          prevPeriodStart,
          prevPeriodEnd,
          null,
          []
        );
        const prevAllZonesValue = prevAllZonesMetrics[metricId];
        if (typeof allZonesValue === "number" && typeof prevAllZonesValue === "number") {
          if (prevAllZonesValue !== 0) {
            allZonesVariance = Math.round((allZonesValue - prevAllZonesValue) / prevAllZonesValue * 100 * 100) / 100;
          } else {
            allZonesVariance = allZonesValue > 0 ? 100 : 0;
          }
        }
      }
      await logAudit({
        userId,
        userEmail: user.email,
        action: "VIEW_ZONE_COMPARISON",
        database,
        details: `Metric: ${metricId}, Period: ${periodStart} to ${periodEnd}`,
        ip: req.ip || void 0
      });
      const responseData = {
        metricId,
        periodStart,
        periodEnd,
        allZones: {
          value: typeof allZonesValue === "number" ? allZonesValue : 0,
          variance: allZonesVariance
        },
        zones: zoneResults
      };
      setInCache(cacheKey, responseData, 36e5);
      res.json({ ...responseData, fromCache: false });
    } catch (err) {
      console.error("Error in zone comparison:", err);
      res.status(500).json({ error: "Failed to calculate zone comparison" });
    }
  });
  app2.get("/api/operations-performance/:database/zone-time-series", isAuthenticated, async (req, res) => {
    try {
      const { database } = req.params;
      const { metricId, periodType = "weekly" } = req.query;
      const refresh = req.query.refresh === "true";
      const userId = req.user?.id;
      const user = await authStorage.getUser(userId);
      if (!user) {
        return res.status(401).json({ error: "Unauthorized" });
      }
      if (!metricId) {
        return res.status(400).json({ error: "Missing required parameter: metricId" });
      }
      const metricSpec = getOperationsMetricSpec(metricId);
      if (!metricSpec) {
        return res.status(400).json({ error: `Invalid metricId: ${metricId}. Use a valid operations metric ID.` });
      }
      const pool2 = getPool(database);
      const cacheKey = `zone-time-series:${database}:${metricId}:${periodType}`;
      if (!refresh) {
        const cachedData = getFromCache(cacheKey);
        if (cachedData) {
          await logAudit({
            userId,
            userEmail: user.email,
            action: "VIEW_ZONE_TIME_SERIES",
            database,
            details: `Metric: ${metricId}, Period Type: ${periodType} (cached)`,
            ip: req.ip || void 0
          });
          return res.json({ ...cachedData, fromCache: true });
        }
      }
      const periods = [];
      if (periodType === "monthly") {
        const now = /* @__PURE__ */ new Date();
        for (let i = 0; i < 12; i++) {
          const monthStart = new Date(now.getFullYear(), now.getMonth() - i, 1);
          const monthEnd = new Date(monthStart.getFullYear(), monthStart.getMonth() + 1, 1);
          const startDateStr = `${monthStart.getFullYear()}-${String(monthStart.getMonth() + 1).padStart(2, "0")}-01T08:00:00.000Z`;
          const endDateStr = `${monthEnd.getFullYear()}-${String(monthEnd.getMonth() + 1).padStart(2, "0")}-01T08:00:00.000Z`;
          const monthLabel = monthStart.toLocaleDateString("en-US", { month: "short", year: "numeric" });
          periods.push({ startUTC: startDateStr, endUTC: endDateStr, label: monthLabel });
        }
      } else {
        const startDate = /* @__PURE__ */ new Date("2025-12-29T00:00:00-08:00");
        const now = /* @__PURE__ */ new Date();
        let weekStart = startDate;
        while (weekStart < now) {
          const weekEnd = new Date(weekStart.getTime() + 7 * 24 * 60 * 60 * 1e3);
          const startDateStr = weekStart.toISOString().split("T")[0] + "T08:00:00.000Z";
          const endDateStr = weekEnd.toISOString().split("T")[0] + "T08:00:00.000Z";
          const weekLabel = `${weekStart.toLocaleDateString("en-US", { month: "short", day: "numeric" })} - ${new Date(weekEnd.getTime() - 864e5).toLocaleDateString("en-US", { month: "short", day: "numeric" })}`;
          periods.push({ startUTC: startDateStr, endUTC: endDateStr, label: weekLabel });
          weekStart = weekEnd;
        }
      }
      if (periodType === "weekly") {
        periods.reverse();
      }
      const limitedPeriods = periods.slice(0, 12);
      const zonesResult = await pool2.query(
        `SELECT DISTINCT abbreviation FROM public.districts WHERE abbreviation IS NOT NULL ORDER BY abbreviation`
      );
      const allZones = zonesResult.rows.map((r) => r.abbreviation);
      const zoneDataPromises = allZones.map(async (zone) => {
        const periodValues = [];
        const metricsPromises = limitedPeriods.map(async (period) => {
          const metrics = await calculateOperationsMetrics(pool2, period.startUTC, period.endUTC, null, [zone]);
          const value = metrics[metricId];
          return {
            periodLabel: period.label,
            periodStart: period.startUTC,
            periodEnd: period.endUTC,
            value: typeof value === "number" ? value : 0
          };
        });
        const results = await Promise.all(metricsPromises);
        periodValues.push(...results);
        return {
          zone,
          periods: periodValues
        };
      });
      const zoneData = await Promise.all(zoneDataPromises);
      const allZonesPeriodsPromises = limitedPeriods.map(async (period) => {
        const metrics = await calculateOperationsMetrics(pool2, period.startUTC, period.endUTC, null, []);
        const value = metrics[metricId];
        return {
          periodLabel: period.label,
          periodStart: period.startUTC,
          periodEnd: period.endUTC,
          value: typeof value === "number" ? value : 0
        };
      });
      const allZonesPeriods = await Promise.all(allZonesPeriodsPromises);
      zoneData.sort((a, b) => {
        const aLatest = a.periods[0]?.value || 0;
        const bLatest = b.periods[0]?.value || 0;
        return bLatest - aLatest;
      });
      await logAudit({
        userId,
        userEmail: user.email,
        action: "VIEW_ZONE_TIME_SERIES",
        database,
        details: `Metric: ${metricId}, Period Type: ${periodType}, Periods: ${limitedPeriods.length}`,
        ip: req.ip || void 0
      });
      const responseData = {
        metricId,
        metricLabel: metricSpec.name,
        periodType,
        periods: limitedPeriods.map((p) => ({ label: p.label, start: p.startUTC, end: p.endUTC })),
        allZones: {
          zone: "All Zones",
          periods: allZonesPeriods
        },
        zones: zoneData
      };
      setInCache(cacheKey, responseData, 36e5);
      res.json({ ...responseData, fromCache: false });
    } catch (err) {
      console.error("Error in zone time-series:", err);
      res.status(500).json({ error: "Failed to calculate zone time-series" });
    }
  });
  return httpServer;
}

// server/index.vercel.ts
var app = express();
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
var serverInitialized = false;
async function registerAndSetup() {
  if (serverInitialized) return app;
  await registerRoutes(app, app);
  serverInitialized = true;
  return app;
}
export {
  app,
  registerAndSetup
};
