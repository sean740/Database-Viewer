# WashOS DataScope

A secure, read-only web application for viewing and browsing PostgreSQL databases with role-based access control. Designed for non-developer users to safely explore database content without risk of data modification.

## Overview

This application connects to one or more Postgres databases using connection strings stored in environment variables and provides a user-friendly interface for:
- Browsing tables within selected databases
- Viewing rows with pagination (50 rows per page)
- Applying filters to narrow down results
- Exporting data to CSV with role-based limits
- Natural language queries using ChatGPT (when OpenAI integration is enabled)
- Role-based access control (Admin, WashOS User, External Customer)

## Tech Stack

- **Frontend**: React with TypeScript, TanStack Query, Shadcn UI components, Tailwind CSS
- **Backend**: Node.js + Express, pg library for PostgreSQL connections
- **AI Integration**: OpenAI via Replit AI Integrations (for natural language queries)

## Project Structure

```
client/
  src/
    components/     # React components (Header, DataTable, FilterPanel, etc.)
    pages/         # Page components (database-viewer.tsx)
    lib/           # Utilities and types
    hooks/         # Custom React hooks
server/
  routes.ts        # All API endpoints
  storage.ts       # Filter definitions storage (reads/writes filters.json)
  stripeClient.ts  # Stripe API client helper for financial metrics
shared/
  schema.ts        # Shared TypeScript types and Zod schemas
filters.json       # Admin-configured filter definitions (per table)
```

## Environment Variables

### Required
- `DATABASE_URLS`: Either a JSON array or single connection string
  - JSON array format: `[{"name":"Production","url":"postgres://..."},{"name":"Staging","url":"postgres://..."}]`
  - Single connection: `postgres://user:pass@host:5432/dbname` (will be named "Default")

### Optional
- `AI_INTEGRATIONS_OPENAI_API_KEY`: For natural language queries (auto-configured by Replit AI Integrations)
- `AI_INTEGRATIONS_OPENAI_BASE_URL`: OpenAI API base URL (auto-configured)
- `DB_SSL_REJECT_UNAUTHORIZED`: Set to "false" to disable SSL certificate verification for self-signed certs (default: true for security)

## Security Features

### SSL Certificate Verification
- Database connections verify SSL certificates by default for production security
- Set `DB_SSL_REJECT_UNAUTHORIZED=false` only for development with self-signed certificates

### Rate Limiting
- General API: 100 requests per minute
- Authentication endpoints: 10 requests per minute
- Export endpoints: 10 requests per minute
- AI/NLQ queries: 20 requests per minute

### Audit Logging
- All data access (viewing rows, exports) is logged to the database
- Logs include: user ID, email, action, database/table, timestamp, IP address
- Admins can view audit logs via the `/api/admin/audit-logs` endpoint

### Role-Based Access Control
- **Admin**: Full access, user management, table visibility settings, view audit logs
- **WashOS User**: Full data access, can manage External Customer grants
- **External Customer**: Access only to specifically granted tables

### Table Visibility vs. AI Access
- **Visibility is cosmetic for UI only**: The "isVisible" setting in table settings controls whether a table appears in the UI dropdown for manual browsing
- **AI has full access**: The AI assistant (NLQ and My Reports) can access ALL tables regardless of visibility settings - this allows users to query any data through natural language even if it's not shown in the UI table list
- **External customer grants are always enforced**: External customers are still restricted to their granted tables in both UI and AI contexts

## API Endpoints

| Method | Endpoint | Description |
|--------|----------|-------------|
| GET | `/api/databases` | List available database connections |
| GET | `/api/tables/:database` | List tables for a database |
| GET | `/api/columns/:database/:schema/:table` | Get columns for a table |
| GET | `/api/filters/:table` | Get filter definitions for a table |
| POST | `/api/filters` | Save filter definitions for a table |
| POST | `/api/rows` | Fetch rows with pagination and filters |
| GET | `/api/export` | Export current view as CSV |
| GET | `/api/nlq/status` | Check if NLQ is enabled |
| POST | `/api/nlq` | Process natural language query |
| GET | `/api/weekly-performance/:database` | Get weekly performance metrics dashboard data |
| POST | `/api/weekly-performance/:database/chat` | AI chat for Marketing dashboard |
| GET | `/api/weekly-performance/:database/drilldown-export` | Export drilldown data as CSV |
| GET | `/api/operations-performance/:database` | Get operations performance metrics dashboard data |
| POST | `/api/operations-performance/:database/chat` | AI chat for Operations dashboard |
| GET | `/api/operations-performance/:database/drilldown-export` | Export operations drilldown data as CSV |
| GET | `/api/stripe-status` | Check if Stripe is connected |
| GET | `/api/stripe-metrics` | Get Stripe financial metrics for a week |

## Features

### Stripe Financial Metrics Integration
- When Stripe is connected via Replit integration, the Weekly Performance Dashboard displays additional Stripe metrics
- Shows metrics for the selected week:
  - **Gross Volume**: Total charges/payments collected via Stripe
  - **Net Volume**: Amount after Stripe fees
  - **Refunds**: Total refunds processed (with count)
  - **Disputes**: Total disputed amount (with count)
- **Revenue Comparison**: Compares database-calculated revenue vs Stripe gross volume to identify discrepancies
- Only visible when Stripe is connected; gracefully hidden otherwise
- Uses read-only API access (balance transactions and disputes)
- Protected by authentication - only logged-in users can access Stripe data

### Weekly Marketing Performance Dashboard
- Accessible at `/weekly-performance` via sidebar button
- Displays 16 key business metrics per week (Monday-Sunday, PST):
  - **Bookings**: Created, Due, Completed, Avg/Day, Conversion (Done/Due)
  - **Revenue**: Avg Booking Price, Total Revenue, Gross Profit, Margin %
  - **Users**: Sign Ups, New Users (with booking), New User Conversion
  - **Membership**: Subscription Revenue, Member Bookings, % from Members, New Memberships
- Shows variance compared to previous week (percentage or percentage point change)
- Data from Dec 29, 2025 onward, current week at top
- Handles PST/PDT timezone correctly for week boundaries
- Week boundaries: Monday 00:00:00 PST to next Monday 00:00:00 PST (exclusive)
- **Zone Filtering**: Multi-select filter to analyze metrics by geographic region
  - Filter button shows "All Zones", "1 Zone", or "X Zones" based on selection
  - Applies to booking-related metrics: Bookings Created/Due/Completed, Revenue, Tips, Refunds, Member Bookings
  - Does NOT apply to user-related metrics: Sign Ups, New Users, Subscriptions
  - Join path: bookings.address_id → addresses.id → addresses.district_id → districts.id → districts.abbreviation
  - Available zones: AUS, BA, CHI, DA, DC, FTL, LA, LV, MIA, NOC, NSH, OC, ORL, PBC, PHX, RVC, SAC, SBC, SD, SF, SFV, SGV, SJ, SLA, SOC, TB, WLA
- **AI Chat Assistant**: Floating chat button in bottom-right corner allows users to ask questions about their dashboard data and get AI-powered insights
  - **Drill-down capability**: When users ask "show me the data" or similar, AI can retrieve the actual database rows that contribute to any metric
  - **Data preview**: Shows first 10 rows inline in chat with full column data
  - **CSV export**: Download button exports up to 10,000 rows for deeper analysis
  - **Revenue breakdown**: Can drill into individual revenue sources (bookings, tips, subscriptions, refunds, credit packs, Stripe fees)
  - **Role restricted**: Drill-down access limited to Admin and WashOS User roles only

#### Revenue Calculation Details
- **Total Revenue** = Booking Revenue + Subscription Fees + Customer Fees + Tips + Credit Packs - Refunds - Stripe Fees
  - Subscription Fees: price_plan_id 11=$96, 10=$9.99, others=$0
  - Tips: Sum of `tip_amount` from `booking_tips` where tip's `created_at` is in the week
  - Credit Packs: Sum of `pay_amount` from `credits_packs` joined with `user_credits_transactions` (type_id=16) on `amount=get_amount`
  - Refunds: Sum of `total` from `booking_refunds` where `created_at` is in the week
  - Stripe Fees: Sum of `stripe_fee` from `bookings` where `date_due` is in the week and `status='done'`
- **Gross Profit** = Booking Margin + Subscription Fees + Customer Fees + Tip Profit - Refunds
  - Tip Profit: `tip_amount - vendor_amount` from `booking_tips`
- **New Users (w/Booking)**: Users who signed up in the week AND have at least one booking ever (any time)

### Operations Performance Dashboard
- Accessible at `/operations-performance` via sidebar button
- Supports both weekly and monthly period views
- Tracks 13+ key operations metrics in two categories:
  - **Network Management**: Bookings Completed, Emergencies, Delivery Rate, Defect %, Overbooked %, Rating, Response Rate, Margin
  - **Supply Management**: Active Vendors, New Vendors, Dismissed Vendors, Scheduled Hours, Utilization
- Shows variance compared to previous period (percentage or percentage point change)
- **AI Chat Assistant**: Floating chat button in bottom-right corner allows users to ask questions about operations data
  - **Drill-down capability**: AI can retrieve the actual database rows that contribute to any metric
  - **Data preview**: Shows first 10 rows inline in chat with full column data
  - **CSV export**: Download button exports up to 10,000 rows for deeper analysis
  - **Role restricted**: Drill-down access limited to Admin and WashOS User roles only

### Multi-Column Sorting
- Click column headers to sort ascending (↑) → descending (↓) → clear
- **Shift+click** to add additional columns to the sort (multi-column sorting)
- Sort order numbers appear next to columns when multiple are sorted
- Sort state persists when switching tables, resets on database change

### Read-Only Safety
- Only SELECT queries are executed
- All identifiers (schema, table, column names) are validated with strict regex
- All values are parameterized to prevent SQL injection

### Admin Filter Definitions
- Admins can configure reusable filters per table via the Settings modal
- Filters are stored in `filters.json`
- Supported operators: eq, contains, gt, gte, lt, lte

### Natural Language Queries
- When OpenAI integration is enabled, users can query in plain English
- AI converts natural language to structured query plans (not raw SQL)
- Queries are validated against actual table/column metadata

### CSV Export
- Exports current page (50 rows) with applied filters
- Proper CSV escaping for commas, quotes, and newlines

### My Reports (AI-Powered Custom Reporting)
- Each user has their own personalized reporting workspace at `/my-reports`
- Users can create multiple report pages with tables, charts, and metrics
- AI chat assistant helps users build reports by describing what they want in natural language
- All reports are user-isolated - users can only see and modify their own reports
- Security features:
  - AI-generated actions are validated against database metadata and user permissions
  - Tables/columns are verified to exist before queries are executed
  - External customers can only access their granted tables
  - All report queries are audited

### Dashboard Caching System
- Both Marketing and Operations dashboards implement server-side global caching
- Cache is stored in-memory and shared across all users (reduces database load significantly)
- **Cache durations**:
  - Current period (week/month still in progress): 1 hour cache
  - Historical periods: 1 week cache
- **Refresh button**: Users can bypass cache by clicking "Refresh" button, which passes `?refresh=true` to the API
- **Cache keys**: Include dashboard type, database name, period type, and zone filters (for Marketing dashboard)
- **Implementation**: `server/dashboardCache.ts` module provides caching utilities
- **LRU Eviction**: Cache has a maximum of 100 entries; oldest accessed entries are evicted when limit is reached
- **Expired Cleanup**: Expired entries are cleaned up on each cache write
- Response includes `fromCache: true/false` to indicate whether data came from cache

## Development

The app runs with `npm run dev` which starts both the Express backend and Vite frontend dev server.
