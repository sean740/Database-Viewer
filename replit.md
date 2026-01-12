# Heroku Postgres Database Viewer

A read-only web application for viewing and browsing PostgreSQL databases. Designed for non-developer users to safely explore database content without risk of data modification.

## Overview

This application connects to one or more Postgres databases using connection strings stored in environment variables and provides a user-friendly interface for:
- Browsing tables within selected databases
- Viewing rows with pagination (50 rows per page)
- Applying filters to narrow down results
- Exporting current view to CSV
- Natural language queries using ChatGPT (when OpenAI integration is enabled)

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
shared/
  schema.ts        # Shared TypeScript types and Zod schemas
filters.json       # Admin-configured filter definitions (per table)
```

## Environment Variables

### Required
- `DATABASE_URLS`: Either a JSON array or single connection string
  - JSON array format: `[{"name":"Production","url":"postgres://..."},{"name":"Staging","url":"postgres://..."}]`
  - Single connection: `postgres://user:pass@host:5432/dbname` (will be named "Default")

### Optional (Auto-configured by Replit AI Integrations)
- `AI_INTEGRATIONS_OPENAI_API_KEY`: For natural language queries
- `AI_INTEGRATIONS_OPENAI_BASE_URL`: OpenAI API base URL

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

## Features

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

## Development

The app runs with `npm run dev` which starts both the Express backend and Vite frontend dev server.
