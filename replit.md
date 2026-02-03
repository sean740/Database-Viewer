# WashOS DataScope

## Overview
WashOS DataScope is a secure, read-only web application designed for non-developer users to safely explore and analyze data from PostgreSQL databases. It provides role-based access control, intuitive browsing, filtering, and export capabilities. The application aims to democratize data access within WashOS, enabling various user roles (Admin, WashOS User, External Customer) to gain insights without requiring SQL knowledge, while maintaining stringent security measures to prevent data modification. Key features include natural language querying, comprehensive marketing and operations dashboards with AI assistance, and integration with Stripe for financial metrics.

## User Preferences
- I prefer clear, concise explanations for technical concepts.
- I expect the agent to prioritize security and data integrity in all proposed changes.
- I like an iterative development approach, where features are built and tested incrementally.
- Ask for confirmation before making significant architectural changes or adding new external dependencies.
- Ensure all changes are thoroughly tested and do not introduce regressions.

## System Architecture
The application is built with a React (TypeScript, TanStack Query, Shadcn UI, Tailwind CSS) frontend and a Node.js (Express, pg library) backend. It utilizes a mono-repo structure with `client/`, `server/`, and `shared/` directories. UI/UX emphasizes a clean, user-friendly interface with consistent design elements from Shadcn UI and Tailwind CSS.

**Key Architectural Decisions:**
-   **Read-Only Design**: All database interactions are strictly `SELECT` queries, and identifiers are validated to prevent SQL injection.
-   **Role-Based Access Control (RBAC)**: Implemented for granular permissions across different user types (Admin, WashOS User, External Customer), controlling database visibility, export limits, and dashboard access.
-   **Configurable Database Connections**: Supports connecting to multiple PostgreSQL databases via environment variables.
-   **Server-Side Pagination & Filtering**: Ensures efficient data retrieval and reduces client-side load.
-   **AI Integration**: Natural Language Queries (NLQ) and AI-powered dashboard assistance convert natural language into structured query plans. The AI has full access to tables for querying, while UI visibility is controlled by user settings.
-   **Dashboard Architecture**: Dedicated dashboards for Marketing and Operations Performance, featuring weekly/monthly period views, zone-based filtering, variance comparison, and AI chat with drill-down capabilities.
-   **Zone Comparison (Time-Series)**: Operations Dashboard includes a "Zone Comparison" view that displays any metric across all zones over time. Zones appear as rows with periods as columns, allowing users to track trends and compare performance across geographic regions. API endpoint: `/api/operations-performance/:database/zone-time-series`.
-   **Dashboard Caching**: Global, server-side caching for dashboard data with varying durations (1 hour for current periods, 1 week for historical) and LRU eviction to optimize performance and reduce database load.
-   **Multi-Column Sorting**: Supports complex sorting logic with persistent state.
-   **Admin Filter Definitions**: Admins can define and save reusable table filters.
-   **Audit Logging**: Comprehensive logging of all data access actions for security and compliance.

## External Dependencies
-   **PostgreSQL**: Primary database for all data storage and retrieval.
-   **OpenAI**: Integrated via Replit AI Integrations for Natural Language Queries and AI chat assistance in dashboards.
-   **Stripe**: Integrated via Replit for fetching financial metrics (Gross Volume, Net Volume, Refunds, Disputes) for the Marketing Performance Dashboard.