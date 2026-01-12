# Heroku Postgres Database Viewer - Design Guidelines

## Design Approach
**Material Design System** - Selected for its strength in data-dense, enterprise applications with clear visual feedback and excellent form/table patterns.

## Core Design Principles
1. **Data First**: Information hierarchy prioritizes clarity and scannability of database content
2. **Functional Efficiency**: Minimize clicks and cognitive load for common tasks
3. **Professional Utility**: Clean, trustworthy interface appropriate for production database access

## Typography System

**Primary Font**: Roboto (via Google Fonts CDN)
- Headers (H1): 32px, Medium weight
- Headers (H2): 24px, Medium weight  
- Headers (H3): 18px, Medium weight
- Body Text: 14px, Regular weight
- Table Data: 13px, Regular weight (monospace for data values)
- Small Text/Labels: 12px, Regular weight

**Secondary Font**: Roboto Mono for all database values, table names, and technical identifiers

## Layout & Spacing System

**Tailwind Spacing Units**: Consistently use 2, 4, 6, 8, 12, 16, 20 units
- Component padding: p-6 or p-8
- Section margins: mb-8 or mb-12
- Form field spacing: gap-4 or gap-6
- Card spacing: p-6
- Button padding: px-6 py-3

**Container Structure**:
- Max width: max-w-7xl
- Side padding: px-6 lg:px-8
- Centered: mx-auto

## Component Library

### Header/Navigation
- Fixed top bar with app title "Heroku Postgres Database Viewer"
- Height: h-16
- Database connection dropdown (right-aligned in header)
- Elevation shadow for depth separation

### Main Content Area
**Two-Column Layout**:
- Left Sidebar (w-64): Table list with search/filter, scrollable
- Main Content (flex-1): Data table viewer with controls

### Control Panel (Above Table)
Single row containing:
- Table dropdown (if not using sidebar)
- Active filter badges (removable chips)
- Page indicator "Page X of Y (Z total rows)"
- Export CSV button
- Reload button
- Admin Settings button (icon-only, top-right corner)

### Filter Section
- Collapsible panel above data table
- Grid layout for filter inputs: grid-cols-1 md:grid-cols-2 lg:grid-cols-3
- Each filter: Label + Input + Operator selector
- "Apply Filters" button (primary action)
- "Clear All" button (secondary action)

### Data Table
- Full-width responsive table with horizontal scroll
- Sticky header row
- Alternating row background for readability (subtle)
- Row hover state with elevation
- Min column width: 120px
- Cell padding: px-4 py-3
- Bordered cells with subtle dividers

### Pagination Controls
Bottom bar containing:
- "Previous" button (disabled state when on page 1)
- Page number input (direct page jump)
- "Next" button (disabled on last page)
- Rows per page: Fixed at 50 (display only, not editable)

### ChatGPT Natural Language Query
- Prominent card above filter section (when enabled)
- Text area input with placeholder "Ask in plain English (e.g., 'show rows where status contains failed')"
- "Ask" button (accent color)
- Response area showing interpreted query structure

### Admin Settings Modal
- Full-screen overlay with centered dialog
- Sections: "Filter Definitions for [table_name]"
- List of existing filters (editable cards)
- "Add New Filter" button
- Each filter card shows: name, column, operator with edit/delete actions
- Save/Cancel buttons at bottom

### Buttons
**Primary**: Filled background, medium elevation
**Secondary**: Outlined border, transparent background
**Icon Buttons**: Circular, 40px diameter

### Cards & Containers
- Rounded corners: rounded-lg
- Elevation shadows for depth
- Border: 1px solid border for delineation
- Background: Surface elevation on card backgrounds

### Form Inputs
- Consistent height: h-10 or h-12
- Border radius: rounded-md
- Focus states with ring effect
- Label above input (not floating)
- Helper text below when needed

### Error/Success States
- Error banner: Full-width bar at top of content, dismissible
- Success toast: Bottom-right corner, auto-dismiss
- Inline validation: Below form fields

## Animations
**Minimal, purposeful only**:
- Filter panel expand/collapse: 200ms ease
- Modal fade-in: 150ms
- Button hover lift: subtle transform
- No scroll animations or decorative motion

## Icons
**Material Icons** (via CDN)
- Navigation: menu, database, table_chart
- Actions: refresh, file_download, settings, add, edit, delete
- States: check_circle, error, info
- Size: 20px for buttons, 24px for headers

## Accessibility
- All interactive elements keyboard navigable
- ARIA labels for icon-only buttons
- Focus indicators on all interactive elements
- Sufficient contrast ratios (WCAG AA)
- Screen reader announcements for dynamic content updates

## Responsive Behavior
- Desktop (lg): Full two-column layout
- Tablet (md): Collapsible sidebar, main content fills
- Mobile (base): Stack all sections vertically, sidebar becomes slide-out drawer