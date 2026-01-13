# WashOS DataScope - Design Guidelines

## Design Approach
**WashOS Style** - Clean, modern enterprise interface with a cool blue-gray palette, lime accent highlights, soft shadows, and generous pill-shaped corners.

## Core Design Principles
1. **Data First**: Information hierarchy prioritizes clarity and scannability of database content
2. **Functional Efficiency**: Minimize clicks and cognitive load for common tasks
3. **Professional Utility**: Clean, trustworthy interface appropriate for production database access

## Color Palette

### Light Mode
- **Background**: #F4F7FE (cool off-white / light blue-gray)
- **Surface/Cards**: #FFFFFF (pure white)
- **Primary Text**: #0B182B (deep navy)
- **Secondary Text**: #647D9C (muted blue-gray)
- **Border**: rgba(11,24,43,0.14)
- **Primary Button**: #0B182B (navy)
- **Accent**: #C9F45D (lime green)

### Dark Mode
- **Background**: Deep navy (#0D1420)
- **Surface/Cards**: Slightly lighter navy
- **Primary**: Lime accent (#C9F45D)
- **Text**: Light gray/white

## Typography System

**Primary Font**: Inter (via Google Fonts CDN)
- Headers (H1): 32px, Semi-bold (600) to Bold (700)
- Headers (H2): 24px, Semi-bold (600)
- Headers (H3): 18px, Medium (500)
- Body Text: 14px, Regular (400)
- Table Data: 13px, Regular (monospace for data values)
- Small Text/Labels: 12px, Regular (400)
- Letter spacing: -0.01em for tighter, modern feel

**Monospace Font**: JetBrains Mono for database values, table names, and technical identifiers

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

## Border Radius System

- **Cards/Panels**: 18px (--radius: 1.125rem)
- **Buttons**: Pill-shaped (rounded-full or 999px for pills)
- **Inputs**: Slightly rounded (rounded-lg)
- **Badges**: Pill-shaped (rounded-full)

## Shadow System

Soft, subtle shadows with navy tint:
- **Small**: 0 4px 14px rgba(11,24,43,0.08)
- **Medium**: 0 10px 30px rgba(11,24,43,0.10)
- **Large**: 0 14px 40px rgba(11,24,43,0.12)

## Component Library

### Header/Navigation
- Fixed top bar with app title "WashOS DataScope"
- Height: h-16
- Database connection dropdown (right-aligned in header)
- Soft shadow for depth separation

### Main Content Area
**Two-Column Layout**:
- Left Sidebar (w-64): Table list with search/filter, scrollable
- Main Content (flex-1): Data table viewer with controls

### Buttons
**Primary**: Navy pill button with white text
- Background: var(--primary) / #0B182B
- Color: white
- Border-radius: 999px (pill)
- Box-shadow: var(--shadow-sm)
- Hover: Subtle brightness adjustment

**Secondary**: Outlined or ghost style
- Background: transparent or white
- Color: navy
- Border: 1px solid var(--border)
- Border-radius: 999px (pill)

### Inputs & Selects
- White background
- Pill radius (rounded-lg to rounded-xl)
- Subtle border: var(--border)
- Focus: Lime accent ring (--accent)

### Cards & Containers
- Background: white surface
- Border: 1px solid var(--card-border)
- Border-radius: 18px
- Box-shadow: var(--shadow-sm)
- Padding: p-6

### Data Table
- Full-width responsive table with horizontal scroll
- Sticky header row with subtle tinted background
- Row borders using --border
- Row hover: subtle highlight
- Min column width: 120px
- Cell padding: px-4 py-3

### Pagination Controls
Bottom bar containing:
- "Previous" button (disabled state when on page 1)
- Page number input (direct page jump)
- "Next" button (disabled on last page)
- Rows per page: Fixed at 50 (display only, not editable)

### ChatGPT Natural Language Query
- Prominent card above filter section (when enabled)
- Text area input with placeholder
- "Ask" button (accent color)
- Response area showing interpreted query structure

### Admin Settings Modal
- Full-screen overlay with centered dialog
- White surface with rounded corners
- Sections clearly labeled
- Save/Cancel buttons at bottom

## Form Inputs
- Consistent height: h-10 or h-12
- Border radius: rounded-lg
- Focus states with lime ring effect
- Label above input (not floating)
- Helper text below when needed

## Animations
**Minimal, purposeful only**:
- Filter panel expand/collapse: 200ms ease
- Modal fade-in: 150ms
- Button hover: subtle brightness shift
- No scroll animations or decorative motion

## Icons
**Lucide Icons**
- Navigation: menu, database, table
- Actions: refresh, download, settings, plus, edit, trash
- States: check-circle, alert-circle, info
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
