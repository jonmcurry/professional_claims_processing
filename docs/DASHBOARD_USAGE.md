# Compliance Dashboard Usage

The React dashboard is served from the `/dashboard` path of the UI build. It visualises
information returned by the `/compliance/dashboard` API endpoint.

## Charts

- **Failure Patterns** – bar chart of the top failure reasons.
- **Processing Trends** – line chart of processed and failed claims over the last 30 days.
- **Revenue Impact** – pie chart showing potential revenue loss grouped by failure category.

Start the UI locally with `npm run dev` in the `ui/` directory and visit `http://localhost:5173/dashboard`.
