# Dashboard V3 terminal redesign review

These screenshots are review-only assets generated from the isolated local fixture server for the `ui-redesign/dashboard-v3-terminal` branch. They are not production captures and do not change `feature/radar-v1` or any running service.

| File | Viewport | Page / route | Source |
| --- | ---: | --- | --- |
| `dashboard-opportunities-1920.png` | 1920×1080 | Opportunities | local `/opportunities` fixture |
| `dashboard-opportunities-1440.png` | 1440×900 | Opportunities | local `/opportunities` fixture |
| `dashboard-opportunities-1280.png` | 1280×800 | Opportunities | local `/opportunities` fixture |
| `dashboard-pair-1440.png` | 1440×900 | QQQ Pair Detail | local `/pair?symbol=QQQ&long_venue=arcus&long_venue_symbol=QQQ-USD&short_venue=lighter_robinhood&short_venue_symbol=QQQ` fixture |
| `dashboard-pair-1280.png` | 1280×800 | QQQ Pair Detail | local exact-identity fixture route |
| `dashboard-anomalies-1280.png` | 1280×800 | Active Anomalies | local `/anomalies` fixture |
| `dashboard-status-1280.png` | 1280×800 | Status | local `/status` fixture |

## Factual visual inventory

- The shell is a compact dark terminal layout: one brand/header row, one small navigation row, then a short health/data-age line.
- Opportunities uses six fixed decision groups: Symbol, Route, Edge, Current, Baseline, and Execution. No sticky identity columns or horizontal table-scroll dependency is used.
- Route is one cell with LONG and SHORT rows, exact venue symbols, and a Pair Detail link. Symbol carries the contextual state instead of a separate state column.
- Edge is the dominant opportunity metric. Current combines net and raw spread. Baseline groups mean, standard deviation, and coverage; Execution groups depth, age, and skew.
- Filters are compact: symbol/venues/min edge/basis state are primary, while the remaining preserved query controls are under `More filters`.
- Pair Detail puts the identity and four decision KPIs above a two-column layout. The directional history chart is in the main column and long leg, short leg, and lifecycle cards form a right rail. Secondary fields are inside native details disclosure below the chart.
- Anomalies uses six grouped columns: Symbol, Route, Deviation, Reference, Episode, and Context. Status keeps the existing operational evidence tables in the aligned shell.
- Typography uses a restrained sans-serif shell with compact monospace numeric values. Color is limited to blue links, green positive/healthy values, red negative/stale values, and amber pending/depth warnings.
- The fixture includes positive and negative edges, pending and unavailable baselines, stale freshness, and unavailable depth so degraded states remain visible without adding a recommendation or score.
