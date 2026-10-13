# Dashboard V3 Phase 1.1 visual review assets

These are full browser-viewport screenshots captured from the tested Dashboard
commit `21069cd3b70ac5a492294b7e41408db0be74a2b2` using an isolated local
read-only fixture. The PerpDexList images are public-site reference captures
for information-architecture review only; no branding, assets, or code were
copied.

## Dashboard captures

| File | Viewport | Page | Source |
|---|---:|---|---|
| `dashboard-opportunities-1920.png` | 1920×1080 | Opportunities | local fixture `/opportunities` |
| `dashboard-opportunities-1440.png` | 1440×900 | Opportunities | local fixture `/opportunities` |
| `dashboard-opportunities-1280.png` | 1280×800 | Opportunities | local fixture `/opportunities` |
| `dashboard-opportunities-680.png` | 680×800 | Opportunities | local fixture `/opportunities` |
| `dashboard-pair-1440.png` | 1440×900 | Pair Detail, QQQ fixture route | local fixture `/pair` |
| `dashboard-anomalies-1280.png` | 1280×800 | Anomalies | local fixture `/anomalies` |
| `dashboard-status-1280.png` | 1280×800 | Status | local fixture `/status` |

## PerpDexList reference captures

| File | Viewport | Page | Source |
|---|---:|---|---|
| `reference-perpdexlist-arbitrage-1440.png` | 1440×900 | Funding arbitrage scanner | https://perpdexlist.com/arbitrage?mode=funding |
| `reference-perpdexlist-arbitrage-1280.png` | 1280×800 | Funding arbitrage scanner | https://perpdexlist.com/arbitrage?mode=funding |
| `reference-perpdexlist-grapher-1440.png` | 1440×900 | Populated spread grapher: Arcus NVDA-USD → Lighter RH NVDA | https://perpdexlist.com/arbitrage/grapher?c=arcus~NVDA-USD~lighter_rh~NVDA |

## Factual visual inventory of PerpDexList

- **Hierarchy:** a persistent dark left navigation rail, compact page controls at the top of the content area, then either a dense scanner table or stacked grapher panels.
- **Scanner structure:** the funding arbitrage page uses a tab-like price/funding switch, a wide multi-column table, clickable rows, pagination, and a refresh countdown.
- **Filters:** exchange selection, symbol search, filter menu, visible-column menu, RWA/crypto toggles, and category selection are grouped in one toolbar above the table.
- **Density and type:** rows are compact and highly numeric; venue symbols are secondary text beneath venue names, while percentages and dollar values use aligned numeric columns.
- **LONG/SHORT presentation:** the scanner combines the two venues inside a compact pair cell; the grapher makes the two legs explicit with colored 多头/空头 labels and separate venue/market selectors.
- **Primary versus secondary metrics:** funding spread, annualized value, and directional funding values receive the strongest color/position; volume, OI, quotes, and historical columns are quieter supporting data.
- **Color and badges:** positive values are green, negative values red, neutral metadata muted gray; small category badges and selected controls provide compact status cues.
- **Horizontal behavior:** the scanner is intentionally wider than the content column and relies on dense columns/table overflow rather than wrapping every value into a card.
- **Detail/grapher hierarchy:** selectors and timeframe sit above the chart; the populated grapher stacks execution-price, settled-funding, and live funding panels, with current market statistics in a side panel.
- **Whitespace and cards:** whitespace is restrained, with thin separators and dark bordered chart/table regions instead of many large cards.
- **Above the fold:** the scanner shows the complete filter toolbar, table header, and many rows; the grapher shows both leg selectors and the first chart panels plus current-leg statistics.
