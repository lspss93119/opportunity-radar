# Opportunity Radar 功能盤點（2026-11-24）

**盤點範圍**：`feature/radar-upgrade-p0` 分支，commit `5cc1140`，直接讀程式碼整理。

**兩個版本的差別**：正式環境（Mac mini）跑的是部署分支 `deploy/ondo-readonly-20261029`（`a51e82a`），它**沒有** P0 升級的功能。下文用「**正式**」表示正式環境現在有的功能，用「**分支**」表示只在 `feature/radar-upgrade-p0` 和 Mac mini 試跑環境裡有的功能。

**系統定位**：唯讀的監控系統。不下單、不使用私鑰、不需要交易權限（見 `AGENTS.md`）。

---

## 第一部分：現況盤點

### 1. 專案結構與入口點

**主要目錄**

| 目錄 / 模組 | 負責內容 |
|---|---|
| `src/radar/collectors/` | 各交易所的資料收集：`lighter.py`、`hyperliquid.py`（含 HIP-3 dex）、`arcus.py`、`backpack.py`、`ondo.py`、`variational.py`、共用的 `base.py`、`http.py` |
| `src/radar/market_data.py` | 最新盤口快取 `LatestMarketData`，依 10 秒採樣時點組出 `MarketSnapshot`，並算出 $1k / $5k / $10k 的 VWAP |
| `src/radar/vwap.py` | `buy_vwap` / `sell_vwap`：沿著盤口吃到指定金額的均價，深度不足時回傳 `None` |
| `src/radar/pipeline.py` | `MarketDataPipeline`：每 10 秒採樣、定期抓資金費與 OI、Var 報價輪詢、寫入 Parquet；`set_universe` 可以在執行中更換市場清單（分支） |
| `src/radar/discovery.py` | 自動探索與配對市場（分支）：`discover_markets`、`select_universe`、`merge_universe` |
| `src/radar/monitors/` | 機會判斷：`spread/`（價差、rolling basis、anomaly v2）、`manual_opportunity.py`（BBO 機會）、`funding.py`（資金費，分支）、`runner.py`、`registry.py` |
| `src/radar/alerts/` | 慢速處理：格式化、畫圖（`chart.py`、`manual_chart.py`）、Telegram 發送（`telegram.py`）、路由（`worker.py`） |
| `src/radar/storage/` | `parquet.py`（歷史資料）、`sqlite.py`（runtime 狀態） |
| `src/radar/history/` | 離線查詢與回放：`spread.py`、`opportunities.py`、`anomaly_replay.py`、`manual_opportunity.py`、`stable_baseline.py`（v1）、`stable_baseline_v2.py`、`variational_opportunities.py`、`funding.py`（分支） |
| `src/radar/dashboard.py`、`dashboard_data.py` | 唯讀網頁 dashboard |
| `src/radar/analysis_daily.py`、`analysis_export.py` | 把 Parquet 匯出成 CSV.GZ，透過 rclone 上傳到 Google Drive |
| `src/radar/diagnostics.py` | event loop 延遲、GC 停頓、RSS、Backpack 工作量等診斷 |

**入口點**（`src/radar/app.py` 的 `build_parser` / `main`）

- `python -m radar.app run --config …`：常駐主程式，由 `build_application` 組裝、`RadarApplication.run` 執行。
- `… telegram-smoke`：送一則測試告警（`run_telegram_smoke`）。
- `… opportunities`：歷史機會報表（`history/opportunities.py`）。
- `… variational-opportunities`：Var $1k 機會報表（`build_variational_opportunity_report`）。

其他的 CLI：

- `python -m radar.dashboard`（`dashboard.main`）
- `radar.analysis_daily` / `radar.analysis_export`
- `radar.history.anomaly_replay`
- `radar.manual_opportunity_replay`
- `radar.history.stable_baseline`
- `radar.history.stable_baseline_v2`

**執行方式**

- **主程式**：在 Mac mini 的 tmux session `radar` 裡常駐執行。repo 裡**沒有**主程式的 launchd 或 systemd 設定。
- **Dashboard**：用 LaunchDaemon 常駐，範本在 `docs/operations/com.opportunity-radar.dashboard.plist.example`。
- **每日匯出**：repo 裡**沒有**排程設定，執行方式不明。

### 2. 資料來源

| 來源 | 盤口 | 標記價 / 指數價 / OI / 成交量 | 資金費 | 多久抓一次 |
|---|---|---|---|---|
| **Lighter 主網** | WebSocket `order_book/{id}`（`LighterOrderBookFeed`） | REST `orderBookDetails`（mark、index、OI、24h 成交量），由 `_refresh_metadata_once` 抓 | 分支：REST `funding-rates` 一次抓全部，8 小時費率除以 8 換成每小時，屬於當下費率（`parse_lighter_funding_rates`）。正式：每個市場打一次 `/fundings`，拿到的是已結算的值 | 盤口即時推送；metadata 60 秒一次；資金費和 OI 依 `context_poll_seconds` 抓（分支 300 秒，正式 3600 秒） |
| **Lighter Robinhood** | 同上（`api.rh.lighter.xyz`） | 同上 | 同上 | 同上 |
| **Hyperliquid core** | WebSocket `l2Book`（`HyperliquidOrderBookFeed`） | REST `metaAndAssetCtxs`（markPx、oraclePx、OI、dayNtlVlm） | 分支：從 `metaAndAssetCtxs.funding` 讀當下費率。正式：每個幣打一次 `fundingHistory` | metadata 60 秒一次 |
| **HL HIP-3 `xyz`（trade_xyz）** | 同上，`dex="xyz"` | 同上 | 同上 | 同上 |
| **HL HIP-3 `io`（entropy）** | 同上，`dex="io"` | 同上 | 同上 | 同上 |
| **HL `para`** | **無** | **無** | **無** | — |
| **Arcus** | REST `l2OrderBook` 每 10 秒，或 WebSocket `l2Orderbook`，由 `ArcusConfig.source` 選擇，可以回退到 REST（`rest_fallback`） | REST `markets`（mark、oracle、OI、成交量） | 分支：`markets.nextFundingRate`，屬於當下費率。正式：每個市場打一次 `fundingRates`，拿到的是已結算的值 | 10 秒一次 |
| **Backpack** | WebSocket `depth.{symbol}`，用 REST `depth` 做初始快照 | REST `markPrices`、`openInterest`、`tickers` | REST `fundingRates`，每個市場一次，**只有已結算的值**（分支標為 `rate_kind="settled"`） | metadata 60 秒一次 |
| **Ondo** | REST `depth`，每 10 秒 | REST `contracts` | REST `funding_rates`（當期的值） | 10 秒一次。**只記錄，不參與監控**（`monitor_enabled: false`） |
| **Variational** | 沒有盤口。REST `metadata/stats` 的 $1k / $100k RFQ 報價（`parse_variational_stats`） | mark、24h 成交量、多空 OI（美元） | 年化的 `funding_rate`，換算成每期費率（`variational_funding_snapshot`） | 30 秒一次；報價本身大約每分鐘更新 |
| **Binance / Bybit** | **無**（只會出現在 Lighter `funding-rates` 回應的其他交易所列裡，程式會忽略） | 無 | 無 | — |

**整體來說**：
- 所有交易所都**沒有抓成交紀錄**（trades）。
- 盤口會存最佳買賣價和數量，以及 $1k / $5k / $10k 的 VWAP，**不存完整的盤口深度**。

### 3. 標的配對

- **正式環境**：寫死在 YAML 的 `markets` 和 `quoted_markets` 清單裡，用 `canonical_symbol` 對齊（`RadarConfig.monitor_market_identities`）。目前實際參與配對的是 140 條資料流、**32 個標的、約 496 組有方向的配對**（Var 和 Ondo 只記錄、不配對）。
- **分支**：YAML 再加上自動探索（`discovery.select_universe`）：
  - **探索的交易所**：Lighter、Lighter RH、HL core、xyz、Var。**不包括** io、Arcus、Backpack、Ondo，這幾家只吃 YAML。
  - **代號標準化**：HL 的 `kPEPE` 換成 `1000PEPE`；`xyz:TSLA` 換成 `TSLA`；YAML 覆寫清單可以自訂，例如 Var 的 `US500` 對應到 `SPY`（`canonical_symbol`）。
  - **篩選條件**：24h 成交量 ≥ $100k；不在排除清單裡（pre-IPO：ANT、ANTHROPIC、OPENAI、SPCX、SHEIN、UNITREE）；mark 價格和同代號的中位數差距在 20% 以內；至少有另一家交易所的對應標的。
  - 每 6 小時重新掃描一次（`RadarApplication._run_universe_rescans`）。
  - 2026-11-23 實測：試跑約 **429 條資料流、約 130 個標的、約 1,290 組有方向的配對**。

### 4. 機會計算

- **價差公式**：`(做空那邊的賣出價 ÷ 做多那邊的買入價 − 1) × 10000`，單位 bps（`spread/models.calculate_raw_spread_bps`）。
- **用的是吃單價**，不是中間價：
  - spread / anomaly：指定金額的 VWAP（`selected_vwap`），正式用 $10k，分支用 $1k；
  - manual_opportunity：BBO 的最佳賣價 / 最佳買價。
- **手續費**：
  - 淨價差 = 原始價差 − 兩邊的吃單手續費（`calculate_net_spread_bps`），手續費來自 `fees_bps`；
  - 來回成本 = 2 ×（兩邊手續費相加）；
  - 但 spread monitor **只把手續費用在顯示**（`require_fees=False`），不影響觸發。
- **滑點**：即時監控用 VWAP 處理深度，**沒有額外的滑點假設**。只有離線研究 `stable_baseline_v2.estimate_cost_scenarios` 會加 0 / 2 bps 的額外滑點情境。
- **歷史錨點**：
  - spread monitor：24 小時滾動**平均值**和標準差（`monitors/spread/basis.RollingBasis`）。觸發條件寫死在 `spread/monitor.py`：偏離 ≥ 15 bps（`BASIS_DEVIATION_MIN_BPS`）、標準差 ≤ 3 bps（`BASIS_STD_MAX_BPS`）、持續 60 秒；至少要 6,912 筆觀測（約 19 小時）才算有效（`MIN_HISTORY_OBSERVATIONS`）。
  - anomaly v2：參考平均值，偏離 ≥ 15 bps、確認 60 秒、回到 ±5 bps 視為收斂（`AnomalyV2Config`）。
  - manual_opportunity：**24h 平均值**當錨點，2h、24h、3d 三個平均值的差距 ≤ 5 bps 才算穩定（`BboRollingHistory`、`reference_a_bps`、`baseline_range_bps`）。
  - 離線 `stable_baseline` v1：用**前 7 個完整日**的中位數當基準。
  - **即時監控沒有使用「2 小時滾動中位數」**。
- **資金費差（分支）**：`monitors/funding.FundingMonitor`。
  - 淨 APR = APR_B − APR_A（做多 A、做空 B），另外算 24h 時間加權平均和持續時間；
  - 飆升：淨 APR ≥ 100%，而且連續 ≥ 2 次採樣，兩邊都必須是當下費率；
  - carry：24h 平均 ≥ 15%，而且 168 小時的預期資金費要大於來回成本（`_round_trip`）；
  - **沒有算「回本需要幾小時」**，只比較固定持有期間內，預期收入有沒有大於成本。
  - 正式環境：**無**。

### 5. 警報

- **通知管道**：只有 Telegram（`alerts/telegram.TelegramTransport`），憑證從環境變數 `RADAR_TELEGRAM_BOT_TOKEN` / `RADAR_TELEGRAM_CHAT_ID` 讀取。**沒有** Discord 或其他管道。
- **路由**：`alerts/worker.AlertRouter`，依 monitor 分給 spread、manual_opportunity、funding（分支）的 processor。
- **spread（價差警報）**：
  - 觸發條件見第 4 點；
  - 每個 episode 只發一次，狀態存在 SQLite（`SpreadMonitor._persist_episodes`）；
  - 附 7 / 30 / 90 日歷史中位數和圖（`SpreadAlertProcessor`、`render_spread_chart`）；
  - 分支另外加上 RFQ 標示、報價年齡，以及 $1k / $5k / $10k 三種金額的價差。
- **anomaly v2**：
  - 事件有 initial、expansion、return、resolved 四種；
  - 同一個標的有 300 秒冷卻（`notification_symbol_cooldown_seconds`）；
  - 要求 12h / 24h / 48h 的平均值彼此接近（`mean_alignment_max_bps` 5 bps）；
  - 可以用 `telegram_enabled` 開關（正式環境目前是 `false`，只記錄不推播）。
- **manual_opportunity**：
  - 確認 60 秒、預期淨利 ≥ 10 bps、24h 成交量 ≥ $1M 才發；
  - 價差每擴大 5 bps 再通知一次；
  - 同一條路由安靜 300 秒後才重新開放通知（`ManualOpportunityNotificationGate`）。
- **funding（分支）**：
  - 同一組配對、同一種條件只發一次；
  - 條件消失後要維持 3600 秒才會重新開放（`FundingMonitor._clear`、`rearm_seconds`）。

### 6. 資料儲存

- **Parquet**（`storage/parquet.ParquetStorage`）：存在 `data/<dataset>/date=YYYY-MM-DD/part-<uuid>.parquet`，先寫暫存檔再用 `os.replace` 換上，保留 90 天。
  - `market`：每 10 秒一筆，BBO、mark、index、1k / 5k / 10k VWAP，分支多了 `price_source`；
  - `funding`：分支多了 `interval_seconds`、`rate_kind`；
  - `hourly_context`：OI 和成交量，分支多了 Var 的多空 OI；
  - `quoted_market`：Var 的 RFQ 報價。
  - 分支裡，長尾資料流只在自己的採樣整點寫入（`_on_storage_cadence`）。
- **SQLite**（`storage/sqlite.SQLiteRuntimeStore`，`runtime/radar.sqlite3`）：存 monitor 的狀態和機會事件（`opportunity_log` 表）。
- **能不能回測**：可以。DuckDB 查詢與回放工具有：`history/spread.SpreadHistory`、`history/opportunities`、`anomaly_replay`、`manual_opportunity_replay`、`stable_baseline` v1 / v2。另外 `analysis_export` 可以匯出 CSV。
- **限制**：只有 3 種固定金額的 VWAP，沒有完整盤口深度和成交紀錄，所以沒辦法重建任意金額的成交價。

### 7. 穩健性

- **過期檢查**：
  - 依本地收到資料的時間（`observed_at`）判斷，超過 30 秒就排除（`market_data._build_market_snapshot`、`is_valid_market_snapshot`）；
  - live 監控另外要求：兩邊都在 3 秒內、兩邊時間差 ≤ 3 秒（`LIVE_QUOTE_MAX_AGE_SECONDS`、`LIVE_QUOTE_MAX_SKEW_SECONDS`、`is_live_synchronized_pair`）；
  - 分支：RFQ 那一邊改用 120 秒門檻（`_is_live_pair`）。
  - **沒有使用交易所時間戳**。Arcus WebSocket 雖然保存了 `venue_timestamp_raw`，但沒有任何地方用它。
- **斷線重連**：
  - HL：指數退避 1 到 30 秒、50 秒心跳、20 秒靜默偵測；分支改成靜默時先重新訂閱，所有 HL feed 共用重連限額（`ReconnectBudget`）。
  - Lighter：快照逾時和重新訂閱。
  - Arcus：WebSocket 5 秒靜默就重連。
  - Backpack：重連延遲。
- **錯誤處理**：collector 出錯一律 `report_collector_error` 記 log，不中斷其他交易所。
- **延遲監控**：
  - 本地：event loop 延遲（`diagnostics.EventLoopLagWatchdog`）、GC 停頓（`GCTelemetry`）、RSS；
  - **交易所端的延遲沒有監控**。
- **資料可用性 log**：每一輪記錄 included、stale、深度不足等數量（`MarketDataPipeline._record_market_availability`）。

### 8. 事件監控

- 配息、拆股、財報、FOMC、合約結清重開、換月、交易所公告：**無**。
- 新上市：**部分**（分支）。探索每 6 小時會自動把新市場加進監控，但**不會發通知**；已下架的 HL 市場會被過濾掉（`parse_hyperliquid_listings`）。
- 美股交易時段：只有離線研究 `stable_baseline_v2.us_session_regime` 有分類，**即時監控沒有**。

### 9. 設定與部署

- **設定**：`config/radar.example.yaml`，由 `config.load_config` 和 pydantic 的 `RadarConfig`（`extra="forbid"`，不允許未知的鍵）讀取；正式設定在 `~/.config/opportunity-radar/radar.yaml`。
- **環境變數**：只有 `RADAR_TELEGRAM_BOT_TOKEN`、`RADAR_TELEGRAM_CHAT_ID`。
- **依賴**（`pyproject.toml`，Python ≥ 3.13，用 uv 管理）：duckdb、httpx、matplotlib、pydantic、pyarrow、pytz、PyYAML、websockets；開發用 pytest、pytest-asyncio。
- **測試**：62 個測試檔，分支上 1,102 個 non-live 測試，另有 8 個 `live` 測試（打真實 API）。**沒有 CI**（沒有 `.github`）。
- **部署**：Mac mini 上直接用 uv 跑，主程式在 tmux 裡、dashboard 用 launchd。**沒有** Docker 或 VPS 設定。

### 10. 已知問題

- **寫死的值**：
  - `sampling_seconds` 只能是 10（`RadarConfig.sampling_must_be_ten_seconds`）；
  - spread 的觸發門檻 15 bps / 3 bps / 60 秒，以及最少 6,912 筆觀測（`spread/monitor.py`、`spread/basis.py`）都不能設定；
  - live 的 3 秒對齊規則（`spread/models.py`）；
  - HL 的心跳、靜默、重連常數；Var 每 30 秒輪詢。
- **容易誤會的設定**：
  - `candidate_net_bps` / `alert_net_bps` 只用在圖表的參考線，不影響觸發（`alerts/spread.py`、`alerts/chart.py`）；
  - `top_n` 仍然接受，但已經不會用來限制配對數（`build_spread_candidates` 的說明）。
- **沒人用的程式碼**：`LighterCollector.FUNDINGS_URL`、`ORDER_BOOK_ORDERS_URL` / `order_book_orders_url`（分支）。
- **lint**：ruff 有 7 個錯誤，集中在 `history/stable_baseline.py`、`stable_baseline_v2.py`（未使用的 import 和變數、lambda 賦值）。
- **文件過時**：`README.md`、`CODEX_START.md` 還停在 v1 和 Task 2 的描述。
- **Dashboard 只讀 YAML**：看不到自動探索到的市場，也沒有資金費的頁面。
- **Backpack 只有已結算的資金費**，會落後最多 1 小時，所以分支只讓它參與 carry。
- **共用 IP 的 HL 連線上限**：同一個公網 IP 上的其他 HL 用戶端會把正式環境的 HL 擠斷（2026-11-23 實際發生過）。
- **正式環境的 RSS 曾從 1.6GB 升到 3GB 左右**，原因還沒查。
- **TODO / FIXME**：程式碼裡沒有任何標記。

---

## 第二部分：差距比對

| 目標規格 | 狀態 | 說明 |
|---|---|---|
| 自動找出各所同名標的配對，涵蓋 HL 所有 dex（io、xyz、para）、Lighter 主網和 RH、Arcus、Variational、Ondo | **部分** | 分支能自動探索 Lighter、Lighter RH、HL core、xyz、Var，並做代號標準化、成交量門檻、價格一致性檢查和 pre-IPO 排除（`discovery.py`）。**不包括** io（只吃 YAML）、Arcus、Ondo（Ondo 不參與監控）、`para`（完全不支援）。正式環境全部寫死在 YAML。 |
| 每 10 秒記錄兩邊的最佳買賣價和吃到一定深度的均價，並持久化保存 | **已有** | 每 10 秒一筆，存 BBO 和 $1k / $5k / $10k 的 VWAP 到 Parquet（`market_data.py`、`storage/parquet.py`）。分支裡長尾資料流的歷史改成 30 秒一筆，價格仍然每 10 秒評估。只有 3 種固定金額，沒有完整深度。 |
| 價差以 bps 計算並扣掉雙邊手續費；用 2 小時滾動中位數當錨點，偏離超過閾值（例如 25 bp）時警報 | **部分** | bps 和扣手續費都有。錨點是 24h 滾動平均（spread）、24h 平均加上 2h / 3d 穩定度（manual），**沒有 2h 中位數**。spread 的偏離門檻寫死 15 bps；manual 是淨利 ≥ 10 bps，可以設定。 |
| 記錄每對標的的價差分佈：標準差、偏離次數、持續時間、回歸時間 | **部分** | live 有 24h 滾動標準差；anomaly episode 有記錄峰值、持續時間、收斂；離線 `stable_baseline_v2` 有持續時間和收斂分析，`opportunities` 報表有 p95。**沒有**每對標的持續累積的分佈統計表。 |
| 用錄下來的數據照實盤規則回測（含手續費、滑點、深度限制） | **部分** | 有 `stable_baseline` v1 / v2（下一個 tick 成交、手續費、額外滑點情境、固定金額的 VWAP）、`anomaly_replay`（用和 live 相同的 `AnomalyTracker`）、`manual_opportunity_replay`。**沒有**用 live monitor 本身做的回測，也不計入資金費（`funding_included: False`），而且不能模擬任意金額的深度。 |
| 數據過期檢查：maker 那邊超過 1500ms、對沖那邊超過 800ms 視為過期 | **部分** | 有：兩邊都在 3 秒內、時間差 ≤ 3 秒、整體 30 秒過期、RFQ 120 秒。**沒有**區分 maker 和對沖腿，門檻比規格寬，也沒有用交易所時間戳。 |
| 危險時段標記：美股開盤前後 5 分鐘、財報、FOMC | **缺少** | 只有離線的 `us_session_regime`，即時監控和告警裡都沒有，也沒有財報或 FOMC 的行事曆。 |
| 資金費差監控與回本週期計算 | **部分** | 分支有 `FundingMonitor`（飆升、carry、24h 平均、來回成本，以及固定期間內預期資金費和成本的比較）。**沒有**直接算出「回本小時數」；正式環境完全沒有。 |
| 事件警報：配息、拆股、結清重開、新上市 | **缺少** | 都沒有。新上市只會被自動加進監控，不會發通知（分支）。 |
| 有 dashboard 或報表可以看各對標的的機會排名 | **已有** | dashboard 有 `/opportunities`、`/anomalies`、`/pair`、`/status` 頁面和對應的 API；CLI 有 `opportunities`、`variational-opportunities` 報表。但 dashboard 看不到自動探索到的市場，也沒有資金費排名。 |

---

## 最值得優先補強的 5 項（依投入產出比排序）

0. **前提**：P0 升級目前只在分支和試跑環境裡。完成 24 小時試跑並切換到正式環境，下面多數項目才會在正式環境生效（約半天的操作工作，不是開發工作）。

1. **在資金費告警裡加上回本小時數，並排出資金費排名**　工作量：**小**
   `FundingMonitor._alert` 已經有來回成本和每小時的淨費率，回本小時數 = 來回成本 ÷ 每小時淨費率；再在 dashboard 或 CLI 加一個資金費排名。直接補上規格裡缺少的「回本週期」。

2. **新上市通知**　工作量：**小**
   `RadarApplication.refresh_universe` 每次重掃時，已經有新舊兩份市場清單，比對出新增的標的、透過現有的 Telegram 發通知即可。新上市市場常常是價差和資金費最極端的時候。

3. **危險時段標記（美股開盤和收盤前後 5 分鐘）**　工作量：**小**
   把 `stable_baseline_v2.us_session_regime` 移到共用模組，在 spread、manual、funding 的告警裡加上時段標籤，必要時在這段時間抑制美股標的的告警。財報和 FOMC 需要外部行事曆，可以晚一點再做（中）。

4. **用交易所時間戳判斷過期，並區分 maker 和對沖腿**　工作量：**中**
   從 HL `l2Book.time`、Arcus `timestamp`、Lighter 等訊息取出交易所時間，計算各交易所的延遲、記進 log，再把過期門檻細分成 maker 1500ms、對沖腿 800ms。目前只用本地收到的時間，看不出交易所端的延遲。

5. **2 小時滾動中位數錨點，並把觸發門檻做成設定值**　工作量：**中**
   把 `BASIS_DEVIATION_MIN_BPS` 等寫死的常數改成設定值，另外加一個 2h 中位數錨點（可以用 `RollingBasis` 的排序結構，或改用分位數估計），對齊「偏離 25 bp」的規格。2h 的窗口也比較快暖機，可以縮短新市場目前要等約 19 小時的問題。

**沒有排進前 5、但值得記下來的**：
- 讓探索也涵蓋 io、Arcus、Ondo，並支援 `para`（中）；
- 用 live monitor 本身做的回測框架，並計入資金費（大）；
- 每對標的持續累積的分佈統計表，並放上 dashboard（中）；
- 配息、拆股、結清重開等事件警報，需要串接外部資料源（大）。
