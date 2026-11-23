# 試跑計畫：Radar 升級 P0（Mac mini 獨立 checkout，24 小時）

日期：2026-11-23 · 分支：`feature/radar-upgrade-p0`（`b1487f9`）· 對應：`docs/HANDOFF_radar_upgrade.md`

## 1. 目標與範圍

在 Mac mini（`trading-mini`）用**獨立 checkout** 跑 24 小時，確認 P0-1～P0-5 在真實環境穩定、不產生明顯假訊號、不影響正式環境。試跑通過後，**再另外取得使用者同意**才切換正式環境。

**不做的事**
- 不動正式環境：`~/Projects/opportunity-radar` 的 checkout、`data/` Parquet、`runtime/radar.sqlite3`、`~/.config/opportunity-radar/radar.yaml`、tmux session `radar`、Dashboard。
- 不下單、不碰私鑰、不用需要交易權限的 API（AGENTS.md）。

## 2. 現況（2026-11-23 以唯讀方式查到的）

| 項目 | 數值 |
|---|---|
| 機器 | 16GB RAM、12 核、macOS 27.0；空閒記憶體 61%、硬碟剩 105GB |
| 正式 Radar | `uv run python -m radar.app run --config ~/.config/opportunity-radar/radar.yaml`，tmux session `radar`；RSS 1.6GB |
| 正式 Dashboard | 另一個程序，RSS 1.1GB |
| 正式 checkout | `deploy/ondo-readonly-20261029`（`a51e82a`），沒有未提交的修改 |
| 正式資料 | `data/` 6.0GB（market 約 95MB/天）、`runtime/` 399MB；共 171 條資料流 |
| 正式設定重點 | `primary_size_usd: 10000`、`manual_opportunity.enabled: true`、`anomaly_v2.enabled: true` / `telegram_enabled: false`、Lighter 主網 30 個、Lighter RH 27 個市場 |
| Telegram | 憑證放在 `~/.config/opportunity-radar/telegram.env`（內容沒有讀取） |

資料路徑都是**相對於工作目錄**（`data/`、`runtime/radar.sqlite3`、`runtime/radar-diagnostics.log`），所以獨立 checkout 會自動寫到自己的目錄，不會碰到正式資料。

## 3. 共用 IP 的風險與對策

試跑和正式環境從同一個 IP 打同樣的公開 API：

| API | 上限 | 正式 | 試跑 | 合計 | 結論 |
|---|---|---|---|---|---|
| Lighter REST（主網） | 60 次／分鐘，超過封鎖 60 秒 | 每小時 :01 一次並發 30 次 + metadata 1 次／分鐘 | 間隔 1.5 秒時約 40 次／分鐘，同樣從 :01 開始 | **約 72 次／分鐘** | ⚠️ 會超過，正式環境也會被封鎖 |
| Lighter REST（RH） | 同上（另一台主機） | 每小時 27 次 | 約 23 個市場 | 約 50 次／分鐘 | 可以 |
| HL WebSocket | 15 條連線 | 3 條 | 3 條 | 6 條 | 可以 |
| HL REST | 1200 權重／分鐘 | 每小時 :01 約 580 | 每分鐘約 60 | 約 640 | 可以 |
| Var stats | 沒有公開上限 | 每 30 秒 1 次 | 每 30 秒 1 次 | 4 次／分鐘 | 可以 |

**對策（試跑前要先改程式，見 §4-A）**：把 Lighter 資金費請求的間隔做成設定值，試跑設為 3 秒（每分鐘 20 次）。這樣合計約 52 次，低於 60。95 個市場一輪約 285 秒，在 5 分鐘的抓取週期內做得完。

**Telegram 重複告警**：試跑也會發價差和資金費告警。如果用同一個 chat，正式環境和試跑會各發一份價差告警，分不出是哪一邊發的。要你決定（§4-B）。

## 4. 試跑前的決定（2026-11-23 使用者已確認）

- **A. Lighter 請求間隔**：已完成。新增設定 `lighter_funding_request_spacing_seconds`（預設 1.5 秒），試跑設為 `3.0`。
- **B. Telegram**：同一個 bot、另外開一個試跑用的 chat。不用改程式，只換 `RADAR_TELEGRAM_CHAT_ID`。**你要先建好 chat、把 bot 加進去，並提供 chat id。**
- **C. 分支傳送方式**：push `feature/radar-upgrade-p0` 到 `origin`（GitHub），再在 Mac mini 上 clone。

## 4a. 第一次試跑中止紀錄（2026-11-23）

- **時間**：11:51 UTC 啟動，12:02 UTC 手動停止。
- **現象**：試跑的 HL feed 在約 14 分鐘內重連 19 次（多數由 l2 靜默超時觸發）。HL 開始以「Cannot open more than 15 connections」拒絕這個 IP 的新連線，**正式環境重連時也被拒絕**。約 11:59–12:06 UTC 這段時間，正式 Radar 的 HL core 和 xyz 共 28 個市場收不到資料。停止試跑後約 3 分鐘自動恢復。
- **共用 IP 的狀況**：開發機和 Mac mini 對外是同一個公網 IP，這個 IP 上還有其他長期執行的 HL 用戶端（合計約 29 條連線）；正式 Radar 只佔 2–3 條。
- **原因**：HL 偶爾會整條連線 20 秒不送 l2Book，正式環境也有這個現象。原本的處理是立刻斷線，並在 1 秒後重連；三個 HL feed 各自這樣做，就把整個 IP 的連線名額推過上限。
- **修正**（`ee087e2`）：
  - 第一次靜默時，先在同一條連線上重新訂閱；第二次靜默才斷線重連。
  - 同一個程序內的所有 HL feed 共用一個重連限額，設定值是 `hyperliquid_reconnect_spacing_seconds`（預設 10 秒）。重掃時重建 feed 也算一次重連。

## 4b. 重試前的額外安全措施

- 試跑設定加上 `hyperliquid_reconnect_spacing_seconds: 30`（比預設更保守）。
- **自動中止**：啟動一個獨立的監看腳本，每 30 秒讀一次正式 Radar 的 tmux 畫面（唯讀）。只要出現新的「Cannot open more than 15 connections」，就立刻對試跑送 Ctrl-C 停止。
- 試跑期間，同一個 IP 上其他用不到的 HL 用戶端可以暫時關掉，以空出餘裕（由使用者決定）。
- 試跑期間，開發機和 Mac mini 上都不會再額外跑任何會連 HL 的 smoke test。

## 5. 試跑步驟（取得同意後才執行）

所有指令都在 Mac mini 上執行，只會建立新的目錄和檔案。

1. **建立獨立 checkout**
   ```bash
   git clone --branch feature/radar-upgrade-p0 https://github.com/lspss93119/opportunity-radar.git ~/Projects/opportunity-radar-trial-p0
   ```
   ```bash
   cd ~/Projects/opportunity-radar-trial-p0 && uv sync && uv run pytest -m "not live" -q
   ```
2. **建立試跑設定**：複製正式設定（只讀取，不修改原檔）。
   ```bash
   cp ~/.config/opportunity-radar/radar.yaml ~/.config/opportunity-radar/radar.trial-p0.yaml
   ```
   在 `radar.trial-p0.yaml` 修改這些項目：
   - `monitors.spread.primary_size_usd: 1000`
   - 新增 `discovery:`，`enabled: true`，其他用預設值（$100k 成交量門檻、每 6 小時重掃）
   - 新增 `monitors.funding:`，`enabled: true`，其他用預設值（飆升 100% / 2 次採樣、carry 15% / 24h、持有 7 天）
   - `manual_opportunity.enabled: false`（正式環境已經在跑，試跑不重複發）
   - `lighter_funding_request_spacing_seconds: 3.0`（§4-A）
   - `hyperliquid_reconnect_spacing_seconds: 30`（§4b）
   - 確認 `fees_bps` 有包含 `variational`（可以設 0，或填查證過的值）
3. **用新的 tmux session 啟動**（不碰 `radar` 這個 session）
   ```bash
   tmux new-session -d -s radar-trial-p0 -c ~/Projects/opportunity-radar-trial-p0
   ```
   在這個 session 裡載入 `telegram.env`，再把 `RADAR_TELEGRAM_CHAT_ID` 換成試跑 chat 的 id（§4-B），然後執行：
   ```bash
   uv run python -m radar.app run --config ~/.config/opportunity-radar/radar.trial-p0.yaml
   ```

## 6. 試跑期間的檢查（T+10 分、T+1 小時、T+6 小時、T+24 小時）

| 檢查項目 | 怎麼看 | 通過條件 |
|---|---|---|
| 程序存活 | `ps` / tmux | 24 小時沒有當掉、沒有重啟 |
| 記憶體 | 試跑程序的 RSS | < 4GB，而且 T+6 小時之後不再持續上升 |
| 正式環境沒受影響 | 正式 Radar 的 log 和 RSS | 沒有新增 Lighter 429/405、HL 1008；RSS 跟試跑前差不多 |
| 自動探索 | log 裡的 `market discovery` / `market universe updated` | 約 390 條資料流；同名不同資產（CVX、QNT）有被擋下；T+6 小時有重新掃描 |
| 資料進來 | log 裡的 market cycle / availability | 大部分資料流都在 batch 裡；Var 報價年齡 ≤ 120 秒 |
| 資金費資料 | 試跑的 `data/funding` | 每 5 分鐘一批，含 Var，`interval_seconds` 不是空值 |
| 告警數量 | Telegram（試跑 chat） | 資金費飆升沒有洗版（同一組配對 1 小時內最多 1 則）；價差告警在合理範圍 |
| Carry | T+19 小時之後 | 覆蓋率達標後才開始出現，不會一啟動就觸發 |
| 硬碟 | `du -sh data` | 約 150MB／天 |

## 7. 驗收（對應 HANDOFF 第 5 節）

- **人工比對**：把試跑期間的資金費告警，和 Fundingo / profundingpro 公開的 Var、Lighter、HL 組合做比較，列出哪些抓到、哪些漏掉、各自的原因。
- **假訊號抽查**：USDC/USDT 基差、過期的 RFQ 報價、盤口太薄，各抽幾則告警檢查。
- **還沒做的**：HANDOFF 第 5 節要求的「用正式 Parquet 回放」，新的資金費 monitor 目前沒有回放工具。這次 24 小時試跑先用實盤觀察代替；回放工具要另外排。

## 8. 停止與回復

- 停止試跑：
  ```bash
  tmux kill-session -t radar-trial-p0
  ```
- 試跑的資料都在 `~/Projects/opportunity-radar-trial-p0/` 和 `radar.trial-p0.yaml` 裡，刪掉這兩個就完全清乾淨，正式環境不受影響。

## 9. 切換正式環境（試跑通過且你同意之後，另外執行）

1. 備份正式設定和 `runtime/radar.sqlite3`（照既有慣例，加上時間戳記）。
2. 停止正式 Radar → 把正式 checkout 切到通過試跑的 commit → `uv sync` → 套用 §5-2 的設定修改（manual_opportunity 維持原本的設定）→ 重新啟動。
3. 已知的一次性影響：
   - 進行中的 spread / anomaly v2 episode 是用 $10k 算的，切換後可能出現一次「回到均值」之類的事件；
   - carry 要再等約 19 小時才會開始觸發；
   - Parquet 新增的欄位對舊檔向下相容（已測試）。
4. Dashboard 不用重啟，但它看不到自動探索到的市場（已知限制）。
