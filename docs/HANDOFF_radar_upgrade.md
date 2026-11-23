# 交接：Opportunity Radar 升級（找出跨所資金費與價差機會）

日期：2026-11-23 · 來源：claude.ai 研究討論 → Claude Code 實作

## 1. 目標

讓雷達能即時找到最近在 X 上被反覆驗證的套利機會，**不限交易所**：

| 類型 | 例子（2026-11 實例） | 特徵 |
|---|---|---|
| A. 價差收斂 | VVV：Lighter 比 Var 貴 1.41%，7 小時收到 0.6%；ZRO：Lighter 比 Var 貴 0.41% | 長尾幣，偏離幾十 bps～1% 以上，持倉數小時 |
| B. 資金費飆升 | ACE：Var 一度每小時付多方約 2%；VVV：Lighter 空方 7 小時平均 325% APR；週末 MSTR/CRCL/COIN 230–340% APR | 持續數小時，常見於長尾幣與週末美股 |
| C. 資金費 carry | S&P：Var swap 固定約 4.9%/年 vs Lighter 約 10.5%；QQQ：RH 約 3.5%（有時十幾 %）vs Var 約 0 | 持倉數天，偏離小但穩定 |

時間窗口：Var 積分活動預計最多再 4–5 週，**P0 要在 1–2 週內上線**。

## 2. 不可違反

- 遵守 `AGENTS.md`：只讀、不下單、不碰私鑰、不用需要交易權限的 API。三個架構邊界不變（Collector ≠ Monitor ≠ App；Fast scan ≠ Slow work）。
- 正式環境在 Mac mini（`trading-mini`）。在 feature branch 開發，不要直接動正式環境的 checkout、Parquet 或 SQLite；上線前先和使用者確認。
- 維持 fail closed：資料缺漏、過期或無法換算時，寧可漏報也不能產生假機會。
- 外部聚合網站（Fundingo、profundingpro、strikeperps、fundingarb.tech 等）只能拿來人工對照，不能成為必要的依賴。

## 3. 現況（claude.ai 讀程式碼後的判斷，請先自行確認）

1. `VariationalCollector` 的報價只寫進 Parquet（`quoted_markets`），**沒有任何即時 monitor 讀取**。
2. `FundingSnapshot` **沒有結算週期欄位**。Lighter/HL 每小時、Backpack 等每 8 小時、Var 依 `funding_interval_s`，原始 rate 無法直接比較。資金費只當 alert 的附加資訊，而且每小時才抓一次。
3. Universe 是手動 YAML，每家約 30 個標的、Var 28 個；VVV、ZRO、INIT、ACE、IRYS、CYBER 都不在清單裡。
4. Spread monitor 主要金額是 $10k，長尾盤口常常不夠深而變成 NULL，被靜默排除；Var 報價只有 $1k / $100k 兩檔。
5. 結構性基差：Var 用 USD/USDC 計價，Lighter 的 BTC 指數跟 USDT（今天在 var-lighter-arb 驗證過約 −13 bps，來自 USDC/USDT 約 1.0013）。絕對門檻會被這個基差干擾。
6. 有四套重疊的價差偵測：spread、anomaly v1、anomaly v2、manual_opportunity，各自有 lifecycle 和 notification 狀態；多個檔案過大；README / CODEX_START 已過時。

## 4. 工作項目

### P0（先做，每項做完都跑完整的 non-live 測試）

**P0-1 資金費標準化**
- `FundingSnapshot` 加上 `interval_seconds`，並統一換算成 `rate_per_hour` 和 `apr`（帶正負號：正值代表多方付錢）。
- 逐一確認每個 collector 的單位，並各寫一個 fixture 測試：
  - Lighter：rate / 100 的換算、方向。
  - HL：每小時。
  - Backpack、Arcus、Ondo、trade_xyz / entropy。
  - Var：`funding_rate` 搭配 `funding_interval_s`。var-lighter-arb 那邊把 BTC 的 0.1095 推斷為年化 10.95%，是比對其他交易所數值得出的，**尚未查證**。請用多個市場和官方文件或前端顯示交叉確認後再寫死。
- 抓取頻率從每小時改成每 5–10 分鐘（可設定）；遵守各家的 rate limit（Lighter REST 429 會被封 IP 60 秒）。

**P0-2 Var 接進即時監控**
- 統一報價介面：訂單簿 VWAP 和 RFQ 報價都轉成「金額 → 可成交的 buy/sell 價」，至少涵蓋 $1k（Var 的 `size_1k`）。
- 讓 Var 和其他交易所一樣參與配對比較。Var 的報價過期判斷沿用 `quotes.updated_at`。

**P0-3 自動發現市場與配對**
- Var：stats 端點一次回傳全部 listings（約 590 個），全部納入。
- Lighter 主網、Lighter RH、HL（含 HIP-3 / trade_xyz）：從各自的市場列表 API 自動列出。
- 自動配對 canonical symbol，再加上 YAML 的覆寫清單（例如 Var `US500` → SPY）和排除清單：
  - pre-IPO 類（股數定義不同，例如 Anthropic、OpenAI）預設排除；
  - 1000 倍面額（`kPEPE`、`1000BONK` 等）要換算或排除；
  - 同名但不同資產要排除。
- 定期重新掃描（例如每 6 小時），新上市的市場要自動加入。

**P0-4 長尾市場用 $1k**
- 機會評估以 $1k 為主、$5k 為輔；$10k 只用在流動性夠的主流市場。
- 長尾市場每 30–60 秒採樣一次就夠，不需要 10 秒。

**P0-5 資金費 monitor（類型 B、C）**
- 對每組「標的 × 交易所 A × 交易所 B」（A 做多、B 做空）計算資金費差 APR，以及近 N 小時的平均和持續時間。
- 兩種告警：
  - **飆升**：例如淨 APR ≥ 100%，且持續 ≥ 2 次採樣；
  - **Carry**：例如 24h 平均淨 APR ≥ 15%，且當下的價差不會吃掉收益。
- 告警內容至少包含：兩邊的資金費率與下次結算時間、當下的可成交價差、來回成本、OI / 24h 成交量、Var 的 long/short OI。
- 門檻全部做成設定值。

### P1

**P1-1 單一機會評分（類型 A 加 B 合併）**
- 預期淨收益（bps，持有期 H）= 價差偏離基準可收斂的部分 + H 期間的預期資金費 − 來回成本（兩邊點差、滑價、手續費）。
- 價差偏離一律用「相對於 causal baseline」來算，不用絕對值。這樣就順便處理掉 USDC/USDT 這類結構性基差。另外記錄 USDC/USDT 參考價（Binance 公開報價），方便判讀。

**P1-2 新增交易所**
Aster、Extended 優先（在 Var 相關的資金費排行最常出現），其次是 QFEX、TxFlow、Phoenix、Decibel、Grvt。只用公開行情和資金費 API。

**P1-3 即時監控加上美股時段**
盤中 / 盤後 / 休市 / 週末各自用不同的基準和門檻。週末美股資金費是重點。

### P2

- 把四套價差偵測整併成一套 lifecycle 和通知機制，舊的監控保留到新的驗證完成再移除。
- 拆大檔案：`manual_opportunity.py`、`stable_baseline_v2.py`、`dashboard_data.py`、`dashboard.py`、`alerts/spread.py`。
- 更新 README、CODEX_START、架構規格，反映現在的範圍。

## 5. 驗收方式

- 回放驗證：用正式環境已有的 Parquet 回放，確認新的 monitor 不會產生明顯的假訊號，特別是 USDC/USDT 基差、過期報價、盤口不足這三種情況。
- 實際對照：上線後 3–5 天，把雷達的告警和 X 上公開的機會人工比對（例如 Fundingo / profundingpro 每日貼出的 Var、Lighter、Aster 組合），整理出哪些抓到、哪些漏了、各自的原因。
- 每個 P0 項目完成時，回報：改了哪些檔案、測試結果、做了哪些 live smoke check、和設計的偏差、已知限制。

## 6. 先問使用者的事

1. 告警送到哪裡：沿用現有的 Telegram bot，還是另外開一個頻道？
2. 門檻的初始值：上面的例子（飆升 100% APR、Carry 15% APR、$1k）可以先用嗎？
3. 新功能先在 Mac mini 的獨立 checkout 試跑多久，再切換到正式環境？
