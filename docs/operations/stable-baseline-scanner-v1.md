# Stable Baseline Scanner V1（離線研究）

這是 **read-only 的市場研究指令**，不是交易策略、實盤 Bot 或即時 Telegram 告警。它不讀取 API 私鑰，也不修改 Radar 現有 10 秒採集、Monitor 或 Dashboard 行為。

## 啟動

在現有 Radar 虛擬環境、資料完整的主機執行：

```sh
uv run python -m radar.history.stable_baseline   --config config/radar.example.yaml   --data-root data   --output runtime/research/stable-baseline-v1   --size-usd 1000   --sample-seconds 60   --extra-slippage-bps 2
```

若使用已匯出的 CSV.GZ（這些檔案的 dataset 必須是 **market**）：

```sh
uv run python -m radar.history.stable_baseline   --config config/radar.example.yaml   --csv-glob '/absolute/path/market/date=*.csv.gz'   --output runtime/research/stable-baseline-v1   --size-usd 1000   --extra-slippage-bps 2
```

輸出三個檔案：

- `pair_ranking.csv`：逐個已設定的跨交易所配對列出有效同步快照、前日基準符合天數、完成交易、收斂交易、總淨損益、每筆平均淨 bps、平均持倉時間（中位數），包含資料不足的市場。
- `completed_trades.csv`：每筆虛擬交易的日期、方向、進場偏移、出場偏移、成交原因、gross/fee/slippage/net。
- `summary.json`：總配對數、已有效觀測數、完成交易數、未平倉數、主要參數與會計假設。

**研究流程的限制 / Fail-closed 原則**

1. 僅選取 `config.markets` 中 `enabled=true` 的兩兩不同交易所同標的組合；報價式 `quoted_markets`（例如 Variational）暫不納入，因其成交模型不同。
2. 僅使用同一 `sample_time` 的 size-specific 買入/賣出 VWAP。任一腿無效、過期（超過 12 秒）、兩腿 `observed_at` 差超過 3 秒時，該時點不計入。
3. 先對每天的有效快照計算 pair 的每日中位數基準；每一天至少有 60% 的預期樣本才算有效。
4. 對於正在評估的日期，僅使用**前 7 個 UTC 日內至少 5 個合格的前日**，絕不使用當天或未來資料。前日基準的 Q90−Q10 必須不超過 12 bps；不符則不評估新進場。
5. 僅在相對凍結的基準偏離達 20 bps，且連續 2 個有效觀測確認時建立候選；**下一個有效快照**才以兩腿 VWAP 進場。若中間缺分鐘則取消候選。
6. 進場前先估計買賣差價、來回 4 腿的 configured taker fee、額外滑價，要求預期淨機會至少 5 bps（以每腿 $1,000 為分母）；不合格不交易。
7. 進場後不反覆調倉；偏移縮回 5 bps、擴大超過入場偏移 50 bps、超過 4 小時或資料斷線時觸發出場；**下一個有效快照**才模擬平倉。如果結尾仍持倉，標示 `open_at_end`，不虛構平倉價格。
8. 回測以兩腿名目各 $1,000、先買/賣再反向平倉、持有固定 base 數量計算 USD PnL；費用按配置、入場與出場實際名目算，滑價固定 4 個成交方向。Funding、資金占用利息、API 拒單、交易隊列、最小下單額及真實延遲 **不在模型中**。
9. `research_only` 的意思是 **至少完成 5 筆**，不是獲利或統計顯著；`insufficient_trades`、`insufficient_history` 的配對不能當成已驗證機會。排行本身只為研究候選，不能等同上線建議。

## 目前未涵蓋的項目

- 美股時段、休市 regime 獨立建模及 Funding 的持倉現金流。
- 多市場共享保證金、同時持倉容量與相關性；各 pair 目前**獨立模擬**，不能直接把排名損益加總視為組合回測。
- Variational quote 的 RFQ／報價可成交機率、被動掛單的 queue 模型。
- 生產部署驗證、Mac Mini 實際執行效能、最新市場排名。使用前需在完整的 production Parquet 資料集做實測及樣本外檢驗。

建議依順序進行：先用本指令整理全市場一致口徑的研究排名，再挑出資料完整且偏移次數充足的配對進行 10 秒級成交回放、Funding/時段與多市場資金組合研究，最後才考慮 Dashboard 排名整合和通知。
