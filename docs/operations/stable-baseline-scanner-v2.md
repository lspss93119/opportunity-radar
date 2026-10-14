# Stable Baseline Scanner V2（離線資料品質與自然收斂研究）

V2 是獨立於 V1 的 read-only 歷史研究指令，不是交易策略、即時 Monitor、Dashboard 或下單 Bot。它不讀取私鑰、不修改 Production Parquet、不重啟 Radar。

## 啟動

在資料主機的獨立 checkout 執行：

```sh
python -m radar.history.stable_baseline_v2 \
  --config /absolute/path/radar.yaml \
  --data-root /absolute/path/data \
  --output /absolute/path/runtime/research/stable-baseline-v2 \
  --v1-trades /absolute/path/v1/completed_trades.csv \
  --sample-seconds 60 --raw-sample-seconds 10 --size-usd 1000
```

若 size 為 1,000，V2 會另外讀取 5,000 VWAP 以做成本敏感度比較；若以 `--size-usd 5000` 啟動，主要 timeline 會改用 5,000 VWAP。

## 輸出

- `data_quality_report.csv`：system、market、pair 及 V1 data-gap event 的根因與樣本損失；包含 3/5/10 秒 skew sensitivity，但 canonical 分析仍使用 3 秒。
- `baseline_stability.csv`：causal 24h、7d、30d window，mid 與兩個 directional executable VWAP basis，日中位數、Q90−Q10、日漂移與覆蓋率。
- `anomaly_convergence.csv`：10/15/20/25 bps、正負方向、mid／executable basis 的 episode 與 aggregate rows。持續異常只算一筆；gap/end-of-data 是 censored/unknown。
- `opportunity_funnel.csv`：configured → synchronized → coverage → stable baseline → anomaly → confirmed → observed convergence → executable convergence → economic candidate。
- `candidate_ranking.csv`：A/B/C/D evidence classification，排序先看資料品質、基準穩定、重複事件與 executable convergence，不能只按 PnL。

## 研究語意

- 所有 baseline 只使用訊號日之前已完成的 UTC 日；當日資料不會進入自己的 reference。
- `mid` basis 只代表理論價格變化；`exec_buy_a_sell_b` 與 `exec_buy_b_sell_a` 才是 size-specific VWAP 的 directional executable proxy。
- cost sensitivity 使用 configured fee assumption、四腿額外滑價及 $1,000／$5,000 每腿名目；Funding、真實延遲、queue、拒單與資金容量不會被當成零成本或已驗證成交。
- `data_gap`、dataset end、baseline 不可用等事件不會被算成 convergence failure 或 success；報告必須使用 known denominator。
- V2 不放寬 V1 的 canonical 3 秒同步門檻；5/10 秒只用來量化「若放寬會付出的資料品質代價」。
