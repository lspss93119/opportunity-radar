# 錨點比較：24h 平均 vs 2h 中位數（2026-11-24）

一次性的離線回放，跑完就結束。它只讀取正式環境 market Parquet 的唯讀副本，沒有下單，也沒有寫入任何 Radar 狀態。

- **指令**：`python -m radar.history.anchor_replay`（`src/radar/history/anchor_replay.py`，commit `83d10a9`）
- **每對明細**：`docs/anchor-comparison-2026-11-24.csv`，共 2976 列，即 496 組有方向的配對 × 2 種錨點 × 3 個門檻。

## 結論與建議

1. **預設錨點：維持 `mean_24h`**。
   - 在 25 bps 和 40 bps，兩種錨點的觸發次數幾乎一樣（940 vs 953、292 vs 283），但 24h 平均的每筆平均可得 bps 略高（+13.6 vs +11.9、+30.6 vs +29.7）。
   - 2h 中位數回歸得比較快（p90 23 分鐘 vs 33 分鐘），但沒有換到更好的損益。在 40 bps，沒回歸那筆的不利偏離更大（45.2 vs 17.2 bps，各只有 1 筆，樣本太少）。
   - 所以 `median_2h` 繼續當作選用的第二錨點，不改預設值。
2. **預設門檻：建議從 15 bps 提高到 25 bps**，這是之後要開啟 anomaly v2 推播時的 `deviation_bps`。
   - 15 bps 扣掉 4 筆吃單手續費之後，平均是 **−1.0 bps**，而且每天約 70–85 次，等於大部分是雜訊。
   - 25 bps 每天約 15 次，平均 +12～+14 bps，回歸比例超過 99%。
   - 40 bps 每天約 5 次，平均約 +30 bps，適合做「高信心」等級的門檻。
3. **排除 entropy ↔ trade_xyz**：來回手續費 36 bps，在每個門檻、兩種錨點下平均都是負值（−13 到 −28 bps）。
4. **正式環境不需要改設定**。這次的建議只用在之後開啟 `anomaly_v2.telegram_enabled` 的時候，到時候再請你確認。

## 重要限制（解讀前務必先看）

- **Hyperliquid 和 trade_xyz 的配對沒有結果**：198 組配對沒有觸發任何一次（另外 2 組是 entropy ↔ trade_xyz）。
  - 原因：HL 和 xyz 的 `observed_at` 平均比採樣時間落後 3.5 秒（p90 是 7 秒）。套用「兩邊時間差 ≤ 3 秒」之後，這些配對只剩約 45% 的樣本，達不到 80% 覆蓋率，錨點永遠不成立。
  - **正式環境也一樣**：live 的 `build_spread_candidates` 會先套用「報價 ≤ 3 秒、時間差 ≤ 3 秒」，**才**把樣本餵給 basis，所以 HL/xyz 配對的 24h basis 很少達到 6912 筆的門檻。
    - 正式 SQLite 最近 30 天的 spread 事件**沒有任何一筆**涉及 hyperliquid 或 trade_xyz，全部是 lighter、lighter_robinhood、arcus、backpack 之間的配對；試跑環境也是這樣。
    - 也就是說，**正式 Radar 目前實際上沒在監控 HL 和 xyz 的價差異常**。
    - 修正方式（例如 basis 改用「品質合格」的樣本而不是「live」樣本，或者 HL 的 observed_at 改用最後一次訊息的時間）需要另外規劃，這次沒有改。
  - 因此**本報告的結論只適用於 lighter、lighter_robinhood、arcus、backpack 之間的配對**。依照「只跑一次」的要求，回放沒有重跑。
- **沒有模擬確認延遲**：回放在偏離達到門檻的當下、用當下的 VWAP 進場。但是：
  - 正式環境的 anomaly v2 要求 `confirmation_seconds: 60`；
  - 回歸時間的 p50 只有 20–70 秒，表示大約一半的觸發在告警發出之前就回歸了，而且人工操作也來不及。
  - 實際上可操作的次數和損益會比表中**低**。上面的建議是兩種錨點之間的「相對」比較，不是絕對的可得收益。
- 其他：
  - 只有 $1k 吃單 VWAP，沒有完整深度；
  - 沒有計入資金費和滑價變化；
  - 採樣間隔 10 秒，期間內的變化看不到；
  - arcus 和 backpack 的資料從 2026-10-08 才開始；
  - 資料只有 64 天（2026-09-22 到 11-24），不是 90 天；
  - 每天的次數一律除以 62 天計算。

## 方法

- **價差**：多 A、空 B 的原始價差 = B 的 $1k 賣出 VWAP ÷ A 的 $1k 買入 VWAP − 1。樣本過濾條件：
  - 過期超過 30 秒的排除；
  - 兩邊時間差 ≤ 3 秒；
  - 與上一筆的斷檔 ≤ 20 秒才可以進場。
- **錨點**：用 DuckDB window 函式計算，**只用過去的資料**（不含當下這一筆）。
  - `mean_24h` 是 24 小時平均，`median_2h` 是 2 小時中位數；
  - 兩者都要求樣本數達到視窗預期的 80% 以上。
- **進場**：價差 − 錨點 ≥ 門檻。同一組配對同時只開一筆；平倉後，偏離要先回到門檻以下，才能再進場。
- **平倉**：
  - 價差回到「進場時鎖定的錨點 + 5 bps」以內，算回歸；
  - 持有超過 24 小時，算沒回歸；
  - 資料結束時還在 24 小時內的倉位，記為 censored，不列入統計。
- **損益**：兩邊相同數量，(A 平倉賣出 ÷ A 進場買入 − 1) + (1 − B 平倉買回 ÷ B 進場賣出)，再扣 2 ×（A 費率 + B 費率），費率取自正式設定的 `fees_bps`。
- **不利偏離**：持倉期間價差比進場時再擴大的最大值。

## 總表：錨點 × 門檻

（所有配對合併計算）

| 錨點 | 門檻 | 觸發次數 | 每天 | 回歸 | 回歸比例 | 平均 bps | 中位數 bps | 回歸 p50 | 回歸 p90 | 回歸 max | 沒回歸最大不利 bps |
|---|---|---|---|---|---|---|---|---|---|---|---|
| mean_24h | 15 | 5228 | 84.3 | 5219 | 99.8% | -1.0 | -1.8 | 30s | 19m | 20.0h | 45.3 |
| mean_24h | 25 | 940 | 15.2 | 937 | 99.7% | 13.6 | 11.7 | 50s | 33m | 19.0h | 28.8 |
| mean_24h | 40 | 292 | 4.7 | 291 | 99.7% | 30.6 | 28.3 | 70s | 36m | 16.5h | 17.2 |
| median_2h | 15 | 4225 | 68.1 | 4206 | 99.6% | -1.0 | 0.0 | 20s | 8m | 19.2h | 46.6 |
| median_2h | 25 | 953 | 15.4 | 946 | 99.3% | 11.9 | 11.4 | 45s | 23m | 15.6h | 45.2 |
| median_2h | 40 | 283 | 4.6 | 282 | 99.6% | 29.7 | 29.0 | 60s | 26m | 15.5h | 45.2 |

## 依交易所方向

依「做多 → 做空」合併計算，只列出至少觸發過一次的組合。

| 做多 → 做空 | 門檻 | 來回手續費 | mean_24h 次數 | mean_24h 平均 bps | median_2h 次數 | median_2h 平均 bps |
|---|---|---|---|---|---|---|
| arcus → backpack | 15 | 12.5 | 535 | -1.7 | 473 | -0.7 |
| arcus → lighter | 15 | 4.5 | 497 | 3.2 | 356 | 6.9 |
| arcus → lighter_robinhood | 15 | 4.5 | 611 | -1.5 | 408 | -2.3 |
| backpack → arcus | 15 | 12.5 | 698 | -4.1 | 487 | -2.4 |
| backpack → lighter | 15 | 8.0 | 400 | 1.3 | 338 | 1.0 |
| backpack → lighter_robinhood | 15 | 8.0 | 338 | -0.9 | 267 | 0.4 |
| entropy → trade_xyz | 15 | 36.0 | 63 | -21.9 | 79 | -28.3 |
| lighter → arcus | 15 | 4.5 | 452 | 1.9 | 353 | 3.9 |
| lighter → backpack | 15 | 8.0 | 435 | 1.0 | 383 | 1.2 |
| lighter → lighter_robinhood | 15 | 0.0 | 184 | 2.7 | 196 | -0.2 |
| lighter_robinhood → arcus | 15 | 4.5 | 525 | -4.1 | 359 | -2.9 |
| lighter_robinhood → backpack | 15 | 8.0 | 356 | -1.9 | 308 | -1.4 |
| lighter_robinhood → lighter | 15 | 0.0 | 102 | 6.2 | 153 | -5.9 |
| trade_xyz → entropy | 15 | 36.0 | 32 | -19.1 | 65 | -28.3 |
| arcus → backpack | 25 | 12.5 | 111 | 10.7 | 109 | 11.2 |
| arcus → lighter | 25 | 4.5 | 80 | 22.5 | 86 | 22.2 |
| arcus → lighter_robinhood | 25 | 4.5 | 71 | 17.3 | 79 | 13.5 |
| backpack → arcus | 25 | 12.5 | 113 | 9.7 | 112 | 9.2 |
| backpack → lighter | 25 | 8.0 | 89 | 14.8 | 89 | 13.8 |
| backpack → lighter_robinhood | 25 | 8.0 | 85 | 11.2 | 73 | 11.0 |
| entropy → trade_xyz | 25 | 36.0 | 19 | -12.9 | 26 | -18.8 |
| lighter → arcus | 25 | 4.5 | 68 | 20.2 | 64 | 21.6 |
| lighter → backpack | 25 | 8.0 | 92 | 12.3 | 90 | 12.1 |
| lighter → lighter_robinhood | 25 | 0.0 | 28 | 31.7 | 27 | 26.3 |
| lighter_robinhood → arcus | 25 | 4.5 | 98 | 10.0 | 88 | 8.7 |
| lighter_robinhood → backpack | 25 | 8.0 | 61 | 8.5 | 68 | 7.6 |
| lighter_robinhood → lighter | 25 | 0.0 | 16 | 34.1 | 21 | 21.6 |
| trade_xyz → entropy | 25 | 36.0 | 9 | -2.1 | 21 | -16.8 |
| arcus → backpack | 40 | 12.5 | 35 | 26.3 | 34 | 29.1 |
| arcus → lighter | 40 | 4.5 | 23 | 49.5 | 24 | 47.8 |
| arcus → lighter_robinhood | 40 | 4.5 | 24 | 30.7 | 25 | 31.2 |
| backpack → arcus | 40 | 12.5 | 35 | 24.3 | 34 | 23.2 |
| backpack → lighter | 40 | 8.0 | 26 | 30.1 | 23 | 30.7 |
| backpack → lighter_robinhood | 40 | 8.0 | 24 | 23.5 | 16 | 26.3 |
| entropy → trade_xyz | 40 | 36.0 | 4 | -7.4 | 6 | -14.4 |
| lighter → arcus | 40 | 4.5 | 29 | 37.3 | 28 | 36.6 |
| lighter → backpack | 40 | 8.0 | 22 | 25.3 | 24 | 24.9 |
| lighter → lighter_robinhood | 40 | 0.0 | 11 | 47.6 | 7 | 50.8 |
| lighter_robinhood → arcus | 40 | 4.5 | 36 | 28.7 | 33 | 28.2 |
| lighter_robinhood → backpack | 40 | 8.0 | 17 | 26.4 | 17 | 27.1 |
| lighter_robinhood → lighter | 40 | 0.0 | 4 | 78.9 | 4 | 78.3 |
| trade_xyz → entropy | 40 | 36.0 | 2 | 1.1 | 8 | -2.3 |

## 每對明細（門檻 25 和 40、每個錨點觸發最多的前 10 組）

完整明細見 CSV，裡面有每一組配對 × 錨點 × 門檻的觸發次數、回歸比例、平均和中位數 bps、回歸時間 p50/p90/max，以及沒回歸的最大不利偏離。

| 門檻 | 錨點 | 標的 | 做多 → 做空 | 次數 | 回歸 | 平均 bps | 中位數 bps | 回歸 p50 | 回歸 p90 | 沒回歸最大不利 |
|---|---|---|---|---|---|---|---|---|---|---|
| 25 | mean_24h | SNDK | backpack → arcus | 31 | 31 | 11.7 | 7.0 | 30s | 6m | — |
| 25 | mean_24h | MU | backpack → arcus | 29 | 28 | 6.9 | 6.3 | 50s | 252m | 14.2 |
| 25 | mean_24h | MU | backpack → lighter | 26 | 26 | 8.8 | 8.7 | 475s | 314m | — |
| 25 | mean_24h | MU | backpack → lighter_robinhood | 25 | 25 | 7.3 | 5.8 | 390s | 327m | — |
| 25 | mean_24h | USO | lighter_robinhood → arcus | 24 | 24 | -10.6 | -8.5 | 515s | 89m | — |
| 25 | mean_24h | CRCL | lighter → backpack | 21 | 21 | 14.8 | 11.2 | 60s | 12m | — |
| 25 | mean_24h | MU | arcus → backpack | 19 | 18 | 6.1 | 6.6 | 65s | 90m | 28.8 |
| 25 | mean_24h | SNDK | entropy → trade_xyz | 19 | 19 | -12.9 | -10.6 | 30s | 10m | — |
| 25 | mean_24h | MU | lighter_robinhood → backpack | 18 | 18 | 7.9 | 6.5 | 255s | 98m | — |
| 25 | mean_24h | SNDK | arcus → backpack | 18 | 18 | 10.4 | 7.9 | 45s | 16m | — |
| 25 | median_2h | SNDK | entropy → trade_xyz | 26 | 26 | -18.8 | -17.4 | 60s | 9m | — |
| 25 | median_2h | SNDK | backpack → arcus | 26 | 26 | 13.4 | 8.8 | 20s | 4m | — |
| 25 | median_2h | MU | backpack → arcus | 24 | 24 | 8.2 | 8.1 | 40s | 40m | — |
| 25 | median_2h | CRCL | lighter → backpack | 24 | 24 | 13.8 | 11.9 | 50s | 10m | — |
| 25 | median_2h | MU | arcus → backpack | 22 | 22 | 8.2 | 7.7 | 70s | 51m | — |
| 25 | median_2h | MU | lighter → backpack | 22 | 22 | 12.1 | 10.2 | 120s | 16m | — |
| 25 | median_2h | MU | lighter_robinhood → backpack | 22 | 22 | 9.4 | 8.9 | 90s | 13m | — |
| 25 | median_2h | SNDK | trade_xyz → entropy | 21 | 21 | -16.8 | -19.7 | 80s | 33m | — |
| 25 | median_2h | MU | backpack → lighter | 20 | 20 | 9.9 | 10.1 | 150s | 53m | — |
| 25 | median_2h | CRCL | lighter_robinhood → backpack | 19 | 19 | 10.5 | 6.1 | 40s | 6m | — |
| 40 | mean_24h | SNDK | backpack → arcus | 10 | 10 | 32.8 | 27.0 | 35s | 4m | — |
| 40 | mean_24h | MU | backpack → arcus | 9 | 9 | 21.9 | 22.8 | 870s | 230m | — |
| 40 | mean_24h | MU | arcus → backpack | 7 | 6 | 17.8 | 20.3 | 1335s | 276m | 17.2 |
| 40 | mean_24h | SNDK | lighter_robinhood → arcus | 7 | 7 | 35.7 | 30.7 | 90s | 7m | — |
| 40 | mean_24h | SNDK | lighter → arcus | 7 | 7 | 47.9 | 38.6 | 90s | 5m | — |
| 40 | mean_24h | NVDA | lighter_robinhood → arcus | 7 | 7 | 40.8 | 35.6 | 130s | 23m | — |
| 40 | mean_24h | SLV | lighter_robinhood → arcus | 6 | 6 | 12.6 | 16.1 | 750s | 34m | — |
| 40 | mean_24h | SNDK | arcus → backpack | 6 | 6 | 25.0 | 20.9 | 405s | 27m | — |
| 40 | mean_24h | MU | lighter → backpack | 6 | 6 | 28.2 | 27.9 | 2125s | 107m | — |
| 40 | mean_24h | NVDA | lighter → arcus | 6 | 6 | 41.5 | 44.3 | 110s | 39m | — |
| 40 | median_2h | SNDK | backpack → arcus | 9 | 9 | 31.6 | 20.4 | 60s | 4m | — |
| 40 | median_2h | SNDK | trade_xyz → entropy | 8 | 8 | -2.3 | -0.5 | 310s | 35m | — |
| 40 | median_2h | CRCL | lighter → backpack | 7 | 7 | 27.7 | 26.3 | 20s | 2m | — |
| 40 | median_2h | SNDK | lighter → arcus | 7 | 7 | 48.1 | 38.6 | 90s | 4m | — |
| 40 | median_2h | MU | lighter → backpack | 7 | 7 | 27.8 | 30.4 | 80s | 23m | — |
| 40 | median_2h | SNDK | lighter_robinhood → arcus | 7 | 7 | 39.4 | 33.7 | 90s | 7m | — |
| 40 | median_2h | NVDA | lighter_robinhood → arcus | 6 | 6 | 41.1 | 39.8 | 95s | 36m | — |
| 40 | median_2h | SNDK | entropy → trade_xyz | 6 | 6 | -14.4 | -14.9 | 175s | 22m | — |
| 40 | median_2h | NVDA | backpack → arcus | 6 | 6 | 25.2 | 25.9 | 125s | 44m | — |
| 40 | median_2h | MU | backpack → arcus | 6 | 6 | 23.0 | 23.5 | 475s | 33m | — |
