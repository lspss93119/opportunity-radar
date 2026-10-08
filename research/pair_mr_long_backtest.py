#!/usr/bin/env python3
import json
import math
import os
from pathlib import Path

import numpy as np
import pandas as pd
import requests

OUT = Path("research_outputs/pair_mr_long_backtest")
OUT.mkdir(parents=True, exist_ok=True)

URLS = {
    "btc": "https://huggingface.co/datasets/humblefool06/btc-usdt-1m-candles/resolve/refs%2Fconvert%2Fparquet/default/train/0000.parquet",
    "eth": "https://huggingface.co/datasets/humblefool06/eth-usdt-1m-candles/resolve/refs%2Fconvert%2Fparquet/default/train/0000.parquet",
}

START = pd.Timestamp("2020-01-01", tz="UTC")
END = pd.Timestamp("2026-08-13 23:59:00", tz="UTC")
RESET = 0.0015
ENTRY_LO = 0.0030
ENTRY_HI = 0.0060
LEG_NOTIONAL = 1000.0
HORIZONS = [4, 6, 8]


def download(url: str, path: Path):
    if path.exists() and path.stat().st_size > 1_000_000:
        return
    with requests.get(url, stream=True, timeout=60) as r:
        r.raise_for_status()
        with open(path, "wb") as f:
            for chunk in r.iter_content(chunk_size=8 * 1024 * 1024):
                if chunk:
                    f.write(chunk)


def load_symbol(name: str) -> pd.Series:
    path = OUT / f"{name}.parquet"
    download(URLS[name], path)
    df = pd.read_parquet(path, columns=["timestamp", "close"])
    ts = pd.to_datetime(df["timestamp"], unit="ms", utc=True)
    s = pd.Series(df["close"].astype(float).to_numpy(), index=ts, name=name)
    s = s[(s.index >= START) & (s.index <= END)]
    s = s[~s.index.duplicated(keep="last")].sort_index()
    return s


def build_frame() -> pd.DataFrame:
    btc = load_symbol("btc")
    eth = load_symbol("eth")
    idx = pd.date_range(max(START, btc.index.min(), eth.index.min()), min(END, btc.index.max(), eth.index.max()), freq="1min", tz="UTC")
    df = pd.DataFrame(index=idx)
    df["btc"] = btc.reindex(idx).ffill(limit=5)
    df["eth"] = eth.reindex(idx).ffill(limit=5)
    df["btc_4h"] = df["btc"] / df["btc"].shift(240) - 1.0
    df["eth_4h"] = df["eth"] / df["eth"].shift(240) - 1.0
    df["d4h"] = df["eth_4h"] - df["btc_4h"]
    # Descriptive regime variables only; not used for entry/exit decisions.
    df["btc_24h_ret"] = df["btc"] / df["btc"].shift(1440) - 1.0
    logret = np.log(df["btc"]).diff()
    df["btc_24h_rv"] = logret.rolling(1440, min_periods=1200).std() * math.sqrt(1440)
    return df


def episode_entries(df: pd.DataFrame) -> pd.DataFrame:
    rows = []
    armed = False
    d = df["d4h"].to_numpy()
    idx = df.index
    btc = df["btc"].to_numpy()
    eth = df["eth"].to_numpy()
    for i, x in enumerate(d):
        if not np.isfinite(x) or not np.isfinite(btc[i]) or not np.isfinite(eth[i]):
            continue
        ax = abs(x)
        if ax <= RESET:
            armed = True
            continue
        if armed and ENTRY_LO <= ax <= ENTRY_HI:
            rows.append((i, idx[i], x, btc[i], eth[i], df["btc_24h_ret"].iat[i], df["btc_24h_rv"].iat[i]))
            armed = False
    return pd.DataFrame(rows, columns=["i","entry_time","d4h","btc_entry","eth_entry","btc_24h_ret","btc_24h_rv"])


def attach_horizon_outcomes(df: pd.DataFrame, ep: pd.DataFrame) -> pd.DataFrame:
    out = ep.copy()
    n = len(df)
    btc = df["btc"].to_numpy()
    eth = df["eth"].to_numpy()
    for h in HORIZONS:
        step = h * 60
        vals = []
        gross_bps = []
        dollars = []
        for r in out.itertuples():
            j = r.i + step
            if j >= n or not np.isfinite(btc[j]) or not np.isfinite(eth[j]):
                vals.append(np.nan); gross_bps.append(np.nan); dollars.append(np.nan); continue
            rb = btc[j] / r.btc_entry - 1.0
            re = eth[j] / r.eth_entry - 1.0
            pnl_per_leg = -np.sign(r.d4h) * (re - rb)
            vals.append(pnl_per_leg)
            gross_bps.append((pnl_per_leg / 2.0) * 10000.0)
            dollars.append(LEG_NOTIONAL * pnl_per_leg)
        out[f"ret_{h}h_per_leg"] = vals
        out[f"gross_bps_{h}h"] = gross_bps
        out[f"pnl_usd_{h}h"] = dollars
    return out


def summarize_matched(out: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for h in HORIZONS:
        x = out[f"pnl_usd_{h}h"].dropna()
        bps = out.loc[x.index, f"gross_bps_{h}h"]
        rows.append({
            "horizon_h": h,
            "episodes": int(len(x)),
            "win_rate": float((x > 0).mean()),
            "avg_pnl_usd": float(x.mean()),
            "median_pnl_usd": float(x.median()),
            "total_pnl_usd": float(x.sum()),
            "avg_gross_bps": float(bps.mean()),
            "median_gross_bps": float(bps.median()),
            "p10_usd": float(x.quantile(0.10)),
            "p90_usd": float(x.quantile(0.90)),
        })
    return pd.DataFrame(rows)


def deployable_subset(out: pd.DataFrame, h: int) -> pd.DataFrame:
    # Same canonical episode list; enforce one active pair by greedily skipping
    # entries that happen before the current fixed-horizon position exits.
    chosen = []
    next_free = pd.Timestamp.min.tz_localize("UTC")
    for r in out.itertuples():
        if r.entry_time < next_free:
            continue
        pnl = getattr(r, f"pnl_usd_{h}h")
        if not np.isfinite(pnl):
            continue
        chosen.append(r.Index)
        next_free = r.entry_time + pd.Timedelta(hours=h)
    return out.loc[chosen].copy()


def max_drawdown_from_pnl(pnl: pd.Series) -> float:
    eq = pnl.cumsum()
    peak = eq.cummax()
    dd = eq - peak
    return float(dd.min()) if len(dd) else 0.0


def summarize_deployable(out: pd.DataFrame) -> pd.DataFrame:
    rows = []
    for h in HORIZONS:
        s = deployable_subset(out, h)
        pnl = s[f"pnl_usd_{h}h"]
        rows.append({
            "horizon_h": h,
            "trades": int(len(s)),
            "win_rate": float((pnl > 0).mean()) if len(s) else np.nan,
            "avg_pnl_usd": float(pnl.mean()) if len(s) else np.nan,
            "median_pnl_usd": float(pnl.median()) if len(s) else np.nan,
            "total_pnl_usd": float(pnl.sum()),
            "max_drawdown_usd": max_drawdown_from_pnl(pnl),
            "profit_factor": float(pnl[pnl>0].sum() / -pnl[pnl<0].sum()) if (pnl<0).any() else np.inf,
        })
    return pd.DataFrame(rows)


def yearly_table(out: pd.DataFrame, h: int) -> pd.DataFrame:
    s = deployable_subset(out, h).copy()
    s["year"] = s["entry_time"].dt.year
    g = s.groupby("year")
    rows = []
    for year, x in g:
        pnl = x[f"pnl_usd_{h}h"]
        rows.append({
            "year": int(year),
            "horizon_h": h,
            "trades": int(len(x)),
            "win_rate": float((pnl > 0).mean()),
            "avg_pnl_usd": float(pnl.mean()),
            "total_pnl_usd": float(pnl.sum()),
            "max_drawdown_usd": max_drawdown_from_pnl(pnl),
        })
    return pd.DataFrame(rows)


def monthly_table(out: pd.DataFrame, h: int) -> pd.DataFrame:
    s = deployable_subset(out, h).copy()
    s["month"] = s["entry_time"].dt.to_period("M").astype(str)
    rows = []
    for month, x in s.groupby("month"):
        pnl = x[f"pnl_usd_{h}h"]
        rows.append({
            "month": month,
            "horizon_h": h,
            "trades": int(len(x)),
            "win_rate": float((pnl > 0).mean()),
            "avg_pnl_usd": float(pnl.mean()),
            "total_pnl_usd": float(pnl.sum()),
        })
    return pd.DataFrame(rows)


def regime_table(out: pd.DataFrame, h: int) -> pd.DataFrame:
    s = deployable_subset(out, h).copy()
    s = s.dropna(subset=["btc_24h_ret","btc_24h_rv"])
    if s.empty:
        return pd.DataFrame()
    # Descriptive sample-relative buckets; never used as a trading filter.
    rv_med = s["btc_24h_rv"].median()
    trend_q = s["btc_24h_ret"].abs().quantile(0.67)
    s["vol_regime"] = np.where(s["btc_24h_rv"] >= rv_med, "high_vol", "low_vol")
    s["trend_regime"] = np.where(s["btc_24h_ret"].abs() >= trend_q, "strong_24h_trend", "weak_24h_trend")
    rows = []
    for dims in ["vol_regime","trend_regime"]:
        for regime, x in s.groupby(dims):
            pnl = x[f"pnl_usd_{h}h"]
            rows.append({
                "dimension": dims,
                "regime": regime,
                "horizon_h": h,
                "trades": int(len(x)),
                "win_rate": float((pnl > 0).mean()),
                "avg_pnl_usd": float(pnl.mean()),
                "total_pnl_usd": float(pnl.sum()),
            })
    return pd.DataFrame(rows)


def cost_sensitivity(out: pd.DataFrame) -> pd.DataFrame:
    # cost_bps is round-trip cost on gross capital (2 legs combined).
    rows = []
    for h in HORIZONS:
        s = deployable_subset(out, h)
        gross = s[f"gross_bps_{h}h"]
        for cost_bps in [0, 2, 4, 6, 8, 10]:
            net_bps = gross - cost_bps
            rows.append({
                "horizon_h": h,
                "round_trip_cost_bps_gross": cost_bps,
                "trades": int(len(net_bps)),
                "net_avg_bps": float(net_bps.mean()),
                "net_win_rate": float((net_bps > 0).mean()),
                "net_total_usd": float((net_bps / 10000.0 * (2 * LEG_NOTIONAL)).sum()),
            })
    return pd.DataFrame(rows)


def main():
    df = build_frame()
    coverage = {
        "start": str(df.index.min()),
        "end": str(df.index.max()),
        "minutes": int(len(df)),
        "btc_missing_after_fill": int(df["btc"].isna().sum()),
        "eth_missing_after_fill": int(df["eth"].isna().sum()),
    }
    ep = episode_entries(df)
    out = attach_horizon_outcomes(df, ep)
    matched = summarize_matched(out)
    deploy = summarize_deployable(out)
    yearly = pd.concat([yearly_table(out,h) for h in HORIZONS], ignore_index=True)
    monthly = pd.concat([monthly_table(out,h) for h in HORIZONS], ignore_index=True)
    regime = pd.concat([regime_table(out,h) for h in HORIZONS], ignore_index=True)
    costs = cost_sensitivity(out)

    out.to_csv(OUT/"episodes.csv", index=False)
    matched.to_csv(OUT/"matched_summary.csv", index=False)
    deploy.to_csv(OUT/"deployable_summary.csv", index=False)
    yearly.to_csv(OUT/"yearly_summary.csv", index=False)
    monthly.to_csv(OUT/"monthly_summary.csv", index=False)
    regime.to_csv(OUT/"regime_summary.csv", index=False)
    costs.to_csv(OUT/"cost_sensitivity.csv", index=False)

    summary = {
        "strategy": {
            "signal": "D4h = ETH 4h simple return - BTC 4h simple return",
            "reset_abs_d4h_lte": RESET,
            "entry_abs_d4h_range": [ENTRY_LO, ENTRY_HI],
            "direction": "mean reversion: D>0 long BTC/short ETH; D<0 long ETH/short BTC",
            "sizing": "$1000 per leg equal notional",
            "horizons_h": HORIZONS,
        },
        "coverage": coverage,
        "canonical_episodes": int(len(out)),
        "matched_summary": matched.to_dict(orient="records"),
        "deployable_summary": deploy.to_dict(orient="records"),
        "yearly_summary": yearly.to_dict(orient="records"),
        "regime_summary": regime.to_dict(orient="records"),
        "cost_sensitivity": costs.to_dict(orient="records"),
    }
    with open(OUT/"summary.json","w") as f:
        json.dump(summary, f, indent=2, allow_nan=False)

    print("===PAIR_MR_LONG_BACKTEST_SUMMARY===")
    print(json.dumps(summary, indent=2, allow_nan=False))
    print("===END_SUMMARY===")


if __name__ == "__main__":
    main()
