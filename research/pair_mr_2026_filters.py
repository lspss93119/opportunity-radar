#!/usr/bin/env python3
import io, zipfile, requests, pandas as pd, numpy as np, json, math
from pathlib import Path

OUT=Path("research_outputs/pair_mr_2026_filters"); OUT.mkdir(parents=True,exist_ok=True)
START=pd.Timestamp("2026-01-01 00:00:00",tz="UTC")
END=pd.Timestamp("2026-10-07 23:59:00",tz="UTC")
DISC_END=pd.Timestamp("2026-06-30 23:59:00",tz="UTC")
OOS_START=pd.Timestamp("2026-07-01 00:00:00",tz="UTC")
RESET=.0015; LO=.003; HI=.006; LEG=1000.; HOLD=6

def read_zip_csv(url):
    r=requests.get(url,timeout=120); r.raise_for_status()
    z=zipfile.ZipFile(io.BytesIO(r.content))
    with z.open(z.namelist()[0]) as f:
        return pd.read_csv(f,header=None,usecols=[0,4],names=["open_time","close"])

def load_symbol(sym):
    fs=[]
    for m in range(1,10):
        fs.append(read_zip_csv(f"https://data.binance.vision/data/spot/monthly/klines/{sym}/1m/{sym}-1m-2026-{m:02d}.zip"))
    for d in range(1,8):
        fs.append(read_zip_csv(f"https://data.binance.vision/data/spot/daily/klines/{sym}/1m/{sym}-1m-2026-10-{d:02d}.zip"))
    d=pd.concat(fs,ignore_index=True)
    raw=pd.to_numeric(d.open_time,errors="coerce")
    unit="us" if raw.dropna().median()>1e14 else "ms"
    ts=pd.to_datetime(raw,unit=unit,utc=True,errors="coerce")
    s=pd.Series(pd.to_numeric(d.close,errors="coerce").to_numpy(),index=ts,name=sym)
    s=s[~s.index.isna()]
    s=s[(s.index>=START)&(s.index<=END)]
    return s[~s.index.duplicated(keep="last")].sort_index()

btc=load_symbol("BTCUSDT"); eth=load_symbol("ETHUSDT")
idx=pd.date_range(START,END,freq="1min",tz="UTC")
df=pd.DataFrame({"btc":btc.reindex(idx),"eth":eth.reindex(idx)},index=idx)
for mins,name in [(30,"30m"),(60,"1h"),(120,"2h"),(240,"4h"),(1440,"24h")]:
    df[f"btc_{name}"]=df.btc/df.btc.shift(mins)-1
    df[f"eth_{name}"]=df.eth/df.eth.shift(mins)-1
    df[f"d_{name}"]=df[f"eth_{name}"]-df[f"btc_{name}"]
df["d4h_chg15"]=df["d_4h"]-df["d_4h"].shift(15)
df["d4h_chg30"]=df["d_4h"]-df["d_4h"].shift(30)
df["d4h_chg60"]=df["d_4h"]-df["d_4h"].shift(60)
lr=np.log(df.btc).diff()
df["btc_rv24"]=lr.rolling(1440,min_periods=1200).std()*math.sqrt(1440)

# canonical reset -> one episode
rows=[]; armed=False; reset_time=None
for i,(ts,r) in enumerate(df.iterrows()):
    x=r["d_4h"]
    if not np.isfinite(x) or not np.isfinite(r.btc) or not np.isfinite(r.eth): continue
    ax=abs(x)
    if ax<=RESET:
        armed=True; reset_time=ts; continue
    if armed and LO<=ax<=HI:
        rec={
            "i":i,"entry_time":ts,"d4h":x,"abs_d4h":ax,"btc0":r.btc,"eth0":r.eth,
            "d30m":r["d_30m"],"d1h":r["d_1h"],"d2h":r["d_2h"],
            "d4h_chg15":r.d4h_chg15,"d4h_chg30":r.d4h_chg30,"d4h_chg60":r.d4h_chg60,
            "btc24":r["btc_24h"],"rv24":r.btc_rv24,
            "minutes_since_reset":(ts-reset_time).total_seconds()/60 if reset_time is not None else np.nan,
        }
        rows.append(rec); armed=False
ep=pd.DataFrame(rows)
ep["entry_time"]=pd.to_datetime(ep.entry_time,utc=True)

b=df.btc.to_numpy(); e=df.eth.to_numpy()
def outcome(row,h=6):
    j=int(row.i+h*60)
    if j>=len(df) or not np.isfinite(b[j]) or not np.isfinite(e[j]): return np.nan
    rb=b[j]/row.btc0-1; re=e[j]/row.eth0-1
    return LEG*(-np.sign(row.d4h)*(re-rb))
ep["pnl6"]=[outcome(r,6) for r in ep.itertuples()]
ep["gross_bps6"]=ep.pnl6/2000*10000

# causal / threshold-light features. Negative signed_widen means d4h is moving toward zero.
for k in [15,30,60]:
    ep[f"signed_widen{k}"]=np.sign(ep.d4h)*ep[f"d4h_chg{k}"]
ep["short_align30"]=np.sign(ep.d4h)*ep.d30m
ep["short_align1h"]=np.sign(ep.d4h)*ep.d1h
ep["short_align2h"]=np.sign(ep.d4h)*ep.d2h
ep["side"]=np.where(ep.d4h>0,"ETH_strong","BTC_strong")
ep["bucket"]=pd.cut(ep.abs_d4h,[.003,.004,.005,.006],labels=["30-40","40-50","50-60"],include_lowest=True)

# Binary candidate rules: no optimized numeric cutoffs, only direction/sign or fixed strategy bounds.
rules={
"baseline": lambda x: pd.Series(True,index=x.index),
"turn15": lambda x: x.signed_widen15<=0,
"turn30": lambda x: x.signed_widen30<=0,
"turn60": lambda x: x.signed_widen60<=0,
"short30_reversing": lambda x: x.short_align30<=0,
"short1h_reversing": lambda x: x.short_align1h<=0,
"short2h_reversing": lambda x: x.short_align2h<=0,
"turn30_and_short1h": lambda x: (x.signed_widen30<=0)&(x.short_align1h<=0),
"turn60_and_short1h": lambda x: (x.signed_widen60<=0)&(x.short_align1h<=0),
"btc_strong_only": lambda x: x.side=="BTC_strong",
"eth_strong_only": lambda x: x.side=="ETH_strong",
"slow_episode_60m+": lambda x: x.minutes_since_reset>=60,
"fast_episode_lt60m": lambda x: x.minutes_since_reset<60,
}

def deployable(x):
    chosen=[]; free=pd.Timestamp("1970-01-01",tz="UTC")
    for r in x.sort_values("entry_time").itertuples():
        if r.entry_time<free or not np.isfinite(r.pnl6): continue
        chosen.append(r.Index); free=r.entry_time+pd.Timedelta(hours=HOLD)
    return x.loc[chosen].copy()

def stats(x):
    s=deployable(x); p=s.pnl6.dropna()
    if len(p)==0:return {"trades":0}
    eq=p.cumsum(); dd=(eq-eq.cummax()).min()
    return {"trades":int(len(p)),"win_rate":float((p>0).mean()),"avg_usd":float(p.mean()),
            "median_usd":float(p.median()),"gross_bps":float((p/2000*10000).mean()),
            "total_usd":float(p.sum()),"max_dd_usd":float(dd),
            "net_bps_cost1":float((p/2000*10000-1).mean()),
            "net_bps_cost2":float((p/2000*10000-2).mean())}

disc=ep[ep.entry_time<=DISC_END].copy()
oos=ep[ep.entry_time>=OOS_START].copy()

rows=[]
for name,fn in rules.items():
    for split_name,x in [("discovery_H1",disc),("oos_H2plus",oos),("full_2026",ep)]:
        mask=fn(x).fillna(False)
        st=stats(x[mask]); st.update({"rule":name,"split":split_name,"episodes_selected":int(mask.sum()),"selection_rate":float(mask.mean())})
        rows.append(st)
res=pd.DataFrame(rows)

# Monthly OOS for top logical candidates + baseline.
monthly=[]
for name in ["baseline","turn30","turn60","short1h_reversing","turn30_and_short1h","turn60_and_short1h"]:
    x=oos[rules[name](oos).fillna(False)].copy()
    x["month"]=x.entry_time.dt.to_period("M").astype(str)
    for m,g in x.groupby("month"):
        st=stats(g); st.update({"rule":name,"month":m})
        monthly.append(st)
monthly=pd.DataFrame(monthly)

# Decile-style descriptive check of widening, derived on H1 cutpoints then applied OOS.
quantile_rows=[]
for feat in ["signed_widen30","signed_widen60","short_align1h","minutes_since_reset"]:
    train=disc[[feat]].dropna()
    if len(train)<100: continue
    qs=train[feat].quantile([0,.2,.4,.6,.8,1]).to_numpy()
    qs=np.unique(qs)
    if len(qs)<3: continue
    for split_name,x in [("discovery_H1",disc),("oos_H2plus",oos)]:
        bucket=pd.cut(x[feat],bins=qs,include_lowest=True,duplicates="drop")
        for k,g in x.groupby(bucket,observed=True):
            st=stats(g); st.update({"feature":feat,"bucket":str(k),"split":split_name})
            quantile_rows.append(st)
quant=pd.DataFrame(quantile_rows)

ep.to_csv(OUT/"episodes_features.csv",index=False)
res.to_csv(OUT/"rule_results.csv",index=False)
monthly.to_csv(OUT/"oos_monthly.csv",index=False)
quant.to_csv(OUT/"feature_buckets.csv",index=False)
summary={
"coverage":{"start":str(START),"end":str(END),"episodes":int(len(ep)),"discovery_episodes":int(len(disc)),"oos_episodes":int(len(oos))},
"rule_results":res.to_dict("records"),
"oos_monthly":monthly.to_dict("records"),
"feature_buckets":quant.to_dict("records")}
with open(OUT/"summary.json","w") as f:json.dump(summary,f,indent=2,allow_nan=False)
print("===PAIR_MR_2026_FILTERS==="); print(json.dumps(summary,indent=2,allow_nan=False)); print("===END_FILTERS===")
