#!/usr/bin/env python3
import io, zipfile, requests, pandas as pd, numpy as np, math, json
from pathlib import Path

OUT=Path("research_outputs/pair_mr_2026"); OUT.mkdir(parents=True,exist_ok=True)
START=pd.Timestamp("2026-01-01 00:00:00",tz="UTC")
END=pd.Timestamp("2026-10-07 23:59:00",tz="UTC")
RESET=.0015; LO=.003; HI=.006; LEG=1000.0
HORIZONS=[4,6,8]

def read_zip_csv(url):
    r=requests.get(url,timeout=120); r.raise_for_status()
    z=zipfile.ZipFile(io.BytesIO(r.content))
    name=z.namelist()[0]
    with z.open(name) as f:
        df=pd.read_csv(f,header=None,usecols=[0,4],names=["open_time","close"])
    return df

def load_symbol(sym):
    frames=[]
    for m in range(1,10):
        url=f"https://data.binance.vision/data/spot/monthly/klines/{sym}/1m/{sym}-1m-2026-{m:02d}.zip"
        frames.append(read_zip_csv(url))
    for d in range(1,8):
        url=f"https://data.binance.vision/data/spot/daily/klines/{sym}/1m/{sym}-1m-2026-10-{d:02d}.zip"
        frames.append(read_zip_csv(url))
    df=pd.concat(frames,ignore_index=True)
    ts=pd.to_datetime(df.open_time,unit="ms",utc=True)
    s=pd.Series(df.close.astype(float).to_numpy(),index=ts,name=sym)
    s=s[(s.index>=START)&(s.index<=END)]
    return s[~s.index.duplicated(keep="last")].sort_index()

btc=load_symbol("BTCUSDT"); eth=load_symbol("ETHUSDT")
idx=pd.date_range(START,END,freq="1min",tz="UTC")
df=pd.DataFrame(index=idx)
df["btc"]=btc.reindex(idx)
df["eth"]=eth.reindex(idx)
# no synthetic fill beyond exact timestamps for primary signal
df["btc4"]=df.btc/df.btc.shift(240)-1
df["eth4"]=df.eth/df.eth.shift(240)-1
df["d4h"]=df.eth4-df.btc4
df["btc24"]=df.btc/df.btc.shift(1440)-1
lr=np.log(df.btc).diff()
df["rv24"]=lr.rolling(1440,min_periods=1200).std()*math.sqrt(1440)

rows=[]; armed=False
for i,(ts,r) in enumerate(df.iterrows()):
    x=r.d4h
    if not np.isfinite(x) or not np.isfinite(r.btc) or not np.isfinite(r.eth): continue
    ax=abs(x)
    if ax<=RESET:
        armed=True; continue
    if armed and LO<=ax<=HI:
        rows.append((i,ts,x,r.btc,r.eth,r.btc24,r.rv24))
        armed=False
ep=pd.DataFrame(rows,columns=["i","entry_time","d4h","btc0","eth0","btc24","rv24"])

b=df.btc.to_numpy(); e=df.eth.to_numpy()
def pnl(row,h):
    j=row.i+h*60
    if j>=len(df) or not np.isfinite(b[j]) or not np.isfinite(e[j]): return np.nan
    rb=b[j]/row.btc0-1; re=e[j]/row.eth0-1
    return LEG*(-np.sign(row.d4h)*(re-rb))

for h in HORIZONS:
    ep[f"pnl_{h}h"]=[pnl(r,h) for r in ep.itertuples()]
    ep[f"gross_bps_{h}h"]=ep[f"pnl_{h}h"]/2000*10000

ep["bucket"]=pd.cut(ep.d4h.abs(),bins=[.003,.004,.005,.006],labels=["30-40","40-50","50-60"],include_lowest=True,right=True)
ep["side"]=np.where(ep.d4h>0,"ETH_strong","BTC_strong")
ep["month"]=ep.entry_time.dt.to_period("M").astype(str)

def deployable(h):
    chosen=[]; free=pd.Timestamp("1970-01-01",tz="UTC")
    for r in ep.itertuples():
        if r.entry_time<free: continue
        p=getattr(r,f"pnl_{h}h")
        if not np.isfinite(p): continue
        chosen.append(r.Index); free=r.entry_time+pd.Timedelta(hours=h)
    return ep.loc[chosen].copy()

overall=[]; monthly=[]; buckets=[]; sides=[]; cross=[]; costs=[]
for h in HORIZONS:
    s=deployable(h); p=s[f"pnl_{h}h"]
    eq=p.cumsum(); dd=(eq-eq.cummax()).min()
    overall.append({"horizon_h":h,"trades":len(s),"win_rate":float((p>0).mean()),"avg_pnl_usd":float(p.mean()),"median_pnl_usd":float(p.median()),"total_pnl_usd":float(p.sum()),"avg_gross_bps":float(s[f"gross_bps_{h}h"].mean()),"max_drawdown_usd":float(dd)})
    for m,x in s.groupby("month"):
        q=x[f"pnl_{h}h"]
        monthly.append({"month":m,"horizon_h":h,"trades":len(x),"win_rate":float((q>0).mean()),"avg_pnl_usd":float(q.mean()),"total_pnl_usd":float(q.sum())})
    for k,x in s.groupby("bucket",observed=True):
        q=x[f"pnl_{h}h"]
        buckets.append({"bucket":str(k),"horizon_h":h,"trades":len(x),"win_rate":float((q>0).mean()),"avg_pnl_usd":float(q.mean()),"avg_gross_bps":float(x[f"gross_bps_{h}h"].mean())})
    for k,x in s.groupby("side"):
        q=x[f"pnl_{h}h"]
        sides.append({"side":k,"horizon_h":h,"trades":len(x),"win_rate":float((q>0).mean()),"avg_pnl_usd":float(q.mean()),"avg_gross_bps":float(x[f"gross_bps_{h}h"].mean())})
    for (side,bucket),x in s.groupby(["side","bucket"],observed=True):
        q=x[f"pnl_{h}h"]
        cross.append({"side":side,"bucket":str(bucket),"horizon_h":h,"trades":len(x),"win_rate":float((q>0).mean()),"avg_pnl_usd":float(q.mean()),"avg_gross_bps":float(x[f"gross_bps_{h}h"].mean())})
    for c in [0,1,2,4,6]:
        nb=s[f"gross_bps_{h}h"]-c
        costs.append({"horizon_h":h,"cost_bps":c,"net_avg_bps":float(nb.mean()),"net_win_rate":float((nb>0).mean()),"net_total_usd":float((nb/10000*2000).sum())})

for name,arr in [("overall",overall),("monthly",monthly),("buckets",buckets),("sides",sides),("cross",cross),("costs",costs)]:
    pd.DataFrame(arr).to_csv(OUT/f"{name}.csv",index=False)
ep.to_csv(OUT/"episodes.csv",index=False)
summary={"coverage":{"start":str(START),"end":str(END),"btc_rows":int(btc.size),"eth_rows":int(eth.size),"canonical_episodes":int(len(ep))},
         "overall":overall,"monthly":monthly,"buckets":buckets,"sides":sides,"cross":cross,"costs":costs}
with open(OUT/"summary.json","w") as f:json.dump(summary,f,indent=2,allow_nan=False)
print("===PAIR_MR_2026_SUMMARY==="); print(json.dumps(summary,indent=2,allow_nan=False)); print("===END_2026===")
