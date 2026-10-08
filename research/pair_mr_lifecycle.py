#!/usr/bin/env python3
import json, math
from pathlib import Path
import numpy as np
import pandas as pd
import requests

OUT=Path("research_outputs/pair_mr_lifecycle"); OUT.mkdir(parents=True,exist_ok=True)
URLS={
"btc":"https://huggingface.co/datasets/humblefool06/btc-usdt-1m-candles/resolve/refs%2Fconvert%2Fparquet/default/train/0000.parquet",
"eth":"https://huggingface.co/datasets/humblefool06/eth-usdt-1m-candles/resolve/refs%2Fconvert%2Fparquet/default/train/0000.parquet"}
START=pd.Timestamp("2020-01-01",tz="UTC"); END=pd.Timestamp("2026-08-13 23:59",tz="UTC")
RESET=.0015; LO=.003; HI=.006; LEG=1000.0

def dl(url,p):
    if p.exists() and p.stat().st_size>1_000_000:return
    with requests.get(url,stream=True,timeout=60) as r:
        r.raise_for_status()
        with open(p,"wb") as f:
            for c in r.iter_content(8*1024*1024):
                if c:f.write(c)

def load(k):
    p=OUT/f"{k}.parquet"; dl(URLS[k],p)
    d=pd.read_parquet(p,columns=["timestamp","close"])
    s=pd.Series(d.close.astype(float).to_numpy(),index=pd.to_datetime(d.timestamp,unit="ms",utc=True))
    return s[(s.index>=START)&(s.index<=END)].sort_index()[lambda x:~x.index.duplicated(keep="last")]

btc=load("btc"); eth=load("eth")
idx=pd.date_range(max(START,btc.index.min(),eth.index.min()),min(END,btc.index.max(),eth.index.max()),freq="1min",tz="UTC")
b=btc.reindex(idx).ffill(limit=5).to_numpy(); e=eth.reindex(idx).ffill(limit=5).to_numpy()
d=np.full(len(idx),np.nan)
d[240:]=(e[240:]/e[:-240]-1)-(b[240:]/b[:-240]-1)

# canonical reset->entry episodes
rows=[]; armed=False
for i,x in enumerate(d):
    if not np.isfinite(x) or not np.isfinite(b[i]) or not np.isfinite(e[i]): continue
    ax=abs(x)
    if ax<=RESET: armed=True; continue
    if armed and LO<=ax<=HI:
        rows.append((i,idx[i],x,b[i],e[i])); armed=False
ep=pd.DataFrame(rows,columns=["i","entry_time","d4h","btc0","eth0"])

def pnl_at(r,j):
    rb=b[j]/r.btc0-1; re=e[j]/r.eth0-1
    return LEG*(-np.sign(r.d4h)*(re-rb))

# matched outcomes / buckets
matched=[]
for r in ep.itertuples():
    rec={"entry_time":r.entry_time,"d4h":r.d4h,"abs_d4h":abs(r.d4h),
         "side":"ETH_strong" if r.d4h>0 else "BTC_strong"}
    a=abs(r.d4h)
    rec["bucket"]="30-40" if a<.004 else ("40-50" if a<.005 else "50-60")
    for h in [4,6,8,12,24]:
        j=r.i+h*60
        rec[f"pnl_{h}h"]=pnl_at(r,j) if j<len(idx) and np.isfinite(b[j]) and np.isfinite(e[j]) else np.nan
    matched.append(rec)
m=pd.DataFrame(matched)

group_rows=[]
for h in [4,6,8]:
    for dim in ["bucket","side"]:
        for key,x in m.groupby(dim):
            p=x[f"pnl_{h}h"].dropna()
            group_rows.append({"dimension":dim,"group":key,"horizon_h":h,"n":len(p),
                "win_rate":float((p>0).mean()),"avg_usd":float(p.mean()),"median_usd":float(p.median()),
                "p05_usd":float(p.quantile(.05)),"p01_usd":float(p.quantile(.01))})
groups=pd.DataFrame(group_rows)

# What happens to trades that are losing exactly at 4h?
recovery=[]
losers=m[m.pnl_4h<0].copy()
for limit in [6,8,12,24]:
    recovered=0; mins=[]; terminal=[]
    for r in ep.loc[losers.index].itertuples():
        start=r.i+240; stop=min(r.i+limit*60,len(idx)-1)
        if start>=len(idx): continue
        js=np.arange(start,stop+1)
        valid=np.isfinite(b[js])&np.isfinite(e[js])
        js=js[valid]
        if len(js)==0: continue
        rb=b[js]/r.btc0-1; re=e[js]/r.eth0-1
        p=LEG*(-np.sign(r.d4h)*(re-rb))
        hit=np.flatnonzero(p>=0)
        if len(hit):
            recovered+=1; mins.append(int(js[hit[0]]-r.i))
        terminal.append(float(p[-1]))
    recovery.append({"deadline_h":limit,"losers_at_4h":int(len(losers)),
        "recovered_by_deadline":recovered,
        "recovery_rate":recovered/len(losers) if len(losers) else np.nan,
        "median_minutes_from_entry_to_recovery":float(np.median(mins)) if mins else np.nan,
        "p90_minutes_from_entry_to_recovery":float(np.quantile(mins,.9)) if mins else np.nan,
        "terminal_avg_usd_for_4h_losers":float(np.mean(terminal)) if terminal else np.nan})
rec=pd.DataFrame(recovery)

# Conditional rule: exit at 4h if positive; otherwise first >=0 through cap, else cap.
rule_rows=[]
for cap in [6,8,12,24]:
    pnls=[]; holds=[]; unresolved=0
    for r in ep.itertuples():
        j4=r.i+240
        jcap=r.i+cap*60
        if jcap>=len(idx) or not np.isfinite(b[j4]) or not np.isfinite(e[j4]): continue
        p4=pnl_at(r,j4)
        if p4>=0:
            pnls.append(p4); holds.append(240); continue
        js=np.arange(j4+1,jcap+1)
        valid=np.isfinite(b[js])&np.isfinite(e[js]); js=js[valid]
        if len(js)==0: continue
        rb=b[js]/r.btc0-1; re=e[js]/r.eth0-1
        p=LEG*(-np.sign(r.d4h)*(re-rb))
        hit=np.flatnonzero(p>=0)
        if len(hit):
            k=hit[0]; pnls.append(float(p[k])); holds.append(int(js[k]-r.i))
        else:
            unresolved+=1; pnls.append(float(p[-1])); holds.append(int(js[-1]-r.i))
    s=pd.Series(pnls)
    rule_rows.append({"cap_h":cap,"trades":len(s),"win_rate":float((s>0).mean()),
        "avg_pnl_usd":float(s.mean()),"median_pnl_usd":float(s.median()),"total_pnl_usd":float(s.sum()),
        "unresolved_at_cap":unresolved,"unresolved_rate":unresolved/len(s),
        "median_hold_h":float(np.median(holds)/60),"p90_hold_h":float(np.quantile(holds,.9)/60),
        "avg_gross_bps":float(s.mean()/2000*10000)})
rules=pd.DataFrame(rule_rows)

# Matched yearly, no path-dependent entry skipping
year_rows=[]
m["year"]=m.entry_time.dt.year
for h in [4,6,8]:
    for y,x in m.groupby("year"):
        p=x[f"pnl_{h}h"].dropna()
        year_rows.append({"year":int(y),"horizon_h":h,"n":len(p),"win_rate":float((p>0).mean()),
            "avg_usd":float(p.mean()),"median_usd":float(p.median())})
years=pd.DataFrame(year_rows)

for name,df in [("matched_groups",groups),("recovery",rec),("conditional_exit_rules",rules),("matched_yearly",years)]:
    df.to_csv(OUT/f"{name}.csv",index=False)
summary={"episodes":len(ep),"recovery":rec.to_dict("records"),"conditional_exit_rules":rules.to_dict("records"),
         "groups":groups.to_dict("records"),"matched_yearly":years.to_dict("records")}
with open(OUT/"summary.json","w") as f:json.dump(summary,f,indent=2,allow_nan=False)
print("===PAIR_MR_LIFECYCLE_SUMMARY==="); print(json.dumps(summary,indent=2,allow_nan=False)); print("===END_LIFECYCLE===")
