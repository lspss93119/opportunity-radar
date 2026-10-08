#!/usr/bin/env python3
import io, zipfile, requests, pandas as pd, numpy as np, json
from pathlib import Path

OUT=Path("research_outputs/pair_mr_2026_delayed"); OUT.mkdir(parents=True,exist_ok=True)
START=pd.Timestamp("2026-01-01 00:00:00",tz="UTC")
END=pd.Timestamp("2026-10-07 23:59:00",tz="UTC")
DISC_END=pd.Timestamp("2026-06-30 23:59:00",tz="UTC")
OOS_START=pd.Timestamp("2026-07-01 00:00:00",tz="UTC")
RESET=.0015; LO=.003; HI=.006; LEG=1000.

def zcsv(url):
    r=requests.get(url,timeout=120); r.raise_for_status()
    z=zipfile.ZipFile(io.BytesIO(r.content))
    with z.open(z.namelist()[0]) as f:
        return pd.read_csv(f,header=None,usecols=[0,4],names=["t","c"])
def load(sym):
    fs=[zcsv(f"https://data.binance.vision/data/spot/monthly/klines/{sym}/1m/{sym}-1m-2026-{m:02d}.zip") for m in range(1,10)]
    fs += [zcsv(f"https://data.binance.vision/data/spot/daily/klines/{sym}/1m/{sym}-1m-2026-10-{d:02d}.zip") for d in range(1,8)]
    x=pd.concat(fs,ignore_index=True); raw=pd.to_numeric(x.t,errors="coerce")
    unit="us" if raw.dropna().median()>1e14 else "ms"
    ts=pd.to_datetime(raw,unit=unit,utc=True,errors="coerce")
    s=pd.Series(pd.to_numeric(x.c,errors="coerce").to_numpy(),index=ts)
    s=s[~s.index.isna()]; s=s[(s.index>=START)&(s.index<=END)]
    return s[~s.index.duplicated(keep="last")].sort_index()

btc=load("BTCUSDT"); eth=load("ETHUSDT")
idx=pd.date_range(START,END,freq="1min",tz="UTC")
df=pd.DataFrame({"btc":btc.reindex(idx),"eth":eth.reindex(idx)},index=idx)
df["d4h"]=(df.eth/df.eth.shift(240)-1)-(df.btc/df.btc.shift(240)-1)

# canonical raw episode trigger
tr=[]; armed=False
for i,(ts,r) in enumerate(df.iterrows()):
    d=r.d4h
    if not np.isfinite(d) or not np.isfinite(r.btc) or not np.isfinite(r.eth): continue
    if abs(d)<=RESET:
        armed=True; continue
    if armed and LO<=abs(d)<=HI:
        tr.append({"trigger_i":i,"trigger_time":ts,"trigger_d4h":d,"trigger_abs":abs(d)})
        armed=False
ep=pd.DataFrame(tr)
b=df.btc.to_numpy(); e=df.eth.to_numpy(); d4=df.d4h.to_numpy()

def build_variant(wait, mode, hold):
    rows=[]
    for r in ep.itertuples():
        j=r.trigger_i+wait
        k=j+hold*60
        if k>=len(df): continue
        if not (np.isfinite(b[j]) and np.isfinite(e[j]) and np.isfinite(d4[j]) and np.isfinite(b[k]) and np.isfinite(e[k])): continue
        cur=d4[j]; same=np.sign(cur)==np.sign(r.trigger_d4h)
        if mode=="wait_only": ok=True
        elif mode=="same_side": ok=same
        elif mode=="not_wider": ok=same and abs(cur)<=r.trigger_abs
        elif mode=="contracted_in_band": ok=same and RESET<abs(cur)<=r.trigger_abs
        elif mode=="still_entry_band": ok=same and LO<=abs(cur)<=HI
        else: raise ValueError(mode)
        if not ok: continue
        # direction remains original trigger direction; avoid direction flipping after trigger.
        rb=b[k]/b[j]-1; re=e[k]/e[j]-1
        pnl=LEG*(-np.sign(r.trigger_d4h)*(re-rb))
        rows.append({"entry_time":idx[j],"trigger_time":r.trigger_time,"pnl":pnl,
                     "gross_bps":pnl/2000*10000,"wait":wait,"mode":mode,"hold":hold,
                     "entry_d4h":cur,"trigger_d4h":r.trigger_d4h})
    return pd.DataFrame(rows)

def deploy(x,hold):
    if x.empty:return x
    out=[]; free=pd.Timestamp("1970-01-01",tz="UTC")
    for r in x.sort_values("entry_time").itertuples():
        if r.entry_time<free:continue
        out.append(r.Index); free=r.entry_time+pd.Timedelta(hours=hold)
    return x.loc[out].copy()
def stat(x,hold):
    s=deploy(x,hold)
    if s.empty:return {"trades":0}
    p=s.pnl; eq=p.cumsum(); dd=(eq-eq.cummax()).min()
    return {"trades":len(s),"win_rate":float((p>0).mean()),"avg_usd":float(p.mean()),
            "gross_bps":float(s.gross_bps.mean()),"total_usd":float(p.sum()),
            "max_dd_usd":float(dd),"net_bps_cost1":float((s.gross_bps-1).mean()),
            "net_bps_cost2":float((s.gross_bps-2).mean())}

variants=[]
for hold in [4,6]:
  for wait in [0,15,30,60]:
    modes=["wait_only"] if wait==0 else ["wait_only","same_side","not_wider","contracted_in_band","still_entry_band"]
    for mode in modes:
        x=build_variant(wait,mode,hold)
        for split,mask in [
            ("discovery_H1",x.entry_time<=DISC_END),
            ("oos_H2plus",x.entry_time>=OOS_START),
            ("full_2026",pd.Series(True,index=x.index))]:
            st=stat(x[mask],hold); st.update({"hold_h":hold,"wait_m":wait,"mode":mode,"split":split,"eligible":int(mask.sum())})
            variants.append(st)
res=pd.DataFrame(variants)

# OOS monthly for variants that are conceptually simple, no parameter-tuned threshold.
monthly=[]
for hold,wait,mode in [(6,0,"wait_only"),(6,15,"wait_only"),(6,30,"wait_only"),(6,60,"wait_only"),
                       (6,15,"not_wider"),(6,30,"not_wider"),(6,60,"not_wider")]:
    x=build_variant(wait,mode,hold)
    x=x[x.entry_time>=OOS_START].copy()
    x["month"]=x.entry_time.dt.to_period("M").astype(str)
    for m,g in x.groupby("month"):
        st=stat(g,hold); st.update({"hold_h":hold,"wait_m":wait,"mode":mode,"month":m})
        monthly.append(st)
monthly=pd.DataFrame(monthly)

res.to_csv(OUT/"variant_results.csv",index=False); monthly.to_csv(OUT/"oos_monthly.csv",index=False)
summary={"episodes":len(ep),"results":res.to_dict("records"),"oos_monthly":monthly.to_dict("records")}
with open(OUT/"summary.json","w") as f: json.dump(summary,f,indent=2,allow_nan=False)
print("===PAIR_MR_2026_DELAYED==="); print(json.dumps(summary,indent=2,allow_nan=False)); print("===END_DELAYED===")
