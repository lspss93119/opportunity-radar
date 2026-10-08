#!/usr/bin/env python3
import io,zipfile,requests,pandas as pd,numpy as np,json
from pathlib import Path
OUT=Path("research_outputs/pair_mr_2026_waitscan");OUT.mkdir(parents=True,exist_ok=True)
START=pd.Timestamp("2026-01-01",tz="UTC");END=pd.Timestamp("2026-10-07 23:59",tz="UTC")
DISC_END=pd.Timestamp("2026-06-30 23:59",tz="UTC");OOS_START=pd.Timestamp("2026-07-01",tz="UTC")
RESET=.0015;LO=.003;HI=.006;LEG=1000.;HOLD=6

def z(url):
 r=requests.get(url,timeout=120);r.raise_for_status();zz=zipfile.ZipFile(io.BytesIO(r.content))
 with zz.open(zz.namelist()[0]) as f:return pd.read_csv(f,header=None,usecols=[0,4],names=["t","c"])
def load(s):
 fs=[z(f"https://data.binance.vision/data/spot/monthly/klines/{s}/1m/{s}-1m-2026-{m:02d}.zip") for m in range(1,10)]
 fs += [z(f"https://data.binance.vision/data/spot/daily/klines/{s}/1m/{s}-1m-2026-10-{d:02d}.zip") for d in range(1,8)]
 x=pd.concat(fs,ignore_index=True);raw=pd.to_numeric(x.t,errors="coerce");u="us" if raw.dropna().median()>1e14 else "ms"
 ts=pd.to_datetime(raw,unit=u,utc=True,errors="coerce");q=pd.Series(pd.to_numeric(x.c,errors="coerce").to_numpy(),index=ts)
 q=q[~q.index.isna()];q=q[(q.index>=START)&(q.index<=END)]
 return q[~q.index.duplicated(keep="last")].sort_index()
btc=load("BTCUSDT");eth=load("ETHUSDT");idx=pd.date_range(START,END,freq="1min",tz="UTC")
df=pd.DataFrame({"btc":btc.reindex(idx),"eth":eth.reindex(idx)},index=idx)
df["d4h"]=(df.eth/df.eth.shift(240)-1)-(df.btc/df.btc.shift(240)-1)
tr=[];armed=False
for i,(ts,r) in enumerate(df.iterrows()):
 d=r.d4h
 if not np.isfinite(d) or not np.isfinite(r.btc) or not np.isfinite(r.eth):continue
 if abs(d)<=RESET:armed=True;continue
 if armed and LO<=abs(d)<=HI:
  tr.append({"i":i,"t":ts,"d":d});armed=False
ep=pd.DataFrame(tr);ep["t"]=pd.to_datetime(ep.t,utc=True)
b=df.btc.to_numpy();e=df.eth.to_numpy()

def variant(wait):
 rows=[]
 for r in ep.itertuples():
  j=r.i+wait;k=j+360
  if k>=len(df) or not(np.isfinite(b[j]) and np.isfinite(e[j]) and np.isfinite(b[k]) and np.isfinite(e[k])):continue
  rb=b[k]/b[j]-1;re=e[k]/e[j]-1;p=LEG*(-np.sign(r.d)*(re-rb))
  rows.append({"entry_time":idx[j],"pnl":p,"gross_bps":p/2000*10000})
 return pd.DataFrame(rows)

def dep(x):
 out=[];free=pd.Timestamp("1970-01-01",tz="UTC")
 for r in x.sort_values("entry_time").itertuples():
  if r.entry_time<free:continue
  out.append(r.Index);free=r.entry_time+pd.Timedelta(hours=6)
 return x.loc[out]
def st(x):
 s=dep(x);p=s.pnl
 eq=p.cumsum();dd=(eq-eq.cummax()).min()
 return {"trades":len(s),"win_rate":float((p>0).mean()),"avg_usd":float(p.mean()),"gross_bps":float(s.gross_bps.mean()),"total_usd":float(p.sum()),"max_dd":float(dd),"net1":float((s.gross_bps-1).mean()),"net2":float((s.gross_bps-2).mean())}
rows=[];monthly=[]
for w in [0,30,60,90,120,180]:
 x=variant(w)
 for split,m in [("discovery_H1",x.entry_time<=DISC_END),("oos_H2plus",x.entry_time>=OOS_START),("full_2026",pd.Series(True,index=x.index))]:
  a=st(x[m]);a.update({"wait_m":w,"split":split});rows.append(a)
 xo=x[x.entry_time>=OOS_START].copy();xo["month"]=xo.entry_time.dt.to_period("M").astype(str)
 for mo,g in xo.groupby("month"):
  a=st(g);a.update({"wait_m":w,"month":mo});monthly.append(a)
res=pd.DataFrame(rows);mon=pd.DataFrame(monthly)
res.to_csv(OUT/"wait_results.csv",index=False);mon.to_csv(OUT/"oos_monthly.csv",index=False)
summary={"results":res.to_dict("records"),"monthly":mon.to_dict("records")}
with open(OUT/"summary.json","w") as f:json.dump(summary,f,indent=2,allow_nan=False)
print("===PAIR_MR_2026_WAITSCAN===");print(json.dumps(summary,indent=2,allow_nan=False));print("===END_WAITSCAN===")
