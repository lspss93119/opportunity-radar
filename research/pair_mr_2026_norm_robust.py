#!/usr/bin/env python3
import io,zipfile,requests,pandas as pd,numpy as np,json,math
from pathlib import Path
OUT=Path("research_outputs/pair_mr_2026_norm_robust");OUT.mkdir(parents=True,exist_ok=True)
START=pd.Timestamp("2026-01-01",tz="UTC");END=pd.Timestamp("2026-10-07 23:59",tz="UTC")
DISC_END=pd.Timestamp("2026-06-30 23:59",tz="UTC");OOS_START=pd.Timestamp("2026-07-01",tz="UTC")
LEG=1000.

def zcsv(url):
 r=requests.get(url,timeout=120);r.raise_for_status();zz=zipfile.ZipFile(io.BytesIO(r.content))
 with zz.open(zz.namelist()[0]) as f:return pd.read_csv(f,header=None,usecols=[0,4],names=["t","c"])
def load(sym):
 fs=[zcsv(f"https://data.binance.vision/data/spot/monthly/klines/{sym}/1m/{sym}-1m-2026-{m:02d}.zip") for m in range(1,10)]
 fs += [zcsv(f"https://data.binance.vision/data/spot/daily/klines/{sym}/1m/{sym}-1m-2026-10-{d:02d}.zip") for d in range(1,8)]
 x=pd.concat(fs,ignore_index=True);raw=pd.to_numeric(x.t,errors="coerce");u="us" if raw.dropna().median()>1e14 else "ms"
 ts=pd.to_datetime(raw,unit=u,utc=True,errors="coerce");s=pd.Series(pd.to_numeric(x.c,errors="coerce").to_numpy(),index=ts)
 s=s[~s.index.isna()];s=s[(s.index>=START)&(s.index<=END)]
 return s[~s.index.duplicated(keep="last")].sort_index()
btc=load("BTCUSDT");eth=load("ETHUSDT");idx=pd.date_range(START,END,freq="1min",tz="UTC")
df=pd.DataFrame({"btc":btc.reindex(idx),"eth":eth.reindex(idx)},index=idx)
df["d4"]=(df.eth/df.eth.shift(240)-1)-(df.btc/df.btc.shift(240)-1)
reldiff=np.log(df.eth).diff()-np.log(df.btc).diff()
for hours in [168,336]:
 w=hours*60
 sigma1=reldiff.rolling(w,min_periods=w//2).std().shift(1)
 df[f"norm_{hours}"]=df.d4/(sigma1*np.sqrt(240))
 mu=df.d4.rolling(w,min_periods=w//2).mean().shift(1)
 sd=df.d4.rolling(w,min_periods=w//2).std().shift(1)
 df[f"z_{hours}"]=(df.d4-mu)/sd
 for q in [.90,.95]:
  df[f"q{int(q*100)}_{hours}"]=df.d4.abs().rolling(w,min_periods=w//2).quantile(q).shift(1)

b=df.btc.to_numpy();e=df.eth.to_numpy()
def episodes(mask):
 m=mask.fillna(False).to_numpy();rows=[];armed=True
 for i,on in enumerate(m):
  if not on:armed=True;continue
  if on and armed:
   d=df.d4.iat[i]
   if np.isfinite(d):rows.append((i,d))
   armed=False
 return rows
def make(mask,wait,hold):
 rows=[]
 for i,d in episodes(mask):
  j=i+wait;k=j+hold*60
  if k>=len(df) or not(np.isfinite(b[j]) and np.isfinite(e[j]) and np.isfinite(b[k]) and np.isfinite(e[k])):continue
  rb=b[k]/b[j]-1;re=e[k]/e[j]-1;p=LEG*(-np.sign(d)*(re-rb))
  rows.append({"entry_time":idx[j],"pnl":p,"gross_bps":p/2000*10000})
 return pd.DataFrame(rows)
def deploy(x,hold):
 if x.empty:return x
 keep=[];free=pd.Timestamp("1970-01-01",tz="UTC")
 for r in x.sort_values("entry_time").itertuples():
  if r.entry_time<free:continue
  keep.append(r.Index);free=r.entry_time+pd.Timedelta(hours=hold)
 return x.loc[keep]
def stat(x,hold):
 s=deploy(x,hold)
 if s.empty:return {"trades":0}
 p=s.pnl;eq=p.cumsum();dd=(eq-eq.cummax()).min()
 return {"trades":len(s),"win_rate":float((p>0).mean()),"avg_usd":float(p.mean()),"gross_bps":float(s.gross_bps.mean()),"total_usd":float(p.sum()),"max_dd":float(dd),"net1":float((s.gross_bps-1).mean()),"net2":float((s.gross_bps-2).mean())}

specs=[]
for hours in [168,336]:
 for thr in [1.25,1.5,1.75,2.0,2.25,2.5]:
  specs.append((f"norm{hours}_{thr}",df[f"norm_{hours}"].abs()>=thr))
  specs.append((f"z{hours}_{thr}",df[f"z_{hours}"].abs()>=thr))
 for q in [90,95]:
  specs.append((f"rank{hours}_q{q}",df.d4.abs()>=df[f"q{q}_{hours}"]))

rows=[];months=[]
for name,mask in specs:
 for wait in [0,30,60]:
  for hold in [4,6,8]:
   x=make(mask,wait,hold)
   for split,m in [("H1",x.entry_time<=DISC_END),("OOS",x.entry_time>=OOS_START),("FULL",pd.Series(True,index=x.index))]:
    a=stat(x[m],hold);a.update({"candidate":name,"wait_m":wait,"hold_h":hold,"split":split});rows.append(a)
   # monthly only for 60m/6h to inspect stability
   if wait==60 and hold==6:
    xo=x[x.entry_time>=OOS_START].copy()
    if len(xo):
     xo["month"]=xo.entry_time.dt.to_period("M").astype(str)
     for mo,g in xo.groupby("month"):
      a=stat(g,hold);a.update({"candidate":name,"month":mo});months.append(a)
res=pd.DataFrame(rows);mon=pd.DataFrame(months)
res.to_csv(OUT/"robustness.csv",index=False);mon.to_csv(OUT/"oos_monthly.csv",index=False)
summary={"results":res.to_dict("records"),"monthly":mon.to_dict("records")}
with open(OUT/"summary.json","w") as f:json.dump(summary,f,indent=2,allow_nan=False)
print("===PAIR_MR_NORM_ROBUST===");print(json.dumps(summary,indent=2,allow_nan=False));print("===END_NORM_ROBUST===")
