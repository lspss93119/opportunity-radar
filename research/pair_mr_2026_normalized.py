#!/usr/bin/env python3
import io,zipfile,requests,pandas as pd,numpy as np,json,math
from pathlib import Path

OUT=Path("research_outputs/pair_mr_2026_normalized");OUT.mkdir(parents=True,exist_ok=True)
START=pd.Timestamp("2026-01-01",tz="UTC");END=pd.Timestamp("2026-10-07 23:59",tz="UTC")
DISC_END=pd.Timestamp("2026-06-30 23:59",tz="UTC");OOS_START=pd.Timestamp("2026-07-01",tz="UTC")
LEG=1000.;HOLD=6;WAIT=60

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
df["br4"]=df.btc/df.btc.shift(240)-1;df["er4"]=df.eth/df.eth.shift(240)-1;df["d4"]=df.er4-df.br4
reldiff=np.log(df.eth).diff()-np.log(df.btc).diff()

# causal trailing vol / z metrics
for hours in [24,72,168,336]:
 w=hours*60
 # 1m relative-return vol scaled to 4h horizon
 sigma1=reldiff.rolling(w,min_periods=max(240,w//2)).std().shift(1)
 df[f"sigma4_{hours}h"]=sigma1*np.sqrt(240)
 mu=df["d4"].rolling(w,min_periods=max(240,w//2)).mean().shift(1)
 sd=df["d4"].rolling(w,min_periods=max(240,w//2)).std().shift(1)
 df[f"z4_{hours}h"]=(df["d4"]-mu)/sd
 df[f"norm4_{hours}h"]=df["d4"]/df[f"sigma4_{hours}h"]

# percentile rank of abs d4 using past window only
for hours in [72,168,336]:
 w=hours*60
 # rolling quantiles, shifted 1
 for q in [.8,.9,.95]:
  df[f"absd_q{int(q*100)}_{hours}h"]=df["d4"].abs().rolling(w,min_periods=max(240,w//2)).quantile(q).shift(1)

# candidate signals: threshold values are coarse, predeclared, not tuned on OOS
cands=[]
for hours in [24,72,168,336]:
 for thr in [1.0,1.5,2.0]:
  cands.append((f"norm_{hours}h_{thr}",lambda x,h=hours,t=thr: x[f"norm4_{h}h"].abs()>=t))
 for thr in [1.0,1.5,2.0]:
  cands.append((f"z_{hours}h_{thr}",lambda x,h=hours,t=thr: x[f"z4_{h}h"].abs()>=t))
for hours in [72,168,336]:
 for q in [80,90,95]:
  cands.append((f"rank_{hours}h_q{q}",lambda x,h=hours,q=q: x["d4"].abs()>=x[f"absd_q{q}_{h}h"]))

# raw frozen baseline comparator
cands.append(("raw_30_60bps",lambda x:(x.d4.abs()>=.003)&(x.d4.abs()<=.006)))

b=df.btc.to_numpy();e=df.eth.to_numpy()

def episodes(mask):
 # reset for normalized signals: must leave signal state before re-arming.
 rows=[];armed=True; prev=False
 m=mask.fillna(False).to_numpy()
 for i,on in enumerate(m):
  if not on:
   armed=True; prev=False; continue
  if on and armed:
   d=df.d4.iat[i]
   if np.isfinite(d) and np.isfinite(b[i]) and np.isfinite(e[i]):
    rows.append((i,idx[i],d))
    armed=False
 return rows

def eval_candidate(name,mask):
 eps=episodes(mask)
 rows=[]
 for i,t,d in eps:
  j=i+WAIT;k=j+HOLD*60
  if k>=len(df):continue
  if not(np.isfinite(b[j]) and np.isfinite(e[j]) and np.isfinite(b[k]) and np.isfinite(e[k])):continue
  rb=b[k]/b[j]-1;re=e[k]/e[j]-1;p=LEG*(-np.sign(d)*(re-rb))
  rows.append({"entry_time":idx[j],"pnl":p,"gross_bps":p/2000*10000})
 return pd.DataFrame(rows)

def deploy(x):
 if x.empty:return x
 keep=[];free=pd.Timestamp("1970-01-01",tz="UTC")
 for r in x.sort_values("entry_time").itertuples():
  if r.entry_time<free:continue
  keep.append(r.Index);free=r.entry_time+pd.Timedelta(hours=HOLD)
 return x.loc[keep]
def stats(x):
 s=deploy(x)
 if s.empty:return {"trades":0}
 p=s.pnl;eq=p.cumsum();dd=(eq-eq.cummax()).min()
 return {"trades":int(len(s)),"win_rate":float((p>0).mean()),"avg_usd":float(p.mean()),
         "gross_bps":float(s.gross_bps.mean()),"total_usd":float(p.sum()),"max_dd":float(dd),
         "net1":float((s.gross_bps-1).mean()),"net2":float((s.gross_bps-2).mean())}

rows=[]; monthly=[]
for name,fn in cands:
 x=eval_candidate(name,fn(df))
 if x.empty: continue
 for split,m in [("discovery_H1",x.entry_time<=DISC_END),("oos_H2plus",x.entry_time>=OOS_START),("full_2026",pd.Series(True,index=x.index))]:
  st=stats(x[m]);st.update({"candidate":name,"split":split,"eligible":int(m.sum())});rows.append(st)
 # OOS month for later stability check
 xo=x[x.entry_time>=OOS_START].copy()
 if len(xo):
  xo["month"]=xo.entry_time.dt.to_period("M").astype(str)
  for mo,g in xo.groupby("month"):
   st=stats(g);st.update({"candidate":name,"month":mo});monthly.append(st)

res=pd.DataFrame(rows);mon=pd.DataFrame(monthly)
res.to_csv(OUT/"candidate_results.csv",index=False);mon.to_csv(OUT/"oos_monthly.csv",index=False)
summary={"results":res.to_dict("records"),"monthly":mon.to_dict("records")}
with open(OUT/"summary.json","w") as f:json.dump(summary,f,indent=2,allow_nan=False)
print("===PAIR_MR_2026_NORMALIZED===");print(json.dumps(summary,indent=2,allow_nan=False));print("===END_NORMALIZED===")
