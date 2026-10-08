#!/usr/bin/env python3
import io,zipfile,requests,pandas as pd,numpy as np,json
from pathlib import Path
OUT=Path("research_outputs/pair_mr_2026_finalcheck");OUT.mkdir(parents=True,exist_ok=True)
START=pd.Timestamp("2026-01-01",tz="UTC");END=pd.Timestamp("2026-10-07 23:59",tz="UTC")
OOS=pd.Timestamp("2026-07-01",tz="UTC");LEG=1000.;WAIT=60;HOLD=6
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
w=336*60;mu=df.d4.rolling(w,min_periods=w//2).mean().shift(1);sd=df.d4.rolling(w,min_periods=w//2).std().shift(1);df["z"]= (df.d4-mu)/sd
mask=df.z.abs()>=2.25
m=mask.fillna(False).to_numpy();eps=[];armed=True
for i,on in enumerate(m):
 if not on:armed=True;continue
 if on and armed:
  d=df.d4.iat[i]
  if np.isfinite(d):eps.append((i,d))
  armed=False
b=df.btc.to_numpy();e=df.eth.to_numpy();rows=[]
for i,d in eps:
 j=i+WAIT;k=j+HOLD*60
 if k>=len(df) or not(np.isfinite(b[j]) and np.isfinite(e[j]) and np.isfinite(b[k]) and np.isfinite(e[k])):continue
 rb=b[k]/b[j]-1;re=e[k]/e[j]-1;p=LEG*(-np.sign(d)*(re-rb))
 rows.append({"entry_time":idx[j],"pnl":p,"bps":p/2000*10000,"side":"ETH_strong" if d>0 else "BTC_strong"})
x=pd.DataFrame(rows);x=x[x.entry_time>=OOS].copy()
# deployable
keep=[];free=pd.Timestamp("1970-01-01",tz="UTC")
for r in x.sort_values("entry_time").itertuples():
 if r.entry_time<free:continue
 keep.append(r.Index);free=r.entry_time+pd.Timedelta(hours=HOLD)
x=x.loc[keep].sort_values("entry_time").reset_index(drop=True);x["month"]=x.entry_time.dt.to_period("M").astype(str)
rng=np.random.default_rng(42)
boot=[]
arr=x.bps.to_numpy()
for _ in range(20000):
 boot.append(float(rng.choice(arr,size=len(arr),replace=True).mean()))
ci=[float(np.quantile(boot,.025)),float(np.quantile(boot,.5)),float(np.quantile(boot,.975))]
res={"trades":len(x),"mean_bps":float(x.bps.mean()),"median_bps":float(x.bps.median()),"win_rate":float((x.bps>0).mean()),"bootstrap_mean_bps_ci95":ci}
# exclude August
noaug=x[x.month!="2026-08"]
res["exclude_aug"]={"trades":len(noaug),"mean_bps":float(noaug.bps.mean()),"win_rate":float((noaug.bps>0).mean())}
# by side
res["by_side"]=[]
for side,g in x.groupby("side"):
 res["by_side"].append({"side":side,"trades":len(g),"mean_bps":float(g.bps.mean()),"win_rate":float((g.bps>0).mean())})
# leave one month out
res["leave_one_month_out"]=[]
for mo in sorted(x.month.unique()):
 g=x[x.month!=mo]
 res["leave_one_month_out"].append({"excluded":mo,"trades":len(g),"mean_bps":float(g.bps.mean()),"win_rate":float((g.bps>0).mean())})
x.to_csv(OUT/"oos_trades.csv",index=False)
with open(OUT/"summary.json","w") as f:json.dump(res,f,indent=2,allow_nan=False)
print("===PAIR_MR_FINALCHECK===");print(json.dumps(res,indent=2,allow_nan=False));print("===END_FINALCHECK===")
