#!/usr/bin/env python3
import io,zipfile,requests,pandas as pd,numpy as np,json,math
from pathlib import Path
OUT=Path("research_outputs/pair_mr_2026_regime");OUT.mkdir(parents=True,exist_ok=True)
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
 q=q[~q.index.isna()];q=q[(q.index>=START)&(q.index<=END)];return q[~q.index.duplicated(keep="last")].sort_index()
btc=load("BTCUSDT");eth=load("ETHUSDT");idx=pd.date_range(START,END,freq="1min",tz="UTC")
df=pd.DataFrame({"btc":btc.reindex(idx),"eth":eth.reindex(idx)},index=idx)
for n,k in [(240,"4h"),(1440,"24h")]:
 df[f"br_{k}"]=df.btc/df.btc.shift(n)-1;df[f"er_{k}"]=df.eth/df.eth.shift(n)-1;df[f"d_{k}"]=df[f"er_{k}"]-df[f"br_{k}"]
lr=np.log(df.btc).diff();df["rv24"]=lr.rolling(1440,min_periods=1200).std()*math.sqrt(1440)
df["mkt4"]=(df.br_4h+df.er_4h)/2;df["abs_mkt4"]=df.mkt4.abs();df["abs_btc24"]=df.br_24h.abs()
# trailing 30d medians, shifted 1 min => strictly past-only
w=30*1440
for col in ["rv24","abs_mkt4","abs_btc24"]:
 df[f"{col}_med30"]=df[col].rolling(w,min_periods=7*1440).median().shift(1)

rows=[];armed=False
for i,(ts,r) in enumerate(df.iterrows()):
 d=r.d_4h
 if not np.isfinite(d) or not np.isfinite(r.btc) or not np.isfinite(r.eth):continue
 if abs(d)<=RESET:armed=True;continue
 if armed and LO<=abs(d)<=HI:
  rec={"i":i,"entry_time":ts,"d4h":d,"btc0":r.btc,"eth0":r.eth,
       "btc24":r.br_24h,"btc4":r.br_4h,"eth4":r.er_4h,"rv24":r.rv24,
       "mkt4":r.mkt4,"abs_mkt4":r.abs_mkt4,"abs_btc24":r.abs_btc24,
       "rv24_med30":r.rv24_med30,"abs_mkt4_med30":r.abs_mkt4_med30,"abs_btc24_med30":r.abs_btc24_med30}
  rows.append(rec);armed=False
ep=pd.DataFrame(rows);ep["entry_time"]=pd.to_datetime(ep.entry_time,utc=True)
b=df.btc.to_numpy();e=df.eth.to_numpy()
p=[]
for r in ep.itertuples():
 j=r.i+360
 if j>=len(df) or not(np.isfinite(b[j]) and np.isfinite(e[j])):p.append(np.nan);continue
 rb=b[j]/r.btc0-1;re=e[j]/r.eth0-1;p.append(LEG*(-np.sign(r.d4h)*(re-rb)))
ep["pnl"]=p;ep["gross_bps"]=ep.pnl/2000*10000
rules={
"baseline":lambda x:pd.Series(True,index=x.index),
"low_rv30":lambda x:x.rv24<x.rv24_med30,
"high_rv30":lambda x:x.rv24>=x.rv24_med30,
"weak_market4_trend":lambda x:x.abs_mkt4<x.abs_mkt4_med30,
"strong_market4_trend":lambda x:x.abs_mkt4>=x.abs_mkt4_med30,
"weak_btc24_trend":lambda x:x.abs_btc24<x.abs_btc24_med30,
"strong_btc24_trend":lambda x:x.abs_btc24>=x.abs_btc24_med30,
"btc24_up":lambda x:x.btc24>0,
"btc24_down":lambda x:x.btc24<=0,
"btc_eth4_same_sign":lambda x:np.sign(x.btc4)==np.sign(x.eth4),
"btc_eth4_opposite_sign":lambda x:np.sign(x.btc4)!=np.sign(x.eth4),
}
def dep(x):
 out=[];free=pd.Timestamp("1970-01-01",tz="UTC")
 for r in x.sort_values("entry_time").itertuples():
  if r.entry_time<free or not np.isfinite(r.pnl):continue
  out.append(r.Index);free=r.entry_time+pd.Timedelta(hours=6)
 return x.loc[out]
def stat(x):
 s=dep(x);q=s.pnl
 if not len(q):return {"trades":0}
 eq=q.cumsum();dd=(eq-eq.cummax()).min()
 return {"trades":len(q),"win_rate":float((q>0).mean()),"avg_usd":float(q.mean()),"gross_bps":float(s.gross_bps.mean()),"total_usd":float(q.sum()),"max_dd":float(dd),"net1":float((s.gross_bps-1).mean()),"net2":float((s.gross_bps-2).mean())}
res=[]
for name,fn in rules.items():
 for split,x in [("discovery_H1",ep[ep.entry_time<=DISC_END]),("oos_H2plus",ep[ep.entry_time>=OOS_START]),("full_2026",ep)]:
  m=fn(x).fillna(False);st=stat(x[m]);st.update({"rule":name,"split":split,"selected":int(m.sum()),"selection_rate":float(m.mean())});res.append(st)
res=pd.DataFrame(res);res.to_csv(OUT/"regime_results.csv",index=False);ep.to_csv(OUT/"episodes.csv",index=False)
summary={"results":res.to_dict("records")}
with open(OUT/"summary.json","w") as f:json.dump(summary,f,indent=2,allow_nan=False)
print("===PAIR_MR_2026_REGIME===");print(json.dumps(summary,indent=2,allow_nan=False));print("===END_REGIME===")
