import datetime as dt
import pandas as pd
import numpy as np
import time, os
from matplotlib import pyplot as plt
filedir = os.path.dirname(__file__)
os.chdir("./" if filedir=="" else filedir)
import config

bbdirname = "../futures/bbg_sebastien/"
outplot   = "../futures/graphs/"

tickers = {"TU":"2Y Note","TY":"10Y Note","ES":"SP500","SI":"Silver","JY":"JPY",
 "HG":"Copper","CL":"Crude Oil","NQ":"Nasdaq","GC":"Gold","PL":"Platinum",
 "NG":"Natural Gas","EC":"Euro","BP":"British Pound","CD":"Canadian dollar",
 "AD":"Australian dollar","FV":"5Y Note","SF":"Swiss Franc"}
 
 
def get_metadata():
    return {'Creator':os.uname()[1] +":"+__file__+":"+str(dt.datetime.utcnow())}
    #return {'Creator':os.uname()[1] +":"+"ib/yahoovalues.ipynb"+":"+str(dt.datetime.utcnow())}

def loadfuturedata(ticker):
    df = pd.read_csv("%s%s.csv" % (bbdirname,ticker))
    df["date"] = pd.to_datetime(df["date"])
    df = df.set_index("date")
    if "price_close_1" not in df.columns and "price_1" in df.columns:
        df = df.rename(columns={"price_1":"price_close_1","price_2":"price_close_2"})
    return df

def adjustroll(ticker,df):
    prerolldates = df.index[df["instrument_code_1"].shift(-1)==df["instrument_code_2"]]
    postrolldates = df.index[df["instrument_code_1"]==df["instrument_code_2"].shift(1)]
    rolladj = df.loc[prerolldates,"price_close_1"]-df.loc[prerolldates,"price_close_2"]
    rolladj = pd.Series(rolladj.array,index=postrolldates)
    pnldf = pd.DataFrame({"pricechg":df["price_close_1"].diff(),"prevroll":rolladj}).fillna(0)
    pnldf["pnl"] = pnldf["pricechg"]+pnldf["prevroll"]
    pricechg = pnldf.loc[postrolldates,"pricechg"]
    plt.scatter(pricechg,pnldf.loc[postrolldates,"prevroll"],color=(1,0,0,0.2))
    plt.xlabel("price change")
    plt.ylabel("roll impact")
    plt.plot(pricechg,-pricechg,color="gray")
    plt.title("%s %s price change on roll date vs previous roll impact" % (ticker,tickers[ticker]))
    plt.savefig(outplot+"scatter-%s.png",metadata=get_metadata())
    plt.close()
    plt.plot(rolladj,label="roll impact")
    plt.axhline(y=0,color="gray")
    plt.title("impact on day before roll for %s" % ticker)
    plt.ylabel("negative is contango")
    plt.savefig(outplot+"rollimpact-%s.png",metadata=get_metadata())
    plt.close()
    plt.title("Price for %s" % ticker)
    plt.plot(df["price_close_1"].array[0]+np.cumsum(pnldf["pnl"]),label="roll adjusted price")
    plt.plot(df["price_close_1"],label="price",color="green")
    plt.axhline(y=0,color="gray")
    plt.legend()
    plt.savefig(outplot+"price-%s.png",metadata=get_metadata())
    plt.close()
    return pnldf

def adjustrolls(ticker,df,n,graphix="I"):
    prerolldates = df.index[df["instrument_code_1"].shift(-1)==df["instrument_code_2"]]
    postrolldates = df.index[df["instrument_code_1"]==df["instrument_code_2"].shift(1)]
    rolladj = df["price_close_1"]-df["price_close_2"]
    pricechange = df["price_close_1"].diff().loc[postrolldates]
    for k in range(1,n+1):
        mycolor=(1-(k-1)/n,0,(k-1)/n,0.2)
        plt.scatter(pricechange,rolladj.shift(k).loc[postrolldates],color=mycolor,label="roll at -%d day" % k)
    plt.plot(pricechange,-pricechange,color="gray")
    plt.xlabel("price change from %s to %s" % (str(df.index[0])[:10],str(df.index[-1])[:10]))
    plt.ylabel("roll impact (neg is contango)")
    plt.title("%s (%s) price change on roll date vs previous roll impact" % (ticker,tickers[ticker]))
    plt.legend()
    if graphix=="I":
        plt.savefig(outplot+"scatter-%s.png"% ticker,metadata=get_metadata())
        plt.close()
    else:
        plt.show()
    for k in range(1,n+1):
        mycolor=(1-(k-1)/n,0,(k-1)/n,0.2)
        rolladj = df["price_close_1"]-df["price_close_2"]
        rolladj = rolladj.shift(k).loc[postrolldates]
        pnldf = pd.DataFrame({"pricechg":df["price_close_1"].diff(),"prevroll":rolladj}).fillna(0)
        pnldf["pnl"] = pnldf["pricechg"]+pnldf["prevroll"]
        plt.plot(rolladj,label="roll impact %d day" % k,color=mycolor)
    plt.axhline(y=0,color="gray")
    plt.title("Price change %s (%s) contract roll" % (ticker,tickers[ticker]))
    plt.ylabel("negative is contango")
    plt.xlabel("from %s to %s" % (str(df.index[0])[:10],str(df.index[-1])[:10]))
    plt.legend()
    if graphix=="I":
        plt.savefig(outplot+"rollprice-%s.png" % ticker,metadata=get_metadata())
        plt.close()
    else:
        plt.show()
    for k in range(1,n+1):
        mycolor=(1-(k-1)/n,0,(k-1)/n,0.2)
        rolladj = df["price_close_1"]-df["price_close_2"]
        rolladj = rolladj.shift(k).loc[postrolldates]
        pnldf = pd.DataFrame({"pricechg":df["price_close_1"].diff(),"prevroll":rolladj}).fillna(0)
        pnldf["pnl"] = pnldf["pricechg"]+pnldf["prevroll"]
        plt.plot(df["price_close_1"].array[0]+np.cumsum(pnldf["pnl"]),
                 label="%d day roll adjusted price" % k,color=mycolor)
    plt.plot(df["price_close_1"],label="on the run price",color="green")
    plt.axhline(y=0,color="gray")
    plt.title("Price and PnL for %s (%s)" %(ticker,tickers[ticker]))
    plt.xlabel("from %s to %s" % (str(df.index[0])[:10],str(df.index[-1])[:10]))
    plt.legend()
    if graphix=="I":
        plt.savefig(outplot+"price-%s.png"% ticker,metadata=get_metadata())
        plt.close()
    else:
        plt.show()

def generate_all_graphs():
    for ticker in [f[:-4] for f in os.listdir(bbdirname)]:
        print("generating graphs for ",ticker)
        df = loadfuturedata(ticker)
        #pnldf = adjustroll(df)
        adjustrolls(ticker,df,5)
    
if __name__ == "__main__":
    generate_all_graphs()
    destdir = config.destdir
    remotedir = destdir+'futrollgraph/'
    cmd = 'rsync -avzhe ssh %s %s' % (outplot, remotedir)
    print(cmd)
    os.system(cmd)
