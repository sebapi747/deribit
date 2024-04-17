import pandas as pd
import numpy as np
import os
import datetime as dt
import pytz
import matplotlib.pyplot as plt

dirname = "futcsv/"
outplot = "pics/"

def get_metadata():
    return {'Creator':os.uname()[1] +":"+__file__+":"+str(dt.datetime.utcnow())}
    
def correct_tqutc(f,df):
    tzmap = {"EDT": "America/New_York", "BST": "Europe/London", "UTC": "UTC"}
    datequotes = []
    tzutc = pytz.timezone("UTC")
    for i,r in df.iterrows():
        tz = pytz.timezone(tzmap[r['timestr'][-3:]])
        tlocal  = dt.datetime.now(tz)
        datequote = tlocal.strptime(r['tlocal'][:11] + r['timestr'][:7], "%Y-%m-%d %I:%M%p")
        datesnap  = tlocal.strptime(r['tlocal'][:18], "%Y-%m-%d %H:%M:%S")
        if datequote>datesnap+dt.timedelta(minutes=5): # check if around the clock
            datequote_prev_day = datequote-dt.timedelta(days=1)
            datequote=datequote_prev_day
            print("INFO: date change %s %04d %s %s %f" % (f, i, tlocal, r['timestr'], r['quote']))
        if datequote+dt.timedelta(minutes=17)>datesnap:
            datequote = datequote.astimezone(tzutc)
        else:
            print("WARN: dropping %s %04d %s %s %f" % (f, i, tlocal, r['timestr'], r['quote']))
            datequote = pd.NA
        datequotes.append(datequote)
    df['tqutc'] = datequotes
    df = df.dropna()
    #df = df.loc[~(df['tqutc'].diff()<=dt.timedelta(seconds=0))]
    return df

def computevol(df):
    dtperyear = 1/np.mean([d.total_seconds()/60./60./24./365.25 for d in df['tqutc'].diff()][1:])
    df['vol'] = df['lnquote'].diff().rolling(20).std()*np.sqrt(dtperyear)
    return df

def readcsv(dirname,f):
    df = pd.read_csv(dirname+f)
    df = correct_tqutc(f,df)
    df['lnquote'] = np.log(df['quote']/df['quote'][0])
    df = computevol(df)
    return df #[['tqutc','quote','lnquote','vol']]

def getdfbytick(dirname):
    dfbytick = {}
    files = os.listdir(dirname)
    for f in files:
        print(f)
        ticker = f[:-4]
        dfbytick[ticker] = readcsv(dirname,f) 
    return dfbytick

def drawallquotesandvol(outplot):
    for ticker, df in dfbytick.items():
        plt.plot(df["tqutc"],df['lnquote'], label=ticker)
    plt.gcf().autofmt_xdate()
    plt.ylabel("log quote")
    plt.title("All Future Quotes")
    plt.legend()
    plt.savefig(outplot+"allfutquote.png")
    plt.close()
    for ticker, df in dfbytick.items():
        plt.plot(df["tqutc"],df['vol'], label=ticker)
    plt.gcf().autofmt_xdate()
    plt.ylabel("annualised log ret vol")
    plt.title("All Volatilities")
    plt.legend()
    plt.savefig(outplot+"allfutvol.png",metadata=get_metadata())
    plt.close()

def plotcorrelpair(t1,t2,dfbytick,outplot):
    print("INFO:",t1,t2)
    cols = ["tqutc", "lnquote"]
    df1  = dfbytick[t1][cols].set_index("tqutc").rename(columns={cols[1]: t1})
    df2  = dfbytick[t2][cols].set_index("tqutc").rename(columns={cols[1]: t2})
    df   = pd.merge(df1, df2, left_index=True, right_index=True, how="outer").interpolate().dropna()
    rollwindow = 50
    diffbar = 5
    dtperyear = 1/np.mean([d.total_seconds()/60./60./24./365.25 for d in np.diff(df.index)][1:])
    v1 = df[t1].diff().rolling(rollwindow).std()*np.sqrt(dtperyear)
    v2 = df[t2].diff().rolling(rollwindow).std()*np.sqrt(dtperyear)
    corr = df[t1].diff(diffbar).rolling(rollwindow).corr(df[t2].diff(diffbar))
    
    if np.abs(np.mean(corr))>0.3:
        plt.plot(df.index, df[t1], label=t1)
        plt.plot(df.index, df[t2], label=t2)
        plt.legend()
        plt.ylabel("ln quote")
        plt.title("log quote")
        plt.gcf().autofmt_xdate()
        plt.savefig(outplot+"futpairquote%s_%s.png" % (t1,t2))
        plt.close()

        plt.plot(df.index, v1, label="%s %.1f%%" % (t1,np.mean(v1)*100))
        plt.plot(df.index, v2, label="%s %.1f%%" % (t2,np.mean(v2)*100))
        plt.legend()
        plt.ylabel("annualised logret vol")
        plt.title("Volatiliy")
        plt.gcf().autofmt_xdate()
        plt.savefig(outplot+"futpairvol%s_%s.png" % (t1,t2),metadata=get_metadata())
        plt.close()

        plt.plot(df.index, corr, label="corr(%s,%s)=%.1f%%" % (t1,t2,np.mean(corr)*100))
        plt.ylabel("rollwindow=%d logretbar=%d" % (rollwindow, diffbar))
        plt.legend()
        plt.title("Correlation")
        plt.gcf().autofmt_xdate()
        plt.savefig(outplot+"futpaircorr%s_%s.png" % (t1,t2),metadata=get_metadata())
        plt.close()
        
if __name__ == "__main__":
    dfbytick = getdfbytick(dirname)
    drawallquotesandvol(outplot)
    for i,t1 in enumerate(dfbytick.keys()):
        for j,t2 in enumerate(dfbytick.keys()):
            if i<j:
                plotcorrelpair(t1,t2,dfbytick,outplot)
