import requests
import csv
import os
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import datetime as dt
import config
remotedir = config.remotedir
dirname = config.dirname
outdir = dirname + "/pics/"


def get_metadata():
    return {'Creator':os.uname()[1] +":"+__file__+":"+str(dt.datetime.utcnow())}
    
dirs = os.listdir( dirname )
#
# Price
#
fig, ax1 = plt.subplots()
ax2 = ax1.twinx()
filename = "BTC-PERPETUAL.csv"
f = "%s/%s" % (dirname, filename)
df = pd.read_csv(f)
df['date'] = df.timestamp.apply(lambda x: dt.datetime.fromtimestamp(x/1000))
ax1.plot(df['date'], df['estimated_delivery_price'], label="%s %.0f" % (filename[0:3], df['estimated_delivery_price'].iloc[-1]))
ax1.set_ylabel(filename[0:3])
filename = "ETH-PERPETUAL.csv"
f = "%s/%s" % (dirname, filename)
df = pd.read_csv(f)
df['date'] = df.timestamp.apply(lambda x: dt.datetime.fromtimestamp(x/1000))
ax2.plot(df['date'], df['estimated_delivery_price'], label="%s %.f" % (filename[0:3], df['estimated_delivery_price'].iloc[-1]), color="orange")
ax2.set_ylabel(filename[0:3])
plt.gcf().autofmt_xdate()
# plt.legend()
ax1.legend(loc="upper left")
ax2.legend(loc="upper right")
plt.title("Price\nUpdated: " + str(pd.Timestamp(df['timestamp'].iloc[-1], unit='ms')))
plt.savefig(outdir+"price.png",metadata=get_metadata())
plt.close()
#
# futures
#
now = dt.datetime.utcnow()
files = {}
for ticker in ['BTC', 'ETH']:
    files[ticker] = {}
    for f in sorted(dirs):
        if f[0:3]!= ticker:
            continue
        d = f[4:11]
        if d == "PERPETU":
            d = now
        else:
            d = dt.datetime.strptime(d,'%d%b%y')
        files[ticker][d] = f

for ticker in ['BTC', 'ETH']:
    # relative spread
    for k in sorted(files[ticker].keys()):
        filename = files[ticker][k]
        f = "%s/%s" % (dirname, filename)
        df = pd.read_csv(f)
        df = df.loc[(df['state']!='closed') & (df['estimated_delivery_price']!='expired')]
        df['date'] = df.timestamp.apply(lambda x: dt.datetime.fromtimestamp(x/1000))
        df['relativeSpread'] = df['best_bid_price'].astype(float)/df['estimated_delivery_price'].astype(float)
        plt.plot(df['date'], df['relativeSpread'], label="%s %.1f%%" % (filename[0:-4], (df['relativeSpread'].iloc[-1]-1)*100))
        plt.gcf().autofmt_xdate()
    plt.xlabel('timestamp(ms) 10 min intervals')
    plt.ylabel('relative spread')
    plt.legend(bbox_to_anchor=(0,1),loc="upper left")
    plt.title(ticker + " Spot to Future Spread\nUpdated: " + str(pd.Timestamp(df['timestamp'].iloc[-1], unit='ms')))
    plt.savefig(outdir+ticker+"-spot-to-future-spread.png",metadata=get_metadata())
    plt.close()
    # plot yield
    miny, maxy = 10., 0.
    plt.ylim(-0.4, 0.4)
    for k in sorted(files[ticker].keys()):
        filename = files[ticker][k]
        if k==now:
            continue
        f = "%s/%s" % (dirname, filename)
        df = pd.read_csv(f)
        df = df.loc[(df['state']!='closed') & (df['estimated_delivery_price']!='expired')]
        df['date'] = df.timestamp.apply(lambda x: dt.datetime.fromtimestamp(x/1000))
        tmat = dt.datetime.strptime(filename[4:-4] + " 21:00", "%d%b%y %H:%M")
        df['yearToMat'] = df.timestamp.apply(lambda x: (tmat-dt.datetime.fromtimestamp(x/1000)).total_seconds()/3600/24/365.25)
        df['yield'] = np.power(df['best_bid_price'].astype(float)/df['estimated_delivery_price'].astype(float),1/df['yearToMat'])-1
        miny = min(df['yield'].iloc[-1], miny)
        maxy = max(df['yield'].iloc[-1], maxy)
        plt.plot(df['date'], df['yield'], label="%s %.1f%%" % (filename[0:-4], df['yield'].iloc[-1]*100))
        plt.gcf().autofmt_xdate()
    plt.xlabel('min=%.2f%% max=%.2f%%' % (miny*100,maxy*100))
    plt.ylabel('annualized yield')
    plt.legend(bbox_to_anchor=(0,1),loc="upper left")
    plt.title(ticker + " Contango Yield\nUpdated: " + str(pd.Timestamp(df['timestamp'].iloc[-1], unit='ms')))
    plt.savefig(outdir+ticker+"-yield.png",metadata=get_metadata())
    plt.close()
    #
    # future contango
    times = []
    bestbids = []
    estimated_delivery_prices = []
    timestamp= ""
    for k in sorted(files[ticker].keys()):
        filename = files[ticker][k]
        f = "%s/%s" % (dirname, filename)
        df = pd.read_csv(f)
        timestamp, state, bestbid, estimated_delivery_price = df.iloc[-1][['timestamp','state', 'best_bid_price', 'estimated_delivery_price']]
        if state=='closed':
            continue
        times.append((k-now).total_seconds()/3600./24./365.)
        bestbids.append(bestbid)
        estimated_delivery_prices.append(estimated_delivery_price)
    plt.plot(times, bestbids, label="best bid", marker="o")
    plt.plot(times, estimated_delivery_prices, label="delivery price", marker="o")
    plt.xlabel('time in year')
    plt.ylabel('price')
    plt.legend()
    plt.title(ticker + " Future Curve\nUpdated: " + str(pd.Timestamp(timestamp, unit='ms')))
    plt.savefig(outdir+ticker+"-future-curve.png",metadata=get_metadata())
    plt.close()

def getvol(filename):
    f = "%s/%s" % (dirname, filename)
    df = pd.read_csv(f)
    #pd.Timestamp(df['timestamp'].iloc[-1], unit='ms'))
    df['date'] = df.timestamp.apply(lambda x: dt.datetime.fromtimestamp(x/1000))
    bar = 1
    df['logprice'] = np.log(df['last_price'])
    df['logret'] = df['logprice'].diff(periods=bar)
    df['std'] = df['logret'].rolling(15).std() * np.sqrt(365*24*60/10./bar)
    return df

def graphvol():
    plt.ylim(0, 3)
    for ticker in ["BTC", "ETH"]:
        filename = "%s-PERPETUAL.csv" % ticker
        df = getvol(filename)
        plt.plot(df['date'],df['std'], label=ticker)
    n = len(df['date'])
    plt.title("Historical Volatility\nUpdated: %s" % str(df['date'][n-1]))
    plt.gcf().autofmt_xdate()
    plt.ylabel("Annualized 10' bar Volatility")
    plt.legend()
    plt.savefig(outdir+"histvol.png",metadata=get_metadata())
    plt.close()

graphvol()
os.system('rsync -avzhe ssh %s %s' % (outdir, remotedir))
