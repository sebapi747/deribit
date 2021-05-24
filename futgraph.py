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
plt.title("Price\n" + str(pd.Timestamp(df['timestamp'].iloc[-1], unit='ms')))
plt.savefig(outdir+"price.png")
plt.close()
#
# futures
#
for ticker in ['BTC', 'ETH']:
    for filename in sorted(dirs):
        if filename[0:3] != ticker:
            continue
        f = "%s/%s" % (dirname, filename)
        df = pd.read_csv(f)
        df['date'] = df.timestamp.apply(lambda x: dt.datetime.fromtimestamp(x/1000))
        df['relativeSpread'] = df['best_bid_price']/df['estimated_delivery_price']
        plt.plot(df['date'], df['relativeSpread'], label="%s %.1f%%" % (filename[0:-4], (df['relativeSpread'].iloc[-1]-1)*100))
        plt.gcf().autofmt_xdate()
    plt.xlabel('timestamp(ms) 10 min intervals')
    plt.ylabel('relative spread')
    plt.legend(bbox_to_anchor=(0,1),loc="upper left")
    plt.title(ticker + " Spot to Future Spread\n" + str(pd.Timestamp(df['timestamp'].iloc[-1], unit='ms')))
    plt.savefig(outdir+ticker+"-spot-to-future-spread.png")
    plt.close()
    miny, maxy = 10., 0.
    for filename in sorted(dirs):
        if filename[0:3] != ticker or filename[4:]=="PERPETUAL.csv":
            continue
        f = "%s/%s" % (dirname, filename)
        df = pd.read_csv(f)
        df['date'] = df.timestamp.apply(lambda x: dt.datetime.fromtimestamp(x/1000))
        tmat = dt.datetime.strptime(filename[4:-4] + " 21:00", "%d%b%y %H:%M")
        df['yearToMat'] = df.timestamp.apply(lambda x: (tmat-dt.datetime.fromtimestamp(x/1000)).total_seconds()/3600/24/365.25)
        df['yield'] = np.power(df['best_bid_price']/df['estimated_delivery_price'],1/df['yearToMat'])-1
        miny = min(df['yield'].iloc[-1], miny)
        maxy = max(df['yield'].iloc[-1], maxy)
        plt.plot(df['date'], df['yield'], label="%s %.1f%%" % (filename[0:-4], df['yield'].iloc[-1]*100))
        plt.gcf().autofmt_xdate()
    plt.xlabel('min=%.2f%% max=%.2f%%' % (miny*100,maxy*100))
    plt.ylabel('annualized yield')
    plt.legend(bbox_to_anchor=(0,1),loc="upper left")
    plt.title(ticker + " Contango Yield\n" + str(pd.Timestamp(df['timestamp'].iloc[-1], unit='ms')))
    plt.savefig(outdir+ticker+"-yield.png")
    plt.close()
os.system('rsync -avzhe ssh %s %s' % (outdir, remotedir))
