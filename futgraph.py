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
df = pd.DataFrame()
for ticker in ['BTC', 'ETH']:
    for filename in sorted(dirs):
        if filename[0:3] != ticker:
            continue
        f = "%s/%s" % (dirname, filename)
        df = pd.read_csv(f)
        df['date'] = df.timestamp.apply(lambda x: dt.datetime.fromtimestamp(x/1000))
        df['relativeSpread'] = df['best_bid_price']/df['estimated_delivery_price']
        plt.plot(df['date'], df['relativeSpread'], label=filename[0:-4])
        plt.gcf().autofmt_xdate()
    plt.xlabel('timestamp(ms) 10 min intervals')
    plt.ylabel('relative spread')
    plt.legend(bbox_to_anchor=(0,1),loc="upper left")
    plt.title(ticker + " Spot to Future Spread\n" + str(pd.Timestamp(df['timestamp'].iloc[-1], unit='ms')))
    plt.savefig(outdir+ticker+"-spot-to-future-spread.png")
    plt.close()
    for filename in sorted(dirs):
        if filename[0:3] != ticker or filename[4:]=="PERPETUAL.csv":
            continue
        f = "%s/%s" % (dirname, filename)
        df = pd.read_csv(f)
        df['date'] = df.timestamp.apply(lambda x: dt.datetime.fromtimestamp(x/1000))
        tmat = dt.datetime.strptime(filename[4:-4] + " 21:00", "%d%b%y %H:%M")
        df['yearToMat'] = df.timestamp.apply(lambda x: (tmat-dt.datetime.fromtimestamp(x/1000)).total_seconds()/3600/24/365.25)
        df['yield'] = np.power(df['best_bid_price']/df['estimated_delivery_price'],1/df['yearToMat'])-1
        plt.plot(df['date'], df['yield'], label=filename[0:-4])
        plt.gcf().autofmt_xdate()
    plt.xlabel('timestamp(ms) 10 min intervals')
    plt.ylabel('annualized yield')
    plt.legend(bbox_to_anchor=(0,1),loc="upper left")
    plt.title(ticker + " Contango Yield\n" + str(pd.Timestamp(df['timestamp'].iloc[-1], unit='ms')))
    plt.savefig(outdir+ticker+"-yield.png")
    plt.close()
os.system('rsync -avzhe ssh %s %s' % (outdir, remotedir))
