import pandas as pd
import datetime as dt
import os
import config
import numpy as np
import scipy.stats as si
import matplotlib.pyplot as plt
dirname = config.dirname
remotedir = config.remotedir
outdir = dirname + "/pics/"
fileout = dirname + "/BTC-USD.csv"

def update_csv(dirname, fileout):
    filein = dirname + "/BTC-PERPETUAL.csv"
    df2 = pd.read_csv(filein).iloc[-1]
    d,p = str(dt.datetime.fromtimestamp(df2['timestamp']/1000))[:10], df2['last_price']
    f = open(fileout, "a")
    f.write("%s,%f\n" % (d,p))
    f.close()
    print("updated %s" % fileout)

def but_atm_premium(F, T, r, sigma):
    df = np.exp(-r * T) 
    sqt = np.sqrt(T)
    K = F
    d1 = (np.log(F / K) + (0.5 * sigma ** 2) * T) / (sigma * sqt)
    d2 = d1 - sigma * sqt
    df = np.exp(-r * T) 
    call = (F * si.norm.cdf(d1, 0.0, 1.0) - K * si.norm.cdf(d2, 0.0, 1.0)) * df
    put  =  call + (K-F) * df
    return call+put

def get_csv(filename):
    df = pd.read_csv(filename)
    df = df.dropna() #[-300:]
    df['Date'] = pd.to_datetime(df['Date'])
    df = pd.DataFrame(df[['Date', 'Close']])
    return df

def graph_vol(df, bar):
    df['logret'] = np.log(df['Close']).diff(bar)
    df['vol'] = df['logret'].rolling(20).std()*np.sqrt(365./bar)
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    ax1.plot(df['Date'], df['vol'], label="vol", color="orange")
    ax1.set_ylabel("annualized monthly rolling %day bar volatility" % bar)
    ax2.plot(df['Date'], np.log10(df['Close']), label="BTC", color="blue")
    ax2.set_ylabel("BTC log10 price")
    plt.title("BTC Log Price and Volatility")
    ax1.legend(loc="upper left")
    ax2.legend(loc="upper right")
    lr = df['logret'].dropna()
    plt.xlabel("skew=%.2f kurt=%.2f" % ( si.skew(lr), si.kurtosis(lr)))
    plt.savefig("%sbtc-vol-%dbar.png" % (outdir, bar))
    plt.close()

def run_backtest(df):
    bars = [1, 2, 7, 14, 30, 91, 182]
    sigmas = [0.6, 0.7, 0.8, 0.9, 1., 1.1]
    res = {}
    for b in bars:
        print("bar=%d" % b)
        res[b] = {}
        for sigma in sigmas:
            t = b/365.
            but = -but_atm_premium(1, t, 0 , sigma)
            pnl = np.abs(df['Close'].diff(b)/df['Close'].shift(b)).fillna(0)
            res[b][sigma] = pnl + but
    for b in bars:
        for sigma in sigmas:
            plt.plot(df['Date'], np.cumsum(res[b][sigma]), label="vol=%f%%" % (sigma*100))
        plt.legend()
        plt.title("cum pnl for %d days buterfly" % b)
        plt.savefig("%sbuterfly-pnl-%dbar.png" % (outdir, b))
        plt.close()
    return res

#update_csv(dirname, fileout)
df = get_csv(fileout)
run_backtest(df)
graph_vol(df, 1)
os.system('rsync -avzhe ssh %s %s' % (outdir, remotedir))


