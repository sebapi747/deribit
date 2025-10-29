import pandas as pd
import datetime as dt
import os
import numpy as np
import scipy.stats as si
import matplotlib.pyplot as plt
import requests
filedir = os.path.dirname(__file__)
os.chdir("./" if filedir=="" else filedir)
import config
dirname = config.dirname
remotedir = config.remotedir
outdir = dirname + "/pics/"
fileout = dirname + "/vol/BTC-USD.csv"

def get_metadata():
    return {'Creator':os.uname()[1] +":"+__file__+":"+str(dt.datetime.utcnow())}

def sendTelegram(text):
    params = {'chat_id': config.telegramchatid, 'text': text, 'parse_mode': 'HTML'}
    resp = requests.post('https://api.telegram.org/bot{}/sendMessage'.format(config.telegramtoken), params)
    resp.raise_for_status()

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

def bs_premium(F, K, T, r, sigma):
    df = np.exp(-r * T) 
    if T<1e-8:
        call = max(K-F, 0.) * df
        vega = 0
    else:
        sqt = np.sqrt(T)
        d1 = (np.log(F / K) + (0.5 * sigma ** 2) * T) / (sigma * sqt)
        d2 = d1 - sigma * sqt
        df = np.exp(-r * T) 
        call = (F * si.norm.cdf(d1, 0.0, 1.0) - K * si.norm.cdf(d2, 0.0, 1.0)) * df
        vega = F * si.norm.pdf(d1, 0.0, 1.0) * sqt * df
    put  =  call + (K-F) * df
    return call, put, vega

def get_csv(filename):
    print(f"reading {filename}")
    df = pd.read_csv(filename)
    df = df.dropna() #[-300:]
    df['Date'] = pd.to_datetime(df['Date'])
    if dt.datetime.utcnow()-np.max(df['Date'])>dt.timedelta(days=2):
        msg = "last date looks too old: %s" % str(np.max(df['Date']))
        sendTelegram(msg)
        exit()
    df = pd.DataFrame(df[['Date', 'Close']])
    return df

def graph_vol(df, bar):
    df['logret'] = np.log(df['Close']).diff(bar)
    df['vol'] = df['logret'].rolling(20).std()*np.sqrt(365./bar)
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    ax1.plot(df['Date'], df['vol'], label="vol", color="orange")
    ax1.set_ylabel("annualized monthly rolling %day bar volatility" % bar)
    ax2.plot(df['Date'], df['Close'], label="BTC", color="blue")
    #ax2.grid(True, axis='y')
    ax2.yscale('log')
    ax2.set_ylabel("BTC log10 price")
    plt.title("BTC Log Price and Volatility\nUpdated: %s" % str(df['Date'].iloc[-1])[:10])
    ax1.legend(loc="upper left")
    ax2.legend(loc="upper right")
    lr = df['logret'].dropna()
    plt.xlabel("skew=%.2f kurt=%.2f" % ( si.skew(lr), si.kurtosis(lr)))
    plt.savefig("%sbtc-vol-%dbar.png" % (outdir, bar),metadata=get_metadata())
    plt.close()

def run_backtest(df):
    bars = [1, 2, 7, 14, 30, 91, 182, 365]
    sigmas = [0.6, 0.7, 0.8, 0.9, 1., 1.1]
    res = {}
    rescall = {}
    resput = {}
    for b in bars:
        print("bar=%d" % b)
        res[b] = {}
        rescall[b] = {}
        resput[b] = {}
        for sigma in sigmas:
            t = b/365.
            c,p,v = bs_premium(1,1,t,0,sigma)
            pnlc = (df['Close'].diff(b)/df['Close'].shift(b)).fillna(0)
            pnlc[pnlc<0] = 0
            rescall[b][sigma] = pnlc - c
            pnlp = (df['Close'].diff(b)/df['Close'].shift(b)).fillna(0)
            pnlp[pnlp>0] = 0
            resput[b][sigma] = -pnlp - p
            res[b][sigma] = pnlc-pnlp -p-c
    for b in bars:
        for sigma in sigmas:
            plt.plot(df['Date'], np.cumsum(res[b][sigma])/np.arange(len(res[b][sigma]))*365./b, label="vol=%.f%%" % (sigma*100))
        plt.legend()
        plt.title("mean annual pnl for %d days butterfly\nUpdated: %s" % (b,str(df['Date'].iloc[-1])[:10]))
        plt.savefig("%sbuterfly-pnl-%dbar.png" % (outdir, b),metadata=get_metadata())
        plt.close()
        for sigma in sigmas:
            plt.plot(df['Date'], np.cumsum(resput[b][sigma])/np.arange(len(res[b][sigma]))*365./b, label="vol=%.f%%" % (sigma*100))
        plt.legend()
        plt.title("mean annual pnl for %d days put\nUpdated: %s" % (b, str(df['Date'].iloc[-1])[:10]))
        plt.savefig("%sput-pnl-%dbar.png" % (outdir, b),metadata=get_metadata())
        plt.close()
        for sigma in sigmas:
            plt.plot(df['Date'], np.cumsum(rescall[b][sigma])/np.arange(len(res[b][sigma]))*365./b, label="vol=%.f%%" % (sigma*100))
        plt.legend()
        plt.title("mean annual pnl for %d days call\nUpdated: %s" % (b, str(df['Date'].iloc[-1])[:10]))
        plt.savefig("%scall-pnl-%dbar.png" % (outdir, b),metadata=get_metadata())
        plt.close()
    return res

update_csv(dirname, fileout)
df = get_csv(fileout)
run_backtest(df)
graph_vol(df, 1)
rsynccmd = 'rsync -avzhe ssh %s %s' % (outdir, remotedir)
print(rsynccmd)
os.system(rsynccmd)


