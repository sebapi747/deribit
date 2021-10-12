import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
from scipy.stats import norm
import os
import config
remotedir = config.remotedir
dirname = config.dirname


def forward_move_simple(t, sigma):
    d1 = .5*sigma*np.sqrt(t)
    d2 = -d1
    return 2*(2*norm.cdf( d1) - 1)

def my_bisection(f, a, b, tol): 
    # approximates a root, R, of f bounded 
    # by a and b to within tolerance 
    # | f(m) | < tol with m the midpoint 
    # between a and b Recursive implementation
    
    # check if a and b bound a root
    if np.sign(f(a)) == np.sign(f(b)):
        raise Exception(
         "The scalars a and b do not bound a root")
        
    # get midpoint
    m = (a + b)/2
    
    if np.abs(f(m)) < tol:
        # stopping condition, report m as root
        return m
    elif np.sign(f(a)) == np.sign(f(m)):
        # case where m is an improvement on a. 
        # Make recursive call with a = m
        return my_bisection(f, m, b, tol)
    elif np.sign(f(b)) == np.sign(f(m)):
        # case where m is an improvement on b. 
        # Make recursive call with b = m
        return my_bisection(f, a, m, tol)
    
def implied_vol(t, price):
    if np.isnan(price):
        return np.nan
    return my_bisection(lambda x : forward_move_simple(t, x)-price, 0, 400, 0.001)

filename =  dirname + "/vol/BTC-USD.csv"
df = pd.read_csv(filename)
df['Date'] = pd.to_datetime(df['Date'])
colors = {91: "blue", 7: "orange"}
for lag in [91, 7]:
    movecol = 'move%d' % lag
    volcol = 'vol%d' % lag
    df[movecol] = np.abs(df['Close'].diff(lag))/df['Close'].shift(lag)
    vols = []
    for i in range(len(df)):
        try:
            vols.append(implied_vol(lag/365., df.iloc[i][movecol]))
        except:
            vols.append(np.nan)
            pass
    df[volcol] = vols

for lag in [91, 7]:
    movecol = 'move%d' % lag
    avg = np.mean(df[movecol])
    med = np.median(df[movecol].dropna())
    plt.plot(df['Date'], df[movecol], label="%dd avg=%.3f median=%.3f" % (lag,avg,med), color=colors[lag])
    plt.axhline(avg, color=colors[lag])
plt.title("BTC Move Contract Hist Price\nlast date:%s" % str(df.iloc[-1]['Date'])[:10])
plt.gcf().autofmt_xdate()
plt.legend()
plt.savefig(dirname + "/pics/btcmove.png")
plt.close()
for lag in [91, 7]:
    volcol = 'vol%d' % lag
    avgvol = implied_vol(lag/365., np.mean(df['move%d' % lag]))
    medvol = np.median(df[volcol].dropna())
    plt.plot(df['Date'], df[volcol], label="%dd avg=%.3f median=%.3f" % (lag,avgvol,medvol),color=colors[lag])
    plt.axhline(medvol, color=colors[lag])
plt.title("BTC Move Contract Hist BE Vol\nlast date:%s" % str(df.iloc[-1]['Date'])[:10])
plt.gcf().autofmt_xdate()
plt.legend()
plt.savefig(dirname + "/pics/btcmovvol.png")
plt.close()

os.system('rsync -avzhe ssh %s %s' % (dirname + "/pics/", remotedir))

