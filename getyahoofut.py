import pandas as pd
import numpy as np
import os,re
import datetime as dt
import pytz
import matplotlib.pyplot as plt

remotecsvdir = "sebapi747@ssh.pythonanywhere.com:/home/sebapi747/marko/static/pics/deribit/futcsv"
dirname = "futcsv/"
pandasdirname = "futpandas/"
outplot = "pics/"
tickers = pd.read_csv("tickers.csv").sort_values(["category","desc"])
namemap = {}
for i,r in tickers.iterrows():
    namemap[r["ticker"]] = r["desc"]

''' ------------------------------------------------------------------------
    get data
    '''
def rsync_csv(remotecsvdir):
    os.system("rsync -auvzhe ssh %s ." % remotecsvdir)
           
def correct_tqutc(f,df):
    tzmap = {"EDT": "America/New_York", "BST": "Europe/London", "UTC": "UTC", "GMT":"Europe/London"}
    datequotes = []
    lasttimestr =""
    cutdate = dt.datetime(2023,6,29).astimezone(pytz.UTC)
    df = df.loc[pd.to_datetime(df["tutc"])>cutdate].copy()
    for i,r in df.iterrows():
        if lasttimestr==r['timestr']:
            print("WARN: duplicate %s %04d timestr=%s %s %f" % (f, i, r['tlocal'], r['timestr'], r['quote']))
            datequote = pd.NA
        else:
            lasttimestr=r['timestr']
            tz = pytz.timezone(tzmap[r['timestr'][-3:]])
            datequote = tz.localize(dt.datetime.strptime(r['tlocal'][:11] + r['timestr'][:7], "%Y-%m-%d %I:%M%p"), is_dst=None)
            datesnap = tz.localize(dt.datetime.strptime(r['tlocal'][:18], "%Y-%m-%d %H:%M:%S"), is_dst=None)
            dateutc = pytz.utc.localize(dt.datetime.strptime(r['tutc'][:18], "%Y-%m-%d %H:%M:%S"), is_dst=None)
            if datesnap.timestamp()!=dateutc.timestamp():
                raise Exception("problem local to utc snapshot date conversion ",datesnap,dateutc)
            if datequote>datesnap+dt.timedelta(minutes=5): # check if around the clock
                datequote_prev_day = datequote-dt.timedelta(days=1)
                datequote=datequote_prev_day
                print("INFO: date change %s %04d %s %s %f" % (f, i, r['tlocal'], r['timestr'], r['quote']))
            if datequote+dt.timedelta(minutes=17)>datesnap:
                datequote = datequote.astimezone(pytz.UTC)
            else:
                print("INFO: drop quote %s more than 17min stale at local time=%s" % (r['timestr'],r['tlocal'][11:18]))
                datequote = pd.NA
        datequotes.append(datequote)
    df['tqutc'] = datequotes
    df = df.dropna().copy()
    df['dtdiff'] = [d.total_seconds() for d in df['tqutc'].diff()]
    df['validdt'] = (df['dtdiff']<60*24*3) & (df['dtdiff']>0)
    #df = df.loc[~(df['tqutc'].diff()<=dt.timedelta(seconds=0))]
    return df

def computevol(df):
    df['vol'] = pd.NA
    validvol = df['validdt'].rolling(20).sum()==20
    timeinyear = df['dtdiff']/60./60./24./365.25
    timenormdiff = df['lnquote'].diff()/np.sqrt(timeinyear)
    df['vol'] = timenormdiff.rolling(20).std()
    #voldf= df.loc[validvol]
    df.loc[~validvol,'vol'] = pd.NA
    return df

def readcsv(dirname,f):
    df = pd.read_csv(dirname+f)
    df = correct_tqutc(f,df)
    df['lnquote'] = np.cumsum(np.log(df['quote']).diff())
    df = computevol(df)
    return df #[['tqutc','quote','lnquote','vol']]

def getdfbytick(dirname):
    dfbytick = {}
    files = os.listdir(dirname)
    for f in files:
        print(f)
        ticker = f[:-4]
        futname = namemap.get(ticker)
        if futname is None:
            raise Exception("ERROR: unknown ticker %s" % ticker)
        ticker = futname + " " + ticker
        dfbytick[ticker] = readcsv(dirname,f) 
    return dfbytick

# write csv to pandasdirname
def getallcsv():
    rsync_csv(remotecsvdir)
    dfbytick = getdfbytick(dirname)
    for k,df in dfbytick.items():
        filename = "%s/%s.csv" % (pandasdirname,k)
        print("INFO:"+filename)
        df.to_csv(filename,index=False)

''' -------------------------------------------------------
     process the data, reading csv from pandasdirname
     '''
def readcsv_dfbytick(dirname):
    dfbytick = {}
    for fn in os.listdir(dirname):
        k = re.sub(" .*","",fn[:-4].replace("5Y Treas","5YTreas"))
        df = pd.read_csv(dirname+fn)
        print("INFO: found %s,\tlen=%s, %s -> %s" % (k, len(df), str(df['tqutc'][0])[:10],
            str(df['tqutc'].array[-1])[:10]))
        df['tqutc'] = pd.to_datetime(df['tqutc'])
        dfbytick[k] = df
    return dfbytick

def getfrisun(df):
    firstdate = df['tqutc'][0]
    firstdate = firstdate.replace(hour=22, minute=0, second=0)
    sun = pd.date_range(start=firstdate, end=df['tqutc'].array[-1], freq="W")
    fri = sun-dt.timedelta(hours=49)
    return fri,sun

def drawallquotesandvol(dfbytick,outplot):
    print("INFO:drawallquotesandvol")
    cutdate = dt.datetime(2023,6,29).astimezone(pytz.UTC)
    for ticker, df in dfbytick.items():
        df = df.loc[df["tqutc"]>cutdate]
        q = np.cumsum(df['lnquote'].diff())
        plt.plot(df["tqutc"],q, label=ticker)
    plt.gcf().autofmt_xdate()
    plt.ylabel("log quote")
    lastdatestr = str(np.max(df["tqutc"]))[:10]
    plt.title("All Future Quotes\nlast: %s" % lastdatestr)
    fri,sun = getfrisun(df)
    for i in range(len(fri)):
        plt.axvspan(fri[i], sun[i], color="grey", alpha=0.5)
    plt.legend()
    plt.savefig(outplot+"allfutquote.png")
    plt.savefig("tex/pics/pdf/"+"allfutquote.pdf")
    #plt.show()
    plt.close()
    for ticker, df in dfbytick.items():
        df = df.loc[df["tqutc"]>cutdate]
        voldf = df.loc[df['vol']>0]
        plt.plot(voldf["tqutc"],voldf['vol'], label=ticker)
    plt.gcf().autofmt_xdate()
    plt.ylabel("annualised log ret vol")
    plt.title("All Volatilities\nlast %s" % lastdatestr)
    for i in range(len(fri)):
        plt.axvspan(fri[i], sun[i], color="grey", alpha=0.5)
    plt.legend()
    plt.savefig(outplot+"allfutvol.png")
    plt.savefig("tex/pics/pdf/"+"allfutvol.pdf")
    #plt.show()
    plt.close()
    for ticker, df in dfbytick.items():
        df = df.loc[df["tqutc"]>cutdate]
        volscaled = df['lnquote'].diff()*0.1/df['vol'].shift(1)
        q = np.cumsum(volscaled)
        plt.plot(df["tqutc"],q, label=ticker)
    plt.gcf().autofmt_xdate()
    plt.ylabel("log quote")
    lastdatestr = str(np.max(df["tqutc"]))[:10]
    plt.title("All Scaled Quotes\nlast: %s" % lastdatestr)
    fri,sun = getfrisun(df)
    for i in range(len(fri)):
        plt.axvspan(fri[i], sun[i], color="grey", alpha=0.5)
    plt.legend()
    plt.savefig(outplot+"allscaledfutquote.png")
    plt.savefig("tex/pics/pdf/"+"allscaledfutquote.pdf")
    #plt.show()
    plt.close()

def maxdrawdown(cumret):
    rollingmax = cumret.cummax()
    dd = 1-cumret/rollingmax
    return dd.max(axis=0)

def compute_logret_metrics(logret,cols):
    assert logret.shape[1] == len(cols)
    dt = logret.index[-1]-logret.index[0]
    dtyears = dt.total_seconds()/60./60/24/365
    barperyears = len(logret)/dtyears
    m = pd.DataFrame({"r":np.mean(logret,axis=0)*barperyears, 
                      "vol":np.std(logret,axis=0)*np.sqrt(barperyears)}, 
                     index=cols)
    m['sharpe'] = m['r']/m['vol']
    cumret     = np.exp(logret.cumsum())
    maxdd = maxdrawdown(cumret)
    maxmdd = maxdrawdown(-cumret)
    m['dd'] = -maxdd
    for c in cols:
        if m.loc[c,'r']<0:
            m.loc[c,'dd'] = -maxmdd[c]
    m['calmar'] = m['r']/maxdd
    return m

''' --------------------------------------------------------------------------------------
    trend signal with buy/sell signal memory, can be used by sma or donchian
'''
def hitminsignal(minhitdt, maxhitdt, signal, isbull):
    if len(minhitdt)==0:
        return
    d = minhitdt[0]
    weightonhit = 0 if isbull else 1
    signal[d:] = weightonhit # 1
    hitmaxsignal(minhitdt[1:], maxhitdt[maxhitdt>d], signal,isbull)
def hitmaxsignal(minhitdt, maxhitdt, signal,isbull):
    if len(maxhitdt)==0:
        return
    d = maxhitdt[0]
    weightonhit = 1 if isbull else 0
    signal[d:] = weightonhit # 0
    hitminsignal(minhitdt[minhitdt>d], maxhitdt[1:], signal,isbull)

def strat_label_str(a, m):
    label = "%s r=%.1f%% v=%.0f%%, s=%.2f c=%.2f" % (a,m['r'][a]*100,m['vol'][a]*100,m['sharpe'][a],m['calmar'][a])
    return label

# compute_logret_metrics and show pnl
# graphics: N:no interactive display, I: interactive, D:disregard (no graphics)
def show_strat_pnl(logret,weight, ma50, ma200, a, b, graphics,csvdir):
    #show_signal(weight, b)
    lr        = logret[[a]].copy()
    lr[b]     = np.log(1+(np.exp(logret[a])-1)*weight[a])
    cumret    = lr.cumsum()
    m         = compute_logret_metrics(lr,[a,b])
    if graphics!="D":
        plt.plot(cumret[a], color="blue", linewidth=0.3, label=strat_label_str(a,m))
        plt.plot(ma50, "-", color="red", linewidth=0.4)
        plt.plot(ma200, "-", color="green", linewidth=0.4)
        plt.plot(cumret[b], color="orange", label=strat_label_str(b,m))
        d = weight[a].diff()
        isdigital = len(d.loc[(d.isna()==False) &(d!=1) & (d!=-1) & (d!=0)])==0
        fri,sun = getfrisun(logret.reset_index())
        for i in range(len(fri)):
            plt.axvspan(fri[i], sun[i], color="grey", alpha=0.5)
        if isdigital:
            buy  = d.loc[d==1].index
            sell = d.loc[d==-1].index
            yr   = (cumret.index[-1]-cumret.index[0]).total_seconds()/3600/24/365.25
            riskonpct = np.sum(weight)/len(weight)
            plt.plot(cumret[a][buy], linewidth=0, markersize=5, marker="^", color="green", label="buy %.1f signal per yr, risk-on=%.0f%%" % (len(buy)/yr,riskonpct*100))
            plt.plot(cumret[a][sell], linewidth=0, markersize=5, marker="v", color="red", label="sell %.1f signal per yr, risk-off=%.0f%%" % (len(sell)/yr,(1-riskonpct)*100))
        plt.gcf().autofmt_xdate()
        plt.title("pnl %s" % b)
        plt.legend()
        plt.ylabel("cumulative return")
        plt.savefig("tex/pics/pdf/signalpnl_%s.pdf" % re.sub(r"[()=+]", "_", b).replace(" ","-"))
        if graphics=='I':
            plt.show()
        plt.close()
    m.to_csv(csvdir+"/signalmetrics_%s.csv" % re.sub(r"[()=+]", "_", b).replace(" ","-"), index=True, index_label="desc")
    
''' --------------------------------------------------------------------------------------
    compute sma or donchian weights
'''
def sma_weight(logret,a,shortwin,longwin,graphics,isbull,csvdir):
    cumret     = np.cumsum(logret[a])
    ma200      = cumret.rolling(longwin).mean()
    ma50       = cumret.rolling(shortwin).mean()
    madiff     = ma50-ma200
    madiffchg  = madiff.diff()
    selldt = madiff.loc[(madiff<0) & (madiffchg<madiff)].index
    buydt  = madiff.loc[(madiff>0) & (madiffchg>madiff)].index
    weight = pd.DataFrame({a:0}, index=cumret.index)
    hitmaxsignal(selldt, buydt, weight[a],isbull)
    weight[a] = weight[a].shift()
    if graphics:
        b = "%s sma %s %d %d" % (a, "bull" if isbull else "bear",shortwin,longwin)
        show_strat_pnl(logret[[a]],weight[[a]],ma50, ma200, a, b, graphics,csvdir)
    return weight[a]

def donchian_weight(logret,a,donchianlen,graphics,isbull,csvdir):
    cumret = np.cumsum(logret[a])
    cumretdonchianmin = cumret.rolling(donchianlen).min()
    cumretdonchianmax = cumret.rolling(donchianlen).max()
    weight = pd.DataFrame({a:0}, index=cumret.index)
    buydt = cumret.loc[cumret==cumretdonchianmax].index
    selldt = cumret.loc[cumret==cumretdonchianmin].index
    hitmaxsignal(selldt, buydt, weight[a],isbull)
    weight[a] = weight[a].shift()
    if graphics:
        b= "%s donchian %s %d" %(a,"bull" if isbull else "bear",donchianlen)
        show_strat_pnl(logret[[a]],weight[[a]], cumretdonchianmin, cumretdonchianmax, a, b, graphics,csvdir)
    return weight[a]

def bollinger_weight(logret,a,window,graphics,isbull,csvdir):
    cumret = np.cumsum(logret[a])
    mme = cumret.rolling(window).mean()
    msd = (cumret-mme).rolling(window).std()
    bup   = mme+2*msd
    bdown = mme-2*msd
    weight = pd.DataFrame({a:0}, index=cumret.index)
    selldt = cumret.loc[np.sign(cumret-bup).diff()==2].index
    buydt = cumret.loc[np.sign(cumret-bdown).diff()==-2].index
    hitmaxsignal(selldt, buydt, weight[a],isbull)
    weight[a] = weight[a].shift()
    if graphics:
        b= "%s bollinger %s %d" %(a,"bull" if isbull else "bear",window)
        show_strat_pnl(logret[[a]],weight[[a]], bdown, bup, a, b, graphics,csvdir)
    return weight[a]

def allstrat(logret,a,csvdir,graphics):
    print("INFO: processing:", a)
    donchianlen = 200
    shortwin = 50
    longwin = 200
    w = sma_weight(logret,a,shortwin=shortwin,longwin=longwin,graphics=graphics,isbull=True,csvdir=csvdir)
    w = sma_weight(logret,a,shortwin=shortwin,longwin=longwin,graphics=graphics,isbull=False,csvdir=csvdir)
    w = donchian_weight(logret,a,donchianlen=donchianlen,graphics=graphics,isbull=True,csvdir=csvdir)
    w = donchian_weight(logret,a,donchianlen=donchianlen,graphics=graphics,isbull=False,csvdir=csvdir)
    w = bollinger_weight(logret,a,window=200,graphics=graphics,isbull=True,csvdir=csvdir)
    w = bollinger_weight(logret,a,window=200,graphics=graphics,isbull=False,csvdir=csvdir)
    
def generate_graphs_and_metrics_csv(dfbytick):
    print("INFO:generate_graphs_and_metrics_csv")
    csvdir = "tex/csv"
    os.system("rm %s/*.csv" % csvdir)
    for k,df in dfbytick.items():
        logret = pd.DataFrame(data={k:df['lnquote'].diff().array},index=df['tqutc'])
        volscaled = df['lnquote'].diff()*0.1/df['vol'].shift(1)
        scaled = pd.DataFrame(data={'Scaled '+k:volscaled.array},index=df['tqutc'])
        allstrat(logret,k,csvdir,graphics='N')
        allstrat(scaled.dropna(),'Scaled '+k,csvdir,graphics='N')

def read_csv_metrics(csvdir):
    mlist = []
    for f in sorted(os.listdir(csvdir)):
        mlist.append(pd.read_csv("%s/%s" % (csvdir,f)))
    metrics = pd.concat(mlist).drop_duplicates("desc").set_index("desc")
    return metrics
    
def generate_week_metrics_csv(dfbytick):
    print("INFO:generate_week_metrics_csv")
    csvdir = "tex/csv2"
    os.system("mkdir -p "+csvdir)
    os.system("rm %s/*.csv" % csvdir)
    for k,df in dfbytick.items():
        fri,sun = getfrisun(df)
        for i in range(len(fri)):
            if i==0:
                continue
            wdf = df.loc[(df.tqutc>sun[i-1]) & (df.tqutc<fri[i])]
            a = k+" %s" % str(sun[i-1])[:10]
            logret = pd.DataFrame(data={a:wdf['lnquote'].diff().array},index=wdf['tqutc'])
            volscaled = wdf['lnquote'].diff()*0.1/wdf['vol'].shift(1)
            scaled = pd.DataFrame(data={'Scaled '+a:volscaled.array},index=wdf['tqutc'])
            allstrat(logret,a,csvdir,graphics="D")
            allstrat(scaled.dropna(),'Scaled '+a,csvdir,graphics="D")
    metrics = read_csv_metrics(csvdir)
    metrics['scaled'] = ""
    metrics['asset'] = ""
    metrics['week'] = ""
    metrics['strat'] = ""
    for s in metrics.index:
        offset = 0
        if s[:6]=="Scaled":
            metrics.loc[s,'scaled'] = "scaled"
            offset = 7
        asset = re.sub(r" [^ ].*",r"",s[offset:])
        offset += len(asset)+1
        d = s[offset:(offset+10)]
        r = s[(offset+11):]
        metrics.loc[s,'asset'] = asset
        metrics.loc[s,'week'] = d
        metrics.loc[s,'strat'] = r
    metrics = metrics.sort_values(by=["scaled","asset","strat","week"])
    return metrics
        
def read_metrics_csv_generate_tex():
    print("INFO:read_metrics_csv_generate_tex")
    texcsvdir = "tex/csv"
    metrics = read_csv_metrics(texcsvdir)
    metrics.to_latex("tex/tables/signal_metrics.tex")
    for k,v in namemap.items():
        asset = re.sub(" .*","",v.replace("5Y Treas","5YTreas"))
        texfilename = "tex/tables/signal_metrics_" + asset + ".tex"
        metrics.loc[metrics.index.str.startswith(asset)].to_latex(texfilename, float_format="%.3f")
        print("INFO: produced ",texfilename)
        texfilename = "tex/tables/signal_metrics_" + "Scaled-"+asset + ".tex"
        metrics.loc[metrics.index.str.startswith("Scaled "+asset)].to_latex(texfilename, float_format="%.3f")
        print("INFO: produced ",texfilename)
    return metrics

def generate_tex_beamer():
    print("INFO:generate_tex_beamer")
    sall = r'''%------------------------------------------------
    \begin{frame}
        \frametitle{Yen Strategy PnL}
        \begin{columns}
        \begin{column}{.3\textwidth}
        \includegraphics[scale=0.25]{pics/pdf/signalpnl_Yen-sma-bull-50-200.pdf}
        \includegraphics[scale=0.25]{pics/pdf/signalpnl_Yen-sma-bear-50-200.pdf}
        \end{column}
        \begin{column}{.3\textwidth}
        \includegraphics[scale=0.25]{pics/pdf/signalpnl_Yen-donchian-bull-200.pdf}
        \includegraphics[scale=0.25]{pics/pdf/signalpnl_Yen-donchian-bear-200.pdf}
        \end{column}
        \begin{column}{.3\textwidth}
        \includegraphics[scale=0.25]{pics/pdf/signalpnl_Yen-bollinger-bull-200.pdf}
        \includegraphics[scale=0.25]{pics/pdf/signalpnl_Yen-bollinger-bear-200.pdf}
        \end{column}
        \end{columns}
    \end{frame}
    %------------------------------------------------
    \begin{frame}
        \frametitle{Yen Metrics with Trend Signal}
        \begin{table}
        \resizebox{\textwidth}{!}{\input{"tables/signal_metrics_Yen.tex"}}
        \end{table}
    \end{frame}
    ''' 
    stotal = ''
    for k,v in namemap.items():
        yenreplace = re.sub(" .*","",v.replace("5Y Treas","5YTreas"))
        stotal += sall.replace("Yen",yenreplace)
    file = open("tex/tables/strat_metrics_by_asset.tex","w")
    file.write(stotal)
    file.close()
    stotal = ''
    for k,v in namemap.items():
        yenreplace = "Scaled-" + re.sub(" .*","",v.replace("5Y Treas","5YTreas"))
        stotal += sall.replace("Yen",yenreplace)
    file = open("tex/tables/strat_metrics_by_scaled_asset.tex","w")
    file.write(stotal)
    file.close()

def plt_metrics_hist(metrics):
    print("INFO: plt_metrics_hist")
    containsScaled = metrics.index.str.contains("Scaled")
    plt.hist(metrics.loc[~containsScaled]['sharpe'], bins=40)
    plt.title("all strat sharpe")
    plt.xlabel("Total %d strategies" % len(metrics))
    plt.savefig("tex/pics/pdf/sharpe-hist-%s-strat.pdf" % "all")
    plt.close()
    plt.hist(metrics.loc[containsScaled]['sharpe'], bins=40)
    plt.title("all strat sharpe")
    plt.xlabel("Total %d strategies" % len(metrics))
    plt.savefig("tex/pics/pdf/sharpe-hist-Scaled-%s-strat.pdf" % "all")
    plt.close()
    for s in ['bollinger','sma','donchian']:
        m = metrics.loc[metrics.index.str.contains(s) & ~containsScaled]
        plt.hist(m['sharpe'], bins=40)
        plt.title("%s strat sharpe" % s)
        plt.xlabel("Total %d strategies" % len(metrics))
        plt.savefig("tex/pics/pdf/sharpe-hist-%s-strat.pdf" % s)
        plt.close()
        m = metrics.loc[metrics.index.str.contains(s) & containsScaled]
        plt.hist(m['sharpe'], bins=40)
        plt.title("%s strat sharpe" % s)
        plt.xlabel("Total %d strategies" % len(metrics))
        plt.savefig("tex/pics/pdf/sharpe-hist-Scaled-%s-strat.pdf" % s)
        plt.close()
    for s in namemap.values():
        m = metrics.loc[metrics.index.str.contains(s) & ~containsScaled]
        plt.hist(m['sharpe'], bins=40)
        plt.title("%s strat sharpe" % s)
        plt.xlabel("Total %d strategies" % len(m))
        plt.savefig("tex/pics/pdf/sharpe-hist-%s-strat.pdf" % s)
        plt.close()
        m = metrics.loc[metrics.index.str.contains(s) & containsScaled]
        plt.hist(m['sharpe'], bins=40)
        plt.title("%s strat sharpe" % s)
        plt.xlabel("Total %d strategies" % len(m))
        plt.savefig("tex/pics/pdf/sharpe-hist-Scaled-%s-strat.pdf" % s)
        plt.close()

def getdata_generate_tex():
    getallcsv()
    dfbytick = readcsv_dfbytick(pandasdirname)
    drawallquotesandvol(dfbytick, outplot)
    generate_graphs_and_metrics_csv(dfbytick)
    metrics = read_metrics_csv_generate_tex()
    plt_metrics_hist(metrics)
    generate_tex_beamer()
    print("INFO: pdflatex")
    os.chdir("tex")
    os.system("pdflatex futintra.tex")
    os.system("pdflatex futintra.tex")
    generate_week_metrics_csv(dfbytick).to_csv("tex/byweek.csv")

if __name__ == "__main__":
    #dfbytick = readcsv_dfbytick(pandasdirname)
    #generate_week_metrics_csv(dfbytick).to_csv("byweek.csv")
    getdata_generate_tex()
