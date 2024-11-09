#import csv
import os,json,time,sqlite3
import datetime as dt
import pandas as pd
import random
import requests
#from lxml import html
import numpy as np
#import urllib
import pytz
import re
import time
import matplotlib.pyplot as plt
import matplotlib as mpl
filedir = os.path.dirname(__file__)
os.chdir("./" if filedir=="" else filedir)
import config
def isCMEClosed():
    t1 = dt.datetime.now(pytz.timezone("America/Chicago"))
    dow = t1.weekday()
    cmecloseoffset = 4     # 17 chicago time is 7am offset by 4 hour to recover last 5 min of market
    return dow==5 or (dow==4 and t1.hour>17+cmecloseoffset) or (dow==6 and t1.hour<18)
remotedir = config.remotedir
immcsvdir = config.dirname+"/immfutcsv/"
outdir = config.dirname + "/immfutpics/"
dirname = config.dirname 
localtz = pytz.timezone("Asia/Hong_Kong")
utctz   = pytz.timezone("UTC")

def get_metadata():
    return {'Creator':os.uname()[1] +":"+__file__+":"+str(dt.datetime.utcnow())}
def sendTelegram(text):
    params = {'chat_id': config.telegramchatid, 'text': os.uname()[1] +":"+__file__+":"+text, 'parse_mode': 'markdown'}  
    resp = requests.post('https://api.telegram.org/bot{}/sendMessage'.format(config.telegramtoken), params)
    resp.raise_for_status()
    
if isCMEClosed():
    print("INFO: market closed")
    exit()

def isImmMonth(date):
    return date.month%3==0 
def firstthismonth(dtnow):
    return dt.datetime(dtnow.year,dtnow.month,1)
def isAfterThirdImmFriday(dtnow):
    if not isImmMonth(dtnow):
        return False
    firstofmonth = firstthismonth(dtnow)
    # 0:mon,1:tue,2:wed,3:tue,4:fri,5:sat,6:sun
    daystofriday = (4-firstofmonth.weekday())%7
    thirdfriday = firstofmonth+dt.timedelta(days=daystofriday+14)
    return dtnow>thirdfriday
def calcnext3rdimmfriday(dtnow,n=2,rolloffset=9):
    dtnow = dtnow+dt.timedelta(days=rolloffset)
    futdates = []
    futcode = []
    for idx in range(n):
        futm = 3*(1+idx)
        yr = dtnow.year
        if isAfterThirdImmFriday(dtnow):
            mt = dtnow.month+futm
        else:
            mt = 1+3*((dtnow.month-1)//3)+futm-1
        yr = yr+(mt-1)//12
        mt = 1+(mt-1)%12
        futdt = dt.datetime(yr,mt,1)
        daystofriday = (4-futdt.weekday())%7
        thirdfriday = futdt+dt.timedelta(days=daystofriday+14-rolloffset)
        futdates.append(thirdfriday)
        futcode.append("%s%d" % ("-FGHJKMNQUVXZ"[thirdfriday.month],thirdfriday.year%100))
    return futcode,futdates

#NG: Trading terminates on the 3rd last business day of the month prior to the contract month. 
#CL: Trading terminates 3 business day before the 25th calendar day of the month prior to the contract month. If the 25th calendar day is not a business day, trading terminates 4 business days before the 25th calendar day of the month prior to the contract month.
#HG: Trading terminates at 12:00 Noon CT on the third last business day of the contract month.
#ZC: Trading terminates on the business day prior to the 15th calendar day of the contract month.
#LBR: Trading terminates at 12:05 p.m. CT on the business day prior to the 16th day of the contract month
#ALI: Trading terminates on the third last business day of the contract month.
#CC: Trading terminates on the day immediately preceding the first notice day of the corresponding trading month of Cocoa futures at ICE Futures U.S.
#LE:Trading terminates at 12:00 Noon CT on the last business day of the contract month.
#BZ:Trading in the February contract month terminates on the 3rd last London and U.S. business day of the month, 2 months prior to the contract month. Trading in all other contract months terminates on the 2nd last London and U.S. business day of the month, 2 months prior to the contract month.
def calc_monthly_imm_codes(dtnow,ticker,n=12):
    mt = dtnow.month
    yr = dtnow.year
    moffset = 0 if dtnow.day<10 else 1
    maturepreviousmonth = {'NG','CL','HG','ZC','LBR','ALI','LE','BZ'}
    if ticker in maturepreviousmonth:
        moffset += 1
    futcode= []
    for i in range(moffset,moffset+12):
        mtadd = i%12
        newmt = 1+(mt-1+mtadd)%12
        newyr = yr+(mt-1+i)//12
        #dlist.append(dt.datetime(newyr,1+newmt,1))
        futcode.append(("%s%d" % ("-FGHJKMNQUVXZ"[newmt],newyr%100)))
    return futcode

def get_tickers():
    ticker = pd.read_csv("../deribit/tickers.csv")
    ticker["exchg"] = ""
    ticker["code"] = ""
    ticker = ticker.loc[~ticker["category"].isin(["fx","crypto"])]
    ticker.loc[ticker["category"]=="rate","exchg"] = "CBT"
    ticker.loc[ticker["category"]=="energy","exchg"] = "NYM"
    ticker.loc[ticker["category"]=="equity","exchg"] = "CME"
    ticker.loc[ticker["category"]=="metal","exchg"] = "CMX"
    ticker.loc[ticker["category"]=="rate","code"] = "HMUZ"
    ticker.loc[ticker["category"]=="energy","code"] = "FGHJKMNQUVXZ"
    ticker.loc[ticker["category"]=="fxfut","code"] = "HMUZ"
    ticker.loc[ticker["category"]=="equity","code"] = "HMUZ"
    ticker.loc[ticker["category"]=="metal","code"] = "GJMQVZ"
    dic = {"CBT":["ZS=F","ZC=F","KE=F","ZW=F","ZR=F"],"NYB":["SB=F","CC=F","KC=F"],
        "CME":["LBR=F","LE=F","HE=F","6A=F","6B=F","6C=F","6E=F","6J=F","6S=F","CNH=F","MIR=F"],"NYM":["PL=F"]}
    diccode = {"PL=F":"FJNV","HG=F":"FGHJKMNQUVXZ","ALI=F":"FGHJKMNQUVXZ","ZS=F":"FHKNQUX",
        "LE=F":"GJMQVZ","HE=F":"GJKMNQVZ","ZC=F":"HKNUZ","ZR=F":"HKNUX",
        "SB=F":"HKNV","CC=F":"HKNUZ","KC=F":"HKNUZ","KE=F":"HKNUZ",
        "ZW=F":"HKNUZ","LBR=F":"FHKNUX","SI=F":"FHKNUZ","MIR=F":"FGHJKMNQUVXZ"}
    for e in dic.keys():
        ticker.loc[ticker["ticker"].isin(list(dic[e])),"exchg"] = e
    for e in diccode.keys():
        ticker.loc[ticker["ticker"]==e,"code"] = diccode[e]
    return ticker

'''
def get_quote(symbol, tz):
    headers = {'accept':'*/*', 'user-agent': 'Mozilla/5.0 (X11; Linux armv7l) AppleWebKit/537.36 (KHTML, like Gecko) Raspbian Chromium/78.0.3904.108 Chrome/78.0.3904.108 Safari/537.36'}
    symburl = urllib.parse.quote(symbol)
    url = "https://finance.yahoo.com/quote/%s?p=%s" % (symburl,symburl)
    time.sleep(0.1)
    resp = requests.get(url,headers=headers)
    if resp.status_code!=200:
        msg = "failed %s %d" % (url,resp.status_code)
        sendTelegram(msg)
        raise Exception(msg)
    print(resp.status_code, symbol)
    tz    = pytz.timezone(tz)
    tzutc = pytz.timezone("UTC")
    tnyc  = dt.datetime.now(tz)
    tutc  = tnyc.astimezone(tzutc)
    parsed_body = html.fromstring(resp.text)
    del resp
    notfound = parsed_body.xpath("//h2/span/text()")
    if len(notfound)==1 and "Symbols similar " in notfound[0]:
        raise Exception(symbol + " not found")
    quote = float(parsed_body.xpath("//fin-streamer[@data-symbol='%s' and @data-field='regularMarketPrice']" % symbol)[0].text_content().replace(",",""))
    volume = float(parsed_body.xpath("//fin-streamer[@data-symbol='%s' and @data-field='regularMarketVolume']" % symbol)[0].text_content().replace(",","").replace("M","000000").replace("k","000").replace("-- ","0"))    
    #date = parsed_body.xpath("//div[@id='quote-market-notice']/span")[0].text
    date = [s for s in parsed_body.xpath("//div/span/text()") if 'Market Open' in s or "At close:" in s][0]
    if "Market open" in date or 'Market Open' in date:
        mkttime = re.sub(r'\. Market [a-z]+.$', '', re.sub(r'^As of[ \t]*', '', date))
        return True, quote, tutc, tnyc, mkttime,volume
    return False, -999, tutc, tnyc, "",-999

def get_imm_futures(r):
    diclist = {}
    dtnow = dt.datetime.utcnow()
    futcode = calc_monthly_imm_codes(dtnow,n=12)
    ticker = r['ticker']
    radical = ticker[:ticker.find("=F")]
    symbols = [radical+f+"."+r["exchg"] for f in futcode if f[0] in r['code']]
    tz = r['tz']
    ifail = 0
    for symbol in symbols:
        try:
            opened, quote, tutc, tlocal, timestr,volume = get_quote(symbol,tz)
            d = {'ticker':ticker,'symbol':symbol,'opened':opened, 'quote':quote,'volume':volume,'tutc':tutc, 'tlocal':tlocal, 'timestr':timestr}
            diclist[symbol] = d
        except Exception as e:
            ifail += 1
            print(str(e))
            time.sleep(5)
            if ifail==2:
                break
    return pd.DataFrame(diclist.values())

def get_all_futures():
    diclist = {}
    dtnow = dt.datetime.utcnow()
    futcode = calc_monthly_imm_codes(dtnow,n=12)
    tickers = get_tickers()
    for i,r in tickers.iterrows():
        ticker = r['ticker']
        print(immcsvdir)
        filename = immcsvdir+ticker+".csv"
        if os.path.exists(filename):
            filehours = (dt.datetime.now().timestamp()-os.path.getmtime(filename))/3600
            if filehours<5:
                print("INFO:skipping recent file for "+ticker)
                continue
        immfutdf = get_imm_futures(r)
        if len(immfutdf)==0:
            print("ERR:no future data for "+ticker)
            continue
        if os.path.exists(filename):
            olddata = pd.read_csv(filename)
            pd.concat([olddata,immfutdf]).to_csv(filename,index=False)
        else:
            immfutdf.to_csv(filename,index=False)
 '''  
 
''' -------------------------------------------------------
save dataframe in sqlite to avoid duplicates
'''
def get_jsonfilename(ticker):
    return "json/%s.json" % (ticker)
def get_futjson_data(ticker):
    url = "https://query1.finance.yahoo.com/v8/finance/chart/" + ticker
    filename = get_jsonfilename(ticker)
    errfilename = filename.replace(".json",".err")
    if os.path.exists(filename):
        filehours = (dt.datetime.now().timestamp()-os.path.getmtime(filename))/3600
        print("INFO: %s found" % filename, end="\r")
        if filehours<0.5:
            with open(filename,"r") as f:
                return json.load(f)
    if os.path.exists(errfilename):
        filehours = (dt.datetime.now().timestamp()-os.path.getmtime(errfilename))/3600
        print("INFO: %s found" % filename)
        if filehours<2:
            raise Exception("ERR: %s occurred less than 2 hours ago" % errfilename)
    sleeptime = random.uniform(1,2)
    print("INFO: %s in %.2fsec" % (url,sleeptime))
    time.sleep(sleeptime)
    os.system("rm -f %s" % filename)
    os.system("curl --silent %s > %s" % (url,filename))
    try:
        if not os.path.exists(filename):
            raise Exception("ERR: could not fetch %s" % url)
        with open(filename,"r") as f:
            jsondata = json.load(f)
        if jsondata['chart'].get('error') is not None:
            raise Exception("ERR: %s %s" % (ticker, jsondata['chart']['error']['description']))
    except Exception as e:
        with open(errfilename, "w") as f:
            f.write(str(e))
        raise
    return jsondata

def convertdate(d):
    return dt.datetime.fromtimestamp(d).astimezone(localtz).astimezone(utctz)
def processjsontopandas(jsondata):
    jsonmeta = jsondata['chart']['result'][0]['meta']
    #echgtz  = pytz.timezone(jsonmeta['exchangeTimezoneName'])
    result  = jsondata['chart']['result'][0]
    if result.get('timestamp') is not None and len(result['indicators']['quote'])>0:
        dates = [convertdate(d) for d in result['timestamp']]
        out = {'utcdate':dates}
        for c in ["open","low","high","close","volume"]:
            out[c] = result['indicators']['quote'][0][c]
    else:
        meta = result['meta']
        #utcdate = convertdate(meta['regularMarketTime'])
        #price = meta['convertdate']
        #volume = meta['regularMarketVolume']
        with open(get_jsonfilename(meta['symbol']).replace(".json",".err"), "w") as f:
            json.dump(meta,f)
        raise Exception("ERR: %s json does not have future time series" % meta['symbol'])
    df = pd.DataFrame(out).dropna()
    df['symbol'] = jsonmeta['symbol']
    return df

def getimmtickdics():
    tickers = get_tickers()
    immtickdic = {}
    for i,r in tickers.iterrows():
        dtnow = dt.datetime.utcnow()
        ticker = r['ticker']
        radical = ticker[:ticker.find("=F")]
        futcode = calc_monthly_imm_codes(dtnow,radical,n=12)
        symbols = [radical+f+"."+r["exchg"] for f in futcode if f[0] in r['code']]
        immtickdic[ticker] = symbols
    return immtickdic
    
def schema(dbfilename):
    if not os.path.exists(dbfilename):
        print("WARN: missing file %s" % dbfilename)
        #os.system("touch %s" % dbfilename)
        with sqlite3.connect(dbfilename) as con:
            con.execute('''
            create table immfut (
            symbol text,
            utcdate datetime,
            open numeric,
            low numeric,
            high numeric,
            close numeric,
            volume numeric,
            primary key(symbol, utcdate)
            )''')
        

def insert_df_to_table(df, tablename, cols,con):
    df[cols].to_sql(name=tablename+'_tmp', con=con, if_exists='replace',index=False)
    sql = 'insert or replace into '+tablename+' ('+','.join(cols)+') select '+','.join(cols)+' from '+tablename+'_tmp'
    con.execute(sql)
    con.execute('drop table '+tablename+'_tmp')
    
def getandinsertfutpandas(ticker,dbfilename):
    df = processjsontopandas(get_futjson_data(ticker))
    print("INFO: %s inserting %d" % (ticker,len(df)),end="\r")
    with sqlite3.connect(dbfilename) as con:
        insert_df_to_table(df, "immfut", df.columns,con)

def inserttickersymbols(ticker, symbols):
    err = ""
    dbfilename = "%s/immfut/%s.db" % (dirname,ticker)
    schema(dbfilename)
    with sqlite3.connect(dbfilename) as con:
        nbbefore = len(pd.read_sql("select 1 from immfut", con=con))
    try:
        getandinsertfutpandas(ticker,dbfilename)
    except Exception as e:
        err += "%s\n" % str(e)
    ifail = 0
    for i,symbol in enumerate(symbols):
        try:
            getandinsertfutpandas(symbol,dbfilename)
        except Exception as e:
            if i<3:
                err += "%s\n" % str(e)
            ifail += 1
            if ifail==2:
                break
    with sqlite3.connect(dbfilename) as con:
        nbafter = len(pd.read_sql("select 1 from immfut", con=con))
    print(err)
    print("INFO: %s: had %d quotes now %d" % (ticker,nbbefore,nbafter))
    return ticker,nbbefore,nbafter,err

def insertalltickers():
    immtickdic = getimmtickdics()
    errors = ""
    out = "\n|ticker|before|after|\n|---|---:|---:|\n"
    for ticker,symbols in immtickdic.items():
        ticker,nbbefore,nbafter,err = inserttickersymbols(ticker, symbols)
        out += "|%s|%d|%d|\n" % (ticker,nbbefore,nbafter)
        errors += err
    sendTelegram(out)
    if len(errors)>0:
        sendTelegram("%s" % errors)


''' -------------------------------------------------------
plot graphs
'''
def get_dfdic():
    tickerdesc = pd.read_csv("tickers.csv")
    dfdic = {}
    for f in os.listdir(immcsvdir):
        r = tickerdesc.loc[tickerdesc["ticker"]==f[:-4]].iloc[0]
        print(immcsvdir+f)
        dfdic[(r["category"],r["ticker"],r["desc"])] = pd.read_csv(immcsvdir+f)
    return dfdic

def colorFader(c1,c2,mix=0): #fade (linear interpolate) from color c1 (at mix=0) to c2 (mix=1)
    c1=np.array(mpl.colors.to_rgb(c1))
    c2=np.array(mpl.colors.to_rgb(c2))
    return mpl.colors.to_hex((1-mix)*c1 + mix*c2)

def immdates_in_interval(mindate,maxdate,ticker,tickersdf):
    out = []
    rolloffset = 0
    expymth = ["FGHJKMNQUVXZ".find(k)+1 for k in tickersdf.loc[tickersdf["ticker"]==ticker,"code"].iloc[0]]
    for yr in range(mindate.year,maxdate.year+1):
        for mt in expymth:
            futdt = dt.datetime(yr,mt,1).replace(tzinfo=pytz.utc)
            daystofriday = (4-futdt.weekday())%7
            thirdfriday = futdt+dt.timedelta(days=daystofriday+14-rolloffset)
            if thirdfriday>=mindate and thirdfriday<=maxdate:
                out.append(thirdfriday)
    return out

def plot_contango_yield(dirname="../deribit"):
    ydf = pd.read_csv(dirname+"/contango.csv")
    ydf["t"] = pd.to_datetime(ydf["t"])
    tickersdf = get_tickers()
    for i,r in tickersdf.iterrows():
        ticker = r["ticker"]
        yi = ydf.loc[ydf["ticker"]==ticker]
        if len(yi)<100:
            continue
        yidiff = yi["y"].diff()
        mindiff, maxdiff = np.quantile(yidiff.dropna(),[0.02,0.98])
        yi = yi.loc[(yidiff>mindiff) & (yidiff<maxdiff)]
        immdates = immdates_in_interval(yi["t"].iloc[0],yi["t"].iloc[-1],ticker,tickersdf)
        plt.plot(yi["t"],yi["y"])
        for immdate in immdates:
            plt.axvline(x=immdate,color="black")
        plt.title("%s %s" % (ticker,r["desc"]))
        plt.ylabel("contango rate")
        plt.gcf().autofmt_xdate()
        plt.xlabel("quotes from:" + str(yi.iloc[0]["t"])[:16]+" to " + str(yi.iloc[-1]["t"])[:16])
        plt.savefig(outdir+"contango-rate-%s.png" % ticker,metadata=get_metadata())
        plt.close()
        
def plot_all_contango():
    c1='green' # yellow: #FFFF00
    c2='blue' 
    dfdic = get_dfdic()
    contangolist = []
    for category,ticker,desc  in  sorted(dfdic.keys()):
        df = dfdic[(category,ticker,desc)]
        df['tutc'] = pd.to_datetime(df['tutc'])
        tdiffgauge = dt.timedelta(days=4/24)
        tdiff = df['tutc'].diff()
        idx = df.loc[~(tdiff<tdiffgauge)].index
        tickdic = {}
        print("INFO: graph for ",ticker)
        for i,ist in enumerate(idx):
            iend = idx[i+1] if i+1<len(idx) else df.index[-1]+1
            dfi = df[ist:iend].copy()
            dfi = dfi.loc[~df['timestr'].isna()]
            dfi = dfi.loc[~dfi["timestr"].str.contains(" at ")]
            dfi = dfi.loc[dfi["timestr"].str.contains("Market Open.")]
            if len(dfi)>0:
                c = colorFader(c1,c2,ist/len(df))
                symbols = [s[:s.find(".")][-3:] for s in dfi["symbol"]]
                symbolterm = [float(s[-2:])+"FGHJKMNQUVXZ".find(s[-3])/12 for s in symbols]
                tickdic.update(dict(zip(symbols,symbolterm)))
                dP = (dfi["quote"].iloc[-1]-dfi["quote"].iloc[0])/dfi["quote"].iloc[0]
                dT = symbolterm[-1]-symbolterm[0]
                plt.plot(symbolterm,dfi["quote"],label="%s" % dfi["tutc"].iloc[0],color=c,alpha=ist/len(df))
                if dT>0:
                    contangolist.append({"ticker":ticker,"t":dfi["tutc"].iloc[0],"y":dP/dT})
                plt.xticks(ticks=symbolterm, labels=symbols,rotation=45)
        plt.title("%s %s %s y=%.2f%%" % (category,ticker,desc,dP/dT*100))
        plt.xlabel("quotes from:" + str(df.iloc[idx[0]]["tutc"])[:16]+" to " + str(df.iloc[idx[-1]]["tutc"])[:16])
        plt.savefig(outdir+"contango-%s.png" % ticker,metadata=get_metadata())
        plt.close()
    pd.DataFrame(contangolist).to_csv("contango.csv",index=False)
    plot_contango_yield()
    os.system('rsync -avzhe ssh %s %s' % (outdir, remotedir))
 
if __name__ == "__main__":
    #get_all_futures()
    insertalltickers()
    #plot_all_contango()
