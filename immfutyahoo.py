import csv
import os
import datetime as dt
import requests
from lxml import html
import numpy as np
import pandas as pd
import urllib
import pytz
import re
import time
import matplotlib.pyplot as plt
import matplotlib as mpl
filedir = os.path.dirname(__file__)
os.chdir("./" if filedir=="" else filedir)
import config
remotedir = config.remotedir
immcsvdir = config.dirname+"/immfutcsv/"
outdir = config.dirname + "/immfutpics/"

def get_metadata():
    return {'Creator':os.uname()[1] +":"+__file__+":"+str(dt.datetime.utcnow())}
def sendTelegram(text):
    params = {'chat_id': config.telegramchatid, 'text': os.uname()[1] +":"+__file__+":"+text, 'parse_mode': 'HTML'}
    resp = requests.post('https://api.telegram.org/bot{}/sendMessage'.format(config.telegramtoken), params)
    resp.raise_for_status()
    

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

def calc_monthly_imm_codes(dtnow,n=12):
    mt = dtnow.month
    yr = dtnow.year
    moffset = 0 if dtnow.day<10 else 1
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
    ticker.loc[ticker["category"]=="equity","code"] = "HMUZ"
    ticker.loc[ticker["category"]=="metal","code"] = "GJMQVZ"
    dic = {"CBT":["ZS=F","ZC=F","KE=F","ZW=F",],"NYB":["SB=F","CC=F","KC=F"],
        "CME":["LBR=F","LE=F","HE=F"],"NYM":["PL=F"]}
    diccode = {"PL=F":"FJNV","HG=F":"FGHJKMNQUVXZ","ZS=F":"FHKNQUX",
        "LE=F":"GJMQVZ","HE=F":"GJKMNQVZ","ZC=F":"HKNUZ",
        "SB=F":"HKNV","CC=F":"HKNUZ","KC=F":"HKNUZ","KE=F":"HKNUZ",
        "ZW=F":"HKNUZ","LBR=F":"FHKNUX","SI=F":"FHKNUZ"}
    for e in dic.keys():
        ticker.loc[ticker["ticker"].isin(list(dic[e])),"exchg"] = e
    for e in diccode.keys():
        ticker.loc[ticker["ticker"]==e,"code"] = diccode[e]
    return ticker


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
    volume = float(parsed_body.xpath("//fin-streamer[@data-symbol='%s' and @data-field='regularMarketVolume']" % symbol)[0].text_content().replace(",","").replace("k","000").replace("-- ","0"))    
    #date = parsed_body.xpath("//div[@id='quote-market-notice']/span")[0].text
    date = [s for s in parsed_body.xpath("//div/span/text()") if 'Market Open' in s][0]
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
    
def get_dfdic():
    tickerdesc = pd.read_csv("tickers.csv")
    csvdir = "immfutcsv/"
    dfdic = {}
    for f in os.listdir(csvdir):
        r = tickerdesc.loc[tickerdesc["ticker"]==f[:-4]].iloc[0]
        dfdic[(r["category"],r["ticker"],r["desc"])] = pd.read_csv(csvdir+f)
    return dfdic

def colorFader(c1,c2,mix=0): #fade (linear interpolate) from color c1 (at mix=0) to c2 (mix=1)
    c1=np.array(mpl.colors.to_rgb(c1))
    c2=np.array(mpl.colors.to_rgb(c2))
    return mpl.colors.to_hex((1-mix)*c1 + mix*c2)

def plot_all_contango():
    c1='green' # yellow: #FFFF00
    c2='blue' 
    dfdic = get_dfdic()
    for category,ticker,desc  in  sorted(dfdic.keys()):
        df = dfdic[(category,ticker,desc)]
        df['tutc'] = pd.to_datetime(df['tutc'])
        tdiffgauge = dt.timedelta(days=4/24)
        tdiff = df['tutc'].diff()
        idx = df.loc[~(tdiff<tdiffgauge)].index
        tickdic = {}
        for i,ist in enumerate(idx):
            iend = idx[i+1] if i+1<len(idx) else df.index[-1]+1
            dfi = df[ist:iend].copy()
            dfi = dfi.loc[~df["timestr"].str.contains(" at ")]
            dfi = dfi.loc[df["timestr"].str.contains("Market Open.")]
            if len(dfi)>0:
                c = colorFader(c1,c2,ist/len(df))
                symbols = [s[:s.find(".")][-3:] for s in dfi["symbol"]]
                symbolterm = [float(s[-2:])+"FGHJKMNQUVXZ".find(s[-3])/12 for s in symbols]
                tickdic.update(dict(zip(symbols,symbolterm)))
                dP = (dfi["quote"].iloc[-1]-dfi["quote"].iloc[0])/dfi["quote"].iloc[0]
                dT = symbolterm[-1]-symbolterm[0]
                plt.plot(symbolterm,dfi["quote"],label="%s" % dfi["tutc"].iloc[0],color=c,alpha=ist/len(df))
                #print(tickdic)
                plt.xticks(ticks=symbolterm, labels=symbols,rotation=45)
        plt.title("%s %s %s y=%.2f%%" % (category,ticker,desc,dP/dT*100))
        plt.savefig(outdir+"contango-%s.png" % ticker,metadata=get_metadata())
        plt.close()
    os.system('rsync -avzhe ssh %s %s' % (outdir, remotedir))
 
if __name__ == "__main__":
    get_all_futures()
    #plot_all_contango()
