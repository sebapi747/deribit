import os,json,time,sqlite3
import datetime as dt
import pandas as pd
import random
import requests
import numpy as np
from matplotlib import pyplot as plt
filedir = os.path.dirname(__file__)
os.chdir("./" if filedir=="" else filedir)
import config
outplot = config.dirname+"/pics/"
  
def get_metadata():
    return {'Creator':os.uname()[1] +":"+__file__+":"+str(dt.datetime.utcnow())}

def sendTelegram(text):
    params = {'chat_id': config.telegramchatid, 'text': os.uname()[1] +":"+__file__+":"+text, 'parse_mode': 'markdown'}  
    resp = requests.post('https://api.telegram.org/bot{}/sendMessage'.format(config.telegramtoken), params)
    resp.raise_for_status()
         
def insert_df_to_table(df, tablename, cols, con):
    if len(df)==0:
        return
    df[cols].to_sql(name=tablename+'_tmp', con=con, if_exists='replace',index=False)
    sql = 'insert or replace into '+tablename+' (['+'],['.join(cols)+']) select ['+'],['.join(cols)+'] from '+tablename+'_tmp'
    con.execute(sql)
    con.commit()
    con.execute('drop table '+tablename+'_tmp')
    

def yahoo1dbarschema():
    with sqlite3.connect("sql/yahoo.db") as db5:
        db5.execute("create table if not exists yahoo_split (ticker text, splitdate datetime, ratio numeric, primary key(ticker, splitdate))")
        db5.execute("create table if not exists yahoo_div (ticker text, divdate datetime, amount numeric, primary key(ticker, divdate))")
        db5.execute('''create table if not exists yahoo_quote_1mo (ticker text, quotedate datetime, 
            high numeric, close numeric,low numeric,volume numeric,open numeric, adjclose numeric, 
            primary key(ticker, quotedate))''')
        db5.execute('''create table if not exists yahoo_latest_price (symbol text, pricedate datetime, 
            longName text, fullExchangeName text,currency text,
            regularMarketPrice numeric,regularMarketDayHigh numeric, regularMarketDayLow numeric, 
            regularMarketVolume numeric,fiftyTwoWeekHigh numeric, fiftyTwoWeekLow numeric, 
            primary key(symbol))''')

    
def insert_dic_to_table(dic, tablename, con):
    sql = "insert or replace into %s ([%s]) values (%s)" % (tablename, '],['.join(dic.keys()), ','.join("?"*len(dic)))
    con.execute(sql, list(dic.values()))
    con.commit()


def get_jsonfilename(ticker):
    return "json1mo/%s.json" % (ticker)
def get_json_data(ticker):
    events = "events=capitalGain%7Cdiv%7Csplit"
    period2 = dt.datetime.utcnow()
    period1 = period2 - dt.timedelta(days=365*25)
    url = "https://query1.finance.yahoo.com/v8/finance/chart/%s?%s&interval=1mo&period1=%.0f&period2=%.0f" % (ticker,events,period1.timestamp(),period2.timestamp())
    filename = get_jsonfilename(ticker)
    errfilename = filename.replace(".json",".err")
    if os.path.exists(filename):
        filehours = (dt.datetime.now().timestamp()-os.path.getmtime(filename))/3600
        print("INFO: %s found                        " % filename, end="\r")
        if filehours<24*6:
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
    os.system("curl --silent '%s' > %s" % (url,filename))
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

def getsplit(jsondata):
    ticker = jsondata['chart']['result'][0]["meta"]["symbol"]
    splitdf = pd.DataFrame(jsondata['chart']['result'][0]['events']['splits'].values())
    splitdf["splitdate"] = pd.to_datetime([dt.datetime.utcfromtimestamp(d) for d in splitdf["date"]])
    splitdf["ratio"] = splitdf["numerator"]/splitdf["denominator"]
    splitdf["ticker"] = ticker
    return splitdf[["ticker","splitdate","ratio"]]

def getdividend(jsondata):
    ticker = jsondata['chart']['result'][0]["meta"]["symbol"]
    splitdf = pd.DataFrame(jsondata['chart']['result'][0]['events']['dividends'].values())
    splitdf["divdate"] = pd.to_datetime([dt.datetime.utcfromtimestamp(d) for d in splitdf["date"]])
    splitdf["ticker"] = ticker
    return splitdf[["ticker","divdate","amount"]]

def getquotes(jsondata):
    qindex = [dt.datetime.utcfromtimestamp(d) for d in jsondata['chart']['result'][0]['timestamp']]
    quotes = jsondata['chart']['result'][0]['indicators']["quote"][0]
    adjclose = jsondata['chart']['result'][0]['indicators']["adjclose"][0]
    dfquote = pd.DataFrame(quotes, index=qindex)
    for c in adjclose.keys():
        dfquote[c] = adjclose[c]
    dfquote["ticker"] = jsondata['chart']['result'][0]["meta"]["symbol"]
    dfquote = dfquote.reset_index().rename(columns={"index":"quotedate"})
    return dfquote

def getprice(jsondata):
    meta = jsondata["chart"]["result"][0]["meta"]
    dic = {}
    dic['pricedate'] = dt.datetime.utcfromtimestamp(meta['regularMarketTime'])
    for c in ['symbol','longName','fullExchangeName','currency','regularMarketPrice','regularMarketDayHigh','regularMarketDayLow',
             'regularMarketVolume','fiftyTwoWeekHigh','fiftyTwoWeekLow']:
        dic[c] = meta[c]
    return dic

def inserttickerdb(jsondata):
    with sqlite3.connect("sql/yahoo.db") as db5:
        dic = getprice(jsondata)
        insert_dic_to_table(dic,tablename="yahoo_latest_price",con=db5)
        quotes = getquotes(jsondata)
        insert_df_to_table(quotes,"yahoo_quote_1mo",quotes.columns,db5)
        if "=X"==dic["symbol"][-2:]:
            return ""
        try:
            divs = getdividend(jsondata)
            insert_df_to_table(divs,"yahoo_div",divs.columns,db5)
        except:
            print("WARN: nodiv for "+dic["symbol"])
        try:
            splits = getsplit(jsondata)
            insert_df_to_table(splits,"yahoo_split",splits.columns,db5)
        except:
            print("WARN: nosplit for "+dic["symbol"])

def getonetickerdata(ticker):
    jsondata = get_json_data(ticker)
    return inserttickerdb(jsondata)

def getccytickerdata():
    ccys = ["HUF","PEN","AED","AUD", "BRL", "CAD", "CHF", "CLP", "CNY", "COP", "CZK", "DKK", "EUR", "GBP", "HKD", "IDR", "ILS", "INR", "JPY", "KRW", "MXN", "MYR", "NOK", "NZD", "PLN", "QAR", "SAR", "SEK", "SGD", "THB", "TRY", "TWD", "VND", "ZAR"]
    tickers = [c+"=X" for c in ccys] 
    errmsg = ""
    for i,t in enumerate(tickers):
        try:
            getonetickerdata(t)
        except Exception as e:
            errmsg += "\nERR: error for %s %s" % (t,str(e))
    msg = "INFO:retrieved %d tickers%s" % (len(tickers),errmsg)
    print(msg)

def getalltickerdata():
    yahoo1dbarschema()
    with open("goodtickers.json","r") as f:
        tickers = json.load(f)
    with open("posticker.json","r") as f:
        postickers = json.load(f)
    ccys = ["HUF","PEN","AED","AUD", "BRL", "CAD", "CHF", "CLP", "CNY", "COP", "CZK", "DKK", "EUR", "GBP", "HKD", "IDR", "ILS", "INR", "JPY", "KRW", "MXN", "MYR", "NOK", "NZD", "PLN", "QAR", "SAR", "SEK", "SGD", "THB", "TRY", "TWD", "VND", "ZAR"]
    tickers = postickers + [c+"=X" for c in ccys] + tickers
    errmsg = ""
    for i,t in enumerate(tickers):
        try:
            getonetickerdata(t)
        except Exception as e:
            errmsg += "\nERR: error for %s %s" % (t,str(e))
    msg = "INFO:retrieved %d tickers%s" % (len(tickers),errmsg)
    print(msg)
    sendTelegram(msg)

def getquote(ticker):
    with sqlite3.connect("sql/yahoo.db") as db5:
        quote = pd.read_sql("select * from yahoo_quote_1mo where ticker=?", con=db5,params=[ticker])
        quote["quotedate"] = pd.to_datetime(quote["quotedate"])
        return quote.set_index("quotedate").rename(columns={"adjclose":ticker})[[ticker]]

def plot_cef():
    quote = getquote("SPY")
    tickers = ["ADX","EOS","BXMX","CSQ","GDV","BDJ","PEO","BCX"]
    for ticker in tickers:
        quote = quote.join(getquote(ticker),how="inner")
    for c in quote.columns:
        quote[c] /= quote[c].array[0]
    plt.plot(quote)
    plt.legend(labels=quote.columns)
    plt.xlabel("from %s to %s" % (str(quote.index.array[0])[:10],str(quote.index.array[-1])[:10]))
    plt.title("CEF Total Return")
    plt.savefig(outplot+"cefquote.png",metadata=get_metadata())
    plt.close()
    
def getceftickerdata():
    yahoo1dbarschema()
    cefdf = pd.read_csv("cef.csv")
    tickers = ["SPY","GLD","TLT","EOI","PEO","BCX"] + list(cefdf["symbol"])
    errmsg = ""
    for i,t in enumerate(tickers):
        try:
            getonetickerdata(t)
        except Exception as e:
            errmsg += "\nERR: error for %s %s" % (t,str(e))
    msg = "INFO:getceftickerdata retrieved %d tickers%s see [cef blog](https://www.markowitzoptimizer.pro/blog/91)" % (len(tickers),errmsg)
    plot_cef()
    print(msg)
    sendTelegram(msg)

if __name__ == "__main__":
    getccytickerdata()
    getceftickerdata()
    getalltickerdata()
