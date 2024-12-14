import os,json,time,sqlite3
import datetime as dt
import pandas as pd
import random
import requests
import numpy as np
filedir = os.path.dirname(__file__)
os.chdir("./" if filedir=="" else filedir)
import config
  
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
    errmsg = ""
    with sqlite3.connect("sql/yahoo.db") as db5:
        dic = getprice(jsondata)
        insert_dic_to_table(dic,tablename="yahoo_latest_price",con=db5)
        quotes = getquotes(jsondata)
        insert_df_to_table(quotes,"yahoo_quote_1mo",quotes.columns,db5)
        try:
            divs = getdividend(jsondata)
            insert_df_to_table(divs,"yahoo_div",divs.columns,db5)
        except:
            errmsg += ("\nERR: nodiv for "+dic["symbol"])
        try:
            splits = getsplit(jsondata)
            insert_df_to_table(splits,"yahoo_split",splits.columns,db5)
        except:
            errmsg += ("\nERR: nosplit for "+dic["symbol"])
    return errmsg

def getonetickerdata(ticker):
    jsondata = get_json_data(ticker)
    return inserttickerdb(jsondata)

def getalltickerdata():
    yahoo1dbarschema()
    with open("goodtickers.json","r") as f:
        tickers = json.load(f)
    errmsg = ""
    for i,t in enumerate(tickers):
        try:
            errmsg += getonetickerdata(t)
        except Exception as e:
            errmsg += "\nERR: error for %s %s" % (t,str(e))
    sendTelegram("retrieved %d tickers%s" % (len(tickers),errmsg))

if __name__ == "__main__":
    #os.system("mkdir json sql")
    getalltickerdata()
