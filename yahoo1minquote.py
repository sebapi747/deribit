import os,json,time,sqlite3
import datetime as dt
import pandas as pd
import random
import requests
import numpy as np
import pytz
import re
import time
import matplotlib.pyplot as plt
import matplotlib as mpl
filedir = os.path.dirname(__file__)
os.chdir("./" if filedir=="" else filedir)
import config
dirname = config.dirname 
localtz = pytz.timezone("Asia/Hong_Kong")
utctz   = pytz.timezone("UTC")

# 0:mon,1:tue,2:wed,3:tue,4:fri,5:sat,6:sun
if dt.datetime.utcnow().weekday()>=6:
    print("INFO: closed on sun")
    exit()
    
def sendTelegram(text):
    params = {'chat_id': config.telegramchatid, 'text': os.uname()[1] +":"+__file__+":"+text, 'parse_mode': 'markdown'}  
    resp = requests.post('https://api.telegram.org/bot{}/sendMessage'.format(config.telegramtoken), params)
    resp.raise_for_status()
    
def get_jsonfilename(ticker):
    return "json/%s.json" % (ticker)
def get_json_data(ticker):
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
        with open(get_jsonfilename(meta['symbol']).replace(".json",".err"), "w") as f:
            json.dump(meta,f)
        raise Exception("ERR: %s json does not have time series" % meta['symbol'])
    df = pd.DataFrame(out).dropna()
    df['symbol'] = jsonmeta['symbol']
    return df
    
def schema(dbfilename):
    if not os.path.exists(dbfilename):
        print("WARN: missing file %s" % dbfilename)
        #os.system("touch %s" % dbfilename)
        with sqlite3.connect(dbfilename) as con:
            con.execute('''
            create table quoteminutebar (
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
    df = processjsontopandas(get_json_data(ticker))
    print("INFO: %s inserting %d" % (ticker,len(df)),end="\r")
    with sqlite3.connect(dbfilename) as con:
        insert_df_to_table(df, "quoteminutebar", df.columns,con)

def inserttickersymbols(ticker):
    err = ""
    dbfilename = "%s/yahoo1minquote/%s.db" % (dirname,ticker)
    schema(dbfilename)
    with sqlite3.connect(dbfilename) as con:
        nbbefore = len(pd.read_sql("select 1 from quoteminutebar", con=con))
    try:
        getandinsertfutpandas(ticker,dbfilename)
    except Exception as e:
        err += "\n%s" % str(e)
    with sqlite3.connect(dbfilename) as con:
        nbafter = len(pd.read_sql("select 1 from quoteminutebar", con=con))
    print(err)
    print("INFO: %s: had %d quotes now %d" % (ticker,nbbefore,nbafter))
    return ticker,nbbefore,nbafter,err

'''
gooddf = pd.read_csv(dirname+"gooddf.csv")
with sqlite3.connect("sql/ib.db") as con:
    yimap = pd.read_sql("select yahooticker,finchatticker,finchatexchg from yahoo_roic_ib_map where finchatticker not like 'MISX-%' and finchatticker is not null",con=con)
tickers = yimap.loc[yimap["finchatticker"].isin(gooddf["ticker"]),"yahooticker"].tolist()
json.dumps(tickers)
json.dumps(list(set(gooddf['ccy'])))
'''
def insertalltickers():
    with open("posticker.json","r") as f:
        postickers = json.load(f)
    with open("goodtickers.json","r") as f:
        tickers = json.load(f)
    ccys = ["AUD", "BRL", "CAD", "CHF", "CLP", "CNY", "COP", "CZK", "DKK", "EUR", "GBP", "HKD", "IDR", "ILS", "JPY", "MXN", "MYR", "NOK", "NZD", "PLN", "QAR", "SAR", "SEK", "SGD", "THB", "TRY", "TWD", "USD", "VND", "ZAR"]
    errors = ""
    out = "\n|ticker|before|after|\n|---|---:|---:|\n"
    for ticker in postickers:
        ticker,nbbefore,nbafter,err = inserttickersymbols(ticker)
        out += "|%s|%d|%d|\n" % (ticker,nbbefore,nbafter)
        errors += err
    for ticker in tickers:
        ticker,nbbefore,nbafter,err = inserttickersymbols(ticker)
        out += "|%s|%d|%d|\n" % (ticker,nbbefore,nbafter)
        errors += err
    for ccy in ccys:
        ticker = ccy+'=X'
        ticker,nbbefore,nbafter,err = inserttickersymbols(ticker)
        out += "|%s|%d|%d|\n" % (ticker,nbbefore,nbafter)
        errors += err
    sendTelegram(out+errors)

if __name__ == "__main__":
    insertalltickers()
