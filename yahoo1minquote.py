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
if dt.datetime.utcnow().weekday()>=5:
    print("INFO: closed on sat and sun")
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
        err += "%s\n" % str(e)
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
    tickers = ["ADRO.JK", "PTBA.JK", "TEL1L.VS", "ARAD.TA", "CMDR.TA", "EMCO.TA", "4190.SR", "4200.SR", "AINS.KW", "ALKOUT.KW", "BKIKWT.KW", "CNG.VN", "DCM.VN", "DVLA.JK", "ELSA.JK", "GEMS.JK", "HUMANSOFT.KW", "ISA.CL", "JTPE.JK", "NELY.JK", "OLTH.AT", "OPAP.AT", "PCEM.KW", "PSSI.JK", "QFLS.QA", "QISI.QA", "ZAIN.KW", "BMW.DE", "NTES", "9999.HK", "FMG.AX", "ONGC.NS", "601225.SS", "CEZ.PR", "FOXA", "0003.HK", "PNDORA.CO", "SIRI", "UI", "600362.SS", "1193.HK", "1618.HK", "601618.SS", "601699.SS", "PAC", "0358.HK", "GAPB.MX", "2688.HK", "A2A.MI", "HL.L", "600132.SS", "603899.SS", "DOO.TO", "OIL.NS", "AZM.MI", "600873.SS", "OMAB", "0868.HK", "CMOCTEZ.MX", "NSP", "EXX.JO", "600612.SS", "0995.HK", "600548.SS", "0548.HK", "FHI", "OMAB.MX", "600098.SS", "KIND-SDB.ST", "600737.SS", "600012.SS", "PETS.L", "ACP.WA", "NTB", "AUSS.OL", "CMRE", "VCT.PA", "MONY.L", "KTY.WA", "2404.TW", "TEP.L", "600575.SS", "MGL.NS", "600269.SS", "BETS-B.ST", "600987.SS", "WAC.DE", "2299.HK", "5423.T", "XTB.WA", "P8Z.SI", "HERDEZ.MX", "DOM.WA", "3306.HK", "2877.HK", "6214.TW", "600035.SS", "4849.T", "600681.SS", "O5RU.SI", "1052.HK", "AX1.AX", "SWTQ.SW", "CYDSASAA.MX", "NOEJ.DE", "FDM.L", "1681.HK", "558.SI", "PCR.WA", "1277.HK", "3733.T", "1830.HK", "1785.HK", "0746.HK", "0331.HK", "0098.HK", "2368.HK", "6525.TW", "CSW-A.TO", "BAN.MI", "SFR.L", "2397.TW", "MSA.TO", "KAMUX.HE", "BBW.SI", "MED", "AOJ-B.CO", "CGS.L", "BEC.MI", "OLY.TO", "REC.L", "3676.T", "FRO.WA", "0240.HK", "1127.HK", "3763.T", "VOX.WA", "6224.TW", "3838.HK", "CLN.SI", "APT.WA", "WTN.WA", "G5EN.ST", "6161.T", "1203.HK", "IFI.WA", "2176.HK", "SON.WA", "569.SI", "1459.HK", "7265.T", "EDI.WA", "HER.MI", "108320.KS", "214320.KS", "234080.KS", "AH.BK", "AI.BK", "AMAG.VI", "ANDINA-B.SN", "CAROZZI.SN", "CSAN3.SA", "DUNCANFOX.SN", "EMBONOR-A.SN", "ENAEX.SN", "ENELGXCH.SN", "FERIAOSOR.SN", "LANNA.BK", "LIPIGAS.SN", "MTI.BK", "SK.SN", "SLCE3.SA", "SPVI.BK", "STG.CO", "SUR.JO", "TOG.BK", "UVAN.BK", "5183.KL", "5209.KL", "5212.KL", "7100.KL", "9172.KL", "900948.SS", "002423.SZ", "000429.SZ", "300298.SZ", "002690.SZ", "TABAK.PR", "000012.SZ", "002701.SZ", "VP.L", "YORE.MC"]
    ccys = ["RUB", "PKR", "MXN", "CHF", "ZAR", "NOK", "MYR", "KRW", "PEN", "THB", "VND", "MAD", "CNY", "COP", "TWD", "EUR", "INR", "AUD", "IDR", "PLN", "CLP", "HKD", "KES", "BRL", "CZK", "SGD", "ILS", "DKK", "JPY", "SEK", "QAR", "CAD", "KWD", "USD", "GBP", "SAR"]
    errors = ""
    out = "\n|ticker|before|after|\n|---|---:|---:|\n"
    for ticker in tickers:
        ticker,nbbefore,nbafter,err = inserttickersymbols(ticker)
        out += "|%s|%d|%d|\n" % (ticker,nbbefore,nbafter)
        errors += err
    for ccy in ccys:
        ticker = ccy+'=X'
        ticker,nbbefore,nbafter,err = inserttickersymbols(ticker)
        out += "|%s|%d|%d|\n" % (ticker,nbbefore,nbafter)
        errors += err
    sendTelegram(out)
    if len(errors)>0:
        sendTelegram("%s" % errors)    

if __name__ == "__main__":
    insertalltickers()
