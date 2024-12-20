import json, time, os,sqlite3
import pandas as pd
import numpy as np
import datetime as dt
from pathlib import Path
import requests
import matplotlib.pyplot as plt
from DrissionPage import ChromiumPage
filedir = os.path.dirname(__file__)
os.chdir("./" if filedir=="" else filedir)
import config
dirname = config.dirname+"/moscow/price/"
outdir = config.dirname+"/moscow/pics/"
remotedir = config.remotedir

def get_outputdir():
    Path(dirname).mkdir(parents=True, exist_ok=True)
    return dirname
    
def sendMail(msg):
    requests.post(
        "https://api.mailgun.net/v3/mg.markowitzoptimizer.pro/messages",
        auth=("api", config.MAILGUN_APIKEY),
        data={"from": "admin <admin@mg.markowitzoptimizer.pro>",
              "to": "admin@mg.markowitzoptimizer.pro",
              "subject": msg,
              "text": msg})
def sendTelegram(text):
    params = {'chat_id': config.telegramchatid, 'text': os.uname()[1]+":"+__file__+":ALERT:" +text, 'parse_mode': 'markdown'}
    resp = requests.post('https://api.telegram.org/bot{}/sendMessage'.format(config.telegramtoken), params)
    resp.raise_for_status()
    
def append_csv(filename,errdf):
    if os.path.exists(filename):
        df = pd.read_csv(filename)
        df = pd.concat([df,errdf])
    else:
        df = errdf
    df.to_csv(filename,index=False)

def get_all_prices(page):
    price,err = {},{}
    moscow_ticks = '["ABRD", "AFKS", "AFLT", "AKRN", "ALRS", "AMEZ", "AQUA", "AVAN", "BANE", "BELU", "BSPB", "CBOM", "CHGZ", "CHMF", "DSKY", "ENPG", "FEES", "FESH", "FLOT", "GAZP", "GCHE", "GEMA", "GMKN", "HYDR", "IRAO", "JNOS", "KAZT", "KMAZ", "KOGK", "KRKN", "KZOS", "LKOH", "LSNG", "LSRG", "MAGN", "MFGS", "MGNT", "MGNZ", "MGTS", "MOEX", "MRKC", "MRKP", "MRKU", "MRKV", "MSNG", "MSRS", "MTLR", "MTSS", "MVID", "NKHP", "NKNC", "NKSH", "NLMK", "NMTP", "NVTK", "OGKB", "PHOR", "PIKK", "PLZL", "POSI", "RASP", "RENI", "RNFT", "ROSN", "RTGZ", "RTKM", "RUAL", "SBER", "SELG", "SFIN", "SGZH", "SIBN", "SMLT", "SNGS", "SVAV", "TATN", "TGKA", "TORS", "TRMK", "TTLK", "UNAC", "VGSB", "VSMO", "VTBR", "YKEN"]'
    for i,ticker in enumerate(json.loads(moscow_ticks)):
        try:
            url = 'https://www.tbank.ru/invest/stocks/%s/' % ticker
            print("INFO: getting %d %s" % (i,ticker))
            page.get(url)
            time.sleep(0.5)
            statedic = json.loads(page.ele("#__TRAMVAI_STATE__").inner_html)
            price[ticker] = statedic['stores']['investSecurity'][ticker]['prices']['close']['value']
            pricedf = pd.DataFrame({'ticker':[ticker],"quote":price[ticker]})
            pricedf["lastupdated"] = dt.datetime.utcnow()
            append_csv(dirname+"%s.csv" % ticker,pricedf)
        except Exception as e:
            err[ticker] = "not found"
            if len(err)>15:
                sendTelegram("failed for ticker:"+ticker)
    errdf = pd.DataFrame({'ticker':err.keys()})
    errdf["lastupdated"] = dt.datetime.utcnow()
    append_csv(dirname+"../moscow_err.csv",errdf)
    if len(price)<60:
        sendTelegram("expected 70 quotes, got only:"+len(price))


def plot_moscow():
    moscowdir = dirname
    moscdf = pd.read_csv("moscow.csv")
    corpname = {}
    for i,r in moscdf.iterrows():
        c = str(r["corpname"])
        for s in ["Public Joint Stock Company","Public Joint-Stock Company","public joint-stock company",
                 "Public Joint-stock company","Public Joint stock company"]:
            c = c.replace(s,"PJSC")
        for s in ["Public Stock Company"]:
            c = c.replace(s,"PSC")
        for s in ["Public Joint Stock"]:
            c = c.replace(s,"PJS")
        for s in ["Joint Stock Company"]:
            c = c.replace(s,"JSC")
        corpname[r["ticker"][5:]] = c
    tickers = [f[:-4] for f in os.listdir(moscowdir) if ".csv" in f and f[:-4] in corpname.keys()]
    tickers = [f for f in corpname.keys() if f in tickers]
    tlists = np.array_split(tickers,len(tickers)//5)
    for i,tlist in enumerate(tlists):
        print("INFO:",i,tlist)
        for ticker in tlist:
            with sqlite3.connect("../ib/sql/yahoo.db") as db:
                df = pd.read_sql("select * from yahoo_quote where ticker=?",con=db,params=[ticker+".ME"])
                df["Date"] = pd.to_datetime(df["Date"])
                df = df.set_index("Date")[["Close"]].rename(columns={"Close":"quote"})
            df2 = pd.read_csv(moscowdir+ticker+".csv")
            df2["lastupdated"] = pd.to_datetime(df2["lastupdated"])
            df2 = df2.set_index("lastupdated")[["quote"]]
            df = pd.concat([df,df2])
            df["quote"] /= df["quote"].array[0]
            plt.plot(df,label="%s %s" % (ticker,corpname[ticker]))
            plt.xlabel("from %s to %s" % (str(df.index.array[0])[:10],str(df.index.array[-1])[:10]))
        plt.legend()
        plt.title("Quote")
        #plt.show()
        plt.savefig(outdir+"moscowquote-%d.png" % i)
        plt.close()
    print("INFO: rsync -avzhe ssh ",outdir,remotedir)
    os.system('rsync -avzhe ssh %s %s' % (outdir, remotedir))
    sendTelegram("updated [moscow quotes](https://www.markowitzoptimizer.pro/blog/39)")

if __name__ == "__main__":
    get_outputdir()
    page = ChromiumPage()
    get_all_prices(page)
    plot_moscow()
    page.quit()
