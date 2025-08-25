import json, time, os,sqlite3
import pandas as pd
import numpy as np
import datetime as dt
from pathlib import Path
import requests
import matplotlib.pyplot as plt
import psutil
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


def analyze_quote_dates():
    """
    Analyze quote dates and return information about max dates and stale tickers
    Returns:
        tuple: (max_date, max_date_count, stale_tickers_map)
    """
    maxquotedateMapByTicker = {}
    mapStaleTickers = {}
    
    # Get all ticker CSV files
    csv_files = [f for f in os.listdir(dirname) if f.endswith('.csv')]
    
    if not csv_files:
        return None, 0, {}
    
    # Read each file and find max date
    for file in csv_files:
        ticker = file.replace('.csv', '')
        try:
            df = pd.read_csv(dirname + file)
            df['lastupdated'] = pd.to_datetime(df['lastupdated'])
            max_date = df['lastupdated'].max().date()
            maxquotedateMapByTicker[ticker] = max_date
        except Exception as e:
            print(f"ERROR: Could not read {file}: {e}")
            continue
    
    if not maxquotedateMapByTicker:
        return None, 0, {}
    
    # Find the overall max date
    overall_max_date = max(maxquotedateMapByTicker.values())
    
    # Count tickers with max date and find stale tickers
    max_date_count = 0
    for ticker, date in maxquotedateMapByTicker.items():
        if date == overall_max_date:
            max_date_count += 1
        else:
            mapStaleTickers[ticker] = date
    
    return overall_max_date, max_date_count, mapStaleTickers
    
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
    max_date, max_date_count, stale_tickers = analyze_quote_dates()
    if max_date:
        max_date_str = max_date.strftime('%Y-%m-%d %H:%M:%S UTC')
        message = f"updated [moscow quotes](https://www.markowitzoptimizer.pro/blog/39)\n"
        message += f"Max date: {max_date_str}\n"
        message += f"Tickers with max date: {max_date_count}\n"
        
        if stale_tickers:
            message += f"Stale tickers ({len(stale_tickers)}):\n"
            for ticker, date in list(stale_tickers.items())[:10]:  # Show first 10 to avoid message being too long
                date_str = date.strftime('%Y-%m-%d %H:%M:%S')
                message += f"  {ticker}: {date_str}\n"
            if len(stale_tickers) > 10:
                message += f"  ... and {len(stale_tickers) - 10} more\n"
        else:
            message += "All tickers are up to date!"
    else:
        message = "updated [moscow quotes](https://www.markowitzoptimizer.pro/blog/39)\nNo quote data found!"
    sendTelegram(message)

if __name__ == "__main__":
    get_outputdir()
    page = ChromiumPage()
    get_all_prices(page)
    plot_moscow()
    page.quit = lambda: [proc.kill() for proc in psutil.process_iter() if proc.name().__contains__('chromium')]
    page.quit()
