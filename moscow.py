import json, time, os
import pandas as pd
import datetime as dt
from pathlib import Path
import requests
from DrissionPage import ChromiumPage
filedir = os.path.dirname(__file__)
os.chdir("./" if filedir=="" else filedir)
import config
dirname = config.dirname+"/moscow/price/"

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
    params = {'chat_id': config.telegramchatid, 'text': os.uname()[1]+":"+__file__+":ALERT:" +text, 'parse_mode': 'HTML'}
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

if __name__ == "__main__":
    get_outputdir()
    page = ChromiumPage()
    get_all_prices(page)
    page.quit()
