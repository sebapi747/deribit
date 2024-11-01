import requests
from lxml import html
import datetime as dt
import urllib
import pytz
import re,csv,os
import pandas as pd
outdir="csv"
filedir = os.path.dirname(__file__)
os.chdir("./" if filedir=="" else filedir)
import config

def sendTelegram(text):
    prefix = os.uname()[1]+":" + __file__ + ":"
    params = {'chat_id': config.telegramchatid, 'text': prefix+text, 'parse_mode': 'HTML'}
    resp = requests.post('https://api.telegram.org/bot{}/sendMessage'.format(config.telegramtoken), params)
    resp.raise_for_status()
    
def get_quote(symbol, tz):
    headers = {'accept':'*/*', 'user-agent': 'Mozilla/5.0 (X11; Linux armv7l) AppleWebKit/537.36 (KHTML, like Gecko) Raspbian Chromium/78.0.3904.108 Chrome/78.0.3904.108 Safari/537.36'}
    symburl = urllib.parse.quote(symbol)
    url = "https://finance.yahoo.com/quote/%s?p=%s" % (symburl,symburl)
    resp = requests.get(url,headers=headers)
    if resp.status_code!=200:
        sendTelegram("failed %s %d" % (url,resp.status_code))
    print(resp.status_code, symbol)
    tz    = pytz.timezone(tz)
    tzutc = pytz.timezone("UTC")
    tnyc  = dt.datetime.now(tz)
    tutc  = tnyc.astimezone(tzutc)
    parsed_body = html.fromstring(resp.text)
    #f = open("fut.html","w")
    #f.write(resp.text)
    #f.close()
    quote = float(parsed_body.xpath("//fin-streamer[@data-symbol='%s' and @data-field='regularMarketPrice']" % symbol)[0].text.replace(",",""))
    date = parsed_body.xpath("//div[@id='quote-market-notice']/span")[0].text
    if "Market open" in date:
        time = re.sub(r'\. Market [a-z]+.$', '', re.sub(r'^As of[ \t]*', '', date))
        return True, quote, tutc, tnyc, time
    return False, -999, tutc, tnyc, ""

def save_to_csv(symbol, tz):
    opened, quote, tutc, tlocal, timestr = get_quote(symbol,tz)
    if opened==False:
        print("skipping %s closed" % symbol)
        return
    dic = {"quote":quote, "tutc":tutc,"tlocal":tlocal,"timestr":timestr}
    filename =  "%s/futcsv/%s.csv" % (config.dirname,symbol)
    fileexists = os.path.isfile(filename)
    with open(filename, 'a') as f:
        w = csv.writer(f)
        if fileexists == False:
            w.writerow(dic.keys())
        w.writerow(dic.values())

def update_all_csv():
    tickers = pd.read_csv("tickers.csv")
    symdic = {}
    for i,r in tickers.iterrows():
        symdic[r["ticker"]] = r["tz"]
    for symbol,tz in symdic.items():
        save_to_csv(symbol,tz)
        
if __name__ == "__main__":
    update_all_csv()
