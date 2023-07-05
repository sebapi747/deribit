import requests, re, os, csv, time
from lxml import html
import datetime as dt
import numpy as np
import pandas as pd
import matplotlib.pyplot as plt
import config
dirname = config.dirname
smskey = config.smskey
smsphone = config.smsphone

#
# exit if market closed UTC 13:30 to 20:30
#
now = dt.datetime.utcnow()
utctime = str(now.time())[:5]
dow = now.weekday()
if not(utctime > '13:30' and utctime < '20:30' and dow<5):
    print("stop %d %s" % (dow,utctime))
    exit()
else:
    print("proceed %d %s" % (dow,utctime))

def sendSMS(msg):
    url = 'https://rest.messagebird.com/messages'
    headers = {'Authorization': 'AccessKey %s' % smskey}
    myobj = {'recipients': smsphone, 'originator': smsphone, 'body':msg}
    x = requests.post(url, data = myobj, headers=headers)
    print(x.text)

def sendMail(msg):
    requests.post(
        "https://api.mailgun.net/v3/mg.markowitzoptimizer.pro/messages",
        auth=("api", config.MAILGUN_APIKEY),
        data={"from": "admin <admin@mg.markowitzoptimizer.pro>",
              "to": "admin@mg.markowitzoptimizer.pro",
              "subject": msg,
              "text": msg})

def sendTelegram(text):
    params = {'chat_id': config.telegramchatid, 'text': text, 'parse_mode': 'HTML'}
    resp = requests.post('https://api.telegram.org/bot{}/sendMessage'.format(config.telegramtoken), params)
    resp.raise_for_status()

def getquotes():
    headers = {'accept':'*/*', 'user-agent': 'Mozilla/5.0 (X11; Linux armv7l) AppleWebKit/537.36 (KHTML, like Gecko) Raspbian Chromium/78.0.3904.108 Chrome/78.0.3904.108 Safari/537.36'}
    ticker = "BTC-USD"
    x = requests.get('https://finance.yahoo.com/quote/%s' % ticker, headers=headers)
    print(x.status_code)
    parsed_body = html.fromstring(x.text)
    # was 32 and 35
    #quote = float(parsed_body.xpath('//div/fin-streamer[@data-reactid=29]/text()')[0].replace(",",""))
    #hr = parsed_body.xpath('//div/span[@data-reactid=36]/text()')[0]
    quote = float(parsed_body.xpath('//div/fin-streamer[@data-symbol="%s"]/text()' % ticker)[0].replace(",",""))
    hr = parsed_body.xpath('//div[@id="quote-market-notice"]/span/text()')[0]
    ymdstr = str(dt.datetime.utcnow()) 
    btcquote=quote
    pershare=0.000943533*btcquote
    time.sleep(1)
    ticker = "GBTC"
    x = requests.get('https://finance.yahoo.com/quote/%s' % ticker, headers=headers)
    print(x.status_code)
    parsed_body = html.fromstring(x.text)
    quote = float(parsed_body.xpath('//div/fin-streamer[@data-symbol="%s"]/text()' % ticker)[0].replace(",",""))
    hr = parsed_body.xpath('//div[@id="quote-market-notice"]/span/text()')[0]
    ymdstr = str(dt.datetime.utcnow()) 
    discount = quote/pershare-1
    dic = {'quote':quote, 'dt':ymdstr, 'hr':hr, 'btc':btcquote, 'disc':discount}
    if hr[0:8]!='At close':
        filename =  "%s/%s.csv" % (dirname,ticker)
        fileexists = os.path.isfile(filename)
        with open(filename, 'a') as f:
            w = csv.writer(f)
            if fileexists == False:
                w.writerow(dic.keys())
            w.writerow(dic.values())
        if discount>-0.08 or discount<-0.24:
            markerfile = "%s/%s.txt" % (dirname, ymdstr[:10])
            if not os.path.isfile(markerfile):
                msg = ("%s: %s disc=%.0f%% gbtc=%.2f theo=%.2f btc=%.0f" % (str(dt.datetime.utcnow()), ticker,discount*100, quote,pershare, btcquote))
                #sendSMS(msg)
                #sendMail(msg)
                sendTelegram(msg)
                with open(markerfile, 'a') as f:
                    f.writelines(["large premium or discount today."])
    else:
        print(dic)

def graphviz(dirname):
    filename = 'GBTC.csv'
    df = pd.read_csv("%s/%s" % (dirname, filename))
    fig, ax1 = plt.subplots()
    ax2 = ax1.twinx()
    df['date'] = pd.to_datetime(df['dt'])
    ax1.plot(df['date'], df['disc'], label="discount", linestyle="None", marker="+")
    ax1.set_ylabel(filename[0:3])
    ax2.plot(df['date'], df['btc'], label="btc", color="gray")
    ax2.set_ylabel(filename[0:3])
    plt.gcf().autofmt_xdate()
    ax1.legend(loc="upper left")
    ax2.legend(loc="upper right")
    plt.title("Greyscale BTC Discount\ndisc=%.1f%% last updated: %s" % (df.iloc[-1]['disc']*-100, str(np.max(df['dt']))))
    plt.savefig("%s/pics/%s" % (dirname, 'gbtcdiscount.png'))
    plt.close(fig)

getquotes()
graphviz(dirname)
