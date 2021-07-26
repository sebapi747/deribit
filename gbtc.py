import requests, re, os, csv
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

def getquotes():
    ticker = "BTC-USD"
    x = requests.get('https://finance.yahoo.com/quote/%s' % ticker)
    print(x.status_code)
    parsed_body = html.fromstring(x.text)
    quote = float(parsed_body.xpath('//div/span[@data-reactid=32]/text()')[0].replace(",",""))
    hr = parsed_body.xpath('//div/span[@data-reactid=35]/text()')[0]
    ymdstr = str(dt.datetime.utcnow()) 
    btcquote=quote
    pershare=0.000943533*btcquote
    ticker = "GBTC"
    x = requests.get('https://finance.yahoo.com/quote/%s' % ticker)
    print(x.status_code)
    parsed_body = html.fromstring(x.text)
    quote = float(parsed_body.xpath('//div/span[@data-reactid=32]/text()')[0].replace(",",""))
    hr = parsed_body.xpath('//div/span[@data-reactid=35]/text()')[0]
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
        if discount<-0.2:
            msg = ("ALERT-%s: %s disc=%.2f%%" % (str(dt.datetime.utcnow()), ticker,discount))
            sendSMS(msg)
            sendMail(msg)
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
    plt.title("Greyscale BTC Discount\nlast updated: %s" % str(np.max(df['dt'])))
    plt.savefig("%s/pics/%s" % (dirname, 'gbtcdiscount.png'))
    plt.close(fig)

getquotes()
graphviz(dirname)
