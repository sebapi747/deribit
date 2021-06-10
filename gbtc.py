import requests, re, os, csv
from lxml import html
import datetime as dt
import config
dirname = config.dirname
smskey = config.smskey
smsphone = config.smsphone

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
    if discount<-0.3:
        msg = ("ALERT-%s: %s disc=%.2f%%" % (str(dt.datetime.utcnow()), ticker,discount))
        sendSMS(msg)
        sendMail(msg)
else:
    print(dic)
