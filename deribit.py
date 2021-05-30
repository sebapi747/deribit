import requests
import csv
import os
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

tickers = ["BTC", "ETH"]
suffix = ["-25JUN21", "-PERPETUAL", "-24SEP21", "-31DEC21", "-25MAR22", "-28MAY21"]
for t in tickers:
    for s in suffix:
        ticker = t + s
        url = "https://www.deribit.com/api/v2/public/ticker?instrument_name=%s" % ticker
        headers = {'Content-Type': 'application/json'}
        resp = requests.get(url,headers=headers)
        print(resp.status_code)
        dic = dict(resp.json()['result'])
        dic.pop('stats')
        filename =  "%s/%s.csv" % (dirname,ticker)
        fileexists = os.path.isfile(filename)
        with open(filename, 'a') as f:
            w = csv.writer(f)
            if fileexists == False:
                w.writerow(dic.keys())
            w.writerow(dic.values())
        if s!="-PERPETUAL":
            continue
        relSpd = dic['best_bid_price']/dic['estimated_delivery_price']
        if relSpd<0.97:
            msg = ("ALERT-%s: %s spd=%.2f%%" % (str(dt.datetime.utcnow()), t,relSpd))
            sendSMS(msg)
            sendMail(msg)

