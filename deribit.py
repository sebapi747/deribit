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
suffix = ["-PERPETUAL", "-30DEC22", "-31MAR23", "-30JUN23", "-29SEP23"]
for t in tickers:
    for s in suffix:
        ticker = t + s
        try:
            url = "https://www.deribit.com/api/v2/public/ticker?instrument_name=%s" % ticker
            headers = {'Content-Type': 'application/json'}
            resp = requests.get(url,headers=headers)
            print(resp.status_code)
            dic = dict(resp.json()['result'])
            dic.pop('stats')
            filename =  "%s/%s.csv" % (dirname,ticker)
            fileexists = os.path.isfile(filename)
            fields = list(dic.keys())
            if fileexists:
                f = open(filename, "r")
                fields = f.readline().strip().split(",")
                f.close()
            else:
                f = open(filename, "a")
                f.write(",".join(fields))
                f.close()
            f = open(filename, "a")
            f.write("\n"+",".join([str(dic.get(f)) for f in fields]))
            f.close()
            if s!="-PERPETUAL":
                continue
            relSpd = dic['best_bid_price']/dic['estimated_delivery_price']
            if relSpd<0.97 or relSpd>1.14:
                msg = ("ALERT-%s: %s spd=%.2f%%" % (str(dt.datetime.utcnow()), t,relSpd))
                sendSMS(msg)
                sendMail(msg)
        except:
            print("failure for %s" % ticker)
            pass

