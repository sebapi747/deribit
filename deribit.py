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

def sendTelegram(text):
    params = {'chat_id': config.telegramchatid, 'text': text, 'parse_mode': 'HTML'}
    resp = requests.post('https://api.telegram.org/bot{}/sendMessage'.format(config.telegramtoken), params)
    resp.raise_for_status()

def firstnextmonth(date):
    mt = dtnow.month+1
    yr = dtnow.year
    yr = yr+mt//12
    return dt.datetime(yr,1+(mt-1)%12,1)

def monthend(date):
    return firstnextmonth(date)-dt.timedelta(days=1)

def isImmMonth(date):
    return date.month%3==0 

def daysToMonthEnd(date):
    return int((monthend(date)-date).total_seconds()/3600/24)

def isAfterImm(date):
    return isImmMonth(date) and (date.weekday()+3)%7+daysToMonthEnd(date)<7

def list_suffix(dtnow):
    futdates = []
    for futm in [3,6,9,12]:
        yr = dtnow.year
        if isAfterImm(dtnow):
            mt = dtnow.month+futm+1
        else:
            mt = 1+3*((dtnow.month-1)//3)+futm
        yr = yr+mt//12
        mt = 1+(mt-1)%12
        futdt = dt.datetime(yr,mt,1)-dt.timedelta(days=1)
        daytofriday = (futdt.weekday()+3)%7
        futdt = futdt-dt.timedelta(days=daytofriday)
        if futdt<=dtnow:
            raise Exception("problem with dt fut=%s now=%s" % (futdt,dtnow))
        futdates.append(futdt)
    return ["-PERPETUAL"] + [d.strftime("-%d%b%y").upper() for d in sorted(futdates)]

tickers = ["BTC", "ETH"]
suffix = list_suffix(dt.datetime.now())

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
                sendTelegram(msg)
                sendMail(msg)
        except:
            print("failure for %s" % ticker)
            pass

