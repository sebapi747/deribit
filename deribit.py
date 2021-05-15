import requests
import csv
import os
import config
dirname = config.dirname
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

