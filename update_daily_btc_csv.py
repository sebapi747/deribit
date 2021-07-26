import pandas as pd
import datetime as dt
import config
dirname = config.dirname
filein = dirname + "/BTC-PERPETUAL.csv"
fileout = dirname + "/BTC-USD.csv"
df2 = pd.read_csv(filein).iloc[-1]
d,p = str(dt.datetime.fromtimestamp(df2['timestamp']/1000))[:10], df2['last_price']
f = open(fileout, "a")
f.write("%s,%f\n" % (d,p))
f.close()

