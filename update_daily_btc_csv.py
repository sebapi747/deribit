import pandas as pd
import datetime as dt
filein = "/media/pi/2d7c7848-427e-4633-96a1-bd3c0cfaf92b/usr/pi/deribit/BTC-PERPETUAL.csv"
fileout = "/media/pi/2d7c7848-427e-4633-96a1-bd3c0cfaf92b/usr/pi/deribit/BTC-USD.csv"
df2 = pd.read_csv(filein).iloc[-1]
d,p = str(dt.datetime.fromtimestamp(df2['timestamp']/1000))[:10], df2['last_price']
f = open(fileout, "a")
f.write("%s,%f\n" % (d,p))
f.close()

