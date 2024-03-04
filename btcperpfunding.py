import pandas as pd
import numpy as np
import os
from matplotlib import pyplot as plt
from sklearn.linear_model import LinearRegression
import config
csvsrcdir = config.csvsrcdir
destdir = config.destdir # 'pi@mypi:/media/pi/2d7c7848-427e-4633-96a1-bd3c0cfaf92b/usr/pi/deribit/'
destdir = destdir+'../pics/deribit/' # 'sebapi747@ssh.remote.com:/home/sebapi747/marko/static/uploads/'

filein = "BTC-PERPETUAL.csv"
os.system("rsync -auvzhe ssh %s%s ." % (csvsrcdir, filein))
df = pd.read_csv(filein)
df['timestamp'] = pd.to_datetime(df['timestamp'],unit="ms")
df['tdiff8hour'] = pd.Series([s.total_seconds()/60/60/8 for s in df['timestamp'].diff()], index=df.index).shift(-1)
df['pricechg'] = np.log(df['index_price']).diff()
df = df.loc[df['tdiff8hour']<1]

fig, ax1 = plt.subplots()
ax2 = ax1.twinx()
#ax2 = ax1
ax1.plot(df['timestamp'], np.cumsum(np.log(1+df["current_funding"]*df['tdiff8hour'])),label="cumulative funding", color="red")
ax1.set_ylabel("cumulative avg funding cost")
#ax2.plot(df['timestamp'], df['index_price'],label="BTC")
ax2.plot(df['timestamp'], np.cumsum(df['pricechg']),label="BTC")
ax2.set_ylabel("BTC cum log return")
ax1.legend(loc="upper left")
ax2.legend(loc="upper right")
plt.gcf().autofmt_xdate()
plt.title("BTC perp price and funding\n%s to %s " %  (str(df['timestamp'].array[0])[:10],str(df['timestamp'].array[-1])[:10]))
fileout = "btcperpfund.png"
plt.savefig(fileout)
plt.close()
os.system("rsync -avzhe ssh %s %s" % (fileout, destdir))

data = df[["current_funding",'pricechg']].dropna()
x,y = data["current_funding"].array.reshape(-1, 1),data['pricechg'].array.reshape(-1, 1)
model = LinearRegression().fit(x, y)
x_new = x
y_new = model.predict(x)
stdev = np.std(y-y_new)
m,R2 = model.coef_[0][0],1-(stdev/np.std(y))**2
plt.scatter(x,y,alpha=0.1)
plt.plot(x_new,y_new, color="red", label="m=%.2f, R2=%.2f%%" %(m,R2*100))
plt.xlabel("funding")
plt.ylabel("price change")
plt.legend()
plt.title("Price Move vs Funding\n%s to %s " %  (str(df['timestamp'].array[0])[:10],str(df['timestamp'].array[-1])[:10]))
fileout = "btcperp-movereg.png"
plt.savefig(fileout)
plt.close()
os.system("rsync -avzhe ssh %s %s" % (fileout, destdir))
